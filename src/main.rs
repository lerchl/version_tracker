mod deploy;
mod provider;

use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Error, Result};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{CONTENT_TYPE, ORIGIN},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::{signal, sync::Mutex};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use deploy::{DeployError, DeployOptions, DeployOutcome, Deployer, build_deployer};
use provider::{ProviderError, ReleaseInfo, SoftwarePackageType, VersionProvider, build_provider};

const CACHE_TTL: Duration = Duration::from_secs(5 * 60);

// ---------- types ----------

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct PageQuery {
    /// 1-based page number (default 1)
    page: Option<usize>,
    /// Items per page (default 30, max 100)
    per_page: Option<usize>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct DeployQuery {
    /// Release tag to deploy, or "latest" (default)
    version: Option<String>,
}

#[derive(Serialize, ToSchema)]
struct Paginated<T> {
    items: Vec<T>,
    page: usize,
    per_page: usize,
    total: usize,
    total_pages: usize,
}

// ---------- state ----------

struct CacheEntry {
    fetched_at: Instant,
    releases: Arc<Vec<ReleaseInfo>>,
}

#[derive(Clone)]
struct AppState {
    provider: Arc<dyn VersionProvider>,
    // Held across the upstream fetch, so concurrent requests on a cold or
    // stale cache trigger only one refresh.
    cache: Arc<Mutex<Option<CacheEntry>>>,
    /// `None` when the package type has no deployer yet
    deployer: Option<Arc<dyn Deployer>>,
    /// Only one deployment at a time
    deploy_lock: Arc<Mutex<()>>,
    /// Origins allowed to call the API from a browser (normalized, no trailing slash)
    allowed_origins: Arc<Vec<String>>,
}

// ---------- errors ----------

struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::error!("{} {}", self.status, self.message);
        (self.status, self.message).into_response()
    }
}

impl From<ProviderError> for AppError {
    fn from(e: ProviderError) -> Self {
        let status = match e {
            ProviderError::NotFound | ProviderError::AssetNotFound(_) => StatusCode::NOT_FOUND,
            ProviderError::Upstream(_) | ProviderError::Http(_) => StatusCode::BAD_GATEWAY,
        };
        Self::new(status, e.to_string())
    }
}

impl From<DeployError> for AppError {
    fn from(e: DeployError) -> Self {
        let status = match &e {
            DeployError::Provider(ProviderError::NotFound | ProviderError::AssetNotFound(_)) => {
                StatusCode::NOT_FOUND
            }
            DeployError::Provider(_) | DeployError::Download(_) => StatusCode::BAD_GATEWAY,
            DeployError::Io(_) | DeployError::Systemctl { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self::new(status, e.to_string())
    }
}

// ---------- cache ----------

async fn cached_releases(state: &AppState) -> Result<Arc<Vec<ReleaseInfo>>, ProviderError> {
    let mut cache = state.cache.lock().await;

    if let Some(entry) = cache.as_ref() {
        if entry.fetched_at.elapsed() < CACHE_TTL {
            return Ok(entry.releases.clone());
        }
    }

    match state.provider.fetch_releases().await {
        Ok(releases) => {
            let releases = Arc::new(releases);
            *cache = Some(CacheEntry {
                fetched_at: Instant::now(),
                releases: releases.clone(),
            });
            Ok(releases)
        }
        Err(e) => {
            // Serve stale data rather than fail if we have any
            if let Some(entry) = cache.as_ref() {
                tracing::warn!(
                    "refresh from {} failed ({e}), serving stale cache",
                    state.provider.name()
                );
                return Ok(entry.releases.clone());
            }
            Err(e)
        }
    }
}

// ---------- handlers ----------

/// List releases of the configured software
#[utoipa::path(
    get,
    path = "/versions",
    tag = "versions",
    params(PageQuery),
    responses(
        (status = 200, description = "Paginated list of releases", body = Paginated<ReleaseInfo>),
        (status = 404, description = "Software not found at the provider", body = String),
        (status = 502, description = "Upstream provider error", body = String),
    )
)]
async fn get_versions(
    State(state): State<AppState>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Paginated<ReleaseInfo>>, AppError> {
    let all = cached_releases(&state).await?;

    let per_page = q.per_page.unwrap_or(30).clamp(1, 100);
    let page = q.page.unwrap_or(1).max(1);
    let total = all.len();
    let total_pages = total.div_ceil(per_page);

    let items = all
        .iter()
        .skip((page - 1).saturating_mul(per_page))
        .take(per_page)
        .cloned()
        .collect();

    Ok(Json(Paginated {
        items,
        page,
        per_page,
        total,
        total_pages,
    }))
}

/// Download a release, swap the binary and restart the service.
/// Requests must carry an `Origin` header matching one of the allowed origins.
#[utoipa::path(
    post,
    path = "/deploy",
    tag = "deploy",
    params(DeployQuery),
    responses(
        (status = 200, description = "Deployed", body = DeployOutcome),
        (status = 400, description = "Invalid version", body = String),
        (status = 403, description = "Origin missing or not allowed", body = String),
        (status = 404, description = "Release or asset not found", body = String),
        (status = 409, description = "A deployment is already running", body = String),
        (status = 500, description = "Install or systemctl failure", body = String),
        (status = 501, description = "No deployer for this package type", body = String),
        (status = 502, description = "Upstream provider error", body = String),
    )
)]
async fn deploy_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<DeployQuery>,
) -> Result<Json<DeployOutcome>, AppError> {
    let origin_ok = headers
        .get(ORIGIN)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|o| {
            let o = o.trim_end_matches('/');
            state.allowed_origins.iter().any(|a| a == o)
        });
    if !origin_ok {
        return Err(AppError::new(
            StatusCode::FORBIDDEN,
            "Origin missing or not allowed",
        ));
    }

    let deployer = state.deployer.clone().ok_or_else(|| {
        AppError::new(
            StatusCode::NOT_IMPLEMENTED,
            "Deploying is not supported for this package type yet",
        )
    })?;

    let version = q.version.unwrap_or_else(|| "latest".to_string());
    if !valid_version(&version) {
        return Err(AppError::new(StatusCode::BAD_REQUEST, "Invalid version"));
    }

    let guard =
        state.deploy_lock.clone().try_lock_owned().map_err(|_| {
            AppError::new(StatusCode::CONFLICT, "A deployment is already in progress")
        })?;

    // Run in its own task: if the client disconnects mid-request, axum drops
    // this handler, and a half-finished stop/swap/start must not be cut off.
    let outcome = tokio::spawn(async move {
        let _guard = guard;
        deployer.deploy(&version).await
    })
    .await
    .map_err(|e| {
        AppError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("deploy task failed: {e}"),
        )
    })??;

    Ok(Json(outcome))
}

fn valid_version(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 100
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '+'))
}

// ---------- OpenAPI ----------

#[derive(OpenApi)]
#[openapi(
    paths(get_versions, deploy_version),
    components(schemas(ReleaseInfo, DeployOutcome)),
    info(
        title = "Version Tracker",
        description = "Release version tracker and deployer"
    )
)]
struct ApiDoc;

// ---------- CLI / main ----------

#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Args {
    #[arg(long, env = "PORT", default_value = "11234")]
    port: u16,
    /// Address to listen on. Defaults to loopback; use 0.0.0.0 to expose it.
    #[arg(long, env = "BIND", default_value = "127.0.0.1")]
    bind: IpAddr,
    #[arg(long, env = "SOFTWARE_URL")]
    software_url: String,
    #[arg(long, env = "SOFTWARE_PACKAGE_TYPE")]
    software_package_type: SoftwarePackageType,

    /// Origin(s) the frontend is served from, e.g. https://ui.example.com
    /// (comma-separated for several). Used for CORS and enforced on /deploy.
    #[arg(long, env = "ALLOWED_ORIGIN", required = true, value_delimiter = ',')]
    allowed_origins: Vec<String>,

    /// systemd unit and binary name (default: the repository name)
    #[arg(long, env = "SERVICE_NAME")]
    service_name: Option<String>,
    /// Directory the binary is installed into
    #[arg(long, env = "INSTALL_DIR", default_value = "/usr/local/bin")]
    install_dir: PathBuf,
    /// Release asset to install; "{name}" is replaced by the service name
    #[arg(long, env = "ASSET_NAME", default_value = "{name}-linux-aarch64")]
    asset_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    // Normalize origins (no trailing slash) and make sure they're valid header values
    let allowed_origins: Vec<String> = args
        .allowed_origins
        .iter()
        .map(|o| o.trim().trim_end_matches('/').to_owned())
        .filter(|o| !o.is_empty())
        .collect();
    if allowed_origins.is_empty() {
        anyhow::bail!("ALLOWED_ORIGIN must contain at least one origin");
    }
    let origin_headers = allowed_origins
        .iter()
        .map(|o| HeaderValue::from_str(o).map_err(|_| anyhow::anyhow!("Invalid origin: {o}")))
        .collect::<Result<Vec<_>>>()?;
    tracing::info!("allowed origins: {}", allowed_origins.join(", "));

    let client = reqwest::Client::builder()
        .user_agent(concat!("version_tracker/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(10))
        .build()?;

    let provider = build_provider(
        client.clone(),
        &args.software_url,
        args.software_package_type,
    )?;
    tracing::info!("using provider: {}", provider.name());

    let service_name = args
        .service_name
        .unwrap_or_else(|| provider.software_name().to_owned());
    let asset_name = args.asset_name.replace("{name}", &service_name);

    let deployer = build_deployer(
        client,
        provider.clone(),
        args.software_package_type,
        DeployOptions {
            service_name: service_name.clone(),
            install_dir: args.install_dir,
            asset_name: asset_name.clone(),
        },
    )?;
    match &deployer {
        Some(_) => tracing::info!("deploy enabled: service '{service_name}', asset '{asset_name}'"),
        None => tracing::warn!("deploy disabled: no deployer for this package type yet"),
    }

    let state = AppState {
        provider,
        cache: Arc::new(Mutex::new(None)),
        deployer,
        deploy_lock: Arc::new(Mutex::new(())),
        allowed_origins: Arc::new(allowed_origins),
    };

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(origin_headers))
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([CONTENT_TYPE]);

    let app = Router::new()
        .route("/versions", get(get_versions))
        .route("/deploy", post(deploy_version))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state)
        .layer(cors);

    let addr = SocketAddr::new(args.bind, args.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("listening on {}", listener.local_addr()?);

    Ok(axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?)
}

async fn shutdown_signal() {
    let ctrl_c = async { signal::ctrl_c().await.expect("ctrl-c handler") };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutting down");
}
