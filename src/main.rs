mod deploy;
mod provider;

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Error, Result};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::{signal, sync::Mutex};
use tower_http::cors::{Any, CorsLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::{
    IntoParams, Modify, OpenApi, ToSchema,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};
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
struct DeployState {
    deployer: Arc<dyn Deployer>,
    token: Arc<str>,
}

#[derive(Clone)]
struct AppState {
    provider: Arc<dyn VersionProvider>,
    // Held across the upstream fetch, so concurrent requests on a cold or
    // stale cache trigger only one refresh.
    cache: Arc<Mutex<Option<CacheEntry>>>,
    /// `None` when deploying is disabled
    deploy: Option<DeployState>,
    /// Only one deployment at a time
    deploy_lock: Arc<Mutex<()>>,
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

/// Download a release, swap the binary and restart the service
#[utoipa::path(
    post,
    path = "/deploy",
    tag = "deploy",
    params(DeployQuery),
    security(("bearer" = [])),
    responses(
        (status = 200, description = "Deployed", body = DeployOutcome),
        (status = 400, description = "Invalid version", body = String),
        (status = 401, description = "Missing or invalid token", body = String),
        (status = 404, description = "Release or asset not found", body = String),
        (status = 409, description = "A deployment is already running", body = String),
        (status = 500, description = "Install or systemctl failure", body = String),
        (status = 501, description = "Deploying is not enabled", body = String),
        (status = 502, description = "Upstream provider error", body = String),
    )
)]
async fn deploy_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<DeployQuery>,
) -> Result<Json<DeployOutcome>, AppError> {
    let cfg = state.deploy.as_ref().ok_or_else(|| {
        AppError::new(
            StatusCode::NOT_IMPLEMENTED,
            "Deploying is not enabled (needs DEPLOY_TOKEN and a package type with a deployer)",
        )
    })?;

    let provided = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if !provided.is_some_and(|t| tokens_match(t.as_bytes(), cfg.token.as_bytes())) {
        return Err(AppError::new(
            StatusCode::UNAUTHORIZED,
            "Missing or invalid bearer token",
        ));
    }

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
    let deployer = cfg.deployer.clone();
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

fn tokens_match(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// ---------- OpenAPI ----------

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(get_versions, deploy_version),
    components(schemas(ReleaseInfo, DeployOutcome)),
    modifiers(&SecurityAddon),
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
    #[arg(long, env = "SOFTWARE_URL")]
    software_url: String,
    #[arg(long, env = "SOFTWARE_PACKAGE_TYPE")]
    software_package_type: SoftwarePackageType,

    /// systemd unit and binary name (default: the repository name)
    #[arg(long, env = "SERVICE_NAME")]
    service_name: Option<String>,
    /// Directory the binary is installed into
    #[arg(long, env = "INSTALL_DIR", default_value = "/usr/local/bin")]
    install_dir: PathBuf,
    /// Release asset to install; "{name}" is replaced by the service name
    #[arg(long, env = "ASSET_NAME", default_value = "{name}-linux-aarch64")]
    asset_name: String,
    /// Bearer token required for POST /deploy. Deploying is disabled without it.
    #[arg(long, env = "DEPLOY_TOKEN", hide_env_values = true)]
    deploy_token: Option<String>,
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

    let deploy = match (args.deploy_token, deployer) {
        (Some(token), Some(deployer)) => {
            tracing::info!("deploy enabled: service '{service_name}', asset '{asset_name}'");
            Some(DeployState {
                deployer,
                token: Arc::from(token),
            })
        }
        (None, _) => {
            tracing::warn!("deploy disabled: DEPLOY_TOKEN is not set");
            None
        }
        (_, None) => {
            tracing::warn!("deploy disabled: no deployer for this package type yet");
            None
        }
    };

    let state = AppState {
        provider,
        cache: Arc::new(Mutex::new(None)),
        deploy,
        deploy_lock: Arc::new(Mutex::new(())),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/versions", get(get_versions))
        .route("/deploy", post(deploy_version))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state)
        .layer(cors);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;
    tracing::debug!("listening on {}", listener.local_addr()?);

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
