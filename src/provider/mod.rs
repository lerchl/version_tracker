mod github;

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use github::GithubProvider;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SoftwarePackageType {
    Binary,
    Image,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ReleaseInfo {
    /// Version string as published, with any leading "v" removed
    pub version: String,
    /// ISO 8601 timestamp
    pub published_at: Option<String>,
    /// Whether this is the latest release
    pub latest: bool,
}

/// A downloadable file belonging to a specific release.
#[derive(Debug, Clone)]
pub struct ResolvedAsset {
    /// Release tag exactly as the provider names it (e.g. "v1.2.3")
    pub version: String,
    pub download_url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("not found")]
    NotFound,
    #[error("release has no asset named '{0}'")]
    AssetNotFound(String),
    #[error("upstream error: {0}")]
    Upstream(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
}

/// A source of released versions (GitHub, a container registry, ...).
#[async_trait]
pub trait VersionProvider: Send + Sync {
    fn name(&self) -> &'static str;

    /// Complete list of releases, newest first. Caching and pagination are
    /// handled by the caller.
    async fn fetch_releases(&self) -> Result<Vec<ReleaseInfo>, ProviderError>;

    /// Finds the named asset of a release. `version` is "latest" or a tag.
    async fn resolve_asset(
        &self,
        version: &str,
        asset_name: &str,
    ) -> Result<ResolvedAsset, ProviderError>;
}

/// Picks a provider for the configured software URL.
/// To add a provider: create a module, implement `VersionProvider`, and add
/// a branch here.
pub fn build_provider(
    client: reqwest::Client,
    url: &str,
    package_type: SoftwarePackageType,
) -> anyhow::Result<Arc<dyn VersionProvider>> {
    let _ = package_type; // available for providers that need it (e.g. registries)

    if let Some(p) = GithubProvider::from_url(client.clone(), url) {
        return Ok(Arc::new(p));
    }
    // if let Some(p) = DockerHubProvider::from_url(client.clone(), url) { ... }
    // if let Some(p) = GitlabProvider::from_url(client.clone(), url) { ... }

    anyhow::bail!("No provider supports URL: {url}")
}
