use async_trait::async_trait;
use serde::Deserialize;

use super::{ProviderError, ReleaseInfo, ResolvedAsset, VersionProvider};

const PER_PAGE: usize = 100;
const MAX_PAGES: usize = 10; // safety cap: at most 1000 releases

pub struct GithubProvider {
    client: reqwest::Client,
    owner: String,
    repo: String,
}

#[derive(Deserialize)]
struct GhRelease {
    tag_name: String,
    published_at: Option<String>,
}

#[derive(Deserialize)]
struct GhReleaseWithAssets {
    tag_name: String,
    assets: Vec<GhAsset>,
}

#[derive(Deserialize)]
struct GhAsset {
    name: String,
    browser_download_url: String,
}

impl GithubProvider {
    /// Returns `None` if the URL is not a GitHub repository URL.
    pub fn from_url(client: reqwest::Client, url: &str) -> Option<Self> {
        // Formats like: https://github.com/owner/repo
        let url = url.trim();
        let start = url.find("github.com/")?;
        let after = &url[start + "github.com/".len()..];
        let (owner, rest) = after.split_once('/')?;
        let repo = rest.split(['/', '?', '#']).next().unwrap_or("");
        let repo = repo.strip_suffix(".git").unwrap_or(repo);

        if owner.is_empty() || repo.is_empty() {
            return None;
        }
        Some(Self {
            client,
            owner: owner.to_string(),
            repo: repo.to_string(),
        })
    }

    /// GET /repos/{owner}/{repo}/releases/{suffix}; `None` on 404.
    async fn fetch_release(
        &self,
        suffix: &str,
    ) -> Result<Option<GhReleaseWithAssets>, ProviderError> {
        let url = format!(
            "https://api.github.com/repos/{}/{}/releases/{suffix}",
            self.owner, self.repo
        );
        let response = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.github+json")
            .send()
            .await?;
        let status = response.status();

        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ProviderError::Upstream(format!("{status} {body}")));
        }
        Ok(Some(response.json().await?))
    }
}

#[async_trait]
impl VersionProvider for GithubProvider {
    fn name(&self) -> &'static str {
        "github"
    }

    async fn fetch_releases(&self) -> Result<Vec<ReleaseInfo>, ProviderError> {
        let (owner, repo) = (&self.owner, &self.repo);
        let mut out = Vec::new();

        // Fetch GitHub's officially designated latest release
        let latest_tag = self
            .fetch_release("latest")
            .await?
            .map(|release| release.tag_name);

        for page in 1..=MAX_PAGES {
            let url = format!(
                "https://api.github.com/repos/{owner}/{repo}/releases?per_page={PER_PAGE}&page={page}"
            );

            let response = self
                .client
                .get(&url)
                .header("Accept", "application/vnd.github+json")
                .send()
                .await?;

            let status = response.status();

            if status == reqwest::StatusCode::NOT_FOUND {
                return Err(ProviderError::NotFound);
            }

            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(ProviderError::Upstream(format!("{status} {body}")));
            }

            let raw: Vec<GhRelease> = response.json().await?;
            let raw_len = raw.len();

            out.extend(raw.into_iter().map(|r| {
                ReleaseInfo {
                    // The "v" prefix is optional
                    version: r
                        .tag_name
                        .strip_prefix(['v', 'V'])
                        .unwrap_or(&r.tag_name)
                        .to_owned(),
                    published_at: r.published_at,
                    latest: latest_tag.as_deref() == Some(r.tag_name.as_str()),
                }
            }));

            // A short page means we've reached the end
            if raw_len < PER_PAGE {
                break;
            }
        }

        Ok(out)
    }

    async fn resolve_asset(
        &self,
        version: &str,
        asset_name: &str,
    ) -> Result<ResolvedAsset, ProviderError> {
        let release = if version == "latest" {
            self.fetch_release("latest").await?
        } else {
            let mut found = self.fetch_release(&format!("tags/{version}")).await?;

            // /versions returns tags without the "v", so also accept those
            if found.is_none() && !version.starts_with(['v', 'V']) {
                found = self.fetch_release(&format!("tags/v{version}")).await?;
            }

            found
        }
        .ok_or(ProviderError::NotFound)?;

        let asset = release
            .assets
            .into_iter()
            .find(|a| a.name == asset_name)
            .ok_or_else(|| ProviderError::AssetNotFound(asset_name.to_owned()))?;

        Ok(ResolvedAsset {
            version: release.tag_name,
            download_url: asset.browser_download_url,
        })
    }
}
