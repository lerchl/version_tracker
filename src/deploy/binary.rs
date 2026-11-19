use std::{
    os::unix::fs::PermissionsExt, path::PathBuf, process::Output, sync::Arc, time::Duration,
};

use async_trait::async_trait;
use tokio::{fs, io::AsyncWriteExt, process::Command};

use super::{DeployError, DeployOptions, DeployOutcome, Deployer};
use crate::provider::VersionProvider;

/// Overrides the shared client's short timeout for the (large) download.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);

pub struct BinaryDeployer {
    client: reqwest::Client,
    provider: Arc<dyn VersionProvider>,
    service_name: String,
    asset_name: String,
    binary_path: PathBuf,
    tmp_path: PathBuf,
}

impl BinaryDeployer {
    pub fn new(
        client: reqwest::Client,
        provider: Arc<dyn VersionProvider>,
        opts: DeployOptions,
    ) -> anyhow::Result<Self> {
        // The name ends up in a file path and in systemctl arguments.
        let valid = !opts.service_name.is_empty()
            && !opts.service_name.starts_with(['.', '-'])
            && opts
                .service_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '@'));
        if !valid {
            anyhow::bail!("Invalid service name: {}", opts.service_name);
        }

        let binary_path = opts.install_dir.join(&opts.service_name);
        // Same directory as the target, so the final rename is atomic
        let mut tmp = binary_path.clone().into_os_string();
        tmp.push(".new");

        Ok(Self {
            client,
            provider,
            service_name: opts.service_name,
            asset_name: opts.asset_name,
            binary_path,
            tmp_path: PathBuf::from(tmp),
        })
    }

    async fn download(&self, url: &str) -> Result<(), DeployError> {
        let mut response = self
            .client
            .get(url)
            .timeout(DOWNLOAD_TIMEOUT)
            .send()
            .await?
            .error_for_status()?;

        let mut file = fs::File::create(&self.tmp_path).await?;
        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn swap_and_restart(&self) -> Result<(), DeployError> {
        let svc = self.service_name.as_str();

        let was_active = systemctl(&["is-active", "--quiet", svc])
            .await?
            .status
            .success();
        if was_active {
            check("stop", systemctl(&["stop", svc]).await?)?;
        }

        if let Err(e) = fs::rename(&self.tmp_path, &self.binary_path).await {
            // Don't leave the service down if the swap failed
            if was_active {
                let _ = systemctl(&["start", svc]).await;
            }
            return Err(e.into());
        }

        check("start", systemctl(&["start", svc]).await?)
    }

    async fn install(&self, url: &str) -> Result<(), DeployError> {
        self.download(url).await?;
        fs::set_permissions(&self.tmp_path, std::fs::Permissions::from_mode(0o755)).await?;
        self.swap_and_restart().await
    }
}

#[async_trait]
impl Deployer for BinaryDeployer {
    async fn deploy(&self, version: &str) -> Result<DeployOutcome, DeployError> {
        let asset = self
            .provider
            .resolve_asset(version, &self.asset_name)
            .await?;
        tracing::info!(
            "deploying {} {} from {}",
            self.service_name,
            asset.version,
            asset.download_url
        );

        let result = self.install(&asset.download_url).await;
        if result.is_err() {
            // No-op if the rename already happened
            let _ = fs::remove_file(&self.tmp_path).await;
        }
        result?;

        tracing::info!("deployed {} {}", self.service_name, asset.version);
        Ok(DeployOutcome {
            service: self.service_name.clone(),
            version: asset.version,
            binary_path: self.binary_path.to_string_lossy().into_owned(),
        })
    }
}

async fn systemctl(args: &[&str]) -> Result<Output, DeployError> {
    Ok(Command::new("systemctl").args(args).output().await?)
}

fn check(action: &'static str, out: Output) -> Result<(), DeployError> {
    if out.status.success() {
        Ok(())
    } else {
        Err(DeployError::Systemctl {
            action,
            detail: String::from_utf8_lossy(&out.stderr).trim().to_owned(),
        })
    }
}
