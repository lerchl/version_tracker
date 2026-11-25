mod binary;
mod image;

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use serde::Serialize;
use utoipa::ToSchema;

use crate::{
    deploy::image::ImageDeployer,
    provider::{ProviderError, SoftwarePackageType, VersionProvider},
};
use binary::BinaryDeployer;

#[derive(Debug, Serialize, ToSchema)]
pub struct DeployOutcome {
    pub service: String,
    /// Release tag that was deployed, as named by the provider
    pub version: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error("download failed: {0}")]
    Download(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("systemctl {action} failed: {detail}")]
    Systemctl {
        action: &'static str,
        detail: String,
    },
    #[error("{action} failed: {detail}")]
    Command {
        action: &'static str,
        detail: String,
    },
}

/// Installs a release of the configured software on this machine.
#[async_trait]
pub trait Deployer: Send + Sync {
    /// `version` is "latest" or a release tag.
    async fn deploy(&self, version: &str) -> Result<DeployOutcome, DeployError>;
}

pub struct DeployOptions {
    pub service_name: String,
    pub install_dir: PathBuf,
    /// Name of the release asset to install
    pub asset_name: String,
    /// Image repository without tag (image)
    pub image: Option<String>,
}

/// Picks a deployer for the package type. `None` means deploying isn't
/// supported for it yet.
pub fn build_deployer(
    client: reqwest::Client,
    provider: Arc<dyn VersionProvider>,
    package_type: SoftwarePackageType,
    opts: DeployOptions,
) -> anyhow::Result<Option<Arc<dyn Deployer>>> {
    match package_type {
        SoftwarePackageType::Binary => {
            Ok(Some(Arc::new(BinaryDeployer::new(client, provider, opts)?)))
        }
        SoftwarePackageType::Image => Ok(Some(Arc::new(ImageDeployer::new(opts)?))),
    }
}
