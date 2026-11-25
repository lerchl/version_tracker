use std::{process::Output, time::Duration};

use async_trait::async_trait;
use tokio::{process::Command, time::timeout};

use super::{DeployError, DeployOptions, DeployOutcome, Deployer};

/// The tag the systemd unit runs; moved by the tracker on every deploy.
const DEPLOYED_TAG: &str = "deployed";

const PULL_TIMEOUT: Duration = Duration::from_secs(600);
const QUICK_TIMEOUT: Duration = Duration::from_secs(30);
const RESTART_TIMEOUT: Duration = Duration::from_secs(120);

pub struct ImageDeployer {
    service_name: String,
    image: String,
    deployed_ref: String,
}

impl ImageDeployer {
    pub fn new(opts: DeployOptions) -> anyhow::Result<Self> {
        let valid_name = !opts.service_name.is_empty()
            && !opts.service_name.starts_with(['.', '-'])
            && opts
                .service_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '@'));
        if !valid_name {
            anyhow::bail!("Invalid service name: {}", opts.service_name);
        }

        let image = opts.image.ok_or_else(|| {
            anyhow::anyhow!(
                "service '{}': package_type image needs `image`",
                opts.service_name
            )
        })?;
        // Repository without tag, e.g. ghcr.io/lerchl/sophie-matrix-ui
        let valid_image = !image.is_empty()
            && !image.starts_with(['-', '/'])
            && !image.contains(':')
            && image
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-'));
        if !valid_image {
            anyhow::bail!("Invalid image '{image}' (give the repository without a tag)");
        }

        Ok(Self {
            service_name: opts.service_name,
            deployed_ref: format!("{image}:{DEPLOYED_TAG}"),
            image,
        })
    }

    async fn restart(&self) -> Result<(), DeployError> {
        run(
            "systemctl",
            &["restart", &self.service_name],
            RESTART_TIMEOUT,
            "systemctl restart",
        )
        .await?;
        run(
            "systemctl",
            &["is-active", "--quiet", &self.service_name],
            QUICK_TIMEOUT,
            "systemctl is-active",
        )
        .await?;
        Ok(())
    }

    /// Image ID currently behind the deployed tag, if there is one
    async fn current_id(&self) -> Option<String> {
        let out = run(
            "podman",
            &[
                "image",
                "inspect",
                "--format",
                "{{.Id}}",
                &self.deployed_ref,
            ],
            QUICK_TIMEOUT,
            "podman inspect",
        )
        .await
        .ok()?;
        let id = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        (!id.is_empty()).then_some(id)
    }
}

/// Registry tags to try for a version ("latest" or a release tag).
fn candidate_tags(version: &str) -> Vec<String> {
    if version == "latest" {
        vec!["latest".to_owned()]
    } else if let Some(stripped) = version.strip_prefix(['v', 'V']) {
        vec![version.to_owned(), stripped.to_owned()]
    } else {
        vec![version.to_owned(), format!("v{version}")]
    }
}

#[async_trait]
impl Deployer for ImageDeployer {
    async fn deploy(&self, version: &str) -> Result<DeployOutcome, DeployError> {
        // 1. pull
        let mut pulled: Option<(String, String)> = None;
        let mut last_err = None;
        for tag in candidate_tags(version) {
            let reference = format!("{}:{tag}", self.image);
            match run(
                "podman",
                &["pull", "--quiet", &reference],
                PULL_TIMEOUT,
                "podman pull",
            )
            .await
            {
                Ok(_) => {
                    pulled = Some((tag, reference));
                    break;
                }
                Err(e) => last_err = Some(e),
            }
        }
        let Some((tag, reference)) = pulled else {
            return Err(last_err.expect("at least one candidate tag"));
        };
        tracing::info!("deploying {} {tag} ({reference})", self.service_name);

        // 2. remember what to roll back to
        let previous = self.current_id().await;

        // 3. move the deployed tag
        run(
            "podman",
            &["tag", &reference, &self.deployed_ref],
            QUICK_TIMEOUT,
            "podman tag",
        )
        .await?;

        // 4. restart, roll back on failure
        if let Err(e) = self.restart().await {
            if let Some(prev) = previous {
                tracing::warn!("restart failed ({e}), rolling back to {prev}");
                let _ = run(
                    "podman",
                    &["tag", &prev, &self.deployed_ref],
                    QUICK_TIMEOUT,
                    "podman tag",
                )
                .await;
                let _ = self.restart().await;
            }
            return Err(e);
        }

        tracing::info!("deployed {} {tag}", self.service_name);
        Ok(DeployOutcome {
            service: self.service_name.clone(),
            version: tag,
        })
    }
}

async fn run(
    program: &'static str,
    args: &[&str],
    limit: Duration,
    action: &'static str,
) -> Result<Output, DeployError> {
    let mut cmd = Command::new(program);
    cmd.args(args).kill_on_drop(true);

    let out = timeout(limit, cmd.output())
        .await
        .map_err(|_| DeployError::Command {
            action,
            detail: format!("timed out after {}s", limit.as_secs()),
        })??;

    if out.status.success() {
        Ok(out)
    } else {
        Err(DeployError::Command {
            action,
            detail: String::from_utf8_lossy(&out.stderr).trim().to_owned(),
        })
    }
}
