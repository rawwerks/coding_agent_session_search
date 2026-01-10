use anyhow::Result;

pub struct GitHubDeployer {
    // Config
}

impl Default for GitHubDeployer {
    fn default() -> Self {
        Self::new()
    }
}

impl GitHubDeployer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn deploy(&self) -> Result<()> {
        Ok(())
    }
}
