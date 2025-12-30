//! Server deployment configuration for IPUMS data products.
//!
//! This module models the server infrastructure where IPUMS data is deployed,
//! including internal, demo, and live environments for 12 data products.
//!
//! # Server Path Structure
//!
//! - **Internal**: `/web/internal.{domain}/share/data/current`
//! - **Demo**: `/web/demo.{domain}/share/data/current`
//! - **Live**: `/web/{live_server}/share/data/current`
//!
//! Within each `current` directory:
//! - Fixed-width files: `{dataset}_{suffix}.dat.gz`
//! - Parquet datasets: `parquet/{dataset}/`
//! - Derived data: `derived/{dataset}/`

use crate::mderror::MdError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Internal and demo server hostname
pub const INTERNAL_SERVER: &str = "ipums-internal-web.pop.umn.edu";

/// Ordered list of all IPUMS products
pub const ALL_PRODUCTS: &[&str] = &[
    "ahtus", "atus", "cps", "dhs", "highered", "ipumsi", "meps", "mics", "mtus", "nhis", "pma",
    "usa",
];

/// Server environments where data can be deployed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Environment {
    Internal,
    Demo,
    Live,
}

impl Environment {
    /// Get the string representation of the environment
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Internal => "internal",
            Environment::Demo => "demo",
            Environment::Live => "live",
        }
    }
}

impl std::fmt::Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Data formats that may be deployed for a product
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataFormat {
    /// Fixed-width .dat.gz files in current/
    FixedWidth,
    /// Parquet directories in current/parquet/
    Parquet,
    /// Derived data in current/derived/
    Derived,
}

impl DataFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            DataFormat::FixedWidth => "fw",
            DataFormat::Parquet => "parquet",
            DataFormat::Derived => "derived",
        }
    }
}

impl std::fmt::Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Configuration for a single IPUMS product's deployment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductDeployment {
    /// Product name (e.g., "usa", "cps", "ipumsi")
    pub name: String,

    /// Domain used for directory naming (e.g., "usa.ipums.org", "ahtusdata.org")
    pub domain: String,

    /// Live server hostname (may differ from domain, e.g., "www.ahtusdata.org")
    pub live_server: String,

    /// Data formats expected for this product
    pub formats: Vec<DataFormat>,

    /// Whether this product is hosted on third-party infrastructure
    pub third_party: bool,

    /// Naming suffix for fixed-width files (e.g., "_health" for meps/nhis)
    /// Default is _{product_name}
    pub naming_suffix: Option<String>,
}

impl ProductDeployment {
    /// Get the effective naming suffix for fixed-width files
    pub fn fw_suffix(&self) -> String {
        self.naming_suffix
            .clone()
            .unwrap_or_else(|| format!("_{}", self.name))
    }

    /// Check if this product expects a given data format
    pub fn expects_format(&self, format: DataFormat) -> bool {
        self.formats.contains(&format)
    }
}

/// A deployment target combining environment, product, and resolved server info
#[derive(Debug, Clone)]
pub struct DeploymentTarget {
    pub environment: Environment,
    pub product: ProductDeployment,
    pub server: String,
    pub base_path: String,
}

impl DeploymentTarget {
    /// Create a new deployment target for a product in an environment
    pub fn new(environment: Environment, product: &ProductDeployment, internal_server: &str) -> Self {
        let (server, base_path) = match environment {
            Environment::Internal => (
                internal_server.to_string(),
                format!("/web/internal.{}/share/data", product.domain),
            ),
            Environment::Demo => (
                internal_server.to_string(),
                format!("/web/demo.{}/share/data", product.domain),
            ),
            Environment::Live => (
                product.live_server.clone(),
                format!("/web/{}/share/data", product.live_server),
            ),
        };

        Self {
            environment,
            product: product.clone(),
            server,
            base_path,
        }
    }

    /// Path to the current data directory
    pub fn current_path(&self) -> String {
        format!("{}/current", self.base_path)
    }

    /// Path to parquet data directory
    pub fn parquet_path(&self) -> String {
        format!("{}/current/parquet", self.base_path)
    }

    /// Path to derived data directory
    pub fn derived_path(&self) -> String {
        format!("{}/current/derived", self.base_path)
    }

    /// Pattern for fixed-width files
    pub fn fw_pattern(&self) -> String {
        format!("{}/*{}.dat.gz", self.current_path(), self.product.fw_suffix())
    }
}

/// Configuration structure for TOML/JSON override file
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DeploymentConfig {
    /// Override the internal server
    pub internal_server: Option<String>,

    /// Product-specific overrides
    pub products: Option<Vec<ProductDeployment>>,
}

impl DeploymentConfig {
    /// Load configuration from a file path (TOML or JSON based on extension)
    pub fn load_from_file(path: &Path) -> Result<Self, MdError> {
        let content =
            std::fs::read_to_string(path).map_err(MdError::IoError)?;

        if path.extension().map_or(false, |ext| ext == "json") {
            serde_json::from_str(&content)
                .map_err(|e| MdError::ParsingError(format!("Invalid JSON config: {}", e)))
        } else {
            toml::from_str(&content)
                .map_err(|e| MdError::ParsingError(format!("Invalid TOML config: {}", e)))
        }
    }
}

/// Registry holding all product deployments, with optional config override
#[derive(Debug, Clone)]
pub struct DeploymentRegistry {
    pub products: HashMap<String, ProductDeployment>,
    pub internal_server: String,
}

impl Default for DeploymentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DeploymentRegistry {
    /// Create with defaults only
    pub fn new() -> Self {
        let products = default_product_deployments()
            .into_iter()
            .map(|p| (p.name.clone(), p))
            .collect();

        Self {
            products,
            internal_server: INTERNAL_SERVER.to_string(),
        }
    }

    /// Create with optional config file override
    pub fn with_config(config_path: Option<&Path>) -> Result<Self, MdError> {
        let mut registry = Self::new();

        if let Some(path) = config_path {
            let config = DeploymentConfig::load_from_file(path)?;

            if let Some(server) = config.internal_server {
                registry.internal_server = server;
            }

            if let Some(products) = config.products {
                for product in products {
                    registry.products.insert(product.name.clone(), product);
                }
            }
        }

        Ok(registry)
    }

    /// Get a product by name
    pub fn get_product(&self, name: &str) -> Option<&ProductDeployment> {
        self.products.get(name)
    }

    /// Get all products in standard order
    pub fn all_products(&self) -> Vec<&ProductDeployment> {
        ALL_PRODUCTS
            .iter()
            .filter_map(|name| self.products.get(*name))
            .collect()
    }

    /// Create a deployment target for a product in an environment
    pub fn target(&self, environment: Environment, product: &ProductDeployment) -> DeploymentTarget {
        DeploymentTarget::new(environment, product, &self.internal_server)
    }
}

/// Get the default product deployments (hardcoded for 12 products)
pub fn default_product_deployments() -> Vec<ProductDeployment> {
    vec![
        ProductDeployment {
            name: "ahtus".to_string(),
            domain: "ahtusdata.org".to_string(),
            live_server: "www.ahtusdata.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet],
            third_party: false,
            naming_suffix: None,
        },
        ProductDeployment {
            name: "atus".to_string(),
            domain: "atusdata.org".to_string(),
            live_server: "www.atusdata.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet],
            third_party: false,
            naming_suffix: None,
        },
        ProductDeployment {
            name: "cps".to_string(),
            domain: "cps.ipums.org".to_string(),
            live_server: "cps.ipums.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet, DataFormat::Derived],
            third_party: false,
            naming_suffix: None,
        },
        ProductDeployment {
            name: "dhs".to_string(),
            domain: "idhsdata.org".to_string(),
            live_server: "www.idhsdata.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet],
            third_party: true,
            naming_suffix: None,
        },
        ProductDeployment {
            name: "highered".to_string(),
            domain: "highered.ipums.org".to_string(),
            live_server: "highered.ipums.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet],
            third_party: false,
            naming_suffix: None,
        },
        ProductDeployment {
            name: "ipumsi".to_string(),
            domain: "international.ipums.org".to_string(),
            live_server: "international.ipums.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet],
            third_party: false,
            naming_suffix: None,
        },
        ProductDeployment {
            name: "meps".to_string(),
            domain: "meps.ipums.org".to_string(),
            live_server: "meps.ipums.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet, DataFormat::Derived],
            third_party: false,
            naming_suffix: Some("_health".to_string()),
        },
        ProductDeployment {
            name: "mics".to_string(),
            domain: "mics.ipums.org".to_string(),
            live_server: "mics.ipums.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet],
            third_party: true,
            naming_suffix: None,
        },
        ProductDeployment {
            name: "mtus".to_string(),
            domain: "mtusdata.org".to_string(),
            live_server: "www.mtusdata.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet],
            third_party: false,
            naming_suffix: None,
        },
        ProductDeployment {
            name: "nhis".to_string(),
            domain: "nhis.ipums.org".to_string(),
            live_server: "nhis.ipums.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet, DataFormat::Derived],
            third_party: false,
            naming_suffix: Some("_health".to_string()),
        },
        ProductDeployment {
            name: "pma".to_string(),
            domain: "pma.ipums.org".to_string(),
            live_server: "pma.ipums.org".to_string(),
            formats: vec![DataFormat::FixedWidth, DataFormat::Parquet, DataFormat::Derived],
            third_party: false,
            naming_suffix: None,
        },
        ProductDeployment {
            name: "usa".to_string(),
            domain: "usa.ipums.org".to_string(),
            live_server: "usa.ipums.org".to_string(),
            formats: vec![DataFormat::Parquet], // USA is parquet-only
            third_party: false,
            naming_suffix: None,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_product_count() {
        let registry = DeploymentRegistry::new();
        assert_eq!(registry.products.len(), 12);
    }

    #[test]
    fn test_product_lookup() {
        let registry = DeploymentRegistry::new();
        let usa = registry.get_product("usa").unwrap();
        assert_eq!(usa.name, "usa");
        assert_eq!(usa.domain, "usa.ipums.org");
        assert!(usa.expects_format(DataFormat::Parquet));
        assert!(!usa.expects_format(DataFormat::FixedWidth));
    }

    #[test]
    fn test_internal_target_paths() {
        let registry = DeploymentRegistry::new();
        let cps = registry.get_product("cps").unwrap();
        let target = registry.target(Environment::Internal, cps);

        assert_eq!(target.server, INTERNAL_SERVER);
        assert_eq!(target.base_path, "/web/internal.cps.ipums.org/share/data");
        assert_eq!(
            target.current_path(),
            "/web/internal.cps.ipums.org/share/data/current"
        );
        assert_eq!(
            target.parquet_path(),
            "/web/internal.cps.ipums.org/share/data/current/parquet"
        );
    }

    #[test]
    fn test_live_target_paths() {
        let registry = DeploymentRegistry::new();
        let ahtus = registry.get_product("ahtus").unwrap();
        let target = registry.target(Environment::Live, ahtus);

        assert_eq!(target.server, "www.ahtusdata.org");
        assert_eq!(target.base_path, "/web/www.ahtusdata.org/share/data");
    }

    #[test]
    fn test_fw_suffix() {
        let registry = DeploymentRegistry::new();

        let cps = registry.get_product("cps").unwrap();
        assert_eq!(cps.fw_suffix(), "_cps");

        let meps = registry.get_product("meps").unwrap();
        assert_eq!(meps.fw_suffix(), "_health");
    }

    #[test]
    fn test_fw_pattern() {
        let registry = DeploymentRegistry::new();
        let cps = registry.get_product("cps").unwrap();
        let target = registry.target(Environment::Internal, cps);

        assert_eq!(
            target.fw_pattern(),
            "/web/internal.cps.ipums.org/share/data/current/*_cps.dat.gz"
        );
    }

    #[test]
    fn test_third_party_products() {
        let registry = DeploymentRegistry::new();

        let dhs = registry.get_product("dhs").unwrap();
        assert!(dhs.third_party);

        let mics = registry.get_product("mics").unwrap();
        assert!(mics.third_party);

        let usa = registry.get_product("usa").unwrap();
        assert!(!usa.third_party);
    }

    #[test]
    fn test_all_products_order() {
        let registry = DeploymentRegistry::new();
        let products = registry.all_products();

        assert_eq!(products.len(), 12);
        assert_eq!(products[0].name, "ahtus");
        assert_eq!(products[11].name, "usa");
    }
}
