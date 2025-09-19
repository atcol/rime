use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub catalogs: HashMap<String, CatalogConfig>,
    pub default_catalog: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum CatalogConfig {
    #[serde(rename = "glue")]
    Glue {
        region: String,
        warehouse: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        profile: Option<String>,
    },
    #[serde(rename = "rest")]
    Rest {
        uri: String,
        warehouse: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        credential: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
    },
}

impl Config {
    pub fn new() -> Self {
        Self {
            catalogs: HashMap::new(),
            default_catalog: None,
        }
    }

    pub fn from_toml_str(content: &str) -> Result<Self, crate::RimeError> {
        toml::from_str(content)
            .map_err(|e| crate::RimeError::Config(format!("Invalid TOML: {}", e)))
    }

    pub fn to_toml_string(&self) -> Result<String, crate::RimeError> {
        toml::to_string_pretty(self)
            .map_err(|e| crate::RimeError::Config(format!("Failed to serialize config: {}", e)))
    }

    pub fn get_catalog(&self, name: &str) -> Option<&CatalogConfig> {
        self.catalogs.get(name)
    }

    pub fn get_default_catalog(&self) -> Option<&CatalogConfig> {
        if let Some(default_name) = &self.default_catalog {
            self.catalogs.get(default_name)
        } else {
            self.catalogs.values().next()
        }
    }
}