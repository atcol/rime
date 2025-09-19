use crate::config::CatalogConfig;
use crate::RimeError;
use async_trait::async_trait;
use iceberg::Catalog;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum CatalogType {
    Glue,
    Rest,
}

#[async_trait]
pub trait CatalogProvider: Send + Sync {
    async fn create_catalog(&self, config: &CatalogConfig) -> Result<Arc<dyn Catalog>, RimeError>;
    fn catalog_type(&self) -> CatalogType;
}

pub struct GlueCatalogProvider;

#[async_trait]
impl CatalogProvider for GlueCatalogProvider {
    async fn create_catalog(&self, config: &CatalogConfig) -> Result<Arc<dyn Catalog>, RimeError> {
        match config {
            CatalogConfig::Glue { region, warehouse, profile } => {
                tracing::info!("Creating Glue catalog with region: {}, warehouse: {}", region, warehouse);
                
                let config_builder = iceberg_catalog_glue::GlueCatalogConfig::builder()
                    .warehouse(warehouse.clone());

                if let Some(profile) = profile {
                    tracing::info!("Setting AWS profile: {}", profile);
                    std::env::set_var("AWS_PROFILE", profile);
                }
                
                tracing::info!("Setting AWS region: {}", region);
                std::env::set_var("AWS_REGION", region);

                let glue_config = config_builder.build();
                tracing::debug!("Glue config built successfully");

                let catalog = iceberg_catalog_glue::GlueCatalog::new(glue_config)
                    .await
                    .map_err(|e| {
                        tracing::error!("Failed to create Glue catalog: {:?}", e);
                        RimeError::Catalog(e)
                    })?;
                
                tracing::info!("Glue catalog created successfully");
                Ok(Arc::new(catalog))
            }
            _ => Err(RimeError::Config("Invalid catalog type for Glue provider".to_string())),
        }
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Glue
    }
}

pub struct RestCatalogProvider;

#[async_trait]
impl CatalogProvider for RestCatalogProvider {
    async fn create_catalog(&self, config: &CatalogConfig) -> Result<Arc<dyn Catalog>, RimeError> {
        match config {
            CatalogConfig::Rest { uri, warehouse, credential, token } => {
                tracing::info!("Creating REST catalog with URI: {}, warehouse: {}", uri, warehouse);
                
                if credential.is_some() {
                    tracing::info!("Using credential authentication");
                }
                if token.is_some() {
                    tracing::info!("Using token authentication");
                }

                let rest_config = iceberg_catalog_rest::RestCatalogConfig::builder()
                    .uri(uri.clone())
                    .warehouse(warehouse.clone())
                    .build();

                tracing::debug!("REST config built successfully");
                let catalog = iceberg_catalog_rest::RestCatalog::new(rest_config);
                tracing::info!("REST catalog created successfully");
                
                Ok(Arc::new(catalog))
            }
            _ => Err(RimeError::Config("Invalid catalog type for REST provider".to_string())),
        }
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Rest
    }
}

pub fn create_catalog_provider(catalog_type: &CatalogType) -> Box<dyn CatalogProvider> {
    match catalog_type {
        CatalogType::Glue => Box::new(GlueCatalogProvider),
        CatalogType::Rest => Box::new(RestCatalogProvider),
    }
}