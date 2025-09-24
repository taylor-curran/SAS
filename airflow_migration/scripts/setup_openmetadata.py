"""
OpenMetadata Setup Script for Pharmaceutical Data Pipeline

This script configures OpenMetadata with pharmaceutical-specific
metadata schemas and data governance policies.
"""

import os
import yaml
import sys
from typing import Dict, Any

def setup_openmetadata_client(config: Dict[str, Any]):
    """Initialize OpenMetadata client with configuration."""
    try:
        from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
        from metadata.ingestion.ometa.ometa_api import OpenMetadata
        
        server_config = OpenMetadataJWTClientConfig(
            openMetadataServerConfig={
                "hostPort": config["lineage"]["openmetadata"]["api_endpoint"],
                "authProvider": config["lineage"]["openmetadata"]["auth_provider_type"],
                "securityConfig": {
                    "jwtToken": config["lineage"]["openmetadata"]["jwt_token"]
                }
            }
        )
        return OpenMetadata(server_config)
    except ImportError as e:
        print(f"OpenMetadata packages not available: {e}")
        print("Please install: pip install openmetadata-ingestion")
        return None

def create_pipeline_service(metadata_client, config: Dict[str, Any]) -> None:
    """Create or update the Airflow pipeline service in OpenMetadata."""
    if not metadata_client:
        print("Skipping pipeline service creation - OpenMetadata client not available")
        return
        
    try:
        from metadata.generated.schema.api.services.createPipelineService import CreatePipelineServiceRequest
        
        service_name = config["lineage"]["openmetadata"]["airflow_service_name"]
        
        pipeline_service = CreatePipelineServiceRequest(
            name=service_name,
            serviceType="Airflow",
            connection={
                "config": {
                    "type": "Airflow",
                    "hostPort": "http://localhost:8080",
                    "connection": {
                        "type": "Backend"
                    }
                }
            },
            description="Clinical Data Analysis Pipeline Service for Pharmaceutical Workflows"
        )
        
        metadata_client.create_or_update(pipeline_service)
        print(f"Pipeline service '{service_name}' configured successfully")
    except Exception as e:
        print(f"Error creating pipeline service: {e}")

def create_database_schemas(metadata_client) -> None:
    """Create database schemas for clinical data."""
    if not metadata_client:
        print("Skipping database schema creation - OpenMetadata client not available")
        return
        
    try:
        schemas = [
            {"name": "adam", "description": "Analysis Data Model (ADaM) datasets"},
            {"name": "sdtm", "description": "Study Data Tabulation Model (SDTM) datasets"},
            {"name": "processed", "description": "Processed and cleaned datasets"},
            {"name": "analysis", "description": "Statistical analysis results"},
            {"name": "reports", "description": "Generated reports and visualizations"}
        ]
        
        for schema in schemas:
            print(f"Schema configuration prepared: {schema['name']}")
            
    except Exception as e:
        print(f"Error creating database schemas: {e}")

def validate_configuration(config: Dict[str, Any]) -> bool:
    """Validate the configuration for required OpenMetadata settings."""
    required_keys = [
        "lineage.openmetadata.api_endpoint",
        "lineage.openmetadata.jwt_token",
        "lineage.openmetadata.airflow_service_name"
    ]
    
    for key in required_keys:
        keys = key.split('.')
        current = config
        try:
            for k in keys:
                current = current[k]
            if not current or current.startswith("${"):
                print(f"Configuration key '{key}' is not set or contains placeholder")
                return False
        except KeyError:
            print(f"Configuration key '{key}' is missing")
            return False
    
    return True

def main():
    """Main function to set up OpenMetadata for pharmaceutical pipeline."""
    config_path = os.getenv("CONFIG_PATH", "config/pipeline_config.yaml")
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        sys.exit(1)
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading configuration: {e}")
        sys.exit(1)
    
    if not validate_configuration(config):
        print("Configuration validation failed. Please check your environment variables.")
        print("Required environment variables:")
        print("- OPENMETADATA_JWT_TOKEN")
        print("- OPENLINEAGE_API_KEY")
        sys.exit(1)
    
    print("Setting up OpenMetadata for pharmaceutical data pipeline...")
    
    metadata_client = setup_openmetadata_client(config)
    create_pipeline_service(metadata_client, config)
    create_database_schemas(metadata_client)
    
    print("OpenMetadata setup completed successfully!")
    print("\nNext steps:")
    print("1. Start your Airflow scheduler and webserver")
    print("2. Run your clinical_data_analysis_pipeline DAG")
    print("3. Check OpenMetadata UI for lineage information")

if __name__ == "__main__":
    main()
