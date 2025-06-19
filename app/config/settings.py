import os
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Google Cloud Configuration
    google_cloud_project_id: str
    google_application_credentials: Optional[str] = None  # Optional: Leave empty to use Application Default Credentials
    bigquery_dataset_id: str
    bigquery_table_id: str
    
    # FastAPI Configuration
    app_name: str = "Retail Plan Visualizer Backend"
    app_version: str = "1.0.0"
    debug: bool = True
    
    # API Configuration
    api_v1_prefix: str = "/api/v1"
    cors_origins: list[str] = ["*"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings() 