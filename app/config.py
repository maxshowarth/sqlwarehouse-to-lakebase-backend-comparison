# Inspired by https://github.com/databricks-solutions/brickhouse-brands-demo/blob/main/backend/app/auth.py
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class AppConfig(BaseSettings):
    """Application configuration using Pydantic BaseSettings.

    Loads configuration from environment variables and .env file (if present).
    Fields are type-checked and validated. Defaults are provided where appropriate.
    """
    # Databricks
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    databricks_client_id: Optional[str] = None
    databricks_client_secret: Optional[str] = None
    databricks_workspace_id: Optional[str] = None
    databricks_account_id: Optional[str] = None
    databricks_config_profile: Optional[str] = None
    databricks_catalog: Optional[str] = None
    databricks_schema: Optional[str] = None
    databricks_volume: Optional[str] = None

    # Application
    app_env: str = "local"
    log_level: str = "DEBUG"
    
    # Data paths
    data_dir: str = "sample_data"
    
    # UI settings
    default_row_limit: int = 2000
    min_row_limit: int = 100
    max_row_limit: int = 100000
    default_top_n: int = 10
    min_top_n: int = 5
    max_top_n: int = 30
    
    # Seed data settings
    default_seed_scale: str = "small"
    default_seed_days: int = 14
    default_seed_value: int = 42

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

_config: Optional[AppConfig] = None

def get_config() -> AppConfig:
    """Return the AppConfig instance (singleton pattern)."""
    global _config
    if _config is None:
        _config = AppConfig()
    return _config

def set_config_for_test(**kwargs):
    """For testing only: override the AppConfig instance with new values."""
    global _config
    _config = AppConfig(**kwargs)
