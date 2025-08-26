from typing import Optional
from databricks.sdk import WorkspaceClient
from backend.config import get_config
from backend.logging import get_logger

class DatabricksAuthentication:
    """Handles Databricks authentication and client creation using AppConfig singleton."""
    def __init__(self) -> None:
        """Initializes the authentication handler using the singleton AppConfig."""
        self.config = get_config()
        self.logger = get_logger(__name__)

    def get_databricks_config(self):
        """Creates a Databricks SDK Config object from AppConfig values.

        Returns:
            DatabricksConfig: The Databricks SDK config object.
        Raises:
            RuntimeError: If required configuration is missing.
        """
        from databricks.sdk.core import Config as DatabricksConfig
        if self.config.databricks_client_id and self.config.databricks_client_secret:
            self.logger.info("Configuring Databricks Apps authentication with service principal")
            return DatabricksConfig(
                host=self.config.databricks_host,
                client_id=self.config.databricks_client_id,
                client_secret=self.config.databricks_client_secret,
            )
        elif self.config.databricks_host and self.config.databricks_token:
            self.logger.info("Configuring Databricks authentication with manual token")
            return DatabricksConfig(
                host=self.config.databricks_host,
                token=self.config.databricks_token,
            )
        elif self.config.databricks_host:
            self.logger.info("Configuring Databricks authentication with CLI")
            return DatabricksConfig(host=self.config.databricks_host)
        else:
            self.logger.error("Missing Databricks configuration values.")
            raise RuntimeError("Missing Databricks configuration values.")

    def get_workspace_client(self) -> WorkspaceClient:
        """Returns an authenticated Databricks WorkspaceClient.

        Returns:
            WorkspaceClient: The Databricks WorkspaceClient instance.
        """
        config = self.get_databricks_config()
        self.logger.info(f"Instantiating WorkspaceClient for host: {self.config.databricks_host}")
        return WorkspaceClient(config=config)

def get_databricks_auth() -> DatabricksAuthentication:
    """Returns a new DatabricksAuthentication instance using the latest config."""
    return DatabricksAuthentication()