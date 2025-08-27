import pytest
from app.databricks.auth import DatabricksAuthentication
from app.config import set_config_for_test, get_config

class MockDatabricksConfig:
    def __init__(self, host=None, token=None, client_id=None, client_secret=None):
        self.host = host
        self.token = token
        self.client_id = client_id
        self.client_secret = client_secret

@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    for var in [
        "DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
        "DATABRICKS_WORKSPACE_ID", "DATABRICKS_ACCOUNT_ID", "DATABRICKS_CONFIG_PROFILE"
    ]:
        monkeypatch.delenv(var, raising=False)

@pytest.fixture(autouse=True)
def patch_sdk_config(monkeypatch):
    monkeypatch.setattr("databricks.sdk.core.Config", MockDatabricksConfig)
    yield

def test_service_principal(monkeypatch):
    """Test service principal config (client_id + client_secret)."""
    set_config_for_test(
        databricks_host="https://test.cloud.databricks.com",
        databricks_client_id="client-id",
        databricks_client_secret="client-secret",
        databricks_token=None,
    )
    auth = DatabricksAuthentication()
    config = auth.get_databricks_config()
    assert config.client_id == "client-id"
    assert config.client_secret == "client-secret"
    assert config.host == "https://test.cloud.databricks.com"
    assert getattr(config, "token", None) is None

def test_manual_token(monkeypatch):
    """Test manual token config (host + token)."""
    set_config_for_test(
        databricks_host="https://test.cloud.databricks.com",
        databricks_token="token-123",
        databricks_client_id=None,
        databricks_client_secret=None,
    )
    auth = DatabricksAuthentication()
    config = auth.get_databricks_config()
    assert getattr(config, "token", None) == "token-123"
    assert config.host == "https://test.cloud.databricks.com"
    assert getattr(config, "client_id", None) is None
    assert getattr(config, "client_secret", None) is None

def test_cli(monkeypatch):
    """Test CLI config (host only)."""
    set_config_for_test(
        databricks_host="https://test.cloud.databricks.com",
        databricks_token=None,
        databricks_client_id=None,
        databricks_client_secret=None,
    )
    auth = DatabricksAuthentication()
    config = auth.get_databricks_config()
    assert config.host == "https://test.cloud.databricks.com"
    assert getattr(config, "token", None) is None
    assert getattr(config, "client_id", None) is None
    assert getattr(config, "client_secret", None) is None

def test_missing_config(monkeypatch):
    """Test error if no config is available."""
    set_config_for_test(
        databricks_host=None,
        databricks_token=None,
        databricks_client_id=None,
        databricks_client_secret=None,
    )
    auth = DatabricksAuthentication()
    with pytest.raises(RuntimeError):
        _ = auth.get_databricks_config()