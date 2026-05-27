import os

import sqlalchemy
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


def get_key_vault_secret(secret_name: str) -> str:
    vault_url = os.getenv("KEY_VAULT_URL")
    if vault_url is None:
        raise ValueError("KEY_VAULT_URL env var cannot be None")

    credential = DefaultAzureCredential()
    key_vault_client = SecretClient(vault_url=vault_url, credential=credential)

    try:
        secret_value = key_vault_client.get_secret(secret_name)
        # Check if the value of the secret is none or if the length is less than or equal to one
        if secret_value.value is None or len(secret_value.value) <= 1:
            raise Exception(f"Error: '{secret_name}' value is empty or 1 character")

        return secret_value.value
    except Exception as e:
        raise Exception(f"Error reading secret '{secret_name}'") from e


def create_db_engine() -> sqlalchemy.engine.Engine:
    engine_name = "sql_engine"
    app_name = "?application_name=polling_poc"

    db_url = os.getenv("DATABASE_URL")
    if db_url is None:
        raise ValueError("DATABASE_URL env var cannot be None")

    connection_str = get_key_vault_secret(db_url)
    connection_string = connection_str + app_name

    return sqlalchemy.create_engine(connection_string, pool_size=2, max_overflow=0, isolation_level="READ COMMITTED", logging_name=engine_name)
