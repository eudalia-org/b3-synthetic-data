"""
OCI Vault secrets retrieval with Instance Principal authentication.

Falls back to environment variables for local development.
"""

import base64
import os
from functools import lru_cache

VAULT_OCID = "ocid1.vault.oc1.sa-saopaulo-1.ffuve2daaaclo.abtxeljrknhhhohvxnnof6hkzimtlpyqbp5oobivm6keewxpnpjpw7hhjvba"

# Map secret names to environment variable names for local fallback
_ENV_VAR_MAP = {
    "datagen-source-db-password": "DATAGEN_SOURCE_DB_PASSWORD",
    "datagen-target-db-password": "DATAGEN_TARGET_DB_PASSWORD",
    "datagen-openai-key": "DATAGEN_OPENAI_KEY",
}


@lru_cache
def _get_secrets_client():
    """Create OCI Secrets client with Instance Principal auth."""
    import oci

    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    return oci.secrets.SecretsClient(config={}, signer=signer)


@lru_cache
def _get_vault_client():
    """Create OCI Vault client with Instance Principal auth."""
    import oci

    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    return oci.vault.VaultsClient(config={}, signer=signer)


@lru_cache
def _get_secret_ocid(secret_name: str) -> str:
    """Get secret OCID by name from the vault."""
    import oci

    vault_client = _get_vault_client()

    # Get vault details to find compartment
    vault = vault_client.get_vault(VAULT_OCID).data
    compartment_id = vault.compartment_id

    # List secrets and find by name
    secrets_client = oci.secrets.SecretsClient(
        config={},
        signer=oci.auth.signers.InstancePrincipalsSecurityTokenSigner(),
    )
    vaults_client = oci.vault.VaultsClient(
        config={},
        signer=oci.auth.signers.InstancePrincipalsSecurityTokenSigner(),
    )

    # Use KMS Vault Management to list secrets
    kms_vault_client = oci.key_management.KmsVaultClient(
        config={},
        signer=oci.auth.signers.InstancePrincipalsSecurityTokenSigner(),
    )

    # Get the management endpoint from vault
    vault_management_client = oci.vault.VaultsClient(
        config={},
        signer=oci.auth.signers.InstancePrincipalsSecurityTokenSigner(),
    )

    # List secrets in compartment
    list_secrets_response = vault_management_client.list_secrets(
        compartment_id=compartment_id,
        vault_id=VAULT_OCID,
        name=secret_name,
    )

    secrets = list_secrets_response.data
    if not secrets:
        raise ValueError(f"Secret '{secret_name}' not found in vault")

    return secrets[0].id


@lru_cache
def get_secret(secret_name: str) -> str:
    """
    Retrieve a secret value from OCI Vault.

    Uses Instance Principal authentication when running on OCI.
    Falls back to environment variables for local development.

    Args:
        secret_name: Name of the secret in OCI Vault

    Returns:
        The secret value as a string

    Raises:
        ValueError: If secret not found in vault or environment
    """
    # Try environment variable first (for local development)
    env_var = _ENV_VAR_MAP.get(secret_name, secret_name.upper().replace("-", "_"))
    env_value = os.environ.get(env_var)
    if env_value:
        return env_value

    # Try OCI Vault with Instance Principal
    try:
        secret_ocid = _get_secret_ocid(secret_name)
        secrets_client = _get_secrets_client()

        response = secrets_client.get_secret_bundle(secret_id=secret_ocid)
        secret_bundle = response.data

        # Decode base64 content
        content = secret_bundle.secret_bundle_content.content
        return base64.b64decode(content).decode("utf-8")

    except Exception as e:
        # If OCI fails, provide helpful error message
        raise ValueError(
            f"Could not retrieve secret '{secret_name}'. "
            f"Set environment variable {env_var} for local development, "
            f"or ensure Instance Principal is configured on OCI. Error: {e}"
        ) from e
