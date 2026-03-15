"""Credential resolution — fetches actual values from all configured sources.

This module answers: "what are the actual credential values a blueprint needs?"

Credential resources in the Platform Specification do NOT embed secret values — they carry
only a reference to the source (a Kubernetes Secret, an env var, a file, AWS Secrets Manager,
or Vault). This module resolves those references into actual key-value pairs and injects them
into BlueprintContext.credentials[*].data, so blueprints can read e.g.:

    context.credentials[0].data.AWS_ACCESS_KEY_ID

without ever implementing their own secret-fetching logic. Blueprints are pure functions.

The `fields` mapping on each Credential translates logical names (what the blueprint sees)
to source keys (what the source uses). For example:
    fields: {AWS_ACCESS_KEY_ID: access-key-id}
means: read the key "access-key-id" from the source and expose it as "AWS_ACCESS_KEY_ID".
If fields is empty, all keys from the source pass through unchanged.

Sources and their resolver functions:
  - kubernetes-secret   → _resolve_kubernetes_secret (base64-decoded Secret.data)
  - configmap           → _resolve_configmap (ConfigMap.data, plain string values)
  - env                 → _resolve_env (operator pod environment variables)
  - file                → _resolve_file (YAML/JSON file mounted into the pod)
  - aws-secrets-manager → _resolve_aws_secrets_manager (boto3, ambient IRSA / instance profile)
  - vault               → _resolve_vault (hvac, kubernetes or token auth)

Security note: resolved values are held only in memory for the duration of the KCL
execution. They are never written to Kubernetes, logged, or persisted anywhere.
"""

import base64
import json
import os
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from ..models.blueprint import BlueprintContext
from ..models.infrastructure import CredentialSpec


class SecretNotFoundError(Exception):
    pass


def _apply_fields(raw: dict[str, str], fields: dict[str, str]) -> dict[str, str]:
    """Translate source keys to logical names using the fields mapping.

    raw: all key-value pairs read from the source (e.g. Secret.data after base64 decode)
    fields: mapping of logical-name → source-key from the Credential spec

    If fields is empty, all raw pairs pass through unchanged (useful for sources that
    already use the names the blueprint expects). If fields is specified, only the
    mapped keys are returned — anything not in fields is dropped.
    """
    if not fields:
        return raw
    return {
        logical: raw[source_key]
        for logical, source_key in fields.items()
        if source_key in raw
    }


def _resolve_kubernetes_secret(spec: CredentialSpec, k8s_client: Any) -> dict[str, str]:
    """Read from a Kubernetes Secret. Secret.data values are base64-encoded by k8s.

    Secret name resolution (first match wins):
      1. spec.secret_ref.name  — explicit secretRef object
      2. spec.location         — generic location field (scoped to the Credential's namespace)
    """
    if spec.secret_ref:
        secret_name = spec.secret_ref.name
        secret_ns = spec.secret_ref.namespace or spec.namespace or "default"
    elif spec.location:
        secret_name = spec.location
        secret_ns = spec.namespace or "default"
    else:
        raise SecretNotFoundError(
            "source=kubernetes-secret but neither secretRef nor location is specified"
        )
    try:
        api = k8s_client.resources.get(api_version="v1", kind="Secret")
        secret = api.get(name=secret_name, namespace=secret_ns)
        # Kubernetes stores Secret.data as base64. Decode each value to a plain string.
        raw = {
            k: base64.b64decode(v).decode()
            for k, v in (secret.data or {}).items()
        }
    except Exception as e:
        raise SecretNotFoundError(f"Secret {secret_name!r}: {e}") from e
    return _apply_fields(raw, spec.fields)


def _resolve_configmap(spec: CredentialSpec, k8s_client: Any) -> dict[str, str]:
    """Read from a Kubernetes ConfigMap. ConfigMap.data values are plain strings (not base64)."""
    if not spec.config_map_ref:
        raise SecretNotFoundError("source=configmap but no configMapRef specified")
    try:
        api = k8s_client.resources.get(api_version="v1", kind="ConfigMap")
        cm = api.get(
            name=spec.config_map_ref.name,
            namespace=spec.config_map_ref.namespace or "default",
        )
        raw = dict(cm.data or {})
    except Exception as e:
        raise SecretNotFoundError(
            f"ConfigMap {spec.config_map_ref.name!r}: {e}"
        ) from e
    return _apply_fields(raw, spec.fields)


def _resolve_env(spec: CredentialSpec) -> dict[str, str]:
    """Read from the operator pod's environment variables.

    fields maps logical-name → env-var-name. This source requires fields to be specified
    because the environment namespace is unbounded — without fields we wouldn't know which
    env vars to read. Missing env vars are logged as warnings, not errors, to allow
    partial configurations (e.g. optional credentials).
    """
    if not spec.fields:
        raise SecretNotFoundError(
            "source=env requires a fields mapping (logical-name → env-var-name)"
        )
    result: dict[str, str] = {}
    for logical, env_var in spec.fields.items():
        val = os.environ.get(env_var)
        if val is not None:
            result[logical] = val
        else:
            logger.warning(
                f"Credential env var {env_var!r} not set in operator environment"
            )
    return result


def _resolve_file(spec: CredentialSpec) -> dict[str, str]:
    """Read from a YAML or JSON file mounted into the operator pod.

    The file must contain a top-level dict (object). Lists and scalars are rejected.
    Typical use: a service account JSON file, or a YAML credentials file mounted via
    a Kubernetes Secret volume.
    """
    if not spec.file_path:
        raise SecretNotFoundError("source=file but no filePath specified")
    path = Path(spec.file_path)
    if not path.exists():
        raise SecretNotFoundError(f"Credential file not found: {spec.file_path}")
    try:
        content = path.read_text()
        raw = yaml.safe_load(content) if content.strip() else {}
        if not isinstance(raw, dict):
            raise SecretNotFoundError(
                f"Credential file {spec.file_path} must contain a YAML/JSON object"
            )
    except SecretNotFoundError:
        raise
    except Exception as e:
        raise SecretNotFoundError(
            f"Failed to read credential file {spec.file_path}: {e}"
        ) from e
    return _apply_fields(raw, spec.fields)


def _resolve_aws_secrets_manager(spec: CredentialSpec) -> dict[str, str]:
    """Fetch from AWS Secrets Manager using ambient credentials (IRSA / instance profile).

    The operator pod must have IAM permissions to call GetSecretValue on the specified
    secret. On EKS, this is typically via IRSA (IAM Roles for Service Accounts).

    awsSecretsManager config keys:
      - secretId (required): the secret name or ARN
      - region (optional): defaults to AWS_REGION env var, then us-east-1
      - versionId (optional): pin to a specific version

    The secret value is expected to be a JSON object. If it's a plain string, it's
    wrapped as {"value": "<string>"} so _apply_fields can still process it.
    """
    if not spec.aws_secrets_manager:
        raise SecretNotFoundError(
            "source=aws-secrets-manager but no awsSecretsManager config specified"
        )
    try:
        import boto3  # type: ignore[import]
    except ImportError as e:
        raise SecretNotFoundError(
            "source=aws-secrets-manager requires boto3: uv add boto3"
        ) from e

    cfg = spec.aws_secrets_manager
    secret_id = cfg.get("secretId")
    region = cfg.get("region") or os.environ.get("AWS_REGION", "us-east-1")
    version_id = cfg.get("versionId")
    if not secret_id:
        raise SecretNotFoundError("awsSecretsManager.secretId is required")

    try:
        client = boto3.client("secretsmanager", region_name=region)
        kwargs: dict[str, Any] = {"SecretId": secret_id}
        if version_id:
            kwargs["VersionId"] = version_id
        response = client.get_secret_value(**kwargs)
        # Secrets Manager returns either SecretString (text) or SecretBinary (bytes).
        raw_str = response.get("SecretString") or base64.b64decode(
            response.get("SecretBinary", b"")
        ).decode()
        raw = json.loads(raw_str) if raw_str.strip().startswith("{") else {"value": raw_str}
    except Exception as e:
        raise SecretNotFoundError(f"AWS Secrets Manager {secret_id!r}: {e}") from e

    return _apply_fields(raw, spec.fields)


def _resolve_vault(spec: CredentialSpec) -> dict[str, str]:
    """Fetch from HashiCorp Vault using Kubernetes or token auth.

    vault config keys:
      - address (required): Vault server URL
      - path (required): secret path (e.g. secret/data/aws-creds for KV v2)
      - authMethod: "kubernetes" (default) or "token"
      - role: Kubernetes auth role name (required for kubernetes auth)
      - namespace: Vault namespace for enterprise Vault
      - kvVersion: 1 or 2 (default 2)
      - jwtPath: path to the service account JWT (default: standard SA token path)
      - token: Vault token (used only when authMethod=token)
    """
    if not spec.vault:
        raise SecretNotFoundError("source=vault but no vault config specified")
    try:
        import hvac  # type: ignore[import]
    except ImportError as e:
        raise SecretNotFoundError(
            "source=vault requires hvac: uv add hvac"
        ) from e

    cfg = spec.vault
    address = cfg.get("address")
    path = cfg.get("path")
    auth_method = cfg.get("authMethod", "kubernetes")
    role = cfg.get("role", "")
    vault_namespace = cfg.get("namespace")
    kv_version = cfg.get("kvVersion", 2)

    if not address or not path:
        raise SecretNotFoundError("vault.address and vault.path are required")

    try:
        client = hvac.Client(url=address, namespace=vault_namespace)
        if auth_method == "kubernetes":
            # Read the pod's service account JWT and use it to authenticate with Vault.
            # The JWT is mounted at the standard path by the kubelet.
            jwt_path = cfg.get(
                "jwtPath",
                "/var/run/secrets/kubernetes.io/serviceaccount/token",
            )
            jwt = Path(jwt_path).read_text().strip()
            client.auth.kubernetes.login(role=role, jwt=jwt)
        elif auth_method == "token":
            # Direct token auth — token is either in config or the VAULT_TOKEN env var.
            client.token = cfg.get("token") or os.environ.get("VAULT_TOKEN", "")
        else:
            raise SecretNotFoundError(
                f"Unsupported vault authMethod {auth_method!r}. "
                "Supported: kubernetes, token"
            )

        # KV v2 wraps the secret data under an extra "data" key; KV v1 does not.
        if kv_version == 2:
            secret = client.secrets.kv.v2.read_secret_version(path=path)
            raw = secret["data"]["data"]
        else:
            secret = client.secrets.kv.v1.read_secret(path=path)
            raw = secret["data"]
    except SecretNotFoundError:
        raise
    except Exception as e:
        raise SecretNotFoundError(f"Vault {path!r}: {e}") from e

    return _apply_fields(raw, spec.fields)


# Dispatch table: source name → resolver function.
# New sources can be added here without changing resolve_secrets().
_RESOLVERS = {
    "kubernetes-secret": _resolve_kubernetes_secret,
    "configmap": _resolve_configmap,
    "env": _resolve_env,
    "file": _resolve_file,
    "aws-secrets-manager": _resolve_aws_secrets_manager,
    "vault": _resolve_vault,
}

_SUPPORTED = ", ".join(_RESOLVERS)


def resolve_secrets(context: BlueprintContext, k8s_client: Any) -> BlueprintContext:
    """Resolve all credential sources and return a new BlueprintContext with data populated.

    Iterates over context.credentials. For each credential, dispatches to the appropriate
    resolver based on credential.source. Returns a new BlueprintContext (immutable update
    via model_copy) with each credential's .data dict populated.

    Raises SecretNotFoundError if any credential cannot be resolved. The calling handler
    will catch this, set a Rendered=False condition on the binding, and requeue.

    The k8s_client is only passed to resolvers that need it (kubernetes-secret, configmap).
    The other resolvers use ambient credentials (IRSA, env vars, mounted files).
    """
    resolved = []
    for cred in context.credentials:
        source = cred.source
        logger.debug(
            f"Resolving credential provider={cred.provider!r} source={source!r}"
        )
        resolver = _RESOLVERS.get(source)
        if resolver is None:
            raise SecretNotFoundError(
                f"Unknown credential source {source!r} for provider {cred.provider!r}. "
                f"Supported: {_SUPPORTED}"
            )
        # k8s-backed resolvers need the client; others are self-contained.
        if source in ("kubernetes-secret", "configmap"):
            data = resolver(cred, k8s_client)  # type: ignore[call-arg]
        else:
            data = resolver(cred)  # type: ignore[call-arg]
        resolved.append(cred.model_copy(update={"data": data}))
    return context.model_copy(update={"credentials": resolved})
