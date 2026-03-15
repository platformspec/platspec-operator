"""Tests for core/secrets.py — credential resolution."""

import base64
import os
from unittest.mock import MagicMock

import pytest

from platspec_operator.core.secrets import (
    SecretNotFoundError,
    _apply_fields,
    _resolve_env,
    _resolve_file,
    _resolve_kubernetes_secret,
    resolve_secrets,
)
from platspec_operator.models.infrastructure import CredentialSpec, EnvironmentSpec


# ---------------------------------------------------------------------------
# _apply_fields
# ---------------------------------------------------------------------------


def test_apply_fields_empty_fields_passes_through_all():
    raw = {"key-a": "val-a", "key-b": "val-b"}
    assert _apply_fields(raw, {}) == raw


def test_apply_fields_maps_keys():
    raw = {"access-key-id": "AKIA", "secret-access-key": "SECRET"}
    fields = {"AWS_ACCESS_KEY_ID": "access-key-id", "AWS_SECRET_ACCESS_KEY": "secret-access-key"}
    result = _apply_fields(raw, fields)
    assert result == {"AWS_ACCESS_KEY_ID": "AKIA", "AWS_SECRET_ACCESS_KEY": "SECRET"}


def test_apply_fields_missing_source_key_is_skipped():
    raw = {"key-a": "val-a"}
    fields = {"LOGICAL": "key-a", "MISSING": "key-b"}
    result = _apply_fields(raw, fields)
    assert result == {"LOGICAL": "val-a"}
    assert "MISSING" not in result


def test_apply_fields_empty_raw_and_empty_fields():
    assert _apply_fields({}, {}) == {}


# ---------------------------------------------------------------------------
# _resolve_kubernetes_secret
# ---------------------------------------------------------------------------


def _make_k8s_secret_client(data: dict[str, str]) -> MagicMock:
    """Build a minimal mock k8s DynamicClient that returns a Secret with given data."""
    encoded = {k: base64.b64encode(v.encode()).decode() for k, v in data.items()}
    secret = MagicMock()
    secret.data = encoded

    api = MagicMock()
    api.get.return_value = secret

    client = MagicMock()
    client.resources.get.return_value = api
    return client


def test_k8s_secret_via_secret_ref():
    spec = CredentialSpec(
        provider="aws",
        source="kubernetes-secret",
        secret_ref={"name": "my-secret", "namespace": "default"},
    )
    client = _make_k8s_secret_client({"access-key": "AKIA"})
    result = _resolve_kubernetes_secret(spec, client)
    assert result == {"access-key": "AKIA"}
    client.resources.get.assert_called_once_with(api_version="v1", kind="Secret")
    client.resources.get.return_value.get.assert_called_once_with(
        name="my-secret", namespace="default"
    )


def test_k8s_secret_via_location_fallback():
    spec = CredentialSpec(
        provider="aws",
        source="kubernetes-secret",
        location="my-location-secret",
        namespace="platspec-system",
    )
    client = _make_k8s_secret_client({"token": "abc"})
    result = _resolve_kubernetes_secret(spec, client)
    assert result == {"token": "abc"}
    client.resources.get.return_value.get.assert_called_once_with(
        name="my-location-secret", namespace="platspec-system"
    )


def test_k8s_secret_location_defaults_namespace_to_default():
    spec = CredentialSpec(
        provider="aws",
        source="kubernetes-secret",
        location="fallback-secret",
    )
    client = _make_k8s_secret_client({"k": "v"})
    _resolve_kubernetes_secret(spec, client)
    client.resources.get.return_value.get.assert_called_once_with(
        name="fallback-secret", namespace="default"
    )


def test_k8s_secret_neither_ref_nor_location_raises():
    spec = CredentialSpec(provider="aws", source="kubernetes-secret")
    with pytest.raises(SecretNotFoundError, match="neither secretRef nor location"):
        _resolve_kubernetes_secret(spec, MagicMock())


def test_k8s_secret_apply_fields():
    spec = CredentialSpec(
        provider="aws",
        source="kubernetes-secret",
        secret_ref={"name": "s", "namespace": "default"},
        fields={"AWS_KEY": "access-key"},
    )
    client = _make_k8s_secret_client({"access-key": "AKIA", "other": "ignore"})
    result = _resolve_kubernetes_secret(spec, client)
    assert result == {"AWS_KEY": "AKIA"}
    assert "other" not in result


def test_k8s_secret_api_error_raises_secret_not_found():
    spec = CredentialSpec(
        provider="aws",
        source="kubernetes-secret",
        secret_ref={"name": "missing", "namespace": "default"},
    )
    client = MagicMock()
    client.resources.get.return_value.get.side_effect = Exception("not found")
    with pytest.raises(SecretNotFoundError, match="missing"):
        _resolve_kubernetes_secret(spec, client)


# ---------------------------------------------------------------------------
# _resolve_env
# ---------------------------------------------------------------------------


def test_resolve_env_reads_vars(monkeypatch):
    monkeypatch.setenv("MY_KEY", "my-value")
    spec = CredentialSpec(provider="aws", source="env", fields={"MY_KEY": "MY_KEY"})
    result = _resolve_env(spec)
    assert result == {"MY_KEY": "my-value"}


def test_resolve_env_missing_var_is_omitted(monkeypatch):
    monkeypatch.delenv("NONEXISTENT_VAR_XYZ", raising=False)
    spec = CredentialSpec(
        provider="aws",
        source="env",
        fields={"PRESENT": "MY_KEY", "MISSING": "NONEXISTENT_VAR_XYZ"},
    )
    monkeypatch.setenv("MY_KEY", "here")
    result = _resolve_env(spec)
    assert "PRESENT" in result
    assert "MISSING" not in result


def test_resolve_env_requires_fields():
    spec = CredentialSpec(provider="aws", source="env")
    with pytest.raises(SecretNotFoundError, match="fields mapping"):
        _resolve_env(spec)


# ---------------------------------------------------------------------------
# _resolve_file
# ---------------------------------------------------------------------------


def test_resolve_file_yaml(tmp_path):
    cred_file = tmp_path / "creds.yaml"
    cred_file.write_text("key-a: val-a\nkey-b: val-b\n")
    spec = CredentialSpec(provider="gcp", source="file", file_path=str(cred_file))
    result = _resolve_file(spec)
    assert result == {"key-a": "val-a", "key-b": "val-b"}


def test_resolve_file_json(tmp_path):
    cred_file = tmp_path / "creds.json"
    cred_file.write_text('{"tok": "secret123"}')
    spec = CredentialSpec(provider="gcp", source="file", file_path=str(cred_file))
    result = _resolve_file(spec)
    assert result == {"tok": "secret123"}


def test_resolve_file_applies_fields(tmp_path):
    cred_file = tmp_path / "creds.yaml"
    cred_file.write_text("service_account_key: SA_JSON\n")
    spec = CredentialSpec(
        provider="gcp",
        source="file",
        file_path=str(cred_file),
        fields={"GCP_SA_KEY": "service_account_key"},
    )
    result = _resolve_file(spec)
    assert result == {"GCP_SA_KEY": "SA_JSON"}


def test_resolve_file_no_path_raises():
    spec = CredentialSpec(provider="gcp", source="file")
    with pytest.raises(SecretNotFoundError, match="filePath"):
        _resolve_file(spec)


def test_resolve_file_missing_file_raises(tmp_path):
    spec = CredentialSpec(provider="gcp", source="file", file_path=str(tmp_path / "nope.yaml"))
    with pytest.raises(SecretNotFoundError, match="not found"):
        _resolve_file(spec)


def test_resolve_file_non_dict_content_raises(tmp_path):
    cred_file = tmp_path / "bad.yaml"
    cred_file.write_text("- item1\n- item2\n")
    spec = CredentialSpec(provider="gcp", source="file", file_path=str(cred_file))
    with pytest.raises(SecretNotFoundError, match="YAML/JSON object"):
        _resolve_file(spec)


# ---------------------------------------------------------------------------
# resolve_secrets — dispatch and context update
# ---------------------------------------------------------------------------


def _make_context(creds: list):
    from platspec_operator.models.blueprint import BlueprintContext, PlatformMeta

    return BlueprintContext(
        platform=PlatformMeta(name="p", namespace="ns"),
        environment=EnvironmentSpec(),
        providers=[],
        networks=[],
        clusters=[],
        credentials=creds,
        config={},
        overrides={},
    )


def test_resolve_secrets_populates_data(monkeypatch):
    monkeypatch.setenv("TOKEN_VAR", "tok-value")
    cred = CredentialSpec(
        provider="aws",
        source="env",
        fields={"TOKEN": "TOKEN_VAR"},
    )
    ctx = _make_context([cred])
    result = resolve_secrets(ctx, k8s_client=None)
    assert result.credentials[0].data == {"TOKEN": "tok-value"}


def test_resolve_secrets_returns_new_context(monkeypatch):
    monkeypatch.setenv("X_VAR", "x")
    cred = CredentialSpec(provider="aws", source="env", fields={"X": "X_VAR"})
    ctx = _make_context([cred])
    result = resolve_secrets(ctx, k8s_client=None)
    assert result is not ctx
    assert ctx.credentials[0].data == {}  # original unchanged


def test_resolve_secrets_unknown_source_raises():
    cred = CredentialSpec(provider="aws", source="unknown-backend")
    ctx = _make_context([cred])
    with pytest.raises(SecretNotFoundError, match="Unknown credential source"):
        resolve_secrets(ctx, k8s_client=None)


def test_resolve_secrets_kubernetes_secret_passes_client():
    cred = CredentialSpec(
        provider="aws",
        source="kubernetes-secret",
        secret_ref={"name": "s", "namespace": "default"},
    )
    client = _make_k8s_secret_client({"key": "val"})
    ctx = _make_context([cred])
    result = resolve_secrets(ctx, k8s_client=client)
    assert result.credentials[0].data == {"key": "val"}


def test_resolve_secrets_empty_credentials():
    ctx = _make_context([])
    result = resolve_secrets(ctx, k8s_client=None)
    assert result.credentials == []
