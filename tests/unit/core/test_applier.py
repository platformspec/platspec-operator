"""Tests for core/applier.py — server-side apply for blueprint output resources."""

from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

from platspec_operator.core.applier import apply_output_resources


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


BINDING_NAME = "my-binding"
BINDING_UID = "uid-1234"
BINDING_API_VERSION = "core.platformspec.io/v1alpha1"
BINDING_NAMESPACE = "platspec-system"
PLATFORM_NAME = "my-platform"
FIELD_MANAGER = "platspec-operator"


def _k8s_client() -> MagicMock:
    """Return a mock k8s dynamic client that records apply calls."""
    client = MagicMock()
    resource_api = MagicMock()
    client.resources.get.return_value = resource_api
    return client


def _apply(manifests: list, k8s: Any = None, binding_ns: str = BINDING_NAMESPACE):
    if k8s is None:
        k8s = _k8s_client()
    return apply_output_resources(
        manifests=manifests,
        owner_binding_name=BINDING_NAME,
        owner_binding_uid=BINDING_UID,
        owner_binding_api_version=BINDING_API_VERSION,
        owner_binding_namespace=binding_ns,
        platform_name=PLATFORM_NAME,
        capability="networking",
        field_manager=FIELD_MANAGER,
        k8s_client=k8s,
    )


def _namespace_manifest(name: str = "test-ns") -> Dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": name},
    }


def _deployment_manifest(name: str = "test-deploy", namespace: str = "test-ns") -> Dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {},
    }


# ---------------------------------------------------------------------------
# ownerReference injection
# ---------------------------------------------------------------------------


def test_same_namespace_gets_owner_ref():
    """Resource in the same namespace as the binding → ownerReference added."""
    manifest = _deployment_manifest(namespace=BINDING_NAMESPACE)
    k8s = _k8s_client()
    _apply([manifest], k8s)

    applied_body = k8s.resources.get.return_value.server_side_apply.call_args[1]["body"]
    owner_refs = applied_body["metadata"].get("ownerReferences", [])
    assert len(owner_refs) == 1
    assert owner_refs[0]["name"] == BINDING_NAME
    assert owner_refs[0]["kind"] == "BlueprintBinding"
    assert owner_refs[0]["uid"] == BINDING_UID
    assert owner_refs[0]["controller"] is True
    assert owner_refs[0]["blockOwnerDeletion"] is True


def test_cross_namespace_does_not_get_owner_ref():
    """Resource in a different namespace → ownerReference NOT added (cross-ns GC issue)."""
    manifest = _deployment_manifest(namespace="other-namespace")
    k8s = _k8s_client()
    _apply([manifest], k8s)

    applied_body = k8s.resources.get.return_value.server_side_apply.call_args[1]["body"]
    owner_refs = applied_body["metadata"].get("ownerReferences", [])
    assert owner_refs == []


def test_cluster_scoped_resource_gets_owner_ref():
    """Cluster-scoped resource (no namespace) → ownerReference IS added."""
    manifest = _namespace_manifest()  # Namespace has no namespace field
    k8s = _k8s_client()
    _apply([manifest], k8s)

    applied_body = k8s.resources.get.return_value.server_side_apply.call_args[1]["body"]
    owner_refs = applied_body["metadata"].get("ownerReferences", [])
    assert len(owner_refs) == 1


# ---------------------------------------------------------------------------
# Labels injection
# ---------------------------------------------------------------------------


def test_labels_always_added():
    """platspec.io/* labels are added regardless of namespace."""
    manifest = _deployment_manifest(namespace="other-namespace")
    k8s = _k8s_client()
    _apply([manifest], k8s)

    applied_body = k8s.resources.get.return_value.server_side_apply.call_args[1]["body"]
    labels = applied_body["metadata"]["labels"]
    assert labels["platspec.io/managed-by"] == "platspec-operator"
    assert labels["platspec.io/binding"] == BINDING_NAME
    assert labels["platspec.io/platform"] == PLATFORM_NAME
    assert labels["platspec.io/capability"] == "networking"


def test_existing_labels_preserved():
    """Pre-existing labels on the manifest are not removed."""
    manifest = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "ns", "labels": {"app": "existing-label"}},
    }
    k8s = _k8s_client()
    _apply([manifest], k8s)

    applied_body = k8s.resources.get.return_value.server_side_apply.call_args[1]["body"]
    labels = applied_body["metadata"]["labels"]
    assert labels["app"] == "existing-label"
    assert "platspec.io/managed-by" in labels


# ---------------------------------------------------------------------------
# Return value — ResourceReference list
# ---------------------------------------------------------------------------


def test_returns_resource_references():
    manifests = [
        _namespace_manifest("ns-1"),
        _deployment_manifest("deploy-1", "platspec-system"),
    ]
    refs = _apply(manifests)

    assert len(refs) == 2
    kinds = {r.kind for r in refs}
    assert kinds == {"Namespace", "Deployment"}


def test_resource_reference_fields():
    manifest = _deployment_manifest("my-deploy", BINDING_NAMESPACE)
    refs = _apply([manifest])

    ref = refs[0]
    assert ref.api_version == "apps/v1"
    assert ref.kind == "Deployment"
    assert ref.name == "my-deploy"
    assert ref.namespace == BINDING_NAMESPACE


def test_cluster_scoped_reference_has_no_namespace():
    refs = _apply([_namespace_manifest("my-ns")])
    assert refs[0].namespace is None


def test_empty_manifests_returns_empty_list():
    refs = _apply([])
    assert refs == []


# ---------------------------------------------------------------------------
# Apply failure raises
# ---------------------------------------------------------------------------


def test_apply_failure_raises():
    manifest = _namespace_manifest()
    k8s = _k8s_client()
    k8s.resources.get.return_value.server_side_apply.side_effect = RuntimeError("API error")

    with pytest.raises(RuntimeError, match="API error"):
        _apply([manifest], k8s)


def test_apply_failure_does_not_return_partial_list():
    """A multi-manifest apply that fails midway should raise, not return partial results."""
    manifests = [_namespace_manifest("ns-1"), _deployment_manifest("d1", "ns-1")]
    k8s = _k8s_client()

    call_count = 0

    def side_effect(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            raise RuntimeError("second resource fails")

    k8s.resources.get.return_value.server_side_apply.side_effect = side_effect

    with pytest.raises(RuntimeError, match="second resource fails"):
        _apply(manifests, k8s)


# ---------------------------------------------------------------------------
# Original manifest not mutated
# ---------------------------------------------------------------------------


def test_apply_does_not_mutate_original_manifests():
    """apply_output_resources should not modify the caller's manifest dicts."""
    manifest = _namespace_manifest("my-ns")
    original_metadata = dict(manifest["metadata"])
    _apply([manifest])

    assert manifest["metadata"] == original_metadata
