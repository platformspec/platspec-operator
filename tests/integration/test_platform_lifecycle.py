"""Integration tests for the full Platform reconciliation lifecycle.

These tests apply real Kubernetes resources and wait for the operator to act.
They require:
  - CRDs installed: make crds-install
  - Operator running: make run-dev (in another terminal)
  - Cluster access: kubectl must be configured

Each test is isolated with a unique name suffix (run_id) so they can run
concurrently without colliding.
"""

import json
import time
from typing import Optional

import pytest

from .conftest import (
    _NAMESPACE,
    apply_cr,
    delete_cr,
    get_platform_phase,
    wait_for,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _platform(name: str) -> dict:
    return {
        "apiVersion": "core.platformspec.io/v1alpha1",
        "kind": "Platform",
        "metadata": {"name": name, "namespace": _NAMESPACE},
        "spec": {
            "organization": "Test",
            "description": "Integration test platform",
            "deletionPolicy": "Delete",
            "resourceSelector": {
                "matchLabels": {"platformspec.io/test-platform": name}
            },
            "capabilities": ["namespace-bootstrap"],
            "requirements": {
                "general": {"cloudProvider": "none"},
            },
        },
    }


def _environment(name: str, platform_name: str) -> dict:
    return {
        "apiVersion": "core.platformspec.io/v1alpha1",
        "kind": "Environment",
        "metadata": {
            "name": name,
            "namespace": _NAMESPACE,
            "labels": {
                "platformspec.io/test-platform": platform_name,
                "environment": "development",
            },
        },
        "spec": {
            "type": "local",
            "providerRefs": [],
        },
    }


def _binding(name: str, platform_name: str) -> dict:
    return {
        "apiVersion": "core.platformspec.io/v1alpha1",
        "kind": "BlueprintBinding",
        "metadata": {
            "name": name,
            "namespace": _NAMESPACE,
            "labels": {
                "platformspec.io/test-platform": platform_name,
            },
        },
        "spec": {
            "platformRef": {"name": platform_name},
            "deletionPolicy": "Delete",
            "selectors": {
                "environmentSelector": {
                    "matchLabels": {"environment": "development"}
                }
            },
            "blueprintMappings": [
                {
                    "capability": "namespace-bootstrap",
                    "blueprint": {
                        "name": "namespace-bootstrap",
                        "version": "0.1.0",
                        "config": {"replicas": 1, "image": "nginx:latest"},
                    },
                }
            ],
        },
    }


def _wait_for_generated_resources(custom_api: object, binding_name: str, timeout: int = 60) -> list:
    """Wait for the generated-resources annotation to be set and return its parsed value."""
    def _annotation_set():
        obj = custom_api.get_namespaced_custom_object(  # type: ignore[attr-defined]
            group="core.platformspec.io", version="v1alpha1",
            namespace=_NAMESPACE, plural="blueprintbindings", name=binding_name,
        )
        raw = obj.get("metadata", {}).get("annotations", {}).get("platspec.io/generated-resources")
        if raw:
            return json.loads(raw)
        return None

    return wait_for(_annotation_set, timeout=timeout, description=f"generated-resources annotation on {binding_name}")


def _cleanup_platform_namespaces(core_api: object, platform_name: str) -> None:
    """Delete all namespaces labelled with the given platform name."""
    try:
        ns_list = core_api.list_namespace(  # type: ignore[attr-defined]
            label_selector=f"platspec.io/platform={platform_name}"
        )
        for ns in ns_list.items:
            try:
                core_api.delete_namespace(ns.metadata.name)  # type: ignore[attr-defined]
            except Exception:
                pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_platform_reaches_ready(custom_api, run_id):
    """A Platform with a namespace-bootstrap binding should reach Ready phase."""
    platform_name = f"inttest-{run_id}"
    env_name = f"inttest-env-{run_id}"
    binding_name = f"inttest-binding-{run_id}"

    try:
        apply_cr(custom_api, "environments", _environment(env_name, platform_name))
        apply_cr(custom_api, "blueprintbindings", _binding(binding_name, platform_name))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        phase = wait_for(
            lambda: get_platform_phase(custom_api, platform_name)
            if get_platform_phase(custom_api, platform_name) in ("Ready", "Failed")
            else None,
            timeout=120,
            description=f"Platform {platform_name} to reach Ready or Failed",
        )

        assert phase == "Ready", f"Expected Ready, got {phase}"

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", binding_name)
        delete_cr(custom_api, "environments", env_name)


@pytest.mark.integration
def test_platform_creates_namespace(custom_api, core_api, run_id):
    """The namespace-bootstrap blueprint should create a Namespace in the cluster."""
    platform_name = f"inttest-ns-{run_id}"
    env_name = f"inttest-ns-env-{run_id}"
    binding_name = f"inttest-ns-binding-{run_id}"

    try:
        apply_cr(custom_api, "environments", _environment(env_name, platform_name))
        apply_cr(custom_api, "blueprintbindings", _binding(binding_name, platform_name))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        wait_for(
            lambda: get_platform_phase(custom_api, platform_name) in ("Ready", "Failed"),
            timeout=120,
            description=f"Platform {platform_name} to settle",
        )

        # Discover the actual namespace name from the binding annotation rather
        # than guessing it — the name depends on the blueprint's naming logic.
        generated = _wait_for_generated_resources(custom_api, binding_name)
        ns_refs = [r for r in generated if r["kind"] == "Namespace"]
        assert len(ns_refs) >= 1, f"Expected at least one Namespace in generated resources, got: {generated}"

        ns_name = ns_refs[0]["name"]
        ns = core_api.read_namespace(ns_name)
        assert ns.metadata.name == ns_name

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", binding_name)
        delete_cr(custom_api, "environments", env_name)
        _cleanup_platform_namespaces(core_api, platform_name)


@pytest.mark.integration
def test_platform_creates_deployment(custom_api, core_api, apps_api, run_id):
    """The namespace-bootstrap blueprint should create a Deployment in the generated namespace."""
    platform_name = f"inttest-dep-{run_id}"
    env_name = f"inttest-dep-env-{run_id}"
    binding_name = f"inttest-dep-binding-{run_id}"

    try:
        apply_cr(custom_api, "environments", _environment(env_name, platform_name))
        apply_cr(custom_api, "blueprintbindings", _binding(binding_name, platform_name))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        wait_for(
            lambda: get_platform_phase(custom_api, platform_name) in ("Ready", "Failed"),
            timeout=120,
            description=f"Platform {platform_name} to settle",
        )

        # Find the generated namespace from the annotation
        generated = _wait_for_generated_resources(custom_api, binding_name)
        ns_refs = [r for r in generated if r["kind"] == "Namespace"]
        assert len(ns_refs) >= 1, f"No Namespace in generated resources: {generated}"
        target_ns = ns_refs[0]["name"]

        def _deploy_exists():
            try:
                apps_api.read_namespaced_deployment("echo", target_ns)
                return True
            except Exception:
                return False

        wait_for(_deploy_exists, timeout=30, description=f"echo Deployment in {target_ns}")

        deploy = apps_api.read_namespaced_deployment("echo", target_ns)
        assert deploy.spec.replicas == 1
        assert deploy.spec.template.spec.containers[0].image == "nginx:latest"

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", binding_name)
        delete_cr(custom_api, "environments", env_name)
        _cleanup_platform_namespaces(core_api, platform_name)


@pytest.mark.integration
def test_platform_deletion_cleans_up_namespace(custom_api, core_api, run_id):
    """Deleting a Platform with deletionPolicy=Delete should remove generated resources."""
    platform_name = f"inttest-del-{run_id}"
    env_name = f"inttest-del-env-{run_id}"
    binding_name = f"inttest-del-binding-{run_id}"

    try:
        apply_cr(custom_api, "environments", _environment(env_name, platform_name))
        apply_cr(custom_api, "blueprintbindings", _binding(binding_name, platform_name))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        wait_for(
            lambda: get_platform_phase(custom_api, platform_name) in ("Ready", "Failed"),
            timeout=120,
            description=f"Platform {platform_name} to settle before deletion",
        )

        # Discover the actual namespace name before we delete anything
        generated = _wait_for_generated_resources(custom_api, binding_name)
        ns_refs = [r for r in generated if r["kind"] == "Namespace"]
        assert len(ns_refs) >= 1, f"No Namespace in generated resources: {generated}"
        ns_name = ns_refs[0]["name"]

        # Confirm namespace exists
        core_api.read_namespace(ns_name)

        # Delete the Platform — finalizer should clean up generated resources
        delete_cr(custom_api, "platforms", platform_name)

        def _ns_gone():
            try:
                ns = core_api.read_namespace(ns_name)
                return ns.status.phase == "Terminating"
            except Exception:
                return True  # 404 = already gone

        # Cascade: Platform finalizer → delete BlueprintBinding → binding finalizer → delete Namespace.
        # Each hop can take up to one kopf event cycle, so allow generous time.
        wait_for(_ns_gone, timeout=180, description=f"Namespace {ns_name} to be removed after Platform deletion")

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", binding_name)
        delete_cr(custom_api, "environments", env_name)
        _cleanup_platform_namespaces(core_api, platform_name)


@pytest.mark.integration
def test_platform_progressing_without_environment(custom_api, run_id):
    """A Platform with no matching Environment should sit in Progressing, not crash."""
    platform_name = f"inttest-prog-{run_id}"
    binding_name = f"inttest-prog-binding-{run_id}"

    try:
        # No Environment resource — binding selector won't match anything
        apply_cr(custom_api, "blueprintbindings", _binding(binding_name, platform_name))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        # Give the operator a couple of reconcile cycles
        time.sleep(10)

        phase = get_platform_phase(custom_api, platform_name)
        # Should be Progressing (no bindings matched) or Ready (vacuously, if
        # no bindings ran) — must NOT be Failed or absent
        assert phase in ("Progressing", "Ready"), f"Unexpected phase: {phase}"

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", binding_name)


@pytest.mark.integration
def test_binding_annotation_tracks_generated_resources(custom_api, core_api, run_id):
    """BlueprintBinding should have platspec.io/generated-resources annotation after apply."""
    platform_name = f"inttest-ann-{run_id}"
    env_name = f"inttest-ann-env-{run_id}"
    binding_name = f"inttest-ann-binding-{run_id}"

    try:
        apply_cr(custom_api, "environments", _environment(env_name, platform_name))
        apply_cr(custom_api, "blueprintbindings", _binding(binding_name, platform_name))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        wait_for(
            lambda: get_platform_phase(custom_api, platform_name) in ("Ready", "Failed"),
            timeout=120,
            description=f"Platform {platform_name} to settle",
        )

        resources = _wait_for_generated_resources(custom_api, binding_name)
        assert len(resources) > 0, "Expected at least one generated resource"
        kinds = {r["kind"] for r in resources}
        assert "Namespace" in kinds, f"Expected Namespace in generated resources, got {kinds}"

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", binding_name)
        delete_cr(custom_api, "environments", env_name)
        _cleanup_platform_namespaces(core_api, platform_name)
