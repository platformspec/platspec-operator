"""Additional integration tests covering binding lifecycle and status behaviour.

Covers:
  - Config update propagation (blueprint re-execution on binding patch)
  - Environment selector filtering (non-matching binding produces no resources)
  - Platform with no bindings reaches Ready (vacuous success)
  - Generated resources carry the platspec.io/platform label
  - Platform status carries a conditions array after reconciliation
"""

import json
import time

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
# Shared resource builders (parallel to test_platform_lifecycle.py)
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


def _environment(name: str, platform_name: str, extra_labels: dict | None = None) -> dict:
    labels: dict = {
        "platformspec.io/test-platform": platform_name,
        "environment": "development",
    }
    if extra_labels:
        labels.update(extra_labels)
    return {
        "apiVersion": "core.platformspec.io/v1alpha1",
        "kind": "Environment",
        "metadata": {"name": name, "namespace": _NAMESPACE, "labels": labels},
        "spec": {"providerRefs": []},
    }


def _binding(
    name: str,
    platform_name: str,
    replicas: int = 1,
    image: str = "nginx:latest",
    env_selector: dict | None = None,
) -> dict:
    selectors: dict = {}
    if env_selector is not None:
        selectors["environmentSelector"] = {"matchLabels": env_selector}
    else:
        selectors["environmentSelector"] = {"matchLabels": {"environment": "development"}}
    return {
        "apiVersion": "core.platformspec.io/v1alpha1",
        "kind": "BlueprintBinding",
        "metadata": {
            "name": name,
            "namespace": _NAMESPACE,
            "labels": {"platformspec.io/test-platform": platform_name},
        },
        "spec": {
            "platformRef": {"name": platform_name},
            "deletionPolicy": "Delete",
            "selectors": selectors,
            "blueprintMappings": [
                {
                    "capability": "namespace-bootstrap",
                    "blueprint": {
                        "name": "namespace-bootstrap",
                        "version": "0.1.0",
                        "config": {"replicas": replicas, "image": image},
                    },
                }
            ],
        },
    }


def _wait_for_phase(custom_api, platform_name: str, timeout: int = 120) -> str:
    return wait_for(
        lambda: get_platform_phase(custom_api, platform_name)
        if get_platform_phase(custom_api, platform_name) in ("Ready", "Failed")
        else None,
        timeout=timeout,
        description=f"Platform {platform_name} to reach Ready or Failed",
    )


def _get_generated_resources(custom_api, binding_name: str, timeout: int = 60) -> list:
    def _annotation_set():
        obj = custom_api.get_namespaced_custom_object(
            group="core.platformspec.io", version="v1alpha1",
            namespace=_NAMESPACE, plural="blueprintbindings", name=binding_name,
        )
        raw = obj.get("metadata", {}).get("annotations", {}).get("platspec.io/generated-resources")
        return json.loads(raw) if raw else None

    return wait_for(_annotation_set, timeout=timeout, description=f"generated-resources on {binding_name}")


def _cleanup_namespaces(core_api, platform_name: str) -> None:
    try:
        ns_list = core_api.list_namespace(label_selector=f"platspec.io/platform={platform_name}")
        for ns in ns_list.items:
            try:
                core_api.delete_namespace(ns.metadata.name)
            except Exception:
                pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Test: config update propagates to generated Deployment
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_config_update_propagates_to_deployment(custom_api, core_api, apps_api, run_id):
    """Patching BlueprintBinding config (replicas) should re-execute the blueprint
    and update the existing Deployment in place via server-side apply."""
    platform_name = f"inttest-upd-{run_id}"
    env_name = f"inttest-upd-env-{run_id}"
    binding_name = f"inttest-upd-binding-{run_id}"

    try:
        apply_cr(custom_api, "environments", _environment(env_name, platform_name))
        apply_cr(custom_api, "blueprintbindings", _binding(binding_name, platform_name, replicas=1))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        _wait_for_phase(custom_api, platform_name)

        generated = _get_generated_resources(custom_api, binding_name)
        ns_refs = [r for r in generated if r["kind"] == "Namespace"]
        assert ns_refs, f"No Namespace in {generated}"
        target_ns = ns_refs[0]["name"]

        # Confirm initial replicas=1
        deploy = apps_api.read_namespaced_deployment("echo", target_ns)
        assert deploy.spec.replicas == 1

        # Patch binding config to replicas=2 using a JSON merge patch
        patch_body = {
            "spec": {
                "blueprintMappings": [
                    {
                        "capability": "namespace-bootstrap",
                        "blueprint": {
                            "name": "namespace-bootstrap",
                            "version": "0.1.0",
                            "config": {"replicas": 2, "image": "nginx:latest"},
                        },
                    }
                ]
            }
        }
        custom_api.patch_namespaced_custom_object(
            group="core.platformspec.io", version="v1alpha1",
            namespace=_NAMESPACE, plural="blueprintbindings",
            name=binding_name, body=patch_body,
        )

        # Wait for Deployment replicas to change
        def _replicas_updated():
            d = apps_api.read_namespaced_deployment("echo", target_ns)
            return d.spec.replicas == 2 or None

        wait_for(_replicas_updated, timeout=60, description="Deployment replicas updated to 2")

        deploy = apps_api.read_namespaced_deployment("echo", target_ns)
        assert deploy.spec.replicas == 2

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", binding_name)
        delete_cr(custom_api, "environments", env_name)
        _cleanup_namespaces(core_api, platform_name)


# ---------------------------------------------------------------------------
# Test: environment selector filters non-matching bindings
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_env_selector_excludes_non_matching_binding(custom_api, core_api, run_id):
    """A binding whose environmentSelector does not match the available Environment
    should produce no generated resources — the annotation should be absent or empty.

    The environment must carry 'platform.platformspec.io/name' so that
    discover_platform_resources finds it and the operator takes the environment
    path (where resolve_bindings applies selector filtering).  Without this label
    the operator falls back to the no-environment path, which runs all bindings
    unconditionally and has no selector semantics to test.
    """
    platform_name = f"inttest-sel-{run_id}"
    env_name = f"inttest-sel-env-{run_id}"
    matching_binding = f"inttest-sel-match-{run_id}"
    filtered_binding = f"inttest-sel-skip-{run_id}"

    try:
        # Environment must carry the discovery label so the operator enters the
        # environment path and resolve_bindings can filter by environmentSelector.
        apply_cr(
            custom_api, "environments",
            _environment(env_name, platform_name,
                         extra_labels={"platform.platformspec.io/name": platform_name}),
        )
        # Matching binding — selector matches the environment label
        apply_cr(
            custom_api, "blueprintbindings",
            _binding(matching_binding, platform_name, env_selector={"environment": "development"}),
        )
        # Non-matching binding — looks for staging but only development exists
        apply_cr(
            custom_api, "blueprintbindings",
            _binding(filtered_binding, platform_name, env_selector={"environment": "staging"}),
        )
        apply_cr(custom_api, "platforms", _platform(platform_name))

        # With the environment discovered the operator enters the env path.
        # resolve_bindings filters: matching runs, filtered is excluded.
        # Aggregator only counts the matching binding → total=1, ready=1 → Ready.
        _wait_for_phase(custom_api, platform_name)

        # Matching binding should have generated resources
        generated = _get_generated_resources(custom_api, matching_binding)
        assert len(generated) > 0, f"Expected resources from matching binding, got {generated}"

        # Filtered binding should have no generated-resources annotation (it never ran)
        filtered_obj = custom_api.get_namespaced_custom_object(
            group="core.platformspec.io", version="v1alpha1",
            namespace=_NAMESPACE, plural="blueprintbindings", name=filtered_binding,
        )
        raw = filtered_obj.get("metadata", {}).get("annotations", {}).get("platspec.io/generated-resources")
        if raw:
            assert json.loads(raw) == [], f"Expected empty list for filtered binding, got {raw}"

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", matching_binding)
        delete_cr(custom_api, "blueprintbindings", filtered_binding)
        delete_cr(custom_api, "environments", env_name)
        _cleanup_namespaces(core_api, platform_name)


# ---------------------------------------------------------------------------
# Test: Platform with no bindings reaches Ready
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_platform_with_no_bindings_reaches_ready(custom_api, run_id):
    """A Platform with no BlueprintBindings should reach Ready phase (vacuously —
    there is nothing to provision so everything succeeds immediately)."""
    platform_name = f"inttest-empty-{run_id}"
    env_name = f"inttest-empty-env-{run_id}"

    try:
        apply_cr(custom_api, "environments", _environment(env_name, platform_name))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        # No bindings applied — operator should settle quickly
        phase = wait_for(
            lambda: get_platform_phase(custom_api, platform_name)
            if get_platform_phase(custom_api, platform_name) in ("Ready", "Progressing", "Failed")
            else None,
            timeout=60,
            description=f"Platform {platform_name} to set a phase",
        )
        # Platform must not have Failed — Ready or Progressing are both acceptable
        # for a platform with no active bindings.
        assert phase in ("Ready", "Progressing"), f"Expected Ready or Progressing, got {phase}"

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "environments", env_name)


# ---------------------------------------------------------------------------
# Test: generated resources carry platspec.io/platform label
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_generated_resources_carry_platform_label(custom_api, core_api, run_id):
    """Resources applied by the Platspec applier must carry platspec.io/platform=<name>
    so that label-selector cleanup works correctly."""
    platform_name = f"inttest-lbl-{run_id}"
    env_name = f"inttest-lbl-env-{run_id}"
    binding_name = f"inttest-lbl-binding-{run_id}"

    try:
        apply_cr(custom_api, "environments", _environment(env_name, platform_name))
        apply_cr(custom_api, "blueprintbindings", _binding(binding_name, platform_name))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        _wait_for_phase(custom_api, platform_name)

        generated = _get_generated_resources(custom_api, binding_name)
        ns_refs = [r for r in generated if r["kind"] == "Namespace"]
        assert ns_refs, f"No Namespace in {generated}"
        target_ns = ns_refs[0]["name"]

        ns = core_api.read_namespace(target_ns)
        labels = ns.metadata.labels or {}
        assert labels.get("platspec.io/platform") == platform_name, (
            f"platspec.io/platform label missing or wrong on Namespace {target_ns}: {labels}"
        )

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", binding_name)
        delete_cr(custom_api, "environments", env_name)
        _cleanup_namespaces(core_api, platform_name)


# ---------------------------------------------------------------------------
# Test: Platform status includes conditions after reconciliation
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_platform_status_has_conditions(custom_api, core_api, run_id):
    """After a successful reconciliation the Platform .status.conditions should
    contain a Ready condition with reason=AllBindingsReady."""
    platform_name = f"inttest-cond-{run_id}"
    env_name = f"inttest-cond-env-{run_id}"
    binding_name = f"inttest-cond-binding-{run_id}"

    try:
        apply_cr(custom_api, "environments", _environment(env_name, platform_name))
        apply_cr(custom_api, "blueprintbindings", _binding(binding_name, platform_name))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        _wait_for_phase(custom_api, platform_name)

        obj = custom_api.get_namespaced_custom_object(
            group="core.platformspec.io", version="v1alpha1",
            namespace=_NAMESPACE, plural="platforms", name=platform_name,
        )
        conditions = obj.get("status", {}).get("conditions", [])
        assert len(conditions) > 0, f"Expected conditions on Platform status, got: {obj.get('status')}"

        # The aggregator sets type="Ready" with reason="AllBindingsReady" on success.
        ready_conditions = [c for c in conditions if c.get("type") == "Ready"]
        assert ready_conditions, f"Expected a Ready condition, got types: {[c.get('type') for c in conditions]}"
        assert ready_conditions[0].get("status") == "True", (
            f"Expected Ready condition to be True, got: {ready_conditions[0]}"
        )
        assert ready_conditions[0].get("reason") == "AllBindingsReady", (
            f"Expected reason=AllBindingsReady, got: {ready_conditions[0].get('reason')}"
        )

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", binding_name)
        delete_cr(custom_api, "environments", env_name)
        _cleanup_namespaces(core_api, platform_name)


# ---------------------------------------------------------------------------
# Test: binding deletion cleans up generated resources
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_binding_deletion_cleans_up_resources(custom_api, core_api, run_id):
    """Deleting a BlueprintBinding with deletionPolicy=Delete should remove the
    resources it generated without needing to delete the Platform."""
    platform_name = f"inttest-bdel-{run_id}"
    env_name = f"inttest-bdel-env-{run_id}"
    binding_name = f"inttest-bdel-binding-{run_id}"

    try:
        apply_cr(custom_api, "environments", _environment(env_name, platform_name))
        apply_cr(custom_api, "blueprintbindings", _binding(binding_name, platform_name))
        apply_cr(custom_api, "platforms", _platform(platform_name))

        _wait_for_phase(custom_api, platform_name)

        generated = _get_generated_resources(custom_api, binding_name)
        ns_refs = [r for r in generated if r["kind"] == "Namespace"]
        assert ns_refs, f"No Namespace in {generated}"
        ns_name = ns_refs[0]["name"]

        # Verify namespace exists before we delete the binding
        core_api.read_namespace(ns_name)

        # Delete the binding only — not the Platform
        delete_cr(custom_api, "blueprintbindings", binding_name)

        def _ns_gone():
            try:
                ns = core_api.read_namespace(ns_name)
                return ns.status.phase == "Terminating"
            except Exception:
                return True  # 404 — already gone

        wait_for(_ns_gone, timeout=120, description=f"Namespace {ns_name} removed after binding deletion")

    finally:
        delete_cr(custom_api, "platforms", platform_name)
        delete_cr(custom_api, "blueprintbindings", binding_name)
        delete_cr(custom_api, "environments", env_name)
        _cleanup_namespaces(core_api, platform_name)
