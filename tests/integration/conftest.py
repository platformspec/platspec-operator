"""Shared fixtures and helpers for integration tests.

Integration tests require a live Kubernetes cluster with:
  - Platspec Operator CRDs installed (make crds-install)
  - A running Platspec Operator instance (make run-dev or in-cluster)
  - A kubeconfig pointing at the cluster (default: ~/.kube/config)

Run with:  make test-integration
Skip with: make test-unit  (never requires a cluster)

Each test gets a unique namespace prefix to avoid cross-test pollution.
Fixtures clean up all resources they create, even on failure.
"""

import time
import uuid
from pathlib import Path
from typing import Any, Dict, Generator, List

import pytest
import yaml

try:
    from kubernetes import client, config as k8s_config
    from kubernetes.client.rest import ApiException

    _k8s_available = True
except ImportError:
    _k8s_available = False

_GROUP = "core.platformspec.io"
_VERSION = "v1alpha1"
_NAMESPACE = "platspec-system"

# Absolute path to the blueprints directory — used to configure the operator's
# blueprint_dir so it can find local blueprints during integration tests.
REPO_ROOT = Path(__file__).parent.parent.parent.parent.parent
BLUEPRINTS_DIR = REPO_ROOT / "blueprints"


def pytest_configure(config: Any) -> None:
    config.addinivalue_line("markers", "integration: mark test as requiring a live k8s cluster")


# ---------------------------------------------------------------------------
# k8s client
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def k8s() -> Any:
    """Load kubeconfig and return a kubernetes.client.ApiClient.

    Skips the entire session if no cluster is reachable.
    """
    if not _k8s_available:
        pytest.skip("kubernetes package not installed")

    try:
        k8s_config.load_kube_config()
    except Exception:
        try:
            k8s_config.load_incluster_config()
        except Exception:
            pytest.skip("No kubeconfig or in-cluster config available")

    api_client = client.ApiClient()
    # Quick liveness check
    try:
        client.CoreV1Api(api_client).list_namespace(limit=1)
    except Exception as e:
        pytest.skip(f"Cluster not reachable: {e}")

    return api_client


@pytest.fixture(scope="session")
def custom_api(k8s: Any) -> Any:
    return client.CustomObjectsApi(k8s)


@pytest.fixture(scope="session")
def core_api(k8s: Any) -> Any:
    return client.CoreV1Api(k8s)


@pytest.fixture(scope="session")
def apps_api(k8s: Any) -> Any:
    return client.AppsV1Api(k8s)


# ---------------------------------------------------------------------------
# Unique test run ID (avoids resource name collisions between parallel runs)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def run_id() -> str:
    return uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# Resource apply / delete helpers
# ---------------------------------------------------------------------------


def apply_cr(
    custom_api: Any,
    plural: str,
    body: Dict[str, Any],
    namespace: str = _NAMESPACE,
) -> Dict[str, Any]:
    """Create or replace a custom resource."""
    name = body["metadata"]["name"]
    try:
        return custom_api.create_namespaced_custom_object(
            group=_GROUP, version=_VERSION, namespace=namespace, plural=plural, body=body
        )
    except ApiException as e:
        if e.status == 409:  # Already exists — replace
            return custom_api.replace_namespaced_custom_object(
                group=_GROUP, version=_VERSION, namespace=namespace,
                plural=plural, name=name, body=body,
            )
        raise


def delete_cr(
    custom_api: Any,
    plural: str,
    name: str,
    namespace: str = _NAMESPACE,
    ignore_not_found: bool = True,
) -> None:
    try:
        custom_api.delete_namespaced_custom_object(
            group=_GROUP, version=_VERSION, namespace=namespace,
            plural=plural, name=name,
        )
    except ApiException as e:
        if e.status == 404 and ignore_not_found:
            return
        raise


def wait_for(
    condition: Any,
    timeout: int = 120,
    interval: float = 2.0,
    description: str = "condition",
) -> Any:
    """Poll `condition()` until it returns a truthy value or timeout expires.

    `condition` is a zero-argument callable. Returns the truthy value.
    Raises TimeoutError on timeout.
    """
    deadline = time.time() + timeout
    last_exc = None
    while time.time() < deadline:
        try:
            result = condition()
            if result:
                return result
        except Exception as e:
            last_exc = e
        time.sleep(interval)
    msg = f"Timed out waiting for {description} after {timeout}s"
    if last_exc:
        msg += f" (last error: {last_exc})"
    raise TimeoutError(msg)


def get_platform_phase(custom_api: Any, name: str, namespace: str = _NAMESPACE) -> str | None:
    try:
        obj = custom_api.get_namespaced_custom_object(
            group=_GROUP, version=_VERSION, namespace=namespace,
            plural="platforms", name=name,
        )
        return obj.get("status", {}).get("phase")
    except ApiException as e:
        if e.status == 404:
            return None
        raise
