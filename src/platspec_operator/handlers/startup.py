"""Operator startup hook — initializes shared Kubernetes client.

This module runs once when the operator process starts, before any event handlers
fire. Its only job is to create a DynamicClient and store it in kopf's `memo` dict,
which is the operator's in-process shared state.

Why DynamicClient instead of the generated typed clients?
  The typed clients in the kubernetes library are generated from a fixed OpenAPI spec
  and don't know about custom resource definitions (CRDs). DynamicClient resolves
  resource types at runtime by querying the cluster's API discovery endpoint, so it
  works with any CRD regardless of when it was registered.

Why memo?
  kopf passes `memo` into every handler call. It is the intended mechanism for sharing
  state between handlers without globals. Storing the k8s client here means every
  handler gets access via `memo["k8s"]` without creating a new connection per event.

Config loading tries in-cluster config first (the normal path when running inside a
pod), then falls back to kubeconfig (local development). This matches the standard
pattern used by nearly every Python operator.
"""

import tempfile
from pathlib import Path

import kopf
from kubernetes import client, config
from kubernetes.dynamic import DynamicClient
from loguru import logger

from ..config import Config


@kopf.on.startup()
async def startup(memo: kopf.Memo, settings: kopf.OperatorSettings, **_: object) -> None:
    """Initialize the Kubernetes dynamic client and configure kopf operator settings.

    Called once at operator startup. Tries in-cluster config first (running as a pod),
    then falls back to the local kubeconfig file (development / testing).

    Also wires operator config into kopf:
      - settings.persistence.finalizer: the finalizer name kopf adds to watched resources
        before calling on.delete handlers. Must match what CRDs expect. Defaults to
        "platspec.io/finalizer" (configured in operator.finalizer_name).

    After this runs, all event handlers can retrieve the client via memo["k8s"].
    """
    cfg = Config.load()

    # Tell kopf which finalizer string to add to resources that have deletion handlers.
    # kopf adds this finalizer on first observation and removes it after the on.delete
    # handler completes. We use our own name rather than kopf's default so it's clear
    # which operator owns it.
    settings.persistence.finalizer = cfg.operator.finalizer_name

    try:
        # In-cluster: reads the service account token and CA cert mounted into every pod.
        config.load_incluster_config()
        logger.info("Loaded in-cluster Kubernetes config")
    except config.ConfigException:
        # Local development: reads ~/.kube/config (or KUBECONFIG env var).
        config.load_kube_config()
        logger.info("Loaded kubeconfig from local file")

    # DynamicClient wraps ApiClient and discovers CRD resource endpoints at runtime.
    # Stored in memo so all handler modules share a single connection pool.
    memo["k8s"] = DynamicClient(client.ApiClient())
    logger.info("Kubernetes dynamic client initialised")

    # Registry map: populated live by handlers/registry.py as BlueprintRegistry
    # resources are created, updated, or deleted. Pre-initialised here so the
    # BlueprintFetcher always has a valid dict even before any registries exist.
    memo["registries"] = {}

    # Blueprint cache directory: remote blueprints are written here after fetching
    # and reused across reconciles. Scoped to this process run — cleaned up by the
    # OS when the operator pod is replaced.
    cache_dir = Path(tempfile.gettempdir()) / "platspec-blueprints"
    cache_dir.mkdir(parents=True, exist_ok=True)
    memo["blueprint_cache_dir"] = cache_dir
    logger.info(f"Blueprint cache directory: {cache_dir}")
