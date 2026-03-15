"""Server-side apply for blueprint output resources.

This module answers: "how do we create or update the Kubernetes resources a blueprint produced?"

It annotates each manifest with ownership metadata (ownerReference, labels), then applies
it to the cluster using the Kubernetes server-side apply mechanism. Server-side apply lets
multiple actors manage different fields of the same resource — the operator owns spec fields
via the "platspec-operator" field manager, and an External Operator owns status fields via
its own field manager. They do not conflict.

The key constraint here is cross-namespace ownership. Kubernetes ownerReferences are
namespace-scoped: the garbage collector looks for the owner in the same namespace as the
owned resource. If the BlueprintBinding lives in platspec-system but creates a Deployment
in smoke-test-development, setting an ownerReference would cause the GC to immediately
delete that Deployment (it cannot find the owner in smoke-test-development). For this
reason, ownerReferences are only set when the resource is in the same namespace as the
binding, or is cluster-scoped (has no namespace at all).

Cross-namespace resources are tracked instead via BlueprintBinding.status.generatedResources
and cleaned up explicitly by the binding deletion handler.
"""

import copy
import json
from typing import Any, Dict, List

from loguru import logger

from ..models.crd import ResourceReference


def apply_output_resources(
    manifests: List[Dict[str, Any]],
    owner_binding_name: str,
    owner_binding_uid: str,
    owner_binding_api_version: str,
    owner_binding_namespace: str,
    platform_name: str,
    capability: str,
    field_manager: str,
    k8s_client: Any,
) -> List[ResourceReference]:
    """Annotate and apply each manifest from a blueprint execution.

    For each manifest:
      1. Conditionally add an ownerReference pointing to the BlueprintBinding.
         This is skipped for cross-namespace resources — see module docstring.
      2. Add platspec.io/* labels so the resources are discoverable and attributable.
      3. Apply via the k8s dynamic client using content-type merge-patch, which is
         what kopf's server_side_apply expects.

    Returns a list of ResourceReference objects (apiVersion, kind, name, namespace)
    representing everything that was successfully applied. The caller stores this list
    in BlueprintBinding.status.generatedResources for cleanup on deletion.

    Raises on any apply failure — a partial apply is worse than no apply because it
    leaves the cluster in an inconsistent state. The calling handler will catch the
    exception, set a Failed condition, and requeue.
    """
    applied: List[ResourceReference] = []

    for manifest in manifests:
        # Work on a deep copy so we don't mutate the original manifest dict (which may
        # be reused if the caller retries or logs it). A shallow dict() copy is not
        # enough because nested dicts like metadata would still be shared.
        manifest = copy.deepcopy(manifest)

        metadata = manifest.setdefault("metadata", {})

        # --- ownerReference ---
        # Only set if the resource is in the same namespace as the binding, or has
        # no namespace (cluster-scoped). Cross-namespace ownerRefs are invalid and
        # cause the GC to delete the owned resource.
        resource_ns = metadata.get("namespace")
        if resource_ns is None or resource_ns == owner_binding_namespace:
            owner_refs = metadata.setdefault("ownerReferences", [])
            owner_refs.append(
                {
                    "apiVersion": owner_binding_api_version,
                    "kind": "BlueprintBinding",
                    "name": owner_binding_name,
                    "uid": owner_binding_uid,
                    "controller": True,
                    "blockOwnerDeletion": True,
                }
            )

        # --- labels ---
        # Always applied regardless of namespace. These allow querying all resources
        # managed by a given platform, binding, or capability across namespaces.
        labels = metadata.setdefault("labels", {})
        labels.update(
            {
                "platspec.io/managed-by": "platspec-operator",
                "platspec.io/binding": owner_binding_name,
                "platspec.io/platform": platform_name,
                "platspec.io/capability": capability,
            }
        )

        api_version = manifest.get("apiVersion", "")
        kind = manifest.get("kind", "")
        name = metadata.get("name", "")
        namespace = metadata.get("namespace")

        try:
            # Resolve the k8s API endpoint for this resource kind, then apply.
            # server_side_apply uses HTTP PATCH with content-type: application/apply-patch+yaml
            # under the hood, allowing multiple field managers to coexist on the same resource.
            resource_api = k8s_client.resources.get(
                api_version=api_version, kind=kind
            )
            resource_api.server_side_apply(
                body=manifest,
                field_manager=field_manager,
                namespace=namespace,
            )
            ref = ResourceReference(
                apiVersion=api_version,
                kind=kind,
                name=name,
                namespace=namespace,
            )
            applied.append(ref)
            logger.debug(f"Applied {kind}/{name}")
        except Exception as e:
            logger.error(f"Failed to apply {kind}/{name}: {e}")
            raise

    logger.info(
        f"Applied {len(applied)} resources for binding '{owner_binding_name}'"
    )
    return applied
