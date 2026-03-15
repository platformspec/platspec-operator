"""BlueprintBinding change handlers — trigger Platform reconciliation and deletion cleanup.

This module has two responsibilities:

1. RELAY changes: when a BlueprintBinding is created or updated, prod the owning Platform
   into re-reconciling so the new/changed config is picked up immediately.

2. FINALIZER cleanup: when a BlueprintBinding is deleted, clean up every resource it
   created, according to the configured deletionPolicy.

Why not reconcile BlueprintBinding directly?
  The Platform is the reconciliation root. A Platform's status reflects the combined
  state of all its bindings. If we reconciled each binding independently, aggregating
  their results back to Platform.status would require a separate coordination step.
  Instead, all state lives in one reconcile loop in platform.py, and binding/infra
  handlers just enqueue it.

How triggering works:
  We patch a dedicated annotation (platspec.io/reconcile-trigger) with the current
  timestamp. kopf sees the metadata change and fires the on.update handler for the
  Platform. This is idempotent and leaves a human-readable audit trail.

Deletion policy:
  BlueprintBinding.spec.deletionPolicy overrides Platform.spec.deletionPolicy, which
  overrides the operator default ("Delete").

  Delete  — explicitly delete every resource in status.generatedResources.
  Orphan  — remove platspec.io/* labels from those resources but leave them running.

  Individual resource errors during cleanup are logged as warnings but do NOT prevent
  the finalizer from completing — a stuck resource should not permanently block a
  binding from being deleted. The user can clean up manually.
"""

import json
from typing import Any, Dict, List

import kopf
from kubernetes.client.exceptions import ApiException
from loguru import logger

_GROUP = "core.platformspec.io"
_VERSION = "v1alpha1"

# Labels applied by the applier that we remove during Orphan cleanup.
_MANAGED_LABELS = [
    "platspec.io/managed-by",
    "platspec.io/binding",
    "platspec.io/platform",
    "platspec.io/capability",
]


def _enqueue_platform(
    platform_name: str,
    namespace: str,
    k8s: Any,
) -> None:
    """Force a Platform reconcile by bumping its platspec.io/reconcile-trigger annotation.

    Patching an annotation causes kopf to fire the on.update handler for the Platform,
    which runs the full reconciliation loop. The timestamp value makes each trigger
    unique so kopf always sees it as a change (not a no-op).

    Failures here are warnings — if the Platform is already gone (e.g. Platform deletion
    triggered this binding's deletion) the Platform no longer needs reconciling.
    """
    if k8s is None or not platform_name:
        return
    try:
        from datetime import datetime, timezone

        api = k8s.resources.get(
            api_version=f"{_GROUP}/{_VERSION}", kind="Platform"
        )
        platform = api.get(name=platform_name, namespace=namespace or None)
        annotations = dict(platform.metadata.annotations or {})
        annotations["platspec.io/reconcile-trigger"] = (
            datetime.now(timezone.utc).isoformat()
        )
        api.patch(
            name=platform_name,
            namespace=namespace or None,
            body={"metadata": {"annotations": annotations}},
            content_type="application/merge-patch+json",
        )
        logger.debug(f"Triggered reconcile for Platform {platform_name}")
    except ApiException as e:
        if e.status == 404:
            # Platform is already gone — this is expected when a Platform deletion
            # cascades to its bindings. Nothing to reconcile, nothing to worry about.
            logger.debug(f"Platform {platform_name} already deleted, skipping trigger")
        else:
            logger.warning(f"Could not trigger Platform {platform_name} reconcile: {e}")
    except Exception as e:
        logger.warning(f"Could not trigger Platform {platform_name} reconcile: {e}")


def _get_platform_deletion_policy(
    platform_name: str,
    namespace: str,
    k8s: Any,
) -> str:
    """Fetch the deletionPolicy from the owning Platform's spec.

    Returns "Delete" if the Platform cannot be found or has no deletionPolicy set.
    This is the safe default — better to clean up than to silently leave orphans.
    """
    if not platform_name or k8s is None:
        return "Delete"
    try:
        api = k8s.resources.get(api_version=f"{_GROUP}/{_VERSION}", kind="Platform")
        platform = api.get(name=platform_name, namespace=namespace or None)
        return platform.spec.get("deletionPolicy") or "Delete"
    except Exception:
        return "Delete"


def _delete_resources(generated: List[Dict[str, Any]], k8s: Any) -> None:
    """Delete each resource in the generatedResources list.

    Errors per resource are logged as warnings and do not abort the loop — we want
    to attempt cleanup of all resources even if some fail (e.g. already deleted,
    RBAC gap, or the resource kind was removed from the cluster).
    """
    for ref in generated:
        api_version = ref.get("apiVersion", "")
        kind = ref.get("kind", "")
        name = ref.get("name", "")
        namespace = ref.get("namespace")
        try:
            res_api = k8s.resources.get(api_version=api_version, kind=kind)
            res_api.delete(name=name, namespace=namespace)
            logger.debug(f"Deleted {kind}/{name}")
        except Exception as e:
            logger.warning(f"Could not delete {kind}/{name}: {e}")


def _orphan_resources(generated: List[Dict[str, Any]], k8s: Any) -> None:
    """Remove platspec.io/* labels from each resource, leaving them running.

    Uses JSON merge patch with null values to remove specific label keys without
    affecting other labels on the resource. No GET is required — merge patch with
    null removes the key if it exists and is a no-op if it doesn't.
    """
    # Build the patch body once — same keys for every resource.
    labels_patch = {label: None for label in _MANAGED_LABELS}

    for ref in generated:
        api_version = ref.get("apiVersion", "")
        kind = ref.get("kind", "")
        name = ref.get("name", "")
        namespace = ref.get("namespace")
        try:
            res_api = k8s.resources.get(api_version=api_version, kind=kind)
            res_api.patch(
                name=name,
                namespace=namespace,
                body={"metadata": {"labels": labels_patch}},
                content_type="application/merge-patch+json",
            )
            logger.debug(f"Orphaned {kind}/{name}")
        except Exception as e:
            logger.warning(f"Could not orphan {kind}/{name}: {e}")


@kopf.on.create(_GROUP, _VERSION, "blueprintbindings")
@kopf.on.update(_GROUP, _VERSION, "blueprintbindings")
async def binding_changed(
    spec: Dict[str, Any],
    meta: Dict[str, Any],
    memo: kopf.Memo,
    **kwargs: Any,
) -> None:
    """Relay a BlueprintBinding create/update into a Platform reconciliation.

    Called whenever a BlueprintBinding is created or any of its fields change.
    Reads spec.platformRef.name to find the owning Platform and enqueues it
    for reconciliation so the new/changed binding config is picked up immediately.
    """
    name = meta["name"]
    namespace = meta.get("namespace", "")
    platform_name = spec.get("platformRef", {}).get("name", "")
    resource_ref = f"BlueprintBinding/{namespace}/{name}" if namespace else f"BlueprintBinding/{name}"
    with logger.contextualize(resource=resource_ref):
        logger.info(
            f"BlueprintBinding {namespace}/{name} changed → "
            f"triggering Platform {platform_name}"
        )
        _enqueue_platform(platform_name, namespace, memo.get("k8s"))


@kopf.on.delete(_GROUP, _VERSION, "blueprintbindings")
async def binding_delete(
    spec: Dict[str, Any],
    meta: Dict[str, Any],
    status: Dict[str, Any],
    memo: kopf.Memo,
    **kwargs: Any,
) -> None:
    """Finalizer handler: clean up generated resources before the binding is deleted.

    kopf adds the operator's finalizer (platspec.io/finalizer) to every BlueprintBinding
    when it is first observed. When the binding is deleted, kopf calls this handler and
    only removes the finalizer (allowing the delete to complete) after this handler
    returns without raising.

    Cleanup logic:
      1. Determine the effective deletionPolicy:
           binding spec  →  platform spec  →  "Delete"
      2. Read status.generatedResources — the list written by the reconciler after
         each successful apply. This is the authoritative record of what was created.
      3. If Delete: delete each resource via the k8s API.
         If Orphan: remove platspec.io/* labels so the resources are no longer
         associated with this binding, then leave them running.
      4. Trigger Platform status re-evaluation (no-op if Platform is already gone).

    Individual resource errors are logged as warnings and do not prevent completion —
    a stuck resource must not permanently block binding deletion.
    """
    k8s = memo.get("k8s")
    name = meta["name"]
    namespace = meta.get("namespace", "")
    platform_name = spec.get("platformRef", {}).get("name", "")
    resource_ref = f"BlueprintBinding/{namespace}/{name}" if namespace else f"BlueprintBinding/{name}"

    with logger.contextualize(resource=resource_ref):
        # Determine effective policy: binding → platform → default.
        binding_policy = spec.get("deletionPolicy")
        if binding_policy:
            policy = binding_policy
        else:
            policy = _get_platform_deletion_policy(platform_name, namespace, k8s)

        # Read generatedResources from the platspec.io/generated-resources annotation.
        # The reconciler writes this annotation (not status.generatedResources) because
        # status is a subresource — writes via the main endpoint are silently ignored.
        # Annotations are always visible here, even when the Platform is already gone.
        raw_annotation = meta.get("annotations", {}).get("platspec.io/generated-resources", "[]")
        try:
            generated: List[Dict[str, Any]] = json.loads(raw_annotation)
        except (json.JSONDecodeError, TypeError):
            generated = []

        logger.info(
            f"BlueprintBinding {namespace}/{name} deleted — "
            f"policy={policy} resources={len(generated)}"
        )

        if k8s is not None and generated:
            if policy == "Orphan":
                _orphan_resources(generated, k8s)
            else:
                _delete_resources(generated, k8s)

        # Let the Platform know its capability list has shrunk. Best-effort —
        # the Platform may already be gone if this was triggered by platform_delete.
        _enqueue_platform(platform_name, namespace, k8s)
