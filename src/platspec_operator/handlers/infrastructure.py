"""Infrastructure resource handlers — fan-in to Platform reconciliation.

Watches all five infrastructure resource kinds (Environment, Provider, Network, Cluster,
Credential) and relays any change into a Platform reconciliation. This module is the
other half of the fan-in pattern: binding.py handles BlueprintBinding changes; this
module handles infra resource changes.

Why watch infra resources at all?
  The Platform's BlueprintContext includes the spec AND status of every infrastructure
  resource. External Operators writes provisioned values (accountId, vpcId, endpoint
  URLs, ARNs) into the status of these resources after provisioning completes. When that
  happens, the Platform must re-reconcile so blueprints receive the updated values and
  status expressions can be re-evaluated.

  Without these watchers, a Platform would only reconcile when its own spec changed, and
  it would never learn about newly provisioned infra.

How the trigger works:
  Same mechanism as binding.py — patch platspec.io/reconcile-trigger on the Platform
  to cause kopf to fire the on.update handler for it. See binding.py for rationale.

Why the factory pattern (_make_handler)?
  kopf requires each decorated function to be a distinct object at module level. We could
  write five near-identical functions, but a factory keeps the intent clear: all five
  kinds use the same logic, just registered for different resource types.

  The module-level assignments (credentials_changed = _make_handler("credentials") etc.)
  ensure kopf sees the handler functions as top-level names, which is required for
  its internal handler registry to work correctly.

Note on kind names:
  kopf handler decorators use plural lowercase kind names ("environments", "clusters"),
  which is how kopf matches the CRD's plural form. This is different from the DynamicClient
  in discovery.py and trigger helpers, which uses singular PascalCase ("Environment", "Platform").
"""

from datetime import datetime, timezone
from typing import Any, Dict

import kopf
from loguru import logger

_CORE_GROUP = "core.platformspec.io"
_BUILD_GROUP = "build.platformspec.io"
_VERSION = "v1alpha1"
_PLATFORM_LABEL = "platform.platformspec.io/name"


def _trigger_platform(labels: Dict[str, str], namespace: str, k8s: Any) -> None:
    """Look up the owning Platform by label and bump its reconcile-trigger annotation.

    Every infrastructure resource that belongs to a Platform carries the label
    platform.platformspec.io/name=<platform-name>. This function reads that label
    and patches the named Platform to enqueue a reconciliation.

    Failures are warnings — a missing Platform label or a temporary API error should
    not crash the handler or prevent other events from being processed.
    """
    platform_name = labels.get(_PLATFORM_LABEL)
    if not platform_name or k8s is None:
        return
    try:
        api = k8s.resources.get(
            api_version=f"{_CORE_GROUP}/{_VERSION}", kind="Platform"
        )
        platform = api.get(name=platform_name, namespace=namespace or None)
        # Touch the annotation to make kopf see a change on the Platform resource.
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
    except Exception as e:
        logger.warning(f"Could not trigger Platform {platform_name}: {e}")


# Register handlers for each infrastructure kind dynamically.
# kopf requires the decorated functions to be defined at module level so we
# use a factory approach with distinct function names to avoid conflicts.

def _make_handler(group: str, kind: str):  # type: ignore[return]
    """Factory that produces a kopf event handler for one infrastructure resource kind.

    All infra handlers share identical logic — log the change, read the platform
    label, and call _trigger_platform. The factory avoids copy-pasting that logic
    while still producing distinct function objects that kopf can register
    independently.

    The returned function is assigned to a module-level name below so kopf's
    introspection finds it at module level.
    """
    @kopf.on.create(group, _VERSION, kind)
    @kopf.on.update(group, _VERSION, kind)
    async def _handler(
        labels: Dict[str, str],
        meta: Dict[str, Any],
        memo: kopf.Memo,
        **kwargs: Any,
    ) -> None:
        ns = meta.get("namespace", "")
        resource_ref = f"{kind}/{ns}/{meta['name']}" if ns else f"{kind}/{meta['name']}"
        with logger.contextualize(resource=resource_ref):
            logger.debug(
                f"{kind}/{meta['name']} changed — "
                f"platform={labels.get(_PLATFORM_LABEL, 'unset')}"
            )
            _trigger_platform(labels, ns, memo.get("k8s"))

    return _handler


# core.platformspec.io handlers
credentials_changed = _make_handler(_CORE_GROUP, "credentials")
providers_changed = _make_handler(_CORE_GROUP, "providers")
environments_changed = _make_handler(_CORE_GROUP, "environments")
networks_changed = _make_handler(_CORE_GROUP, "networks")
clusters_changed = _make_handler(_CORE_GROUP, "clusters")

# build.platformspec.io handlers
images_changed = _make_handler(_BUILD_GROUP, "images")
nodes_changed = _make_handler(_BUILD_GROUP, "nodes")
software_groups_changed = _make_handler(_BUILD_GROUP, "softwaregroups")
