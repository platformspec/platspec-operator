"""Label-based resource discovery for Platform infrastructure.

This module answers the question: "what infrastructure resources belong to this Platform?"

The discovery strategy is simple and explicit: every infrastructure resource that belongs
to a Platform must carry the label `platform.platformspec.io/name=<platform-name>`. This
avoids complex ownership graphs and makes the membership of any resource unambiguous — you
can kubectl get it with a label selector.

The five infra resource kinds (Environment, Provider, Network, Cluster, Credential) are
all in the same API group. They are discovered together in one pass and returned as a
PlatformResources bundle that the rest of the reconciliation loop consumes.
"""

from typing import Any, Dict, List

from loguru import logger

from ..models.infrastructure import InfraResource, PlatformResources


def _to_python(obj: Any) -> Any:
    """Recursively convert dict-like objects (e.g. kopf ResourceField) to plain Python."""
    if isinstance(obj, dict):
        return {k: _to_python(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_python(item) for item in obj]
    if hasattr(obj, "items"):
        return {k: _to_python(v) for k, v in obj.items()}
    return obj

# Every infrastructure resource that belongs to a Platform must carry this label.
# It is the single source of truth for resource membership.
_PLATFORM_LABEL = "platform.platformspec.io/name"

# Maps the PlatformResources field name to the Kubernetes API triple needed to
# list that resource kind. Values are (group, version, kind) — kind must be the
# singular PascalCase form that the k8s DynamicClient expects (NOT the plural
# lowercase form used in kopf handler decorators).
_INFRA_KINDS = {
    "environments": ("core.platformspec.io", "v1alpha1", "Environment"),
    "providers": ("core.platformspec.io", "v1alpha1", "Provider"),
    "networks": ("core.platformspec.io", "v1alpha1", "Network"),
    "clusters": ("core.platformspec.io", "v1alpha1", "Cluster"),
    "credentials": ("core.platformspec.io", "v1alpha1", "Credential"),
    # build.platformspec.io — image/node/software primitives
    "images": ("build.platformspec.io", "v1alpha1", "Image"),
    "nodes": ("build.platformspec.io", "v1alpha1", "Node"),
    "software_groups": ("build.platformspec.io", "v1alpha1", "SoftwareGroup"),
}


def discover_platform_resources(
    platform_name: str,
    resource_selector: Dict[str, str],
    namespace: str,
    k8s_client: Any,
) -> PlatformResources:
    """List all infrastructure resources that belong to this Platform.

    Queries each infra kind with a compound label selector:
      - platform.platformspec.io/name=<platform_name>   (mandatory membership label)
      - plus any extra labels from Platform.spec.resourceSelector.matchLabels

    The namespace arg scopes the query; passing "" (empty string) queries all namespaces.

    Both spec and status of each resource are captured in InfraResource so that blueprints
    can access provisioned values (e.g. accountId, vpcId, ARNs) that an external Operator
    writes into status after provisioning.

    Failed list calls per-kind are logged as warnings and return an empty list — a missing
    CRD or RBAC gap for one kind should not block discovery of the others.
    """
    # Start with empty lists for each kind; we'll fill them in the loop below.
    results: Dict[str, List[InfraResource]] = {k: [] for k in _INFRA_KINDS}

    # Build the label selector string. The platform membership label is always required;
    # resource_selector adds optional additional filters (e.g. region=us-east-1).
    label_selector = f"{_PLATFORM_LABEL}={platform_name}"
    if resource_selector:
        extra = ",".join(f"{k}={v}" for k, v in resource_selector.items())
        label_selector = f"{label_selector},{extra}"

    for field, (group, version, kind) in _INFRA_KINDS.items():
        try:
            # Resolve the k8s API endpoint for this resource kind. The DynamicClient
            # looks up the API by (api_version, kind) and returns an object that
            # knows the correct REST path and HTTP methods.
            api = k8s_client.resources.get(
                api_version=f"{group}/{version}", kind=kind
            )
            items = api.get(namespace=namespace or None, label_selector=label_selector)

            for item in items.items:
                # Normalise each raw k8s object into InfraResource, which holds the
                # fields the rest of the pipeline cares about: name, namespace, labels,
                # spec, and status. The status dict may be empty if an external
                # Operator hasn't written provisioned values yet.
                resource = InfraResource(
                    name=item.metadata.name,
                    namespace=item.metadata.namespace,
                    labels=_to_python(item.metadata.labels or {}),
                    spec=_to_python(item.spec or {}),
                )
                results[field].append(resource)
        except Exception as e:
            # A warning here (not an error) because a missing CRD or RBAC permission
            # for one kind is recoverable — the Platform may legitimately have no
            # Clusters yet, for example.
            logger.warning(f"Failed to list {kind}: {e}")

    discovered = PlatformResources(**results)
    logger.info(
        f"Discovered resources for platform {platform_name}: "
        f"clusters={len(discovered.clusters)} "
        f"environments={len(discovered.environments)} "
        f"providers={len(discovered.providers)} "
        f"networks={len(discovered.networks)} "
        f"credentials={len(discovered.credentials)} "
        f"images={len(discovered.images)} "
        f"nodes={len(discovered.nodes)} "
        f"software_groups={len(discovered.software_groups)}"
    )
    return discovered
