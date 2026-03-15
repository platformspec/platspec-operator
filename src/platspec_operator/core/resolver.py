"""Selector-based BlueprintBinding resolution.

This module answers: "for a given Environment, which blueprints should run?"

A Platform can have many BlueprintBindings, each potentially scoped to different
environments via selector labels. This module filters down to only the bindings that
apply to the current environment, expands each binding's blueprintMappings list into
individual ResolvedBinding objects, and selects a single winner per capability when
multiple bindings compete for the same capability.

The result is a flat list of ResolvedBinding — one per winning capability — that the
executor will run in dependency order.
"""

from typing import Any, Dict, List

from loguru import logger

from ..models.blueprint import ResolvedBinding
from ..models.infrastructure import InfraResource


def _labels_match(selector: Dict[str, str], labels: Dict[str, str]) -> bool:
    """Return True if all selector key=value pairs are present in labels.

    An empty selector matches everything (no constraints). This is consistent with
    how Kubernetes label selectors work: an empty matchLabels map selects all resources.
    """
    return all(labels.get(k) == v for k, v in selector.items())


def resolve_bindings(
    bindings: List[Dict[str, Any]],
    environment: InfraResource,
    clusters: List[InfraResource],
) -> List[ResolvedBinding]:
    """Resolve which blueprints apply to this environment.

    The resolution algorithm:

    1. FILTER — for each BlueprintBinding, check its selectors against the environment
       and cluster labels. A binding passes if ALL specified selectors match:
         - environmentSelector: matched against environment.labels
         - clusterSelector: passes if ANY cluster in the environment matches
         - locationSelector: passes if ANY cluster in the environment matches
       Omitted selectors are treated as "match everything".

    2. EXPAND — each matching binding carries a blueprintMappings list. Each entry in
       that list becomes one ResolvedBinding with its own capability, blueprint name,
       version, and config. A single binding can deliver multiple capabilities.

    3. SELECT — group all ResolvedBindings by capability. When multiple bindings compete
       for the same capability, the one with the lowest precedence number wins (lower
       number = higher priority, defaulting to 100). This lets environment-specific
       bindings override platform-wide defaults without duplicating every other capability.

    Returns one ResolvedBinding per winning capability, ready for dependency ordering
    and KCL execution.
    """
    # Accumulate all candidates grouped by capability before selecting the winner.
    by_capability: Dict[str, List[ResolvedBinding]] = {}

    for b in bindings:
        spec = b.get("spec", {})
        selectors = spec.get("selectors", {})

        # --- environmentSelector check ---
        # matchLabels under spec.selectors.environmentSelector are matched against
        # this environment's labels. Missing selector = unconditional match.
        env_sel = selectors.get("environmentSelector", {}).get("matchLabels", {})
        if env_sel and not _labels_match(env_sel, environment.labels):
            continue  # This binding is not for this environment.

        # --- clusterSelector check ---
        # A binding can require clusters with specific labels (e.g. tier=prod).
        # The selector passes if any cluster in this environment matches it.
        cluster_sel = selectors.get("clusterSelector", {}).get("matchLabels", {})
        if cluster_sel and not any(_labels_match(cluster_sel, c.labels) for c in clusters):
            continue  # No matching cluster found.

        # --- locationSelector check ---
        # Similar to clusterSelector but intended for geographic/region filtering.
        # Also matched against cluster labels since clusters carry region labels.
        location_sel = selectors.get("locationSelector", {}).get("matchLabels", {})
        if location_sel and not any(_labels_match(location_sel, c.labels) for c in clusters):
            continue  # No matching location found.

        # This binding passes all selectors. Now expand its blueprintMappings.
        binding_name = b.get("metadata", {}).get("name", "")
        # precedence controls tie-breaking: lower number wins (like CSS specificity in reverse).
        precedence = spec.get("precedence", 100)
        mappings = spec.get("blueprintMappings", [])

        for mapping in mappings:
            # Each mapping entry is one capability → blueprint pairing.
            capability = mapping.get("capability", "default")
            blueprint = mapping.get("blueprint", {})
            resolved = ResolvedBinding(
                binding_name=binding_name,
                capability=capability,
                blueprint_name=blueprint.get("name", ""),
                blueprint_version=blueprint.get("version", "latest"),
                blueprint_registry=blueprint.get("registry"),
                # config is nested under blueprint.config in the CRD schema.
                # Platform.spec.overrides are merged in the context assembly step.
                merged_config=blueprint.get("config", {}),
                precedence=precedence,
            )
            by_capability.setdefault(capability, []).append(resolved)

    # --- Select the winner per capability ---
    result: List[ResolvedBinding] = []
    for capability, candidates in by_capability.items():
        # Lower precedence number = higher priority.
        best = min(candidates, key=lambda r: r.precedence)
        result.append(best)
        logger.debug(
            f"Resolved capability '{capability}' → binding '{best.binding_name}' "
            f"blueprint='{best.blueprint_name}'"
        )

    return result
