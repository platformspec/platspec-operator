"""Platform resource handlers — reconciliation root.

This is the central module of the operator. Every meaningful action — discovering
infrastructure, resolving bindings, executing blueprints, applying resources, evaluating
status — is orchestrated from here. All other handlers (binding.py, infrastructure.py,
status.py) converge here by triggering a Platform reconciliation.

Module structure:
  - _now(), _condition()        Small helpers for building condition dicts.
  - _list_bindings()            Lists all BlueprintBindings for a Platform.
  - _binding_uid/namespace()    Safe extractors for binding metadata (handles both
                                 attribute-style and dict-style k8s objects).
  - _raw_binding()              Normalises a k8s binding object to a plain dict.
  - _run_binding()              Executes one resolved binding: KCL → apply → evaluate.
  - _reconcile()                The full reconciliation loop, called by all three
                                 public handlers below.
  - platform_reconcile()        kopf on.create/on.update handler.
  - platform_delete()           kopf on.delete handler — cleans up BlueprintBindings.

The reconcile loop (_reconcile) runs steps 1–8 of the reconciliation model described
in the spec. It is a plain async function (not a kopf handler) so that the timer in
status.py can call it directly without duplicating the logic.

Two execution paths:
  Most platforms have one or more Environment resources. The main loop iterates over
  them, assembles a full BlueprintContext for each (walking providerRefs → providers
  → credentialRefs → credentials, and selecting env-local networks and clusters), then
  runs each resolved binding's blueprint.

  A Platform with no Environment resources (e.g. one that just bootstraps cloud account
  structure before any environments exist, or the namespace-bootstrap smoke test) takes
  the no-environment path: bindings are expanded directly from blueprintMappings with a
  minimal "local" context and no infra ref-graph.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import yaml

import kopf
from loguru import logger

from ..config import Config
from ..core.aggregator import aggregate_platform_status
from ..core.applier import apply_output_resources
from ..core.context import assemble_blueprint_context, assemble_local_context
from ..core.discovery import discover_platform_resources
from ..core.evaluator import evaluate_status_expressions
from ..core.executor import BlueprintExecutionError, execute_blueprint
from ..core.fetcher import BlueprintFetcher
from ..core.resolver import resolve_bindings
from ..core.secrets import SecretNotFoundError, resolve_secrets
from ..models.blueprint import BlueprintContext, ResolvedBinding
from ..models.infrastructure import InfraResource
from ..models.platform import BindingStatus, BlueprintBindingStatus
from ..models.crd import Condition

_GROUP = "core.platformspec.io"
_VERSION = "v1alpha1"
_PLURAL = "platforms"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _condition(ctype: str, status: str, reason: str, message: str) -> Dict[str, Any]:
    """Build a Kubernetes condition dict with the current timestamp."""
    return {
        "type": ctype,
        "status": status,
        "reason": reason,
        "message": message,
        "lastTransitionTime": _now(),
    }


def _binding_phase(statuses: List[BindingStatus]) -> str:
    """Compute the phase for a single BlueprintBinding from all its BindingStatus entries.

    A binding runs one blueprint per capability (via _run_binding). This aggregates
    across all of them:
      - Any Applied=False → Failed (resources couldn't be applied; cluster state unknown).
      - All have Ready=True → Ready.
      - Otherwise (some still rendering/progressing) → Progressing.
    """
    if not statuses:
        return "Progressing"
    all_conditions = [c for bs in statuses for c in bs.conditions]
    if any(c.type == "Applied" and c.status == "False" for c in all_conditions):
        return "Failed"
    if all(
        any(c.type == "Ready" and c.status == "True" for c in bs.conditions)
        for bs in statuses
    ):
        return "Ready"
    return "Progressing"


def _populate_requires(fetcher: BlueprintFetcher, bindings: List[ResolvedBinding]) -> None:
    """Read each blueprint's blueprint.yaml and set binding.requires from its requires: [] list.

    Called once per resolved binding list before topo-sorting. Errors are swallowed and
    treated as empty requires so a missing or unreadable blueprint.yaml never blocks execution.
    """
    for binding in bindings:
        try:
            bp_path = fetcher.fetch(
                binding.blueprint_name,
                binding.blueprint_version,
                binding.blueprint_registry,
            )
            bp_yaml = bp_path / "blueprint.yaml"
            if not bp_yaml.exists():
                continue
            with open(bp_yaml) as f:
                meta: Dict[str, Any] = yaml.safe_load(f) or {}
            binding.requires = [str(r) for r in meta.get("requires", []) or []]
        except Exception:
            pass  # No requires if blueprint is inaccessible at this stage.


def _topo_sort(bindings: List[ResolvedBinding]) -> List[ResolvedBinding]:
    """Return bindings in dependency order using Kahn's topological sort.

    Only requires entries that reference a capability in the current binding set affect
    ordering — external dependencies are handled at runtime by the completed_capabilities
    check. Raises ValueError if a circular dependency is detected.
    """
    by_cap: Dict[str, ResolvedBinding] = {b.capability: b for b in bindings}

    in_degree: Dict[str, int] = {b.capability: 0 for b in bindings}
    # dependents[cap] = list of capabilities that require cap to run first.
    dependents: Dict[str, List[str]] = {b.capability: [] for b in bindings}

    for b in bindings:
        for req in b.requires:
            if req in by_cap:  # Only order against capabilities in this binding set.
                in_degree[b.capability] += 1
                dependents[req].append(b.capability)

    queue = [cap for cap, deg in in_degree.items() if deg == 0]
    result: List[ResolvedBinding] = []
    while queue:
        cap = queue.pop(0)
        result.append(by_cap[cap])
        for dep_cap in dependents[cap]:
            in_degree[dep_cap] -= 1
            if in_degree[dep_cap] == 0:
                queue.append(dep_cap)

    if len(result) != len(bindings):
        cycle_caps = [cap for cap, deg in in_degree.items() if deg > 0]
        raise ValueError(
            f"Circular dependency detected among capabilities: {cycle_caps}"
        )
    return result


def _list_bindings(k8s: Any, name: str, namespace: str) -> List[Any]:
    """List all BlueprintBindings whose spec.platformRef.name matches this Platform.

    Returns raw k8s objects (not dicts) — callers use _raw_binding() to normalise them.
    Falls back to an empty list on any API error so a temporary RBAC issue or missing
    CRD doesn't abort the entire reconciliation.
    """
    try:
        bb_api = k8s.resources.get(
            api_version=f"{_GROUP}/{_VERSION}", kind="BlueprintBinding"
        )
        raw = bb_api.get(namespace=namespace or None)
        return [
            b for b in raw.items
            if b.spec.get("platformRef", {}).get("name") == name
        ]
    except Exception as e:
        logger.warning(f"Could not list BlueprintBindings: {e}")
        return []


def _binding_uid(binding_resource: Any) -> str:
    """Extract the UID from a binding object, handling both attribute and dict access.

    The k8s DynamicClient returns objects with attribute-style access (object.metadata.uid),
    but code paths that normalise through _raw_binding() produce plain dicts. Both forms
    are handled here so callers don't need to care which they have.
    """
    if binding_resource is None:
        return ""
    if hasattr(binding_resource, "metadata"):
        return binding_resource.metadata.uid or ""
    return binding_resource.get("metadata", {}).get("uid", "")


def _binding_namespace(binding_resource: Any) -> str:
    """Extract the namespace from a binding object, handling both attribute and dict access.

    Used by the applier to decide whether to set ownerReferences — ownerRefs are only
    valid when the owned resource is in the same namespace as the binding. See applier.py.
    """
    if binding_resource is None:
        return ""
    if hasattr(binding_resource, "metadata"):
        return binding_resource.metadata.namespace or ""
    return binding_resource.get("metadata", {}).get("namespace", "")


def _raw_binding(b: Any) -> Dict[str, Any]:
    """Normalise a k8s binding object to a plain dict for use by the resolver.

    The resolver (resolver.py) works with plain dicts (b.get("spec", {}) etc.) rather
    than attribute-style objects. This function converts either form.
    """
    return b.to_dict() if hasattr(b, "to_dict") else dict(b)


async def _run_binding(
    binding: ResolvedBinding,
    ctx: BlueprintContext,
    all_bindings: List[Any],
    k8s: Any,
    config: Any,
    fetcher: BlueprintFetcher,
    platform_name: str,
    capability_results: Dict[str, Dict[str, Any]],
) -> BindingStatus:
    """Execute one resolved binding end-to-end: KCL → apply → status evaluation.

    This is the inner loop body. It runs for each (environment × capability) pair and
    produces a BindingStatus recording what happened at each step.

    Steps:
      1. KCL execution: run the blueprint to produce a list of k8s manifests.
         Failure sets Rendered=False and returns early (no point applying).

      2. Apply: server-side apply each manifest. Look up the live binding object to
         get its UID and namespace (needed for ownerReference and cross-namespace check).
         Failure sets Applied=False and returns early.

      3. Status evaluation: fetch each applied resource live from the cluster, then run
         the blueprint's KCL status expressions against them. Results are accumulated
         into capability_results (shared across all bindings for the same Platform).
         Expression failures yield None for the field but do not fail the binding.

    Returns a BindingStatus with conditions reflecting what succeeded or failed.
    The caller appends it to binding_statuses for platform-level aggregation.
    """
    bs = BindingStatus(binding_name=binding.binding_name, capability=binding.capability)

    # --- Step 6: Execute KCL blueprint ---
    registry_hint = (
        f" (registry={binding.blueprint_registry})" if binding.blueprint_registry else ""
    )
    logger.info(
        f"Running blueprint {binding.blueprint_name}@{binding.blueprint_version}"
        f"{registry_hint}"
    )
    try:
        output = execute_blueprint(
            fetcher=fetcher,
            blueprint_name=binding.blueprint_name,
            blueprint_version=binding.blueprint_version,
            blueprint_registry=binding.blueprint_registry,
            context=ctx,
            timeout=config.blueprint.kcl_timeout,
        )
        bs.conditions.append(
            Condition(
                type="Rendered", status="True", reason="KCLSuccess",
                message=f"Produced {len(output.resources)} resources",
                lastTransitionTime=_now(),
            )
        )
    except BlueprintExecutionError as e:
        logger.error(
            f"Blueprint {binding.blueprint_name} execution failed: {e}"
        )
        bs.conditions.append(
            Condition(
                type="Rendered", status="False", reason="KCLFailure",
                message=str(e), lastTransitionTime=_now(),
            )
        )
        return bs  # Cannot apply if rendering failed.

    # --- Step 7: Apply output resources ---
    # Find the live binding object so we can pass its UID and namespace to the applier.
    # The applier needs the UID to set ownerReferences, and the namespace to determine
    # whether an ownerReference is valid (cross-namespace ownerRefs cause GC deletion).
    binding_resource = next(
        (b for b in all_bindings
         if (b.metadata.name if hasattr(b, "metadata")
             else b.get("metadata", {}).get("name")) == binding.binding_name),
        None,
    )
    try:
        refs = apply_output_resources(
            manifests=output.resources,
            owner_binding_name=binding.binding_name,
            owner_binding_uid=_binding_uid(binding_resource),
            owner_binding_api_version=f"{_GROUP}/{_VERSION}",
            owner_binding_namespace=_binding_namespace(binding_resource),
            platform_name=platform_name,
            capability=binding.capability,
            field_manager=config.operator.field_manager,
            k8s_client=k8s,
        )
        bs.generated_resources = refs  # Stored for cleanup on binding deletion.
        bs.conditions.append(
            Condition(
                type="Applied", status="True", reason="ApplySuccess",
                message=f"Applied {len(refs)} resources",
                lastTransitionTime=_now(),
            )
        )
    except Exception as e:
        bs.conditions.append(
            Condition(
                type="Applied", status="False", reason="ApplyFailure",
                message=str(e), lastTransitionTime=_now(),
            )
        )
        return bs  # Cannot evaluate status if apply failed.

    # --- Step 8: Evaluate status expressions ---
    # Fetch each applied resource live from the cluster. Resources are keyed by
    # "apiVersion/kind" (e.g. "apps/v1/Deployment") so KCL expressions can address
    # them by type. Values are lists to support multiple instances of the same kind.
    child_resources: Dict[str, Any] = {}
    for ref in refs:
        key = f"{ref.api_version}/{ref.kind}"
        try:
            res_api = k8s.resources.get(api_version=ref.api_version, kind=ref.kind)
            live = res_api.get(name=ref.name, namespace=ref.namespace)
            child_resources.setdefault(key, []).append(live.to_dict())
        except Exception as e:
            # A resource that was just applied may not be immediately fetchable (e.g.
            # still being admitted). Log at debug and continue — the status timer will
            # retry on the next interval once the resource is fully created.
            logger.debug(f"Could not fetch live {key} ({ref.name}): {e}")

    field_values = evaluate_status_expressions(
        status_schema=output.status_schema,
        child_resources=child_resources,
        config=binding.merged_config,
        context=ctx,
    )
    # Merge field values into the shared capability_results dict. Multiple bindings
    # for the same capability would merge here (the last one wins per field).
    capability_results.setdefault(binding.capability, {}).update(field_values)

    bs.conditions.append(
        Condition(
            type="Ready", status="True", reason="Reconciled",
            message="Reconciliation complete", lastTransitionTime=_now(),
        )
    )
    return bs


async def _reconcile(
    name: str,
    namespace: str,
    spec: Dict[str, Any],
    patch: kopf.Patch,
    memo: kopf.Memo,
    **kwargs: Any,
) -> None:
    """Run the full Platform reconciliation loop.

    This is the core of the operator — called by platform_reconcile (on create/update),
    platform_status_timer (periodic), and indirectly by binding/infra handlers via the
    annotation-trigger mechanism.

    Steps:
      1. DISCOVER: list all infra resources labelled for this Platform.
      2. VALIDATE: check spec.requirements.resources minimum counts.
      3. LIST BINDINGS: find all BlueprintBindings referencing this Platform.
      4a. ENVIRONMENT PATH (if environments exist):
          For each environment, resolve its bindings, assemble context, resolve secrets,
          run each binding via _run_binding.
      4b. NO-ENVIRONMENT PATH (if no environments exist):
          Run all bindings with a minimal local context.
      8. AGGREGATE: compute Platform.status.phase + conditions + capabilities from
         the binding results collected in steps 4a/4b.

    All status is written via kopf's patch.status, which kopf applies atomically after
    the handler returns. Raising an exception causes kopf to set a reconcile error
    condition and requeue with backoff.
    """
    resource_ref = f"Platform/{namespace}/{name}" if namespace else f"Platform/{name}"
    with logger.contextualize(resource=resource_ref):
        await _reconcile_inner(
            name=name,
            namespace=namespace,
            spec=spec,
            patch=patch,
            memo=memo,
        )


async def _reconcile_inner(
    name: str,
    namespace: str,
    spec: Dict[str, Any],
    patch: kopf.Patch,
    memo: kopf.Memo,
    **kwargs: Any,
) -> None:
    config = Config.load()
    logger.info(f"Reconciling Platform {namespace}/{name}")
    k8s = memo.get("k8s")

    # Construct a BlueprintFetcher from the current registry map and cache dir.
    # The registry map is kept live by handlers/registry.py; the cache dir was
    # created at startup. Both are safe to read from any reconcile goroutine.
    fetcher = BlueprintFetcher(
        registries=dict(memo.get("registries", {})),
        local_blueprint_dir=config.blueprint.blueprint_dir,
        cache_dir=memo.get("blueprint_cache_dir", config.blueprint.blueprint_dir),
        k8s=k8s,
    )

    try:
        # --- Step 1: DISCOVER infrastructure resources ---
        # Platform.spec.resourceSelector.matchLabels are additional label filters on top
        # of the mandatory platform.platformspec.io/name=<name> label.
        resource_selector = spec.get("resourceSelector", {}).get("matchLabels", {})
        platform_resources = discover_platform_resources(
            platform_name=name,
            resource_selector=resource_selector,
            namespace=namespace or "",
            k8s_client=k8s,
        )

        # --- Step 2: VALIDATE resource requirements ---
        # spec.requirements.resources is a list of {kind, minimum} constraints.
        # If any kind has fewer resources than required, fail validation immediately
        # and write a Validated=False condition. No blueprint execution happens.
        requirements = (spec.get("requirements") or {})
        resource_reqs = requirements.get("resources") or []
        kind_counts: Dict[str, int] = {
            "Cluster":     len(platform_resources.clusters),
            "Environment": len(platform_resources.environments),
            "Provider":    len(platform_resources.providers),
            "Network":     len(platform_resources.networks),
            "Credential":  len(platform_resources.credentials),
        }
        failures: List[str] = []
        for req in resource_reqs:
            kind = req.get("kind", "")
            minimum = req.get("minimum", 0)
            found = kind_counts.get(kind, 0)
            if found < minimum:
                failures.append(f"{kind}: found {found}, need {minimum}")
        if failures:
            patch.status["conditions"] = [
                _condition(
                    "Validated", "False", "InsufficientResources",
                    "; ".join(failures),
                )
            ]
            return  # Do not proceed to blueprint execution.
        patch.status["conditions"] = [
            _condition(
                "Validated", "True", "ResourcesFound",
                f"environments={kind_counts['Environment']} "
                f"clusters={kind_counts['Cluster']} "
                f"providers={kind_counts['Provider']}",
            )
        ]

        # --- Step 3: LIST BINDINGS ---
        all_bindings = _list_bindings(k8s, name, namespace)

        # Shared accumulators across the environment loop below.
        binding_statuses: List[BindingStatus] = []
        capability_results: Dict[str, Dict[str, Any]] = {}
        overrides = spec.get("overrides", {})

        if platform_resources.environments:
            # --- Step 4a: ENVIRONMENT PATH ---
            # The normal execution path for cloud-backed platforms. Each environment
            # is an independent reconciliation context: its providerRefs determine
            # which providers (and therefore which credentials) are in scope; its
            # environmentRef-labelled clusters and networks complete the picture.
            for environment in platform_resources.environments:
                # Narrow clusters to those that belong to this specific environment.
                env_clusters = [
                    c for c in platform_resources.clusters
                    if c.spec.get("environmentRef", {}).get("name") == environment.name
                ]

                # Resolve which bindings (and therefore which blueprints) apply to
                # this environment based on selector labels.
                resolved = resolve_bindings(
                    bindings=[_raw_binding(b) for b in all_bindings],
                    environment=environment,
                    clusters=env_clusters,
                )

                # Read requires from each blueprint.yaml and sort in dependency order.
                # Cycles fail the reconcile immediately rather than running in an undefined order.
                _populate_requires(fetcher, resolved)
                try:
                    resolved = _topo_sort(resolved)
                except ValueError as cycle_err:
                    patch.status["conditions"] = [
                        _condition("Ready", "False", "CyclicDependency", str(cycle_err))
                    ]
                    return

                # Track which capabilities have been successfully applied this cycle.
                # Used to gate dependent blueprints — a blueprint whose requires are not
                # yet satisfied is skipped with RequirementNotMet rather than run blind.
                completed_capabilities: Set[str] = set()

                for binding in resolved:
                    # Gate on declared requirements — skip if any required capability
                    # hasn't been applied yet this cycle.
                    missing = [r for r in binding.requires if r not in completed_capabilities]
                    if missing:
                        bs = BindingStatus(
                            binding_name=binding.binding_name,
                            capability=binding.capability,
                        )
                        bs.conditions.append(Condition(
                            type="Rendered", status="False",
                            reason="RequirementNotMet",
                            message=f"Required capabilities not yet applied: {missing}",
                            lastTransitionTime=_now(),
                        ))
                        binding_statuses.append(bs)
                        continue

                    # Walk the ref-graph: environment → providerRefs → providers → credentialRefs
                    provider_names = [
                        ref.get("name") for ref in
                        environment.spec.get("providerRefs", [])
                    ]
                    providers = [
                        p for p in platform_resources.providers
                        if p.name in provider_names
                    ]
                    # Networks are scoped to the platform (not to a specific environment
                    # via a ref), so we include all networks labelled for this platform.
                    networks = [
                        n for n in platform_resources.networks
                        if n.labels.get("platform.platformspec.io/name") == name
                    ]
                    # Credentials are pulled via each provider's credentialRef.
                    cred_names = [
                        p.spec.get("credentialRef", {}).get("name")
                        for p in providers if p.spec.get("credentialRef")
                    ]
                    credentials = [
                        c for c in platform_resources.credentials
                        if c.name in cred_names
                    ]

                    ctx = assemble_blueprint_context(
                        environment=environment,
                        providers=providers,
                        networks=networks,
                        clusters=env_clusters,
                        credentials=credentials,
                        binding=binding,
                        platform_name=name,
                        platform_namespace=namespace or "",
                        platform_overrides=overrides,
                    )
                    # Pass outputs from already-applied capabilities so downstream
                    # blueprints can read values via context.capabilities["cap-name"].
                    ctx.capabilities = dict(capability_results)
                    ctx.bound_capabilities = [b.capability for b in resolved]
                    # Resolve actual secret values before passing context to KCL.
                    # Failure here means the blueprint can't run — record and skip.
                    try:
                        ctx = resolve_secrets(ctx, k8s)
                    except SecretNotFoundError as e:
                        bs = BindingStatus(
                            binding_name=binding.binding_name,
                            capability=binding.capability,
                        )
                        bs.conditions.append(Condition(
                            type="Rendered", status="False",
                            reason="SecretNotFound", message=str(e),
                            lastTransitionTime=_now(),
                        ))
                        binding_statuses.append(bs)
                        continue
                    bs = await _run_binding(
                        binding, ctx, all_bindings, k8s, config,
                        fetcher, name, capability_results,
                    )
                    binding_statuses.append(bs)
                    # Mark capability as completed if apply succeeded.
                    if any(c.type == "Applied" and c.status == "True" for c in bs.conditions):
                        completed_capabilities.add(binding.capability)

        else:
            # --- Step 4b: NO-ENVIRONMENT PATH ---
            # Used when no Environment resources exist yet — typically during initial
            # bootstrapping (creating cloud accounts before environments are declared)
            # or for purely local blueprints like namespace-bootstrap.
            # Bindings are run directly without environment-specific context.
            logger.info(
                f"No environment resources found for Platform {name} — "
                "running without environment context"
            )
            # Collect all resolved bindings first so they can be sorted before execution.
            no_env_resolved: List[ResolvedBinding] = []
            for raw in all_bindings:
                raw_dict = _raw_binding(raw)
                b_spec = raw_dict.get("spec", {})
                b_meta = raw_dict.get("metadata", {})
                b_name = b_meta.get("name", "") if isinstance(b_meta, dict) else getattr(b_meta, "name", "")
                precedence = b_spec.get("precedence", 100)
                # Expand blueprintMappings directly (no resolver needed — no environments
                # to filter against, so all mappings run unconditionally).
                for mapping in b_spec.get("blueprintMappings", []):
                    bp = mapping.get("blueprint", {})
                    no_env_resolved.append(ResolvedBinding(
                        binding_name=b_name,
                        capability=mapping.get("capability", "default"),
                        blueprint_name=bp.get("name", ""),
                        blueprint_version=bp.get("version", "latest"),
                        blueprint_registry=bp.get("registry"),
                        merged_config=dict(bp.get("config") or {}),
                        precedence=precedence,
                    ))

            # Deduplicate by capability — lowest precedence number wins, matching the
            # same rule used by resolve_bindings in the environment path. Without this,
            # two bindings with the same capability produce duplicate keys in _topo_sort,
            # which incorrectly raises CyclicDependency.
            _dedup: Dict[str, ResolvedBinding] = {}
            for _b in no_env_resolved:
                _existing = _dedup.get(_b.capability)
                if _existing is None or _b.precedence < _existing.precedence:
                    _dedup[_b.capability] = _b
            no_env_resolved = list(_dedup.values())

            _populate_requires(fetcher, no_env_resolved)
            try:
                no_env_resolved = _topo_sort(no_env_resolved)
            except ValueError as cycle_err:
                patch.status["conditions"] = [
                    _condition("Ready", "False", "CyclicDependency", str(cycle_err))
                ]
                return

            completed_capabilities_no_env: Set[str] = set()
            for binding in no_env_resolved:
                missing = [r for r in binding.requires if r not in completed_capabilities_no_env]
                if missing:
                    bs = BindingStatus(
                        binding_name=binding.binding_name,
                        capability=binding.capability,
                    )
                    bs.conditions.append(Condition(
                        type="Rendered", status="False",
                        reason="RequirementNotMet",
                        message=f"Required capabilities not yet applied: {missing}",
                        lastTransitionTime=_now(),
                    ))
                    binding_statuses.append(bs)
                    continue

                ctx = assemble_local_context(
                    binding=binding,
                    platform_name=name,
                    platform_namespace=namespace or "",
                    platform_overrides=overrides,
                )
                ctx.capabilities = dict(capability_results)
                ctx.bound_capabilities = [b.capability for b in no_env_resolved]
                try:
                    ctx = resolve_secrets(ctx, k8s)
                except SecretNotFoundError as e:
                    bs = BindingStatus(
                        binding_name=binding.binding_name,
                        capability=binding.capability,
                    )
                    bs.conditions.append(Condition(
                        type="Rendered", status="False",
                        reason="SecretNotFound", message=str(e),
                        lastTransitionTime=_now(),
                    ))
                    binding_statuses.append(bs)
                    continue
                bs = await _run_binding(
                    binding, ctx, all_bindings, k8s, config,
                    fetcher, name, capability_results,
                )
                binding_statuses.append(bs)
                if any(c.type == "Applied" and c.status == "True" for c in bs.conditions):
                    completed_capabilities_no_env.add(binding.capability)

        # --- Persist generated-resources annotation (once per binding) ---
        # A single binding runs one blueprint per capability, so _run_binding is called
        # multiple times for the same binding name. Writing the annotation inside
        # _run_binding caused each call to overwrite the previous one — the annotation
        # alternated between capability resource lists on every reconcile, triggering
        # binding_changed → _enqueue_platform → infinite loop.
        #
        # Fix: accumulate all refs from all _run_binding calls, then write once per
        # binding name after all blueprints have run. Only write when the value changed.
        binding_refs: Dict[str, List[Any]] = {}
        for bs in binding_statuses:
            if bs.generated_resources:
                binding_refs.setdefault(bs.binding_name, []).extend(bs.generated_resources)

        for b_name, refs in binding_refs.items():
            b_resource = next(
                (b for b in all_bindings
                 if (b.metadata.name if hasattr(b, "metadata")
                     else b.get("metadata", {}).get("name")) == b_name),
                None,
            )
            new_annotation = json.dumps([r.model_dump(by_alias=True) for r in refs])
            if hasattr(b_resource, "metadata"):
                existing_annotation = (b_resource.metadata.annotations or {}).get(
                    "platspec.io/generated-resources", ""
                )
            else:
                existing_annotation = (
                    (b_resource or {})
                    .get("metadata", {})
                    .get("annotations", {})
                    .get("platspec.io/generated-resources", "")
                ) if b_resource else ""

            if new_annotation != existing_annotation:
                try:
                    bb_api = k8s.resources.get(
                        api_version=f"{_GROUP}/{_VERSION}", kind="BlueprintBinding"
                    )
                    bb_api.patch(
                        name=b_name,
                        namespace=_binding_namespace(b_resource),
                        body={"metadata": {"annotations": {
                            "platspec.io/generated-resources": new_annotation
                        }}},
                        content_type="application/merge-patch+json",
                    )
                except Exception as patch_err:
                    logger.warning(
                        f"Could not persist generatedResources to binding "
                        f"'{b_name}': {patch_err}"
                    )

        # --- Write BlueprintBinding.status ---
        # Group binding_statuses by binding name (one entry per blueprint per capability),
        # then write a single status patch per binding. Uses the status subresource so
        # the write doesn't bump spec resourceVersion and re-trigger binding_changed.
        binding_statuses_by_name: Dict[str, List[BindingStatus]] = {}
        for bs in binding_statuses:
            binding_statuses_by_name.setdefault(bs.binding_name, []).append(bs)

        try:
            bb_api = k8s.resources.get(
                api_version=f"{_GROUP}/{_VERSION}", kind="BlueprintBinding"
            )
            for b_name, b_statuses in binding_statuses_by_name.items():
                b_resource = next(
                    (b for b in all_bindings
                     if (b.metadata.name if hasattr(b, "metadata")
                         else b.get("metadata", {}).get("name")) == b_name),
                    None,
                )
                observed_gen = None
                if b_resource is not None:
                    observed_gen = (
                        b_resource.metadata.generation
                        if hasattr(b_resource, "metadata")
                        else b_resource.get("metadata", {}).get("generation")
                    )
                all_conditions = [c for bs in b_statuses for c in bs.conditions]
                all_refs = binding_refs.get(b_name, [])
                bb_status = BlueprintBindingStatus(
                    phase=_binding_phase(b_statuses),
                    conditions=all_conditions,
                    generatedResources=[
                        r.model_dump(by_alias=True) for r in all_refs
                    ],
                    observedGeneration=observed_gen,
                    lastStatusUpdate=_now(),
                )
                try:
                    bb_api.status.patch(
                        name=b_name,
                        namespace=_binding_namespace(b_resource),
                        body={
                            "apiVersion": f"{_GROUP}/{_VERSION}",
                            "kind": "BlueprintBinding",
                            "metadata": {"name": b_name},
                            "status": bb_status.model_dump(
                                by_alias=True, exclude_none=True
                            ),
                        },
                        content_type="application/merge-patch+json",
                    )
                    logger.debug(
                        f"Updated BlueprintBinding '{b_name}' status → "
                        f"phase={bb_status.phase}"
                    )
                except Exception as status_err:
                    logger.warning(
                        f"Could not write status to BlueprintBinding "
                        f"'{b_name}': {status_err}"
                    )
        except Exception as e:
            logger.warning(f"Could not look up BlueprintBinding API for status write: {e}")

        # --- Step 8: AGGREGATE STATUS ---
        # Compute Platform.status.phase (Ready / Progressing / Failed), the Ready
        # condition, and the capabilities map from individual binding outcomes.
        platform_status = aggregate_platform_status(
            platform_name=name,
            capability_results=capability_results,
            binding_statuses=binding_statuses,
        )
        # kopf applies patch.status atomically after the handler returns, so all
        # status fields are updated in a single PATCH call.
        patch.status.update(
            platform_status.model_dump(by_alias=True, exclude_none=True)
        )
        logger.info(
            f"Platform {name} reconciliation complete — phase={platform_status.phase}"
        )

    except Exception as e:
        # Top-level catch: something unexpected happened outside the per-binding error
        # handling. Mark the platform as broken and re-raise so kopf requeues with backoff.
        logger.error(f"Reconciliation failed for Platform {name}: {e}")
        patch.status["conditions"] = [
            _condition("Ready", "False", "ReconcileError", str(e))
        ]
        raise


@kopf.on.create(_GROUP, _VERSION, _PLURAL)
@kopf.on.update(_GROUP, _VERSION, _PLURAL)
async def platform_reconcile(
    spec: Dict[str, Any],
    meta: Dict[str, Any],
    status: Dict[str, Any],
    patch: kopf.Patch,
    memo: kopf.Memo,
    **kwargs: Any,
) -> None:
    """kopf handler for Platform create and update events.

    Fires when a Platform is first created or any of its fields change. Also fires
    when another handler bumps the platspec.io/reconcile-trigger annotation (the
    mechanism binding.py and infrastructure.py use to enqueue reconciliation).

    Delegates entirely to _reconcile — this function exists only to satisfy kopf's
    requirement for a decorated top-level function.
    """
    await _reconcile(
        name=meta["name"],
        namespace=meta.get("namespace", ""),
        spec=spec,
        patch=patch,
        memo=memo,
    )


@kopf.on.timer(_GROUP, _VERSION, _PLURAL, interval=30, idle=30)
async def platform_status_timer(
    spec: Dict[str, Any],
    meta: Dict[str, Any],
    status: Dict[str, Any],
    patch: kopf.Patch,
    memo: kopf.Memo,
    **kwargs: Any,
) -> None:
    """Periodic re-reconcile to update status from downstream resources.

    Blueprint status expressions evaluate against live Kubernetes resources
    that were applied by previous reconciliations. Those resources may change
    status asynchronously (e.g. a provisioning controller marks a resource
    Ready). This timer ensures the Platform's status reflects the latest
    state without requiring explicit event triggers from downstream systems.
    """
    if meta.get("deletionTimestamp"):
        return
    await _reconcile(
        name=meta["name"],
        namespace=meta.get("namespace", ""),
        spec=spec,
        patch=patch,
        memo=memo,
    )


@kopf.on.delete(_GROUP, _VERSION, _PLURAL)
async def platform_delete(
    spec: Dict[str, Any],
    meta: Dict[str, Any],
    memo: kopf.Memo,
    **kwargs: Any,
) -> None:
    """kopf handler for Platform deletion — cascades deletion to BlueprintBindings.

    When a Platform is deleted, its BlueprintBindings must also be deleted. Each
    binding's own deletion handler (binding.py) will then trigger cleanup of any
    generated resources (same-namespace ones via ownerRef GC; cross-namespace ones
    via the binding finalizer, once that is implemented).

    This handler lists all BlueprintBindings with platformRef.name matching the
    deleted Platform and deletes them one by one. Errors are logged as warnings
    rather than raised — we want to attempt cleanup of all bindings even if one fails.

    Note: the Platform's own finalizer (platspec.io/finalizer) is managed by kopf.
    kopf removes it after this handler returns without raising.
    """
    name = meta["name"]
    namespace = meta.get("namespace", "")
    resource_ref = f"Platform/{namespace}/{name}" if namespace else f"Platform/{name}"
    with logger.contextualize(resource=resource_ref):
        logger.info(f"Platform {namespace}/{name} deleted — finalizer cleanup")
        k8s = memo.get("k8s")
        if k8s is None:
            return
        try:
            bb_api = k8s.resources.get(
                api_version=f"{_GROUP}/{_VERSION}", kind="BlueprintBinding"
            )
            items = bb_api.get(namespace=namespace or None)
            for b in items.items:
                if b.spec.get("platformRef", {}).get("name") == name:
                    bb_api.delete(name=b.metadata.name, namespace=b.metadata.namespace)
                    logger.debug(f"Deleted BlueprintBinding {b.metadata.name}")
        except Exception as e:
            logger.warning(f"Error during Platform {name} cleanup: {e}")
