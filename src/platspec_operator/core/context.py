"""Ref-graph walker and BlueprintContext assembly.

This module answers: "what data does a blueprint receive as input?"

A blueprint is a pure function — it takes a BlueprintContext and returns a list of
Kubernetes manifests. The context carries everything the blueprint needs to know about
the world: the platform metadata, the environment, all infrastructure resources associated
with it (providers, networks, clusters, credentials), the merged config, and any overrides
from the Platform resource.

The two public functions cover two execution paths:

- assemble_blueprint_context: the normal path for cloud-backed environments. Receives the
  pre-walked ref-graph (providers, networks, clusters, credentials already fetched by the
  handler) and packages them into a BlueprintContext.

- assemble_local_context: the shortcut for local-cluster blueprints (e.g. the
  namespace-bootstrap smoke test) where there are no cloud resources to walk. Returns a
  minimal context with a synthetic "local" environment and empty infra lists.

The private _to_* helpers translate raw InfraResource objects (untyped dicts from the
Kubernetes API) into the typed spec models that BlueprintContext requires. This is the
boundary between "raw k8s data" and "typed domain model".
"""

from typing import Any, Dict, List, Optional

from ..models.blueprint import BlueprintContext, PlatformMeta, ResolvedBinding


def _to_python(obj: Any) -> Any:
    """Recursively convert dict-like and list-like objects to plain Python types.

    kopf's ResourceField is dict-like but not a plain dict — pydantic v2's
    'any' serializer can't build a SchemaSerializer for it. Shallow dict()
    conversion fixes the top level but leaves nested ResourceField objects in
    place (e.g. config.labels stays as ResourceField). This function walks the
    entire structure so no kopf types survive into pydantic models.
    """
    if isinstance(obj, dict):
        return {k: _to_python(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_python(item) for item in obj]
    if hasattr(obj, "items"):  # duck-type: ResourceField and similar dict-like objects
        return {k: _to_python(v) for k, v in obj.items()}
    return obj
from ..models.infrastructure import (
    ClusterSpec,
    CredentialSpec,
    EnvironmentSpec,
    ImageBuilder,
    ImageBuilderSoftwareGroupRef,
    ImageReference,
    ImageSpec,
    InfraResource,
    NamespacedRef,
    NetworkSpec,
    NodeSpec,
    ProviderSpec,
    SoftwareGroupSpec,
    SoftwarePackage,
)


def _to_env(r: InfraResource) -> EnvironmentSpec:
    """Translate an Environment InfraResource into a typed EnvironmentSpec.

    r.name is the Kubernetes resource name (e.g. "development"). The KCL blueprint
    reads context.environment.name to construct output resource names like
    "<platform>-<environment>", so the name must be populated from the resource.
    providerRefs in the raw spec is a list of {name, namespace} dicts; each becomes
    a typed NamespacedRef.
    """
    s = r.spec
    provider_refs = [
        NamespacedRef(name=ref.get("name", ""), namespace=ref.get("namespace"))
        for ref in s.get("providerRefs", [])
    ]
    return EnvironmentSpec(
        name=r.name,
        providerRefs=provider_refs,
        config=_to_python(s.get("config") or {}),
    )


def _to_provider(r: InfraResource) -> ProviderSpec:
    """Translate a Provider InfraResource into a typed ProviderSpec.

    credentialRef is optional — a provider without one uses ambient credentials
    (e.g. IRSA on EKS, Workload Identity on GKE).
    """
    s = r.spec
    cred = s.get("credentialRef")
    return ProviderSpec(
        name=r.name,
        category=s.get("category", ""),
        engine=s.get("engine", ""),
        credentialRef=(
            NamespacedRef(name=cred["name"], namespace=cred.get("namespace"))
            if cred
            else None
        ),
        config=_to_python(s.get("config") or {}),
    )


def _to_network(r: InfraResource) -> NetworkSpec:
    """Translate a Network InfraResource into a typed NetworkSpec.

    providerRef links the network to its cloud account, which the blueprint may
    need when constructing VPC ARNs or subnet IDs.
    """
    s = r.spec
    pref = s.get("providerRef")
    return NetworkSpec(
        providerRef=(
            NamespacedRef(name=pref["name"], namespace=pref.get("namespace"))
            if pref
            else None
        ),
        cidr=s.get("cidr"),
        config=_to_python(s.get("config") or {}),
    )


def _to_cluster(r: InfraResource) -> ClusterSpec:
    """Translate a Cluster InfraResource into a typed ClusterSpec.

    environmentRef links the cluster back to its parent environment. networkRefs
    is a list — a cluster may span multiple networks (e.g. private + public subnets).
    """
    s = r.spec
    env_ref = s.get("environmentRef")
    net_refs = [
        NamespacedRef(name=ref.get("name", ""), namespace=ref.get("namespace"))
        for ref in s.get("networkRefs", [])
    ]
    return ClusterSpec(
        environmentRef=(
            NamespacedRef(name=env_ref["name"], namespace=env_ref.get("namespace"))
            if env_ref
            else None
        ),
        networkRefs=net_refs,
        config=_to_python(s.get("config") or {}),
    )


def _to_credential(r: InfraResource) -> CredentialSpec:
    """Translate a Credential InfraResource into a typed CredentialSpec.

    At this stage the credential only carries references (secretRef, configMapRef,
    filePath, etc.) — actual secret values are NOT yet resolved. The source field
    tells secrets.py which resolution path to take. After resolve_secrets() runs,
    credential.data will be populated with the actual key-value pairs the blueprint
    can read.
    """
    s = r.spec
    secret = s.get("secretRef")
    configmap = s.get("configMapRef")
    return CredentialSpec(
        provider=s.get("provider", ""),
        source=s.get("source", "kubernetes-secret"),
        location=s.get("location"),
        namespace=r.namespace,
        fields=_to_python(s.get("fields") or {}),
        secretRef=(
            NamespacedRef(name=secret["name"], namespace=secret.get("namespace"))
            if secret
            else None
        ),
        configMapRef=(
            NamespacedRef(name=configmap["name"], namespace=configmap.get("namespace"))
            if configmap
            else None
        ),
        filePath=s.get("filePath"),
        awsSecretsManager=_to_python(s.get("awsSecretsManager")) if s.get("awsSecretsManager") else None,
        vault=_to_python(s.get("vault")) if s.get("vault") else None,
    )


def _to_image(r: InfraResource) -> ImageSpec:
    """Translate an Image InfraResource into a typed ImageSpec."""
    s = r.spec
    provider_refs = [
        NamespacedRef(name=ref.get("name", ""), namespace=ref.get("namespace"))
        for ref in s.get("providerRefs", [])
    ]
    environment_refs = [
        NamespacedRef(name=ref.get("name", ""), namespace=ref.get("namespace"))
        for ref in s.get("environmentRefs", [])
    ]
    raw_builder = s.get("builder")
    builder = None
    if raw_builder:
        sg_refs = [
            ImageBuilderSoftwareGroupRef(name=sg.get("name", ""))
            for sg in raw_builder.get("softwareGroups", [])
        ]
        builder = ImageBuilder(
            driver=raw_builder.get("driver"),
            config=_to_python(raw_builder.get("config") or {}),
            softwareGroups=sg_refs,
        )
    raw_ref = s.get("reference")
    reference = None
    if raw_ref:
        reference = ImageReference(id=raw_ref.get("id"), location=raw_ref.get("location"))
    return ImageSpec(
        category=s.get("category", ""),
        providerRefs=provider_refs,
        environmentRefs=environment_refs,
        version=s.get("version"),
        builder=builder,
        reference=reference,
    )


def _to_node(r: InfraResource) -> NodeSpec:
    """Translate a Node InfraResource into a typed NodeSpec."""
    s = r.spec
    provider_refs = [
        NamespacedRef(name=ref.get("name", ""), namespace=ref.get("namespace"))
        for ref in s.get("providerRefs", [])
    ]
    raw_env_ref = s.get("environmentRef")
    network_refs = [
        NamespacedRef(name=ref.get("name", ""), namespace=ref.get("namespace"))
        for ref in s.get("networkRefs", [])
    ]
    return NodeSpec(
        providerRefs=provider_refs,
        environmentRef=(
            NamespacedRef(name=raw_env_ref["name"], namespace=raw_env_ref.get("namespace"))
            if raw_env_ref
            else None
        ),
        region=s.get("region"),
        networkRefs=network_refs,
        config=_to_python(s.get("config") or {}),
    )


def _to_software_group(r: InfraResource) -> SoftwareGroupSpec:
    """Translate a SoftwareGroup InfraResource into a typed SoftwareGroupSpec."""
    s = r.spec
    packages = [
        SoftwarePackage(
            name=pkg.get("name", ""),
            engine=pkg.get("engine", "custom"),
            config=_to_python(pkg.get("config") or {}),
        )
        for pkg in s.get("packages", [])
    ]
    return SoftwareGroupSpec(packages=packages)


def assemble_local_context(
    binding: ResolvedBinding,
    platform_name: str,
    platform_namespace: str,
    platform_overrides: Dict[str, Any],
) -> BlueprintContext:
    """Build a minimal BlueprintContext for local-cluster blueprints.

    Used when there is no Environment resource to walk (e.g. a Platform whose
    cloudProvider is "none", or the namespace-bootstrap smoke test). The environment
    is given type="local" with no name; clusters, networks, providers, and credentials
    are all empty lists. The blueprint must handle these empty fields gracefully.
    """
    return BlueprintContext(
        platform=PlatformMeta(
            name=platform_name,
            namespace=platform_namespace,
            overrides=platform_overrides,
        ),
        environment=EnvironmentSpec(),
        clusters=[],
        networks=[],
        providers=[],
        credentials=[],
        images=[],
        nodes=[],
        software_groups=[],
        config=binding.merged_config,
        overrides=platform_overrides,
    )


def assemble_blueprint_context(
    environment: InfraResource,
    providers: List[InfraResource],
    networks: List[InfraResource],
    clusters: List[InfraResource],
    credentials: List[InfraResource],
    binding: ResolvedBinding,
    platform_name: str,
    platform_namespace: str,
    platform_overrides: Dict[str, Any],
    images: Optional[List[InfraResource]] = None,
    nodes: Optional[List[InfraResource]] = None,
    software_groups: Optional[List[InfraResource]] = None,
) -> BlueprintContext:
    """Build a full BlueprintContext from the ref-graph of a cloud-backed environment.

    The caller (platform.py handler) has already walked the environment's providerRefs
    and collected all associated resources from the cluster. This function translates
    each raw InfraResource into its typed spec model and packages everything into a
    BlueprintContext ready for secret resolution and KCL execution.

    The resulting context is passed to:
      1. resolve_secrets() — injects actual credential values into credential.data
      2. execute_blueprint() — serialises the context as JSON for the KCL template
    """
    return BlueprintContext(
        platform=PlatformMeta(
            name=platform_name,
            namespace=platform_namespace,
            overrides=platform_overrides,
        ),
        environment=_to_env(environment),
        clusters=[_to_cluster(c) for c in clusters],
        networks=[_to_network(n) for n in networks],
        providers=[_to_provider(p) for p in providers],
        credentials=[_to_credential(c) for c in credentials],
        images=[_to_image(i) for i in (images or [])],
        nodes=[_to_node(n) for n in (nodes or [])],
        software_groups=[_to_software_group(sg) for sg in (software_groups or [])],
        config=binding.merged_config,
        overrides=platform_overrides,
    )
