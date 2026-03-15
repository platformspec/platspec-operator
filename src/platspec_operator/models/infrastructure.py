"""Models for Platform Specification infrastructure resources."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class NamespacedRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    namespace: Optional[str] = None


class CredentialSpec(BaseModel):
    """Parsed credential spec + resolved values.

    `source` determines which source-specific field is used.
    `fields` maps logical-name → key-in-source (env var name, secret key, JSON path, etc.).
    `data` is populated by core/secrets.py after resolution; never present in the CRD spec.
    """

    model_config = ConfigDict(populate_by_name=True)

    provider: str
    source: str = "kubernetes-secret"
    # Generic location — interpreted by the source handler:
    #   kubernetes-secret → Secret name in the same namespace
    #   aws-ssm          → SSM parameter path
    #   vault            → Vault secret path
    location: Optional[str] = None
    # Kubernetes namespace of the Credential resource — used to scope Secret/ConfigMap lookups.
    namespace: Optional[str] = None
    fields: Dict[str, str] = Field(default_factory=dict)

    # Source-specific refs — set the one matching source, leave others None.
    secret_ref: Optional[NamespacedRef] = Field(None, alias="secretRef")
    config_map_ref: Optional[NamespacedRef] = Field(None, alias="configMapRef")
    file_path: Optional[str] = Field(None, alias="filePath")
    aws_secrets_manager: Optional[Dict[str, Any]] = Field(None, alias="awsSecretsManager")
    vault: Optional[Dict[str, Any]] = None

    # Populated by resolve_secrets() — not stored in Kubernetes.
    data: Dict[str, str] = Field(default_factory=dict)


class ProviderSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # Kubernetes resource name — set by _to_provider() from InfraResource.name
    name: str = ""
    category: str
    engine: str
    credential_ref: Optional[NamespacedRef] = Field(None, alias="credentialRef")
    config: Dict[str, Any] = Field(default_factory=dict)


class EnvironmentSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str = ""
    provider_refs: List[NamespacedRef] = Field(default_factory=list, alias="providerRefs")
    config: Dict[str, Any] = Field(default_factory=dict)


class NetworkSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    provider_ref: Optional[NamespacedRef] = Field(None, alias="providerRef")
    cidr: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)


class ClusterSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    environment_ref: Optional[NamespacedRef] = Field(None, alias="environmentRef")
    network_refs: List[NamespacedRef] = Field(default_factory=list, alias="networkRefs")
    config: Dict[str, Any] = Field(default_factory=dict)


class InfraResource(BaseModel):
    """A discovered infrastructure resource with its name, labels, spec, and status.

    Both spec (declared intent) and status (provisioned reality) are captured so
    blueprints can access values like accountId, vpcId, and ARNs that are written
    to status by an external Operator after provisioning.
    """

    model_config = ConfigDict(populate_by_name=True)

    name: str
    namespace: Optional[str] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    spec: Dict[str, Any] = Field(default_factory=dict)
    status: Dict[str, Any] = Field(default_factory=dict)


class ImageBuilderSoftwareGroupRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str


class ImageBuilder(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    driver: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    software_groups: List[ImageBuilderSoftwareGroupRef] = Field(
        default_factory=list, alias="softwareGroups"
    )


class ImageReference(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: Optional[str] = None
    location: Optional[str] = None


class ImageSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    category: str = ""  # "machine" or "container"
    provider_refs: List[NamespacedRef] = Field(default_factory=list, alias="providerRefs")
    environment_refs: List[NamespacedRef] = Field(default_factory=list, alias="environmentRefs")
    version: Optional[str] = None
    builder: Optional[ImageBuilder] = None
    reference: Optional[ImageReference] = None


class SoftwarePackage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    engine: str  # "helm", "docker", "custom"
    config: Dict[str, Any] = Field(default_factory=dict)


class SoftwareGroupSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    packages: List[SoftwarePackage] = Field(default_factory=list)


class NodeSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    provider_refs: List[NamespacedRef] = Field(default_factory=list, alias="providerRefs")
    environment_ref: Optional[NamespacedRef] = Field(None, alias="environmentRef")
    region: Optional[str] = None
    network_refs: List[NamespacedRef] = Field(default_factory=list, alias="networkRefs")
    config: Dict[str, Any] = Field(default_factory=dict)


class PlatformResources(BaseModel):
    """All infrastructure resources discovered for a Platform."""

    environments: List[InfraResource] = Field(default_factory=list)
    providers: List[InfraResource] = Field(default_factory=list)
    networks: List[InfraResource] = Field(default_factory=list)
    clusters: List[InfraResource] = Field(default_factory=list)
    credentials: List[InfraResource] = Field(default_factory=list)
    images: List[InfraResource] = Field(default_factory=list)
    nodes: List[InfraResource] = Field(default_factory=list)
    software_groups: List[InfraResource] = Field(default_factory=list, alias="softwareGroups")
