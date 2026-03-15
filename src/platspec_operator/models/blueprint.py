"""Models for blueprint processing."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from .infrastructure import (
    ClusterSpec,
    CredentialSpec,
    EnvironmentSpec,
    ImageSpec,
    NetworkSpec,
    NodeSpec,
    ProviderSpec,
    SoftwareGroupSpec,
)


class PlatformMeta(BaseModel):
    name: str
    namespace: Optional[str] = None
    overrides: Dict[str, Any] = Field(default_factory=dict)


class BlueprintContext(BaseModel):
    """Full context passed to a KCL blueprint."""

    platform: PlatformMeta
    environment: EnvironmentSpec
    clusters: List[ClusterSpec] = Field(default_factory=list)
    networks: List[NetworkSpec] = Field(default_factory=list)
    providers: List[ProviderSpec] = Field(default_factory=list)
    credentials: List[CredentialSpec] = Field(default_factory=list)
    images: List[ImageSpec] = Field(default_factory=list)
    nodes: List[NodeSpec] = Field(default_factory=list)
    software_groups: List[SoftwareGroupSpec] = Field(default_factory=list)
    config: Dict[str, Any] = Field(default_factory=dict)
    overrides: Dict[str, Any] = Field(default_factory=dict)
    # Accumulated outputs from capabilities that have already been applied this cycle.
    # Populated by the reconcile loop in dependency order so downstream blueprints
    # can read values produced by upstream ones via context.capabilities["cap-name"].
    capabilities: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class StatusFieldSchema(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    expression: str
    description: Optional[str] = None


class StatusSchema(BaseModel):
    fields: Dict[str, StatusFieldSchema] = Field(default_factory=dict)


class BlueprintOutput(BaseModel):
    """Output produced by executing a KCL blueprint."""

    resources: List[Dict[str, Any]] = Field(default_factory=list)
    status_schema: StatusSchema = Field(default_factory=StatusSchema)


class ResolvedBinding(BaseModel):
    """A BlueprintBinding that has been matched to an Environment."""

    binding_name: str
    capability: str
    blueprint_name: str
    blueprint_version: str
    # Optional registry name — if set, the fetcher uses only that registry.
    blueprint_registry: Optional[str] = None
    merged_config: Dict[str, Any] = Field(default_factory=dict)
    precedence: int = 100
    # Capabilities this blueprint requires to have been Applied before it runs.
    # Populated from blueprint.yaml's requires: [] list by the reconcile loop.
    requires: List[str] = Field(default_factory=list)
