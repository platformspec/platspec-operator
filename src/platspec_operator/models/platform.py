"""Models for Platform and BlueprintBinding resources."""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from .crd import Condition, ResourceReference


class ResourceSelector(BaseModel):
    match_labels: Dict[str, str] = Field(default_factory=dict, alias="matchLabels")

    model_config = ConfigDict(populate_by_name=True)


class RequiredResources(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    min_count: Optional[int] = Field(None, alias="minCount")
    required_labels: Dict[str, str] = Field(default_factory=dict, alias="requiredLabels")


class PlatformRequirements(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    capabilities: List[str] = Field(default_factory=list)
    resources: Optional[RequiredResources] = None


class PlatformSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    resource_selector: ResourceSelector = Field(
        default_factory=ResourceSelector, alias="resourceSelector"
    )
    requirements: Optional[PlatformRequirements] = None
    overrides: Dict[str, Any] = Field(default_factory=dict)
    deletion_policy: Literal["Delete", "Orphan"] = Field("Delete", alias="deletionPolicy")


class PlatformStatus(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    phase: Optional[str] = None
    conditions: List[Condition] = Field(default_factory=list)
    capabilities: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    observed_generation: Optional[int] = Field(None, alias="observedGeneration")
    last_status_update: Optional[str] = Field(None, alias="lastStatusUpdate")


class BlueprintRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    version: str = "latest"


class LabelSelector(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    match_labels: Dict[str, str] = Field(default_factory=dict, alias="matchLabels")


class BlueprintBindingSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    platform_ref: ResourceReference = Field(..., alias="platformRef")
    blueprint: BlueprintRef
    capability: str
    precedence: int = Field(default=100)
    environment_selector: Optional[LabelSelector] = Field(
        None, alias="environmentSelector"
    )
    location_selector: Optional[LabelSelector] = Field(None, alias="locationSelector")
    cluster_selector: Optional[LabelSelector] = Field(None, alias="clusterSelector")
    config: Dict[str, Any] = Field(default_factory=dict)
    # Optional override of the Platform-level deletionPolicy. If unset, falls back to
    # Platform.spec.deletionPolicy, then the operator default ("Delete").
    deletion_policy: Optional[Literal["Delete", "Orphan"]] = Field(
        None, alias="deletionPolicy"
    )


class BindingStatus(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    binding_name: str
    capability: str
    conditions: List[Condition] = Field(default_factory=list)
    generated_resources: List[ResourceReference] = Field(
        default_factory=list, alias="generatedResources"
    )


class BlueprintBindingStatus(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    phase: Optional[str] = None
    conditions: List[Condition] = Field(default_factory=list)
    generated_resources: List[Dict[str, Any]] = Field(
        default_factory=list, alias="generatedResources"
    )
    observed_generation: Optional[int] = Field(None, alias="observedGeneration")
    last_status_update: Optional[str] = Field(None, alias="lastStatusUpdate")
