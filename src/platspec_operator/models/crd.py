"""Base Kubernetes types shared across all models."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class Condition(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str
    status: str
    reason: Optional[str] = None
    message: Optional[str] = None
    last_transition_time: Optional[str] = Field(None, alias="lastTransitionTime")
    observed_generation: Optional[int] = Field(None, alias="observedGeneration")


class ResourceReference(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    api_version: str = Field(..., alias="apiVersion")
    kind: str
    name: str
    namespace: Optional[str] = None
    uid: Optional[str] = None


class ObjectMeta(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    namespace: Optional[str] = None
    uid: Optional[str] = None
    generation: Optional[int] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    annotations: Dict[str, str] = Field(default_factory=dict)
    owner_references: List[Dict[str, Any]] = Field(
        default_factory=list, alias="ownerReferences"
    )
    finalizers: List[str] = Field(default_factory=list)
