"""Pydantic model for the BlueprintRegistry CRD spec."""

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class SecretRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    namespace: str


class RegistryAuth(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # "secret" — read credentials from a k8s Secret.
    # "serviceAccount" — use the pod's ambient IAM identity (IRSA / Workload Identity).
    # "anonymous" — no authentication (public registry).
    type: Literal["secret", "serviceAccount", "anonymous"] = "anonymous"
    secret_ref: Optional[SecretRef] = Field(None, alias="secretRef")


class BlueprintRegistrySpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # Registry type determines which backend fetches the blueprint.
    #   oci        — OCI Distribution API (container registries, GHCR, ECR, etc.)
    #   git        — git repository; directory = blueprint, ref = branch/tag/commit
    #   http       — HTTP(S) artifact server serving {url}/{name}/{version}.tar.gz
    #   s3         — S3-compatible object storage
    #   filesystem — local path (same host as the operator pod)
    type: Literal["oci", "git", "http", "s3", "filesystem"]
    url: str
    auth: RegistryAuth = Field(default_factory=RegistryAuth)
    # Optional subdirectory within the registry root (e.g. "components/general").
    # Blueprints are resolved at <path>/<name> when set.
    path: Optional[str] = None
    # Git ref (branch, tag, or commit SHA) to use when cloning.
    # Applies to git registries only. Defaults to the blueprint version.
    ref: Optional[str] = None
    # For s3 type: AWS region for the bucket.
    region: Optional[str] = None
