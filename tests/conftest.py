"""Shared fixtures for the Platspec Operator test suite."""

from pathlib import Path
from typing import Any, Dict, List

import pytest

from platspec_operator.models.blueprint import (
    BlueprintContext,
    PlatformMeta,
    ResolvedBinding,
)
from platspec_operator.models.infrastructure import (
    EnvironmentSpec,
    InfraResource,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
BLUEPRINTS_DIR = FIXTURES_DIR / "blueprints"


# ---------------------------------------------------------------------------
# Infrastructure fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def environment() -> InfraResource:
    return InfraResource(
        name="development",
        namespace="platspec-system",
        labels={"env": "dev", "tier": "non-prod"},
        spec={"providerRefs": [{"name": "aws-dev", "namespace": "platspec-system"}]},
    )


@pytest.fixture
def clusters() -> List[InfraResource]:
    return [
        InfraResource(
            name="dev-cluster",
            labels={"tier": "non-prod", "region": "us-east-1"},
            spec={"environmentRef": {"name": "development"}},
        )
    ]


@pytest.fixture
def providers() -> List[InfraResource]:
    return [
        InfraResource(
            name="aws-dev",
            labels={},
            spec={"category": "iaas", "engine": "aws-organizations"},
        )
    ]


@pytest.fixture
def networks() -> List[InfraResource]:
    return [
        InfraResource(
            name="dev-vpc",
            labels={},
            spec={"providerRef": {"name": "aws-dev"}, "cidr": "10.0.0.0/16"},
        )
    ]


@pytest.fixture
def credentials() -> List[InfraResource]:
    return [
        InfraResource(
            name="aws-creds",
            labels={},
            spec={
                "provider": "aws",
                "source": "kubernetes-secret",
                "secretRef": {"name": "aws-secret", "namespace": "platspec-system"},
                "fields": {"accessKeyId": "AWS_ACCESS_KEY_ID"},
            },
        )
    ]


# ---------------------------------------------------------------------------
# Binding fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_binding() -> Dict[str, Any]:
    return {
        "metadata": {"name": "my-binding", "namespace": "platspec-system"},
        "spec": {
            "platformRef": {"name": "my-platform"},
            "precedence": 100,
            "blueprintMappings": [
                {
                    "capability": "networking",
                    "blueprint": {"name": "aws-vpc", "version": "1.0"},
                    "config": {"cidr": "10.0.0.0/16"},
                }
            ],
        },
    }


@pytest.fixture
def resolved_binding() -> ResolvedBinding:
    return ResolvedBinding(
        binding_name="my-binding",
        capability="networking",
        blueprint_name="aws-vpc",
        blueprint_version="1.0",
        merged_config={"cidr": "10.0.0.0/16"},
    )


# ---------------------------------------------------------------------------
# BlueprintContext fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def local_context() -> BlueprintContext:
    return BlueprintContext(
        platform=PlatformMeta(name="my-platform", namespace="platspec-system"),
        environment=EnvironmentSpec(),
        config={"replicas": 2},
        overrides={},
    )
