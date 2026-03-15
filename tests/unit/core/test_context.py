"""Tests for core/context.py — BlueprintContext assembly."""

import pytest

from platspec_operator.core.context import (
    assemble_blueprint_context,
    assemble_local_context,
)
from platspec_operator.models.blueprint import ResolvedBinding
from platspec_operator.models.infrastructure import InfraResource


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def binding() -> ResolvedBinding:
    return ResolvedBinding(
        binding_name="my-binding",
        capability="networking",
        blueprint_name="aws-vpc",
        blueprint_version="1.0",
        merged_config={"cidr": "10.0.0.0/16"},
    )


@pytest.fixture
def environment() -> InfraResource:
    return InfraResource(
        name="development",
        namespace="platspec-system",
        labels={"env": "dev"},
        spec={
            "providerRefs": [{"name": "aws-dev", "namespace": "platspec-system"}],
        },
    )


@pytest.fixture
def provider() -> InfraResource:
    return InfraResource(
        name="aws-dev",
        spec={"category": "iaas", "engine": "aws-organizations", "credentialRef": {"name": "creds"}},
    )


@pytest.fixture
def network() -> InfraResource:
    return InfraResource(
        name="dev-vpc",
        spec={"providerRef": {"name": "aws-dev"}, "cidr": "10.0.0.0/16"},
    )


@pytest.fixture
def cluster() -> InfraResource:
    return InfraResource(
        name="dev-cluster",
        spec={
            "environmentRef": {"name": "development"},
            "networkRefs": [{"name": "dev-vpc"}],
        },
    )


@pytest.fixture
def credential() -> InfraResource:
    return InfraResource(
        name="aws-creds",
        spec={
            "provider": "aws",
            "source": "kubernetes-secret",
            "secretRef": {"name": "aws-secret", "namespace": "platspec-system"},
            "fields": {"accessKeyId": "KEY"},
        },
    )


# ---------------------------------------------------------------------------
# assemble_local_context
# ---------------------------------------------------------------------------


def test_local_context_platform_meta(binding):
    ctx = assemble_local_context(binding, "my-platform", "platspec-system", {})
    assert ctx.platform.name == "my-platform"
    assert ctx.platform.namespace == "platspec-system"


def test_local_context_environment_is_local(binding):
    ctx = assemble_local_context(binding, "my-platform", "platspec-system", {})
    assert ctx.environment.name == ""
    assert ctx.environment.provider_refs == []


def test_local_context_infra_lists_are_empty(binding):
    ctx = assemble_local_context(binding, "my-platform", "platspec-system", {})
    assert ctx.clusters == []
    assert ctx.networks == []
    assert ctx.providers == []
    assert ctx.credentials == []


def test_local_context_config_from_binding(binding):
    ctx = assemble_local_context(binding, "my-platform", "platspec-system", {})
    assert ctx.config == {"cidr": "10.0.0.0/16"}


def test_local_context_overrides_passed_through(binding):
    overrides = {"global_tag": "production"}
    ctx = assemble_local_context(binding, "my-platform", "platspec-system", overrides)
    assert ctx.overrides == {"global_tag": "production"}
    assert ctx.platform.overrides == {"global_tag": "production"}


# ---------------------------------------------------------------------------
# assemble_blueprint_context
# ---------------------------------------------------------------------------


def _full_context(binding, environment, provider, network, cluster, credential, overrides=None):
    return assemble_blueprint_context(
        environment=environment,
        providers=[provider],
        networks=[network],
        clusters=[cluster],
        credentials=[credential],
        binding=binding,
        platform_name="my-platform",
        platform_namespace="platspec-system",
        platform_overrides=overrides or {},
    )


def test_blueprint_context_platform_meta(binding, environment, provider, network, cluster, credential):
    ctx = _full_context(binding, environment, provider, network, cluster, credential)
    assert ctx.platform.name == "my-platform"
    assert ctx.platform.namespace == "platspec-system"


def test_blueprint_context_environment(binding, environment, provider, network, cluster, credential):
    ctx = _full_context(binding, environment, provider, network, cluster, credential)
    assert ctx.environment.name == "development"


def test_blueprint_context_environment_provider_refs(binding, environment, provider, network, cluster, credential):
    ctx = _full_context(binding, environment, provider, network, cluster, credential)
    assert len(ctx.environment.provider_refs) == 1
    assert ctx.environment.provider_refs[0].name == "aws-dev"


def test_blueprint_context_providers(binding, environment, provider, network, cluster, credential):
    ctx = _full_context(binding, environment, provider, network, cluster, credential)
    assert len(ctx.providers) == 1
    assert ctx.providers[0].category == "iaas"
    assert ctx.providers[0].engine == "aws-organizations"


def test_blueprint_context_provider_credential_ref(binding, environment, provider, network, cluster, credential):
    ctx = _full_context(binding, environment, provider, network, cluster, credential)
    assert ctx.providers[0].credential_ref is not None
    assert ctx.providers[0].credential_ref.name == "creds"


def test_blueprint_context_networks(binding, environment, provider, network, cluster, credential):
    ctx = _full_context(binding, environment, provider, network, cluster, credential)
    assert len(ctx.networks) == 1
    assert ctx.networks[0].cidr == "10.0.0.0/16"
    assert ctx.networks[0].provider_ref is not None
    assert ctx.networks[0].provider_ref.name == "aws-dev"


def test_blueprint_context_clusters(binding, environment, provider, network, cluster, credential):
    ctx = _full_context(binding, environment, provider, network, cluster, credential)
    assert len(ctx.clusters) == 1
    assert ctx.clusters[0].environment_ref is not None
    assert ctx.clusters[0].environment_ref.name == "development"
    assert len(ctx.clusters[0].network_refs) == 1


def test_blueprint_context_credentials(binding, environment, provider, network, cluster, credential):
    ctx = _full_context(binding, environment, provider, network, cluster, credential)
    assert len(ctx.credentials) == 1
    cred = ctx.credentials[0]
    assert cred.provider == "aws"
    assert cred.source == "kubernetes-secret"
    assert cred.secret_ref is not None
    assert cred.secret_ref.name == "aws-secret"


def test_blueprint_context_config_from_binding(binding, environment, provider, network, cluster, credential):
    ctx = _full_context(binding, environment, provider, network, cluster, credential)
    assert ctx.config == {"cidr": "10.0.0.0/16"}


def test_blueprint_context_overrides(binding, environment, provider, network, cluster, credential):
    overrides = {"account_id": "123456789"}
    ctx = _full_context(binding, environment, provider, network, cluster, credential, overrides)
    assert ctx.overrides == overrides


def test_blueprint_context_no_credential_ref(binding, environment, network, cluster, credential):
    """Provider without credentialRef should still parse cleanly."""
    provider_no_cred = InfraResource(name="aws-anon", spec={"category": "iaas", "engine": "aws-organizations"})
    ctx = assemble_blueprint_context(
        environment=environment,
        providers=[provider_no_cred],
        networks=[network],
        clusters=[cluster],
        credentials=[credential],
        binding=binding,
        platform_name="p",
        platform_namespace="ns",
        platform_overrides={},
    )
    assert ctx.providers[0].credential_ref is None


def test_blueprint_context_no_environment_ref(binding, environment, provider, network, credential):
    """Cluster without environmentRef should still parse cleanly."""
    cluster_no_env = InfraResource(name="bare-cluster", spec={"networkRefs": []})
    ctx = assemble_blueprint_context(
        environment=environment,
        providers=[provider],
        networks=[network],
        clusters=[cluster_no_env],
        credentials=[credential],
        binding=binding,
        platform_name="p",
        platform_namespace="ns",
        platform_overrides={},
    )
    assert ctx.clusters[0].environment_ref is None
