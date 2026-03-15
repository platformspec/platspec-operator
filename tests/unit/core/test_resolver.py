"""Tests for core/resolver.py — selector-based BlueprintBinding resolution."""

from typing import Any, Dict, List

import pytest

from platspec_operator.core.resolver import _labels_match, resolve_bindings
from platspec_operator.models.infrastructure import InfraResource


# ---------------------------------------------------------------------------
# _labels_match unit tests
# ---------------------------------------------------------------------------


def test_labels_match_empty_selector_matches_all():
    assert _labels_match({}, {"env": "dev", "tier": "non-prod"}) is True


def test_labels_match_exact_match():
    assert _labels_match({"env": "dev"}, {"env": "dev", "tier": "non-prod"}) is True


def test_labels_match_missing_key():
    assert _labels_match({"env": "prod"}, {"env": "dev"}) is False


def test_labels_match_partial_miss():
    assert _labels_match({"env": "dev", "tier": "prod"}, {"env": "dev"}) is False


def test_labels_match_all_keys_required():
    assert _labels_match({"a": "1", "b": "2"}, {"a": "1", "b": "2", "c": "3"}) is True


# ---------------------------------------------------------------------------
# resolve_bindings — filtering
# ---------------------------------------------------------------------------


def _binding(
    name: str = "b1",
    capability: str = "networking",
    blueprint_name: str = "my-blueprint",
    version: str = "latest",
    registry: str | None = None,
    config: Dict[str, Any] | None = None,
    precedence: int = 100,
    env_labels: Dict[str, str] | None = None,
    cluster_labels: Dict[str, str] | None = None,
    location_labels: Dict[str, str] | None = None,
) -> Dict[str, Any]:
    selectors: Dict[str, Any] = {}
    if env_labels is not None:
        selectors["environmentSelector"] = {"matchLabels": env_labels}
    if cluster_labels is not None:
        selectors["clusterSelector"] = {"matchLabels": cluster_labels}
    if location_labels is not None:
        selectors["locationSelector"] = {"matchLabels": location_labels}

    bp: Dict[str, Any] = {"name": blueprint_name, "version": version}
    if registry is not None:
        bp["registry"] = registry
    if config is not None:
        bp["config"] = config

    return {
        "metadata": {"name": name},
        "spec": {
            "platformRef": {"name": "plat"},
            "selectors": selectors,
            "precedence": precedence,
            "blueprintMappings": [
                {
                    "capability": capability,
                    "blueprint": bp,
                }
            ],
        },
    }


@pytest.fixture
def env() -> InfraResource:
    return InfraResource(name="dev", labels={"env": "dev", "tier": "non-prod"})


@pytest.fixture
def clus() -> List[InfraResource]:
    return [InfraResource(name="c1", labels={"tier": "non-prod", "region": "us-east-1"})]


def test_resolve_empty_bindings(env, clus):
    result = resolve_bindings([], env, clus)
    assert result == []


def test_resolve_single_binding_no_selectors(env, clus):
    result = resolve_bindings([_binding()], env, clus)
    assert len(result) == 1
    assert result[0].capability == "networking"
    assert result[0].blueprint_name == "my-blueprint"


def test_resolve_env_selector_match(env, clus):
    result = resolve_bindings([_binding(env_labels={"env": "dev"})], env, clus)
    assert len(result) == 1


def test_resolve_env_selector_no_match(env, clus):
    result = resolve_bindings([_binding(env_labels={"env": "prod"})], env, clus)
    assert result == []


def test_resolve_cluster_selector_match(env, clus):
    result = resolve_bindings([_binding(cluster_labels={"tier": "non-prod"})], env, clus)
    assert len(result) == 1


def test_resolve_cluster_selector_no_match(env, clus):
    result = resolve_bindings([_binding(cluster_labels={"tier": "prod"})], env, clus)
    assert result == []


def test_resolve_location_selector_match(env, clus):
    result = resolve_bindings([_binding(location_labels={"region": "us-east-1"})], env, clus)
    assert len(result) == 1


def test_resolve_location_selector_no_match(env, clus):
    result = resolve_bindings([_binding(location_labels={"region": "eu-west-1"})], env, clus)
    assert result == []


def test_resolve_multiple_selectors_all_must_pass(env, clus):
    result = resolve_bindings(
        [_binding(env_labels={"env": "dev"}, cluster_labels={"tier": "prod"})],
        env,
        clus,
    )
    assert result == []


# ---------------------------------------------------------------------------
# resolve_bindings — precedence & capability deduplication
# ---------------------------------------------------------------------------


def test_resolve_lower_precedence_wins(env, clus):
    high = _binding(name="b-high", precedence=50)
    low = _binding(name="b-low", precedence=200)
    result = resolve_bindings([low, high], env, clus)
    assert len(result) == 1
    assert result[0].binding_name == "b-high"


def test_resolve_same_capability_single_winner(env, clus):
    b1 = _binding(name="b1", precedence=100, capability="net")
    b2 = _binding(name="b2", precedence=50, capability="net")
    result = resolve_bindings([b1, b2], env, clus)
    assert len(result) == 1
    assert result[0].binding_name == "b2"


def test_resolve_different_capabilities_both_returned(env, clus):
    b1 = _binding(name="b1", capability="networking")
    b2 = _binding(name="b2", capability="monitoring")
    result = resolve_bindings([b1, b2], env, clus)
    caps = {r.capability for r in result}
    assert caps == {"networking", "monitoring"}


def test_resolve_multiple_mappings_per_binding(env, clus):
    binding = {
        "metadata": {"name": "multi"},
        "spec": {
            "platformRef": {"name": "plat"},
            "selectors": {},
            "precedence": 100,
            "blueprintMappings": [
                {"capability": "networking", "blueprint": {"name": "vpc"}, "config": {}},
                {"capability": "monitoring", "blueprint": {"name": "prom"}, "config": {}},
            ],
        },
    }
    result = resolve_bindings([binding], env, clus)
    assert {r.capability for r in result} == {"networking", "monitoring"}


# ---------------------------------------------------------------------------
# resolve_bindings — field propagation
# ---------------------------------------------------------------------------


def test_resolve_blueprint_registry_propagated(env, clus):
    result = resolve_bindings([_binding(registry="my-registry")], env, clus)
    assert result[0].blueprint_registry == "my-registry"


def test_resolve_blueprint_registry_none_when_absent(env, clus):
    result = resolve_bindings([_binding()], env, clus)
    assert result[0].blueprint_registry is None


def test_resolve_config_propagated(env, clus):
    result = resolve_bindings([_binding(config={"cidr": "192.168.0.0/16"})], env, clus)
    assert result[0].merged_config == {"cidr": "192.168.0.0/16"}


def test_resolve_version_propagated(env, clus):
    result = resolve_bindings([_binding(version="2.3.1")], env, clus)
    assert result[0].blueprint_version == "2.3.1"


# ---------------------------------------------------------------------------
# resolve_bindings — cluster selector with empty cluster list
# ---------------------------------------------------------------------------


def test_resolve_cluster_selector_with_empty_cluster_list(env):
    """A clusterSelector against an empty cluster list should exclude the binding."""
    result = resolve_bindings([_binding(cluster_labels={"tier": "non-prod"})], env, [])
    assert result == []


def test_resolve_no_cluster_selector_with_empty_cluster_list(env):
    """No clusterSelector means the binding is not filtered, even with no clusters."""
    result = resolve_bindings([_binding()], env, [])
    assert len(result) == 1


def test_resolve_location_selector_with_empty_cluster_list(env):
    """A locationSelector against an empty cluster list should exclude the binding."""
    result = resolve_bindings([_binding(location_labels={"region": "us-east-1"})], env, [])
    assert result == []
