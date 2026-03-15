"""Tests for handlers/platform.py — _topo_sort() dependency ordering."""

import pytest

from platspec_operator.handlers.platform import _topo_sort
from platspec_operator.models.blueprint import ResolvedBinding


def _binding(capability: str, requires: list[str] | None = None) -> ResolvedBinding:
    return ResolvedBinding(
        binding_name=f"binding-{capability}",
        capability=capability,
        blueprint_name=f"bp-{capability}",
        blueprint_version="1.0",
        requires=requires or [],
    )


# ---------------------------------------------------------------------------
# Basic ordering
# ---------------------------------------------------------------------------


def test_topo_sort_no_deps_preserves_original_order():
    bindings = [_binding("a"), _binding("b"), _binding("c")]
    result = _topo_sort(bindings)
    assert [r.capability for r in result] == ["a", "b", "c"]


def test_topo_sort_single_dep():
    # b requires a → a must come first
    bindings = [_binding("b", requires=["a"]), _binding("a")]
    result = _topo_sort(bindings)
    caps = [r.capability for r in result]
    assert caps.index("a") < caps.index("b")


def test_topo_sort_chain():
    # c requires b, b requires a → a, b, c
    bindings = [_binding("c", requires=["b"]), _binding("b", requires=["a"]), _binding("a")]
    result = _topo_sort(bindings)
    caps = [r.capability for r in result]
    assert caps.index("a") < caps.index("b") < caps.index("c")


def test_topo_sort_diamond():
    # d requires b and c; b requires a; c requires a
    bindings = [
        _binding("d", requires=["b", "c"]),
        _binding("b", requires=["a"]),
        _binding("c", requires=["a"]),
        _binding("a"),
    ]
    result = _topo_sort(bindings)
    caps = [r.capability for r in result]
    assert caps.index("a") < caps.index("b")
    assert caps.index("a") < caps.index("c")
    assert caps.index("b") < caps.index("d")
    assert caps.index("c") < caps.index("d")


def test_topo_sort_external_dep_ignored():
    # b requires "external" which is not in the binding set — should not affect ordering.
    bindings = [_binding("a"), _binding("b", requires=["external-cap"])]
    result = _topo_sort(bindings)
    assert len(result) == 2  # Both included, external dep ignored


# ---------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------


def test_topo_sort_cycle_raises():
    # a requires b, b requires a → cycle
    bindings = [_binding("a", requires=["b"]), _binding("b", requires=["a"])]
    with pytest.raises(ValueError, match="Circular dependency"):
        _topo_sort(bindings)


def test_topo_sort_three_way_cycle_raises():
    bindings = [
        _binding("x", requires=["z"]),
        _binding("y", requires=["x"]),
        _binding("z", requires=["y"]),
    ]
    with pytest.raises(ValueError, match="Circular dependency"):
        _topo_sort(bindings)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_topo_sort_empty():
    assert _topo_sort([]) == []


def test_topo_sort_single_binding():
    result = _topo_sort([_binding("solo")])
    assert len(result) == 1
    assert result[0].capability == "solo"


def test_topo_sort_duplicate_capability_raises():
    """_topo_sort requires unique capabilities. Duplicate entries are a caller bug.
    The no-environment path in platform.py must deduplicate before calling _topo_sort.
    """
    bindings = [_binding("dup"), _binding("dup")]
    with pytest.raises(ValueError):
        _topo_sort(bindings)


def test_topo_sort_returns_all_bindings():
    bindings = [_binding("a"), _binding("b", requires=["a"]), _binding("c")]
    result = _topo_sort(bindings)
    assert len(result) == 3
    assert {r.capability for r in result} == {"a", "b", "c"}
