"""Tests for core/aggregator.py — Platform status roll-up."""

import pytest

from platspec_operator.core.aggregator import aggregate_platform_status
from platspec_operator.models.crd import Condition
from platspec_operator.models.platform import BindingStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _binding(name: str, conditions: list[Condition]) -> BindingStatus:
    return BindingStatus(binding_name=name, capability="test", conditions=conditions)


def _ready_condition(status: str = "True") -> Condition:
    return Condition(type="Ready", status=status, reason="OK", lastTransitionTime="2024-01-01T00:00:00Z")


def _applied_condition(status: str = "True") -> Condition:
    return Condition(type="Applied", status=status, reason="OK", lastTransitionTime="2024-01-01T00:00:00Z")


# ---------------------------------------------------------------------------
# Phase computation
# ---------------------------------------------------------------------------


def test_no_bindings_is_ready():
    result = aggregate_platform_status("my-platform", {}, [])
    assert result.phase == "Ready"
    assert any(c.type == "Ready" and c.status == "True" for c in result.conditions)
    assert any(c.reason == "NoBlueprintsConfigured" for c in result.conditions)


def test_all_bindings_ready():
    bindings = [
        _binding("b1", [_ready_condition("True")]),
        _binding("b2", [_ready_condition("True")]),
    ]
    result = aggregate_platform_status("my-platform", {}, bindings)
    assert result.phase == "Ready"
    assert any(c.reason == "AllBindingsReady" for c in result.conditions)


def test_some_bindings_not_ready_is_progressing():
    bindings = [
        _binding("b1", [_ready_condition("True")]),
        _binding("b2", [_ready_condition("False")]),
    ]
    result = aggregate_platform_status("my-platform", {}, bindings)
    assert result.phase == "Progressing"
    assert any(c.reason == "BindingsProgressing" for c in result.conditions)


def test_all_bindings_not_ready_is_progressing():
    bindings = [
        _binding("b1", [_ready_condition("False")]),
        _binding("b2", [_ready_condition("False")]),
    ]
    result = aggregate_platform_status("my-platform", {}, bindings)
    assert result.phase == "Progressing"


def test_binding_with_applied_false_is_failed():
    bindings = [
        _binding("b1", [_applied_condition("False")]),
        _binding("b2", [_ready_condition("True")]),
    ]
    result = aggregate_platform_status("my-platform", {}, bindings)
    assert result.phase == "Failed"
    assert any(c.reason == "BindingsFailed" for c in result.conditions)


def test_failed_takes_priority_over_progressing():
    # One failed, one still progressing
    bindings = [
        _binding("b1", [_applied_condition("False")]),
        _binding("b2", [_ready_condition("False")]),
    ]
    result = aggregate_platform_status("my-platform", {}, bindings)
    assert result.phase == "Failed"


# ---------------------------------------------------------------------------
# Capabilities pass-through
# ---------------------------------------------------------------------------


def test_capabilities_passed_through():
    caps = {
        "networking": {"vpcId": "vpc-123", "ready": True},
        "monitoring": {"ready": False},
    }
    result = aggregate_platform_status("my-platform", caps, [])
    assert result.capabilities == caps


def test_capabilities_empty_by_default():
    result = aggregate_platform_status("my-platform", {}, [])
    assert result.capabilities == {}


# ---------------------------------------------------------------------------
# Condition structure
# ---------------------------------------------------------------------------


def test_ready_condition_always_present():
    result = aggregate_platform_status("my-platform", {}, [])
    ready_conds = [c for c in result.conditions if c.type == "Ready"]
    assert len(ready_conds) == 1


def test_ready_condition_has_timestamp():
    result = aggregate_platform_status("my-platform", {}, [])
    cond = next(c for c in result.conditions if c.type == "Ready")
    assert cond.last_transition_time is not None


def test_last_status_update_is_set():
    result = aggregate_platform_status("my-platform", {}, [])
    assert result.last_status_update is not None


def test_progressing_condition_message_includes_counts():
    bindings = [
        _binding("b1", [_ready_condition("True")]),
        _binding("b2", [_ready_condition("False")]),
    ]
    result = aggregate_platform_status("my-platform", {}, bindings)
    cond = next(c for c in result.conditions if c.type == "Ready")
    # Message should indicate partial progress, e.g. "1/2"
    assert "1" in cond.message
    assert "2" in cond.message


def test_binding_with_no_conditions_counts_as_not_ready():
    bindings = [_binding("b1", [])]
    result = aggregate_platform_status("my-platform", {}, bindings)
    assert result.phase == "Progressing"
