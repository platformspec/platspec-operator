"""Tests for core/evaluator.py — status expression evaluation via sandboxed KCL.

kcl_lib is mocked throughout since the evaluator dynamically imports it inside the
function body, making it straightforward to patch at the module import level.
"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

from platspec_operator.core.evaluator import evaluate_status_expressions
from platspec_operator.models.blueprint import (
    BlueprintContext,
    PlatformMeta,
    StatusFieldSchema,
    StatusSchema,
)
from platspec_operator.models.infrastructure import EnvironmentSpec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context() -> BlueprintContext:
    return BlueprintContext(
        platform=PlatformMeta(name="test-platform", namespace="ns"),
        environment=EnvironmentSpec(type="local"),
        config={},
        overrides={},
    )


def _schema(*fields: tuple[str, str]) -> StatusSchema:
    """Build a StatusSchema from (field_name, expression) pairs."""
    return StatusSchema(
        fields={name: StatusFieldSchema(expression=expr) for name, expr in fields}
    )


def _mock_kcl_result(value: Any) -> MagicMock:
    result = MagicMock()
    result.err_message = ""
    result.yaml_result = yaml.dump({"result": value})
    return result


def _mock_kcl_error(message: str) -> MagicMock:
    result = MagicMock()
    result.err_message = message
    result.yaml_result = ""
    return result


# ---------------------------------------------------------------------------
# Empty schema
# ---------------------------------------------------------------------------


def test_evaluate_empty_schema():
    """Empty status schema produces an empty result dict without calling KCL."""
    ctx = _make_context()
    result = evaluate_status_expressions(StatusSchema(), {}, {}, ctx)
    assert result == {}


# ---------------------------------------------------------------------------
# Successful evaluation
# ---------------------------------------------------------------------------


def test_evaluate_single_field():
    ctx = _make_context()
    schema = _schema(("ready", "True"))

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.return_value = _mock_kcl_result(True)

        result = evaluate_status_expressions(schema, {}, {}, ctx)

    assert result["ready"] is True


def test_evaluate_integer_result():
    ctx = _make_context()
    schema = _schema(("replicas", "3"))

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.return_value = _mock_kcl_result(3)

        result = evaluate_status_expressions(schema, {}, {}, ctx)

    assert result["replicas"] == 3


def test_evaluate_string_result():
    ctx = _make_context()
    schema = _schema(("phase", '"Running"'))

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.return_value = _mock_kcl_result("Running")

        result = evaluate_status_expressions(schema, {}, {}, ctx)

    assert result["phase"] == "Running"


def test_evaluate_multiple_fields():
    ctx = _make_context()
    schema = _schema(("ready", "True"), ("replicas", "2"))
    call_count = 0

    def side_effect(args):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _mock_kcl_result(True)
        return _mock_kcl_result(2)

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.side_effect = side_effect

        result = evaluate_status_expressions(schema, {}, {}, ctx)

    assert set(result.keys()) == {"ready", "replicas"}


# ---------------------------------------------------------------------------
# Multi-line expressions
# ---------------------------------------------------------------------------


def test_evaluate_multiline_expression():
    """Multi-line expression: intermediate lines are assignments; last line is result."""
    ctx = _make_context()
    # Two-line expression: first line is an assignment, second is the result
    expr = "x = 5\nx + 1"
    schema = _schema(("answer", expr))

    captured_programs = []

    def capture(args):
        # Read the temp KCL file that was written
        kcl_file = args.k_filename_list[0]
        with open(kcl_file) as f:
            captured_programs.append(f.read())
        return _mock_kcl_result(6)

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.side_effect = capture

        evaluate_status_expressions(schema, {}, {}, ctx)

    program = captured_programs[0]
    # Intermediate assignment line should appear verbatim
    assert "x = 5" in program
    # Final line wrapped as result
    assert "result = x + 1" in program


# ---------------------------------------------------------------------------
# Error handling — errors do not fail the call
# ---------------------------------------------------------------------------


def test_evaluate_kcl_error_yields_none():
    ctx = _make_context()
    schema = _schema(("broken", "invalid!!"))

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.return_value = _mock_kcl_error("KCL parse error")

        result = evaluate_status_expressions(schema, {}, {}, ctx)

    assert result["broken"] is None


def test_evaluate_exception_yields_none():
    ctx = _make_context()
    schema = _schema(("boom", "1 / 0"))

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.side_effect = RuntimeError("unexpected crash")

        result = evaluate_status_expressions(schema, {}, {}, ctx)

    assert result["boom"] is None


def test_evaluate_one_error_does_not_block_other_fields():
    ctx = _make_context()
    schema = _schema(("broken", "bad!!"), ("ok", "42"))
    call_count = 0

    def side_effect(args):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _mock_kcl_error("error")
        return _mock_kcl_result(42)

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.side_effect = side_effect

        result = evaluate_status_expressions(schema, {}, {}, ctx)

    # Both fields should be present
    assert "broken" in result
    assert "ok" in result
    assert result["ok"] == 42


# ---------------------------------------------------------------------------
# Data bundling — childResources and config passed to KCL
# ---------------------------------------------------------------------------


def test_evaluate_child_resources_in_data_bundle():
    ctx = _make_context()
    schema = _schema(("count", "len(childResources)"))
    child_resources = {"v1/Namespace": [{"metadata": {"name": "test"}}]}

    captured_data_args = []

    def capture(args):
        for arg in args.args:
            if arg.name == "data":
                captured_data_args.append(arg.value)
        return _mock_kcl_result(1)

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.side_effect = capture

        evaluate_status_expressions(schema, child_resources, {}, ctx)

    import json
    data = json.loads(captured_data_args[0])
    assert "childResources" in data
    assert "v1/Namespace" in data["childResources"]


def test_evaluate_config_in_data_bundle():
    ctx = _make_context()
    schema = _schema(("replicas", "config.replicas"))
    config = {"replicas": 3}

    captured_data_args = []

    def capture(args):
        for arg in args.args:
            if arg.name == "data":
                captured_data_args.append(arg.value)
        return _mock_kcl_result(3)

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.side_effect = capture

        evaluate_status_expressions(schema, {}, config, ctx)

    import json
    data = json.loads(captured_data_args[0])
    assert data["config"]["replicas"] == 3


def test_evaluate_context_in_data_bundle():
    ctx = _make_context()
    schema = _schema(("platform", "context.platform.name"))

    captured_data_args = []

    def capture(args):
        for arg in args.args:
            if arg.name == "data":
                captured_data_args.append(arg.value)
        return _mock_kcl_result("test-platform")

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.side_effect = capture

        evaluate_status_expressions(schema, {}, {}, ctx)

    import json
    data = json.loads(captured_data_args[0])
    assert data["context"]["platform"]["name"] == "test-platform"
