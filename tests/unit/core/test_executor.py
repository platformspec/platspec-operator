"""Tests for core/executor.py — KCL blueprint execution.

KCL execution is mocked via pytest-mock to avoid requiring the kcl binary in CI.
Tests that need real KCL output are marked with @pytest.mark.kcl and skipped unless
KCL is available.
"""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

from platspec_operator.core.executor import BlueprintExecutionError, execute_blueprint
from platspec_operator.models.blueprint import BlueprintContext, PlatformMeta
from platspec_operator.models.infrastructure import EnvironmentSpec

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "blueprints"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(config: dict | None = None) -> BlueprintContext:
    return BlueprintContext(
        platform=PlatformMeta(name="test-platform", namespace="platspec-system"),
        environment=EnvironmentSpec(type="local", name="local"),
        config=config or {},
        overrides={},
    )


def _make_fetcher(path: Path) -> Any:
    """Return a mock BlueprintFetcher that always resolves to the given path."""
    fetcher = MagicMock()
    fetcher.fetch.return_value = path
    return fetcher


def _kcl_result(resources: list) -> Any:
    """Build a mock kcl_api.API().exec_program() return value."""
    result = MagicMock()
    result.err_message = ""
    result.yaml_result = yaml.dump({"resources": resources})
    return result


# ---------------------------------------------------------------------------
# Blueprint not found
# ---------------------------------------------------------------------------


def test_execute_blueprint_not_found(tmp_path):
    from platspec_operator.core.fetcher import BlueprintFetchError

    fetcher = MagicMock()
    fetcher.fetch.side_effect = BlueprintFetchError("not found")
    ctx = _make_context()

    with pytest.raises(BlueprintExecutionError, match="not found"):
        execute_blueprint(fetcher, "missing", "latest", ctx)


def test_execute_blueprint_missing_main_k(tmp_path):
    """Fetcher returns a dir without main.k → BlueprintExecutionError."""
    bp_dir = tmp_path / "empty-blueprint"
    bp_dir.mkdir()
    fetcher = _make_fetcher(bp_dir)
    ctx = _make_context()

    with pytest.raises(BlueprintExecutionError, match="Blueprint entry point not found"):
        execute_blueprint(fetcher, "empty", "latest", ctx)


# ---------------------------------------------------------------------------
# Successful execution — output parsing
# ---------------------------------------------------------------------------


def test_execute_blueprint_list_output(tmp_path):
    """Blueprint returns a top-level list of manifests."""
    bp_dir = tmp_path / "list-blueprint"
    bp_dir.mkdir()
    (bp_dir / "main.k").write_text("resources = []")

    ns_manifest = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}}
    fetcher = _make_fetcher(bp_dir)
    ctx = _make_context()

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        result_obj = MagicMock()
        result_obj.err_message = ""
        result_obj.yaml_result = yaml.dump([ns_manifest])
        instance.exec_program.return_value = result_obj

        output = execute_blueprint(fetcher, "list-bp", "latest", ctx)

    assert len(output.resources) == 1
    assert output.resources[0]["kind"] == "Namespace"


def test_execute_blueprint_dict_with_resources_key(tmp_path):
    """Blueprint returns a dict with a 'resources' key."""
    bp_dir = tmp_path / "dict-blueprint"
    bp_dir.mkdir()
    (bp_dir / "main.k").write_text("resources = []")

    ns_manifest = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}}
    fetcher = _make_fetcher(bp_dir)
    ctx = _make_context()

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        result_obj = MagicMock()
        result_obj.err_message = ""
        result_obj.yaml_result = yaml.dump({"resources": [ns_manifest]})
        instance.exec_program.return_value = result_obj

        output = execute_blueprint(fetcher, "dict-bp", "latest", ctx)

    assert len(output.resources) == 1


def test_execute_blueprint_single_manifest_dict(tmp_path):
    """Blueprint returns a single manifest dict without a 'resources' key."""
    bp_dir = tmp_path / "single-blueprint"
    bp_dir.mkdir()
    (bp_dir / "main.k").write_text("apiVersion = 'v1'")

    manifest = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cfg"}}
    fetcher = _make_fetcher(bp_dir)
    ctx = _make_context()

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        result_obj = MagicMock()
        result_obj.err_message = ""
        result_obj.yaml_result = yaml.dump(manifest)
        instance.exec_program.return_value = result_obj

        output = execute_blueprint(fetcher, "single-bp", "latest", ctx)

    assert len(output.resources) == 1
    assert output.resources[0]["kind"] == "ConfigMap"


def test_execute_blueprint_empty_output(tmp_path):
    """Blueprint produces no output (returns None from yaml.safe_load)."""
    bp_dir = tmp_path / "empty-blueprint"
    bp_dir.mkdir()
    (bp_dir / "main.k").write_text("# empty")

    fetcher = _make_fetcher(bp_dir)
    ctx = _make_context()

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        result_obj = MagicMock()
        result_obj.err_message = ""
        result_obj.yaml_result = ""
        instance.exec_program.return_value = result_obj

        output = execute_blueprint(fetcher, "empty-bp", "latest", ctx)

    assert output.resources == []


# ---------------------------------------------------------------------------
# KCL error handling
# ---------------------------------------------------------------------------


def test_execute_blueprint_kcl_error(tmp_path):
    bp_dir = tmp_path / "error-blueprint"
    bp_dir.mkdir()
    (bp_dir / "main.k").write_text("bad kcl syntax !!!")

    fetcher = _make_fetcher(bp_dir)
    ctx = _make_context()

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        result_obj = MagicMock()
        result_obj.err_message = "syntax error at line 1"
        result_obj.yaml_result = ""
        instance.exec_program.return_value = result_obj

        with pytest.raises(BlueprintExecutionError, match="syntax error"):
            execute_blueprint(fetcher, "error-bp", "latest", ctx)


# ---------------------------------------------------------------------------
# Status schema loading
# ---------------------------------------------------------------------------


def test_execute_blueprint_loads_status_schema(tmp_path):
    """Blueprint with blueprint.yaml — status_schema fields are loaded."""
    bp_dir = tmp_path / "schema-blueprint"
    bp_dir.mkdir()
    (bp_dir / "main.k").write_text("resources = []")
    (bp_dir / "blueprint.yaml").write_text(
        "status:\n  fields:\n    - field: ready\n      expr: 'True'\n"
    )

    fetcher = _make_fetcher(bp_dir)
    ctx = _make_context()

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        result_obj = MagicMock()
        result_obj.err_message = ""
        result_obj.yaml_result = yaml.dump([])
        instance.exec_program.return_value = result_obj

        output = execute_blueprint(fetcher, "schema-bp", "latest", ctx)

    assert "ready" in output.status_schema.fields
    assert output.status_schema.fields["ready"].expression == "True"


def test_execute_blueprint_no_blueprint_yaml_gives_empty_schema(tmp_path):
    bp_dir = tmp_path / "no-schema-blueprint"
    bp_dir.mkdir()
    (bp_dir / "main.k").write_text("resources = []")

    fetcher = _make_fetcher(bp_dir)
    ctx = _make_context()

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        result_obj = MagicMock()
        result_obj.err_message = ""
        result_obj.yaml_result = yaml.dump([])
        instance.exec_program.return_value = result_obj

        output = execute_blueprint(fetcher, "no-schema-bp", "latest", ctx)

    assert output.status_schema.fields == {}


# ---------------------------------------------------------------------------
# Context serialisation — by_alias=True
# ---------------------------------------------------------------------------


def test_execute_blueprint_passes_camel_case_context(tmp_path):
    """Context passed to KCL uses camelCase keys (by_alias=True)."""
    bp_dir = tmp_path / "ctx-blueprint"
    bp_dir.mkdir()
    (bp_dir / "main.k").write_text("resources = []")

    fetcher = _make_fetcher(bp_dir)
    ctx = _make_context()

    captured_args = []

    def capture(args):
        captured_args.append(args)
        result_obj = MagicMock()
        result_obj.err_message = ""
        result_obj.yaml_result = yaml.dump([])
        return result_obj

    with patch("kcl_lib.api.API") as mock_api_cls:
        instance = mock_api_cls.return_value
        instance.exec_program.side_effect = capture

        execute_blueprint(fetcher, "ctx-bp", "latest", ctx)

    assert len(captured_args) == 1
    import json
    context_arg = next(a for a in captured_args[0].args if a.name == "context")
    context_data = json.loads(context_arg.value)
    # Top-level key should be camelCase-serialised fields like "platform"
    assert "platform" in context_data
