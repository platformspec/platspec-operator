"""Tests for operator behavior with broken blueprints.

Covers the two failure modes a blueprint can produce:

  1. KCL render failure — BlueprintExecutionError raised before any resources are
     applied. The operator records Rendered=False and returns early from _run_binding.
     The platform phase becomes Progressing (not Failed — failed is reserved for
     apply-level failures where we know the cluster state is inconsistent).

  2. Apply failure — KCL succeeds but the k8s server-side apply call raises. The
     operator records Applied=False and returns early. The platform phase becomes
     Failed.

In both cases the operator must not crash and must allow other bindings in the same
reconcile cycle to complete normally.

The broken-kcl-syntax blueprint in blueprints/broken-kcl-syntax/ exists as the
canonical fixture for these failure modes.
"""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from platspec_operator.core.aggregator import aggregate_platform_status
from platspec_operator.core.executor import BlueprintExecutionError
from platspec_operator.handlers.platform import _run_binding
from platspec_operator.models.blueprint import BlueprintContext, PlatformMeta, ResolvedBinding
from platspec_operator.models.crd import Condition
from platspec_operator.models.infrastructure import EnvironmentSpec
from platspec_operator.models.platform import BindingStatus

BROKEN_BLUEPRINT_DIR = (
    Path(__file__).parent.parent.parent.parent
    / "blueprints"
    / "broken-kcl-syntax"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _binding(name: str = "test-binding", blueprint: str = "broken-kcl-syntax") -> ResolvedBinding:
    return ResolvedBinding(
        binding_name=name,
        capability="test",
        blueprint_name=blueprint,
        blueprint_version="0.1.0",
        merged_config={},
    )


def _ctx() -> BlueprintContext:
    return BlueprintContext(
        platform=PlatformMeta(name="test-platform", namespace="platspec-system"),
        environment=EnvironmentSpec(type="local", name="local"),
        config={},
        overrides={},
    )


def _k8s() -> Any:
    return MagicMock()


def _config() -> Any:
    cfg = MagicMock()
    cfg.blueprint.kcl_timeout = 30
    cfg.operator.field_manager = "platspec-operator"
    return cfg


def _fetcher() -> Any:
    return MagicMock()


def _good_output() -> Any:
    output = MagicMock()
    output.resources = []
    output.status_schema = MagicMock()
    output.status_schema.fields = {}
    return output


# ---------------------------------------------------------------------------
# KCL render failure
# ---------------------------------------------------------------------------


async def test_render_failure_sets_rendered_false():
    """BlueprintExecutionError → Rendered=False condition, no Applied or Ready."""
    with patch(
        "platspec_operator.handlers.platform.execute_blueprint",
        side_effect=BlueprintExecutionError("syntax error at line 2"),
    ):
        bs = await _run_binding(
            _binding(), _ctx(), [], _k8s(), _config(), _fetcher(),
            "test-platform", {},
        )

    rendered = next((c for c in bs.conditions if c.type == "Rendered"), None)
    assert rendered is not None
    assert rendered.status == "False"
    assert "syntax error" in rendered.message

    # Early return — no apply attempted, no Ready condition.
    assert not any(c.type == "Applied" for c in bs.conditions)
    assert not any(c.type == "Ready" for c in bs.conditions)


async def test_render_failure_includes_error_message():
    """The Rendered=False condition message contains the KCL error text."""
    error_text = "KCLError: undefined variable 'undefined_variable' at main.k:6"
    with patch(
        "platspec_operator.handlers.platform.execute_blueprint",
        side_effect=BlueprintExecutionError(error_text),
    ):
        bs = await _run_binding(
            _binding(), _ctx(), [], _k8s(), _config(), _fetcher(),
            "test-platform", {},
        )

    rendered = next(c for c in bs.conditions if c.type == "Rendered")
    assert error_text in rendered.message


def test_render_failure_yields_progressing_platform():
    """Rendered=False in a BindingStatus → platform phase=Progressing (not Failed).

    Failed is reserved for apply-level failures (Applied=False). A render failure
    means the operator couldn't determine what to apply — the cluster state is
    unchanged, so the platform is Progressing toward a known-good state once the
    blueprint is fixed.
    """
    bs = BindingStatus(binding_name="b1", capability="test")
    bs.conditions.append(
        Condition(
            type="Rendered", status="False", reason="KCLFailure",
            message="syntax error", lastTransitionTime="2024-01-01T00:00:00Z",
        )
    )

    result = aggregate_platform_status("my-platform", {}, [bs])
    assert result.phase == "Progressing"


# ---------------------------------------------------------------------------
# Broken blueprint fixture — main.k is present and intentionally invalid
# ---------------------------------------------------------------------------


def test_broken_blueprint_fixture_exists():
    """Confirm the broken-kcl-syntax blueprint directory and main.k are present."""
    assert BROKEN_BLUEPRINT_DIR.is_dir(), (
        f"broken-kcl-syntax blueprint directory not found at {BROKEN_BLUEPRINT_DIR}"
    )
    assert (BROKEN_BLUEPRINT_DIR / "main.k").exists()


def test_broken_blueprint_main_k_has_syntax_error():
    """main.k must contain content that KCL cannot parse — not a valid program."""
    content = (BROKEN_BLUEPRINT_DIR / "main.k").read_text()
    # The file should not be empty — it should have the broken content.
    assert content.strip(), "broken-kcl-syntax/main.k must not be empty"
    # Confirm it looks broken (not accidentally valid KCL-compatible syntax).
    assert "???" in content or "+++" in content or "undefined_variable" in content


async def test_render_failure_via_fetcher_pointing_to_broken_blueprint(tmp_path):
    """When the fetcher points to the real broken-kcl-syntax dir, KCL raises.

    Uses the actual broken blueprint fixture with a mocked KCL result simulating
    the error the real KCL binary would produce for the invalid syntax.
    """
    fetcher = MagicMock()
    fetcher.fetch.return_value = BROKEN_BLUEPRINT_DIR

    with patch(
        "platspec_operator.handlers.platform.execute_blueprint",
        side_effect=BlueprintExecutionError(
            "KCLError: compile error in broken-kcl-syntax/main.k"
        ),
    ):
        bs = await _run_binding(
            _binding(), _ctx(), [], _k8s(), _config(), fetcher,
            "test-platform", {},
        )

    assert any(c.type == "Rendered" and c.status == "False" for c in bs.conditions)


# ---------------------------------------------------------------------------
# Apply failure
# ---------------------------------------------------------------------------


async def test_apply_failure_sets_applied_false():
    """k8s apply raises → Applied=False condition, no Ready."""
    with (
        patch(
            "platspec_operator.handlers.platform.execute_blueprint",
            return_value=_good_output(),
        ),
        patch(
            "platspec_operator.handlers.platform.apply_output_resources",
            side_effect=Exception("forbidden: cannot create namespaces"),
        ),
    ):
        bs = await _run_binding(
            _binding(), _ctx(), [], _k8s(), _config(), _fetcher(),
            "test-platform", {},
        )

    applied = next((c for c in bs.conditions if c.type == "Applied"), None)
    assert applied is not None
    assert applied.status == "False"
    assert "forbidden" in applied.message
    assert not any(c.type == "Ready" for c in bs.conditions)


def test_apply_failure_yields_failed_platform():
    """Applied=False in a BindingStatus → platform phase=Failed."""
    bs = BindingStatus(binding_name="b1", capability="test")
    bs.conditions.append(
        Condition(
            type="Applied", status="False", reason="ApplyFailure",
            message="forbidden", lastTransitionTime="2024-01-01T00:00:00Z",
        )
    )

    result = aggregate_platform_status("my-platform", {}, [bs])
    assert result.phase == "Failed"


# ---------------------------------------------------------------------------
# Mixed: broken + healthy bindings in the same reconcile
# ---------------------------------------------------------------------------


async def test_broken_binding_does_not_block_healthy_binding():
    """A render failure in one binding does not prevent other bindings from running.

    The platform handler calls _run_binding per binding and collects all results.
    A failure in one should not raise — it records conditions and returns early so
    the loop continues with the next binding.
    """
    broken = _binding(name="broken-binding", blueprint="broken-kcl-syntax")
    healthy = ResolvedBinding(
        binding_name="healthy-binding",
        capability="healthy",
        blueprint_name="good-blueprint",
        blueprint_version="0.1.0",
        merged_config={},
    )

    def _execute_side_effect(**kwargs: Any) -> Any:
        if kwargs.get("blueprint_name") == "broken-kcl-syntax":
            raise BlueprintExecutionError("syntax error")
        return _good_output()

    capability_results: dict = {}

    with patch(
        "platspec_operator.handlers.platform.execute_blueprint",
        side_effect=_execute_side_effect,
    ):
        bs_broken = await _run_binding(
            broken, _ctx(), [], _k8s(), _config(), _fetcher(),
            "test-platform", capability_results,
        )

        with patch(
            "platspec_operator.handlers.platform.apply_output_resources",
            return_value=[],
        ):
            bs_healthy = await _run_binding(
                healthy, _ctx(), [], _k8s(), _config(), _fetcher(),
                "test-platform", capability_results,
            )

    assert any(c.type == "Rendered" and c.status == "False" for c in bs_broken.conditions)
    assert any(c.type == "Ready" and c.status == "True" for c in bs_healthy.conditions)


def test_broken_and_healthy_binding_yields_progressing():
    """One binding with Rendered=False + one Ready → platform phase=Progressing."""
    bs_broken = BindingStatus(binding_name="broken", capability="test")
    bs_broken.conditions.append(
        Condition(
            type="Rendered", status="False", reason="KCLFailure",
            message="syntax error", lastTransitionTime="2024-01-01T00:00:00Z",
        )
    )

    bs_healthy = BindingStatus(binding_name="healthy", capability="test")
    bs_healthy.conditions.append(
        Condition(
            type="Ready", status="True", reason="Reconciled",
            message="ok", lastTransitionTime="2024-01-01T00:00:00Z",
        )
    )

    result = aggregate_platform_status("my-platform", {}, [bs_broken, bs_healthy])
    assert result.phase == "Progressing"
