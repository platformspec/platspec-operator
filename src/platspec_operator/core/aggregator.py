"""Status aggregation to Platform Spec resources.

This module answers: "what is the overall state of this Platform?"

After all bindings have been resolved, executed, applied, and their status expressions
evaluated, this module rolls up the results into a single PlatformStatus that gets
written to Platform.status. It computes:

  - phase: a simple string (Ready / Progressing / Failed) summarising overall health
  - conditions: a Ready condition with a human-readable message and reason code
  - capabilities: the raw field→value maps from each capability's status expression evaluation

The logic is deliberately simple: a Platform is Ready when all bindings are Ready,
Progressing when some are still pending, and Failed when any have an Apply failure.
Status expression failures (None values in capabilities) do not contribute to phase —
they are informational and should not block a Platform from being Ready.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List

from ..models.platform import BindingStatus, PlatformStatus
from ..models.crd import Condition


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_condition(
    condition_type: str,
    status: str,
    reason: str,
    message: str,
) -> Condition:
    """Construct a Condition with the current timestamp."""
    return Condition(
        type=condition_type,
        status=status,
        reason=reason,
        message=message,
        lastTransitionTime=_now(),
    )


def aggregate_platform_status(
    platform_name: str,
    capability_results: Dict[str, Dict[str, Any]],
    binding_statuses: List[BindingStatus],
) -> PlatformStatus:
    """Compute the overall Platform status from individual binding outcomes.

    Readiness is determined by scanning each BindingStatus's conditions:
      - A binding is "ready" if it has a condition of type="Ready" with status="True".
      - A binding is "failed" if it has a condition of type="Applied" with status="False".

    Phase logic:
      - No bindings configured → Ready (NoBlueprintsConfigured): vacuously satisfied.
      - Any binding failed to apply → Failed (BindingsFailed).
      - All bindings are ready → Ready (AllBindingsReady).
      - Otherwise → Progressing (BindingsProgressing): some are still running.

    capability_results contains the raw field→value maps from status expression evaluation
    (e.g. {"namespace-bootstrap": {"ready": True, "phase": "MinimumReplicasAvailable"}}).
    These are written to Platform.status.capabilities as-is for consumers to read.
    """
    conditions: List[Condition] = []

    total = len(binding_statuses)
    # Count bindings that have explicitly reached Ready=True.
    ready = sum(
        1
        for bs in binding_statuses
        if any(
            c.type == "Ready" and c.status == "True" for c in bs.conditions
        )
    )
    # Count bindings that failed at the Apply step (resources couldn't be applied).
    failed = sum(
        1
        for bs in binding_statuses
        if any(
            c.type == "Applied" and c.status == "False" for c in bs.conditions
        )
    )

    # Determine phase and the Ready condition based on the counts above.
    if total == 0:
        phase = "Ready"
        ready_condition = _make_condition(
            "Ready", "True", "NoBlueprintsConfigured",
            "Platform has no blueprint bindings"
        )
    elif failed > 0:
        phase = "Failed"
        ready_condition = _make_condition(
            "Ready", "False", "BindingsFailed",
            f"{failed}/{total} binding(s) failed to apply"
        )
    elif ready == total:
        phase = "Ready"
        ready_condition = _make_condition(
            "Ready", "True", "AllBindingsReady",
            f"All {total} binding(s) are ready"
        )
    else:
        phase = "Progressing"
        ready_condition = _make_condition(
            "Ready", "False", "BindingsProgressing",
            f"{ready}/{total} binding(s) ready"
        )

    conditions.append(ready_condition)

    return PlatformStatus(
        phase=phase,
        conditions=conditions,
        # capability_results carries the raw evaluated status fields from each blueprint.
        # Consumers (e.g. the CLI, UI, or downstream automation) read these to get
        # values like vpcId, clusterEndpoint, etc. without parsing conditions.
        capabilities=capability_results,
        lastStatusUpdate=_now(),
    )
