"""Periodic status re-evaluation timer for Platforms.

kopf's event model is edge-triggered: handlers fire when a watched resource changes.
This works well for Platform and BlueprintBinding changes, but there is a gap: when an
external Operator updates the status of an output resource (e.g. Deployment.status.
readyReplicas increases as pods come up), no kopf event fires on the Platform. The
Platform's status expressions would therefore never be re-evaluated after initial apply.

This module closes that gap with a timer. Every `reconcile_interval` seconds (default
300s, configured in operator.reconcile_interval), the timer fires the full _reconcile
loop for each active Platform. This ensures:

  1. Status expressions are periodically re-evaluated even when no k8s events occur.
  2. Any drift between desired and actual state is detected and corrected.
  3. The operator is eventually consistent even in the face of missed events.

The timer calls the same _reconcile function as the on.create/on.update handlers,
so there is no separate "status-only" code path — reconciliation always runs the full
loop (discover → validate → resolve → execute → apply → evaluate → aggregate).

initial_delay=30.0 prevents the timer from firing immediately on startup before the
operator has finished its initial reconciliation pass for all existing Platforms.
"""

import os
from typing import Any, Dict

import kopf
from loguru import logger

from ..handlers.platform import _reconcile

_GROUP = "core.platformspec.io"
_VERSION = "v1alpha1"

# Timer interval in seconds. kopf requires this to be known at import time (it
# is baked into the decorator), so we read PLATSPEC_OPERATOR_RECONCILE_INTERVAL
# from the environment directly here rather than going through Config.load().
# Default: 30s — fast enough to detect a ready Deployment within one pod
# startup cycle, without hammering the API server.
_TIMER_INTERVAL = float(
    os.environ.get("PLATSPEC_OPERATOR_RECONCILE_INTERVAL", "30")
)


@kopf.timer(
    _GROUP,
    _VERSION,
    "platforms",
    interval=_TIMER_INTERVAL,
    initial_delay=30.0,  # wait 30s after startup before first firing
)
async def platform_status_timer(
    spec: Dict[str, Any],
    meta: Dict[str, Any],
    status: Dict[str, Any],
    patch: kopf.Patch,
    memo: kopf.Memo,
    **kwargs: Any,
) -> None:
    """Periodically re-run the full Platform reconciliation loop.

    Fires for every Platform resource in the cluster on the configured interval.
    This is the mechanism by which the operator discovers changes in output resource
    status (e.g. a Deployment becoming ready) that did not trigger a kopf event on
    the Platform itself.

    Exceptions are caught and logged as warnings rather than raised. A failed timer
    cycle is not fatal — the timer will fire again on the next interval, and the
    on.update handler will catch any spec-driven changes in the meantime.
    """
    name = meta["name"]
    namespace = meta.get("namespace", "")
    resource_ref = f"Platform/{namespace}/{name}" if namespace else f"Platform/{name}"

    with logger.contextualize(resource=resource_ref):
        logger.debug(f"Status timer firing for Platform {namespace}/{name}")
        try:
            await _reconcile(
                name=name,
                namespace=namespace,
                spec=spec,
                patch=patch,
                memo=memo,
            )
        except Exception as e:
            # Log and swallow — a timer failure should not crash the operator or
            # prevent other Platforms' timers from firing.
            logger.warning(f"Status timer reconcile failed for Platform {name}: {e}")
