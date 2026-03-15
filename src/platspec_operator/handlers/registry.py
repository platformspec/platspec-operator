"""BlueprintRegistry CRD handler — keeps memo["registries"] live.

BlueprintRegistry resources are watched by this module. On create or update,
the registry spec is stored in memo["registries"] keyed by the resource name.
On delete, it is removed. Every Platform reconcile reads this dict via the
BlueprintFetcher to resolve remote blueprint locations.

memo["registries"] is initialised as an empty dict in handlers/startup.py so
the fetcher always has a valid dict to work with, even before any registries
are defined.
"""

from typing import Any, Dict

import kopf
from loguru import logger

_GROUP = "core.platformspec.io"
_VERSION = "v1alpha1"


@kopf.on.resume(_GROUP, _VERSION, "blueprintregistries")
@kopf.on.create(_GROUP, _VERSION, "blueprintregistries")
@kopf.on.update(_GROUP, _VERSION, "blueprintregistries")
async def registry_changed(
    name: str,
    spec: Dict[str, Any],
    memo: kopf.Memo,
    **kwargs: Any,
) -> None:
    """Register or update a BlueprintRegistry in the in-memory registry map."""
    memo.setdefault("registries", {})[name] = dict(spec)
    logger.info(
        f"BlueprintRegistry '{name}' registered (type={spec.get('type')}, "
        f"url={spec.get('url')})"
    )


@kopf.on.delete(_GROUP, _VERSION, "blueprintregistries")
async def registry_deleted(
    name: str,
    memo: kopf.Memo,
    **kwargs: Any,
) -> None:
    """Remove a BlueprintRegistry from the in-memory registry map."""
    memo.get("registries", {}).pop(name, None)
    logger.info(f"BlueprintRegistry '{name}' removed")
