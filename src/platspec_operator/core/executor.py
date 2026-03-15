"""KCL blueprint execution.

This module answers: "given a BlueprintContext, what Kubernetes resources should be created?"

It locates the blueprint via the BlueprintFetcher (which handles local filesystem and
remote registry backends), loads its metadata from blueprint.yaml, injects the
BlueprintContext as a KCL option, runs the KCL interpreter, and parses the output into
a BlueprintOutput (list of k8s manifests + status schema for post-apply evaluation).

KCL is a configuration language designed for Kubernetes resource generation. Blueprints
are pure KCL programs: they read option("context") and produce either a list of manifests
or a dict with a "resources" key. The operator treats blueprints as black boxes — it does
not inspect or validate the KCL source beyond running it.

Blueprint location is handled entirely by BlueprintFetcher. The executor only sees a
local filesystem path — it is always a directory containing main.k.
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING

import yaml
from loguru import logger

from ..models.blueprint import BlueprintContext, BlueprintOutput, StatusSchema

if TYPE_CHECKING:
    from .fetcher import BlueprintFetcher


class BlueprintExecutionError(Exception):
    pass


def _run_kcl(main_k: Path, context_data: Dict[str, Any]) -> str:
    """Execute a single KCL file with the BlueprintContext injected as option("context").

    The context is serialised to JSON and passed via the KCL Argument mechanism, which
    makes it available in KCL as option("context"). The KCL runtime parses the JSON
    automatically — blueprints receive a native KCL dict, not a JSON string.

    Returns the raw YAML string produced by KCL (the blueprint's stdout equivalent).
    Raises BlueprintExecutionError if KCL reports any error.
    """
    import kcl_lib.api as kcl_api

    ctx_json = json.dumps(context_data, ensure_ascii=True)
    args = kcl_api.ExecProgram_Args(
        k_filename_list=[str(main_k)],
        args=[kcl_api.Argument(name="context", value=ctx_json)],
    )
    result = kcl_api.API().exec_program(args)

    if result.err_message:
        raise BlueprintExecutionError(result.err_message)
    return result.yaml_result


def execute_blueprint(
    fetcher: "BlueprintFetcher",
    blueprint_name: str,
    blueprint_version: str,
    context: BlueprintContext,
    blueprint_registry: Optional[str] = None,
    timeout: int = 30,
) -> BlueprintOutput:
    """Locate and execute a KCL blueprint, returning its output manifests + status schema.

    Steps:
      1. Fetch the blueprint directory via the BlueprintFetcher. The fetcher handles
         local filesystem lookup, remote registry pull, and caching transparently.
      2. Load blueprint.yaml to extract the status.fields schema (used later by the
         evaluator to know what expressions to run against applied resources).
      3. Serialise the BlueprintContext to JSON and pass it to the KCL interpreter.
      4. Parse the KCL YAML output into a list of k8s manifests.

    blueprint.yaml status.fields format: the canonical format in blueprint.yaml is a
    list of {field, type, description, expr} entries. The internal StatusSchema uses a
    dict keyed by field name. This function converts between the two.

    KCL output format: blueprints may return either:
      - A list of manifests (the common case: `resources = [...]`)
      - A dict with a "resources" key containing the list
      - A single manifest dict (treated as a one-element list)

    Raises BlueprintExecutionError on any failure (blueprint not found, fetch error,
    KCL error, unexpected output format).
    """
    from .fetcher import BlueprintFetchError

    try:
        bp_path = fetcher.fetch(blueprint_name, blueprint_version, blueprint_registry)
    except BlueprintFetchError as e:
        raise BlueprintExecutionError(str(e)) from e

    main_k = bp_path / "main.k"
    if not main_k.exists():
        raise BlueprintExecutionError(f"Blueprint entry point not found: {main_k}")

    # Load the status schema from blueprint.yaml. The status.fields section tells
    # the evaluator what KCL expressions to run against the applied resources after
    # they're created — e.g. "is the Deployment ready?", "what is the VPC ID?".
    blueprint_yaml = bp_path / "blueprint.yaml"
    status_schema = StatusSchema()
    if blueprint_yaml.exists():
        with open(blueprint_yaml) as f:
            bp_meta: Dict[str, Any] = yaml.safe_load(f) or {}
        status_fields = bp_meta.get("status", {}).get("fields", {})
        if status_fields:
            # blueprint.yaml uses a list [{field, expr, description, ...}];
            # StatusSchema expects a dict {field_name: {expression, description}}.
            # Convert here so the rest of the pipeline only sees the dict form.
            if isinstance(status_fields, list):
                status_fields = {
                    entry["field"]: {
                        "expression": entry.get("expr", entry.get("expression", "")),
                        "description": entry.get("description"),
                    }
                    for entry in status_fields
                    if "field" in entry
                }
            status_schema = StatusSchema.model_validate({"fields": status_fields})

    logger.info(f"Executing blueprint {blueprint_name}@{blueprint_version}")

    try:
        # Serialise the full BlueprintContext to a dict and run KCL.
        # by_alias=True ensures camelCase field names (e.g. providerRefs, not provider_refs)
        # which matches what KCL templates expect to receive.
        # exclude_none=True drops None-valued optional fields — pydantic v2 can't build a
        # SchemaSerializer for NoneType, and KCL blueprints handle absent keys via ?. anyway.
        yaml_result = _run_kcl(main_k, context.model_dump(by_alias=True, exclude_none=True))

        # Parse the KCL YAML output into Python objects.
        output_data: Any = yaml.safe_load(yaml_result)
        if output_data is None:
            # Blueprint produced no output (empty file or all-comment KCL). Treat as
            # zero resources — not an error; some blueprints are conditional.
            resources = []
        elif isinstance(output_data, list):
            # Blueprint returned a top-level list — each item is a k8s manifest.
            resources = output_data
        elif isinstance(output_data, dict):
            # Blueprint returned a dict. If it has a "resources" key, use that list.
            # Otherwise treat the whole dict as a single manifest.
            resources = output_data.get("resources", [output_data])
        else:
            resources = []
    except BlueprintExecutionError:
        raise
    except Exception as e:
        import traceback
        logger.error(f"Blueprint {blueprint_name} full traceback:\n{traceback.format_exc()}")
        raise BlueprintExecutionError(
            f"Failed to execute blueprint {blueprint_name}: {e}"
        ) from e

    logger.info(f"Blueprint {blueprint_name} produced {len(resources)} resources")
    return BlueprintOutput(resources=resources, status_schema=status_schema)
