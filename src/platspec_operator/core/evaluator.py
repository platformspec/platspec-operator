"""Status expression evaluation via sandboxed KCL.

This module answers: "now that the resources are applied, what is their status?"

After the operator applies a blueprint's output resources to the cluster, it fetches each
resource back to get its live state (e.g. Deployment.status.readyReplicas, which the
cluster populates asynchronously). The blueprint author defines KCL expressions in
blueprint.yaml status.fields to extract the values they care about from those live
resources. This module evaluates those expressions.

Why KCL for expressions? Because the blueprint already uses KCL for resource generation,
so authors get the same language for both generation and status extraction. KCL also
supports safe field access (e.g. deploy?.status?.readyReplicas to avoid null pointer
errors) that makes it practical for querying live resource state.

How expressions work:
  Each expression can span multiple lines. All lines except the last are intermediate
  variable assignments (e.g. `deploy = childResources["apps/v1/Deployment"][0]`). Only
  the final line is the return value and gets wrapped as `result = <expr>`.

  The evaluator writes a small KCL program to a temp file, injects the context data via
  option("data") (which the KCL runtime parses from JSON — no manual json.decode needed),
  and reads back the `result` key from the YAML output.

childResources key format: "apiVersion/kind" (e.g. "apps/v1/Deployment", "v1/Namespace").
Values are lists of full live k8s resource dicts, allowing expressions to subscript with
[0] to get the first (usually only) instance.
"""

import json
import os
import tempfile
from typing import Any, Dict

import yaml
from loguru import logger

from ..models.blueprint import BlueprintContext, StatusSchema


def evaluate_status_expressions(
    status_schema: StatusSchema,
    child_resources: Dict[str, Any],
    config: Dict[str, Any],
    context: BlueprintContext,
) -> Dict[str, Any]:
    """Evaluate each KCL expression in status_schema.fields against live resources.

    For each field defined in the blueprint's status.fields section, this function:
      1. Builds a small KCL program that:
           - Reads the shared data bundle from option("data") — already parsed by KCL
           - Binds childResources, config, context to local variables
           - Emits any intermediate assignment lines from the expression unchanged
           - Wraps only the last line as `result = <expr>`
      2. Writes that program to a temporary file and runs it via kcl-lib.
      3. Parses the `result` key from the YAML output.

    Expression errors (KCL parse/runtime errors, missing resources, etc.) yield None
    for that field and log a warning. They do NOT fail the reconciliation — a blueprint
    should never prevent a platform from reaching Ready just because a status expression
    is still pending (e.g. the resource is still being created).

    Returns a dict of {field_name: evaluated_value} for all fields in the schema.
    """
    import kcl_lib.api as kcl_api

    results: Dict[str, Any] = {}

    if not status_schema.fields:
        return results

    # Bundle all the data the KCL expressions might need into a single JSON object.
    # We pass this as a single option("data") argument rather than multiple separate
    # options so that adding new context fields doesn't require changing the injector.
    ctx_data = {
        "childResources": child_resources,
        "config": config,
        "context": context.model_dump(by_alias=True),
    }
    ctx_json = json.dumps(ctx_data, ensure_ascii=True)

    for field_name, field_schema in status_schema.fields.items():
        expression = field_schema.expression.rstrip()

        # Split multi-line expression into body (intermediate assignments) and the
        # final line (the return value). Only the final line gets `result = ` prepended.
        # Example multi-line expression:
        #   deploy  = childResources["apps/v1/Deployment"][0]
        #   desired = deploy.spec.replicas if deploy?.spec?.replicas else 1
        #   ready >= desired         ← this becomes: result = ready >= desired
        expr_lines = expression.splitlines()
        body_lines = expr_lines[:-1]
        result_line = expr_lines[-1].strip()

        # Build the complete KCL program:
        #   - option("data") is parsed by the KCL runtime from the JSON argument value.
        #     It returns a native KCL dict — no json.decode() needed inside KCL.
        #   - Binding to named variables lets expressions use natural names like
        #     childResources[...] instead of verbose option("data").childResources[...].
        kcl_lines = [
            '_ctx = option("data")',
            "childResources = _ctx.childResources",
            "config = _ctx.config",
            "context = _ctx.context",
        ]
        kcl_lines.extend(body_lines)          # Intermediate assignment lines, verbatim
        kcl_lines.append(f"result = {result_line}")  # Final line becomes the result
        kcl_code = "\n".join(kcl_lines)

        # Write the generated KCL to a temp file. kcl-lib requires a file path rather
        # than accepting source code as a string, so we use a named temp file.
        fd, temp_path = tempfile.mkstemp(suffix=".k")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(kcl_code)

            args = kcl_api.ExecProgram_Args(
                k_filename_list=[temp_path],
                # Pass the data bundle via the named "data" argument. KCL parses the
                # JSON value and makes it available as option("data") in the program.
                args=[kcl_api.Argument(name="data", value=ctx_json)],
            )
            out = kcl_api.API().exec_program(args)

            if out.err_message:
                logger.warning(
                    f"Status expression error for field '{field_name}': {out.err_message}"
                )
                results[field_name] = None
            else:
                # The KCL program outputs YAML with a single "result" key.
                # Parse it and extract that key's value.
                parsed = yaml.safe_load(out.yaml_result)
                results[field_name] = parsed.get("result") if parsed else None
        except Exception as e:
            logger.warning(f"Failed to evaluate status field '{field_name}': {e}")
            results[field_name] = None
        finally:
            # Always clean up the temp file, even if evaluation raised an exception.
            try:
                os.unlink(temp_path)
            except OSError:
                pass

    return results
