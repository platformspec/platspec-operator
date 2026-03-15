# main.k — configmap-platform-metadata blueprint
#
# Creates a single ConfigMap containing platform identity metadata.
# Used as a smoke test for the synchronous evaluation path:
#   KCL execution → server-side apply → immediate ConfigMap status check.
#
# Unlike namespace-bootstrap, this blueprint requires no pod scheduling or
# image pulls — status resolves in the same reconcile cycle as apply.
# No cloud provider or remote cluster required.

context = option("context", default={})

_cfg      = context?.config or {}
_platform = context?.platform?.name or "unknown-platform"
_env      = context?.environment?.name or "unknown-env"
_ns       = _cfg?.namespace or "platspec-example"
_tier     = _cfg?.tier or "development"
_cm_name  = _platform + "-" + _env + "-metadata"

_labels = {
    "platform.platformspec.io/managed-by"  = "platspec-operator"
    "platform.platformspec.io/platform"    = _platform
    "platform.platformspec.io/environment" = _env
}

resources = [
    {
        apiVersion = "v1"
        kind       = "ConfigMap"
        metadata   = {
            name      = _cm_name
            namespace = _ns
            labels    = _labels
        }
        data = {
            platform    = _platform
            environment = _env
            tier        = _tier
            managedBy   = "platspec-operator"
        }
    }
]
