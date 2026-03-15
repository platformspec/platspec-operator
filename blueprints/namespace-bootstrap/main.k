# main.k — namespace-bootstrap blueprint
#
# Creates a Namespace and a lightweight Deployment in the local management
# cluster. Used to verify the Platspec Operator pipeline end-to-end:
#   KCL execution → server-side apply → async Deployment status evaluation.
#
# No cloud provider, Cluster API, or remote cluster required.

context = option("context", default={})

_cfg      = context?.config or {}
_platform = context?.platform?.name or "unknown-platform"
_env      = context?.environment?.name or "unknown-env"
_ns       = _platform + "-" + _env
_replicas = _cfg?.replicas or 1
_image    = _cfg?.image or "nginx:latest"

_labels = {
    "platform.platformspec.io/managed-by"  = "platspec-operator"
    "platform.platformspec.io/platform"    = _platform
    "platform.platformspec.io/environment" = _env
}

resources = [
    {
        apiVersion = "v1"
        kind       = "Namespace"
        metadata   = {
            name   = _ns
            labels = _labels
        }
    }
    {
        apiVersion = "apps/v1"
        kind       = "Deployment"
        metadata   = {
            name      = "echo"
            namespace = _ns
            labels    = _labels
        }
        spec = {
            replicas = _replicas
            selector = {
                matchLabels = {
                    "app" = "echo"
                }
            }
            template = {
                metadata = {
                    labels = {
                        "app"                                      = "echo"
                        "platform.platformspec.io/managed-by"  = "platspec-operator"
                        "platform.platformspec.io/platform"    = _platform
                        "platform.platformspec.io/environment" = _env
                    }
                }
                spec = {
                    containers = [
                        {
                            name  = "echo"
                            image = _image
                            ports = [{containerPort = 8080}]
                            resources = {
                                requests = {cpu = "10m",  memory = "16Mi"}
                                limits   = {cpu = "100m", memory = "64Mi"}
                            }
                        }
                    ]
                }
            }
        }
    }
]
