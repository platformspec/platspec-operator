context = option("context", default={})

_platform = context?.platform?.name or "test"
_ns       = context?.config?.namespace or "default"
_replicas = context?.config?.replicas or 1

resources = [
    {
        apiVersion = "apps/v1"
        kind       = "Deployment"
        metadata   = {
            name      = "test-deploy"
            namespace = _ns
        }
        spec = {
            replicas = _replicas
            selector = {matchLabels = {app = "test"}}
            template = {
                metadata = {labels = {app = "test"}}
                spec = {
                    containers = [{name = "test", image = "nginx:latest"}]
                }
            }
        }
    }
]
