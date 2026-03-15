context = option("context", default={})

_platform = context?.platform?.name or "test"
_env      = context?.environment?.name or "local"
_ns       = _platform + "-" + _env

resources = [
    {
        apiVersion = "v1"
        kind       = "Namespace"
        metadata   = {name = _ns}
    }
]
