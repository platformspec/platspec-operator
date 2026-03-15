# main.k — namespace-rbac blueprint
#
# Creates RBAC resources (ServiceAccount, Roles, RoleBindings) in the namespace
# provisioned by the namespace-bootstrap capability. Reads the target namespace
# from context.capabilities["namespace-bootstrap"].namespace so it does not need
# to recompute or re-configure the name.
#
# No cloud provider or remote cluster required.

context = option("context", default={})

_cfg          = context?.config or {}
_platform     = context?.platform?.name or "unknown-platform"
_env          = context?.environment?.name or "unknown-env"
_capabilities = context?.capabilities or {}

# Prefer the namespace emitted by namespace-bootstrap; fall back to config or derived name.
_ns_cap_data = _capabilities["namespace-bootstrap"] if "namespace-bootstrap" in _capabilities else {}
_ns_from_cap = _ns_cap_data?.namespace or ""
_ns          = _ns_from_cap if _ns_from_cap else (_cfg?.namespace or (_platform + "-" + _env))

_admin_subjects  = _cfg?.adminSubjects  or []
_viewer_subjects = _cfg?.viewerSubjects or []

_labels = {
    "platform.platformspec.io/managed-by"  = "platspec-operator"
    "platform.platformspec.io/platform"    = _platform
    "platform.platformspec.io/environment" = _env
}

_sa_name = _platform + "-admin"

# Admin binding always includes the platform ServiceAccount plus any configured subjects.
_admin_binding_subjects = [
    {
        kind      = "ServiceAccount"
        name      = _sa_name
        namespace = _ns
    }
] + _admin_subjects

resources = [
    # Platform admin ServiceAccount — used by CI/CD and automated tooling.
    {
        apiVersion = "v1"
        kind       = "ServiceAccount"
        metadata   = {
            name      = _sa_name
            namespace = _ns
            labels    = _labels
        }
    }
    # Admin Role: full CRUD on workload resources within the namespace.
    # RBAC resources themselves are read-only to prevent privilege escalation.
    {
        apiVersion = "rbac.authorization.k8s.io/v1"
        kind       = "Role"
        metadata   = {
            name      = "platform-admin"
            namespace = _ns
            labels    = _labels
        }
        rules = [
            {
                apiGroups = ["", "apps", "batch", "autoscaling", "policy"]
                resources = ["*"]
                verbs     = ["*"]
            }
            {
                apiGroups = ["rbac.authorization.k8s.io"]
                resources = ["roles", "rolebindings"]
                verbs     = ["get", "list", "watch"]
            }
        ]
    }
    # Viewer Role: read-only access to workload resources.
    {
        apiVersion = "rbac.authorization.k8s.io/v1"
        kind       = "Role"
        metadata   = {
            name      = "platform-viewer"
            namespace = _ns
            labels    = _labels
        }
        rules = [
            {
                apiGroups = ["", "apps", "batch", "autoscaling"]
                resources = ["*"]
                verbs     = ["get", "list", "watch"]
            }
        ]
    }
    # Admin RoleBinding — binds the ServiceAccount and any configured admin subjects.
    {
        apiVersion = "rbac.authorization.k8s.io/v1"
        kind       = "RoleBinding"
        metadata   = {
            name      = "platform-admin"
            namespace = _ns
            labels    = _labels
        }
        roleRef = {
            apiGroup = "rbac.authorization.k8s.io"
            kind     = "Role"
            name     = "platform-admin"
        }
        subjects = _admin_binding_subjects
    }
    # Viewer RoleBinding — subjects may be empty; no access is granted until configured.
    {
        apiVersion = "rbac.authorization.k8s.io/v1"
        kind       = "RoleBinding"
        metadata   = {
            name      = "platform-viewer"
            namespace = _ns
            labels    = _labels
        }
        roleRef = {
            apiGroup = "rbac.authorization.k8s.io"
            kind     = "Role"
            name     = "platform-viewer"
        }
        subjects = _viewer_subjects
    }
]
