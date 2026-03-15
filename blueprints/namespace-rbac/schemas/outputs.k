schema PlatformServiceAccount:
    apiVersion: str = "v1"
    kind: str = "ServiceAccount"
    metadata: {str: any}


schema PlatformRole:
    apiVersion: str = "rbac.authorization.k8s.io/v1"
    kind: str = "Role"
    metadata: {str: any}
    rules: [{str: any}]


schema PlatformRoleBinding:
    apiVersion: str = "rbac.authorization.k8s.io/v1"
    kind: str = "RoleBinding"
    metadata: {str: any}
    roleRef: {str: str}
    subjects: [{str: any}]
