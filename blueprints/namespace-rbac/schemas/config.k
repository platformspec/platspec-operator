schema RbacSubject:
    kind: str
    name: str
    namespace?: str

    check:
        kind in ["User", "Group", "ServiceAccount"], "kind must be User, Group, or ServiceAccount"
        len(name) > 0, "name must not be empty"


schema NamespaceRbacConfig:
    namespace: str = ""
    adminSubjects: [RbacSubject] = []
    viewerSubjects: [RbacSubject] = []
