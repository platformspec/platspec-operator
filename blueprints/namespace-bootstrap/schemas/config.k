schema NamespaceBootstrapConfig:
    replicas: int = 1
    image: str = "nginxinc/nginx-unprivileged:stable-alpine"

    check:
        replicas >= 1, "replicas must be at least 1"
        len(image) > 0, "image must not be empty"
