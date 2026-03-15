schema BootstrapNamespace:
    apiVersion: str = "v1"
    kind: str = "Namespace"
    metadata: {str: any}

schema EchoDeployment:
    apiVersion: str = "apps/v1"
    kind: str = "Deployment"
    metadata: {str: any}
    spec: {str: any}
