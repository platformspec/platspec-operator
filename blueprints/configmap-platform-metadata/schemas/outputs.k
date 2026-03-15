schema PlatformMetadataConfigMap:
    apiVersion: str = "v1"
    kind: str = "ConfigMap"
    metadata: {str: any}
    data: {str: str}
