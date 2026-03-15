schema ConfigmapPlatformMetadataConfig:
    namespace: str = "platspec-system"
    tier: str = "development"

    check:
        len(namespace) > 0, "namespace must not be empty"
        len(tier) > 0, "tier must not be empty"
