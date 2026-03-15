# Tiltfile — platspec-operator
#
# Run from this directory:
#   cd operator/platspec && tilt up
#
# Requires a local Kubernetes cluster (kind, k3d, rancher-desktop, etc.)
# with kubectl context set to it.
#
# The build context is the repository root so that the blueprints/ directory
# is available to the Dockerfile. Source changes to operator/platspec/src/
# are synced live into the running container without a full image rebuild.
#
# Podman: uses the podman Tilt extension as a drop-in replacement for docker_build.

load('ext://namespace', 'namespace_yaml')
load('ext://helm_remote', 'helm_remote')
load('ext://helm_resource', 'helm_resource', 'helm_repo')
load('ext://podman', 'podman_build')
load('ext://secret', 'secret_from_dict')


IMAGE     = 'ghcr.io/foundationio/platspec-operator'
NAMESPACE = 'platspec-system'
REPO_ROOT = '../..'

# ── Namespace ─────────────────────────────────────────────────────────────────
k8s_yaml(namespace_yaml(NAMESPACE), allow_duplicates=True)

# ── Operator Helm ─────────────────────────────────────────────────────────────

k8s_yaml(
    helm(
        'chart',
        name      = 'platspec-operator',
        namespace = NAMESPACE,
        values    = ['chart/values.yaml'],
        set = [
            'image.repository=' + IMAGE,
            'image.tag=dev',
            'image.pullPolicy=IfNotPresent',
            # Dev overrides — verbose logging, no caching, fast reconcile loop.
            'operator.logLevel=DEBUG',
            'operator.devMode=true',
            'operator.blueprintCacheEnabled=false',
            'operator.reconcileInterval=30',
            'operator.kclTimeout=15',
        ],
    )
)

# ── Image ─────────────────────────────────────────────────────────────────────
if os.getenv("CONTAINER_RUNTIME") == "podman":
    podman_build(
        IMAGE,
        context = ".",
        ignore = [
          "./.venv",
          "./.pytest_cache",
          "./coverage",
          "./examples",
          "./scripts"
        ],
        live_update = [
            # Sync Python source changes directly into the installed package path.
            # No image rebuild required for handler/model/core edits.
            sync(
                'src/platspec_operator/',
                '/app/.venv/lib/python3.13/site-packages/platspec_operator/',
            ),
            # Restart the operator process so kopf picks up the new code.
            run('kill 1'),
        ],
    )
else:
    docker_build(
        IMAGE,
        context = ".",
        live_update = [
            # Sync Python source changes directly into the installed package path.
            # No image rebuild required for handler/model/core edits.
            sync(
                'src/platspec_operator/',
                '/app/.venv/lib/python3.13/site-packages/platspec_operator/',
            ),
            # Restart the operator process so kopf picks up the new code.
            run('kill 1'),
        ],
    )




# Group the operator Deployment for the Tilt UI and disable readiness gating
# so Tilt doesn't block on kopf's slower startup.
k8s_resource(
    'platspec-operator',
    labels        = ['operator'],
    pod_readiness = 'ignore',
)
