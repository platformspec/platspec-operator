# Platspec Operator

The open-source Kubernetes operator for the [Platform Specification](https://platformspec.io).

The Platspec Operator is the processing layer between your declarative platform definition and the infrastructure that realizes it. It watches Platform Specification resources, resolves blueprint assignments, executes [KCL](https://kcl-lang.io) blueprints, applies the resulting Kubernetes manifests to your cluster, and aggregates status back up to the `Platform` resource.

## What it does

You define your platform declaratively using Platform Specification resources:

```yaml
apiVersion: core.platformspec.io/v1alpha1
kind: Platform
metadata:
  name: my-platform
spec:
  environments:
    - name: production
```

```yaml
apiVersion: core.platformspec.io/v1alpha1
kind: BlueprintBinding
metadata:
  name: my-platform-namespaces
spec:
  platform: my-platform
  blueprint:
    name: namespace-bootstrap
    version: "0.1.0"
    registry: my-registry
  capability: namespace-bootstrap
```

The operator:

1. **Resolves** which blueprints serve which capabilities for each `Platform`
2. **Fetches** blueprint packages from a `BlueprintRegistry` (OCI, Git, HTTP, S3, or local filesystem)
3. **Executes** the blueprint's KCL logic with full platform context (environments, providers, credentials, network topology)
4. **Applies** the generated Kubernetes manifests to the cluster using server-side apply
5. **Evaluates** status expressions defined in `blueprint.yaml` against live output resources
6. **Aggregates** per-binding status up to the `Platform` resource

## Installing

### Helm

```bash
helm repo add platspec https://platformspec.github.io/platspec-operator
helm repo update

helm install platspec-operator platspec/platspec-operator \
  --namespace platspec-system \
  --create-namespace
```

### Configuration

Key Helm values:

| Value | Default | Description |
| --- | --- | --- |
| `operator.logLevel` | `INFO` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `operator.logFormat` | `json` | Log format: `json` or `text` |
| `operator.dryRun` | `false` | Reconcile without applying changes |
| `operator.reconcileInterval` | `300` | Seconds between full reconciliation cycles |
| `operator.blueprintCacheEnabled` | `true` | Cache blueprints fetched from remote registries |

See the [operator documentation](https://platformspec.io/docs/usage/operator) for the full values reference.

## Blueprint Registries

Blueprints are fetched on demand from a `BlueprintRegistry` resource:

```yaml
apiVersion: core.platformspec.io/v1alpha1
kind: BlueprintRegistry
metadata:
  name: my-registry
spec:
  type: git                              # oci | git | http | s3 | filesystem
  url: https://github.com/org/blueprints.git
  path: components                       # optional subdirectory within the repo
  ref: main                              # optional branch/tag/commit (git only)
  auth:
    type: secret                         # secret | serviceAccount | anonymous
    secretRef:
      name: registry-credentials
      namespace: platspec-system
```

See [Finding and Using Blueprints](https://platformspec.io/docs/usage/blueprints) for more.

## Bundled blueprints

This repository includes a small set of reference blueprints under `blueprints/`:

| Blueprint | Description |
| --- | --- |
| `namespace-bootstrap` | Creates a namespace per environment with standard labels |
| `namespace-rbac` | Applies RBAC roles and bindings within bootstrapped namespaces |
| `configmap-platform-metadata` | Publishes platform metadata as a ConfigMap |

These are baked into the operator image at `/blueprints` and used automatically when no registry is configured.

## Developing

Requires Python 3.13 and [uv](https://docs.astral.sh/uv/).

```bash
# Install dependencies
uv sync --extra dev

# Run unit tests
make test-unit

# Run with live reloading against a local cluster
make run-dev

# Build the container image
make podman-build
```

Integration tests require a live Kubernetes cluster with the Platform Specification CRDs installed. They skip automatically if no cluster is reachable.

## License

Apache 2.0. See [LICENSE](LICENSE).

---

Part of the [Platform Specification](https://platformspec.io) ecosystem.
