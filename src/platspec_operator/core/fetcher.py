"""Blueprint fetcher — resolves blueprint names to local filesystem paths.

Supports multiple backend types: filesystem (local path), OCI registry,
git repository, HTTP artifact server, and S3-compatible object storage.
A process-scoped cache avoids re-fetching on every reconcile.

Resolution order (when no registry is pinned in the BlueprintBinding):
  1. Local filesystem (fast path — no network, no credentials).
  2. Registered remote registries in definition order (first match wins).

Cache behaviour:
  - Remote blueprints are cached under cache_dir/{registry}/{name}/{version}/.
  - Versioned blueprints (anything other than "latest") are served from cache
    on subsequent calls without hitting the network again.
  - "latest" is always re-fetched so operators pick up new content without
    needing an explicit version bump.
"""

import json
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger


class BlueprintFetchError(Exception):
    pass


# ---------------------------------------------------------------------------
# Tarball extraction helper
# ---------------------------------------------------------------------------

def _safe_extract(tf: tarfile.TarFile, dest_dir: Path) -> None:
    """Extract a tarball, rejecting any member whose path escapes dest_dir.

    Defends against zip-slip attacks (tarball members with '../' or absolute
    paths that write outside the intended destination directory).
    """
    dest_resolved = dest_dir.resolve()
    for member in tf.getmembers():
        target = (dest_resolved / member.name).resolve()
        if not target.is_relative_to(dest_resolved):
            raise BlueprintFetchError(
                f"Tarball contains unsafe path traversal: {member.name!r}"
            )
    tf.extractall(dest_dir)


# ---------------------------------------------------------------------------
# Credential helper
# ---------------------------------------------------------------------------

def _read_secret(secret_name: str, secret_namespace: str, k8s: Any) -> Dict[str, bytes]:
    """Read a Kubernetes Secret and return its decoded data dict."""
    import base64
    try:
        api = k8s.resources.get(api_version="v1", kind="Secret")
        secret = api.get(name=secret_name, namespace=secret_namespace)
        return {k: base64.b64decode(v) for k, v in (secret.data or {}).items()}
    except Exception as e:
        raise BlueprintFetchError(
            f"Cannot read secret {secret_namespace}/{secret_name}: {e}"
        ) from e


def _basic_credentials(auth: Dict[str, Any], k8s: Any) -> Optional[str]:
    """Return base64(user:password) for Basic auth, or None for anonymous/SA auth."""
    import base64
    if auth.get("type") != "secret":
        return None
    ref = auth.get("secretRef") or {}
    if not ref:
        return None
    secret_data = _read_secret(ref.get("name", ""), ref.get("namespace", ""), k8s)
    # Docker config JSON (imagePullSecret format)
    dcj = secret_data.get(".dockerconfigjson") or secret_data.get("dockerconfigjson")
    if dcj:
        config = json.loads(dcj)
        for _, creds in config.get("auths", {}).items():
            if creds.get("auth"):
                return creds["auth"]
    # Plain username/password
    user = secret_data.get("username", b"").decode()
    password = secret_data.get("password", b"").decode()
    if user and password:
        return base64.b64encode(f"{user}:{password}".encode()).decode()
    return None


# ---------------------------------------------------------------------------
# Filesystem backend
# ---------------------------------------------------------------------------

def _fetch_filesystem(blueprint_dir: Path, name: str) -> Path:
    """Locate a blueprint directory by name within a local filesystem path."""
    direct = blueprint_dir / name
    if direct.is_dir() and (direct / "main.k").exists():
        return direct
    for candidate in sorted(blueprint_dir.rglob(name)):
        if candidate.is_dir() and (candidate / "main.k").exists():
            return candidate
    raise BlueprintFetchError(f"Blueprint '{name}' not found under {blueprint_dir}")


# ---------------------------------------------------------------------------
# OCI backend
# ---------------------------------------------------------------------------

def _oci_auth_headers(
    registry_host: str,
    repo: str,
    credentials: Optional[str],
) -> Dict[str, str]:
    """Exchange credentials for a Bearer token using the OCI token auth flow.

    The OCI Distribution Spec uses the Docker token challenge protocol:
      1. Probe GET /v2/ — expect a 401 with a WWW-Authenticate: Bearer header.
      2. Parse the header to get the token endpoint (realm), service, and scope.
      3. Fetch a token from the realm, optionally presenting Basic credentials.
      4. Use the returned Bearer token for all subsequent requests.

    If the registry accepts Basic auth directly (returns 200 on the probe),
    we skip the token exchange and use Basic auth headers instead.
    """
    import urllib.error
    import urllib.request

    probe = urllib.request.Request(f"https://{registry_host}/v2/")
    if credentials:
        probe.add_header("Authorization", f"Basic {credentials}")

    www_auth = ""
    try:
        urllib.request.urlopen(probe)
        # 200 — registry accepts our credentials directly.
        return {"Authorization": f"Basic {credentials}"} if credentials else {}
    except urllib.error.HTTPError as e:
        if e.code != 401:
            raise
        www_auth = e.headers.get("Www-Authenticate", "")

    if not www_auth.startswith("Bearer "):
        return {"Authorization": f"Basic {credentials}"} if credentials else {}

    # Parse realm, service, scope from the Bearer challenge.
    params: Dict[str, str] = {}
    for part in www_auth[len("Bearer "):].split(","):
        k, _, v = part.strip().partition("=")
        params[k.strip()] = v.strip().strip('"')

    scope = params.get("scope") or f"repository:{repo}:pull"
    token_url = (
        f"{params['realm']}?service={params.get('service', '')}&scope={scope}"
    )
    token_req = urllib.request.Request(token_url)
    if credentials:
        token_req.add_header("Authorization", f"Basic {credentials}")

    with urllib.request.urlopen(token_req) as resp:
        token_data = json.loads(resp.read())

    token = token_data.get("token") or token_data.get("access_token", "")
    return {"Authorization": f"Bearer {token}"}


def _fetch_oci(
    url: str,
    name: str,
    version: str,
    auth: Dict[str, Any],
    dest_dir: Path,
    k8s: Any,
) -> Path:
    """Pull a blueprint OCI artifact from a container registry.

    Blueprint OCI artifacts follow the OCI Distribution Spec:
      - Each blueprint lives at {registry}/{base_path}/{name}:{version}
      - The image has a single layer that is a tar.gz of the blueprint directory

    The layer is extracted into dest_dir. If the tarball wraps the files inside
    a subdirectory matching the blueprint name, that subdirectory is returned;
    otherwise dest_dir itself is returned.
    """
    import urllib.request

    host_and_path = url.removeprefix("oci://")
    registry_host, _, base_path = host_and_path.partition("/")
    repo = f"{base_path.rstrip('/')}/{name}" if base_path else name

    credentials = _basic_credentials(auth, k8s)
    headers = _oci_auth_headers(registry_host, repo, credentials)

    def oci_get(path: str, accept: str) -> bytes:
        req = urllib.request.Request(
            f"https://{registry_host}/v2/{repo}/{path}",
            headers={**headers, "Accept": accept},
        )
        with urllib.request.urlopen(req) as resp:
            return resp.read()

    manifest_json = oci_get(
        f"manifests/{version}",
        "application/vnd.oci.image.manifest.v1+json,"
        "application/vnd.docker.distribution.manifest.v2+json",
    )
    manifest = json.loads(manifest_json)

    layers = manifest.get("layers") or manifest.get("fsLayers", [])
    if not layers:
        raise BlueprintFetchError(
            f"OCI manifest for {name}:{version} has no layers"
        )
    digest = layers[0].get("digest") or layers[0].get("blobSum", "")

    blob = oci_get(f"blobs/{digest}", "application/octet-stream")

    dest_dir.mkdir(parents=True, exist_ok=True)
    blob_path = dest_dir / "layer.tar.gz"
    blob_path.write_bytes(blob)
    with tarfile.open(blob_path, "r:gz") as tf:
        _safe_extract(tf, dest_dir)
    blob_path.unlink()

    return dest_dir / name if (dest_dir / name).is_dir() else dest_dir


# ---------------------------------------------------------------------------
# Git backend
# ---------------------------------------------------------------------------

def _fetch_git(
    url: str,
    name: str,
    version: str,
    auth: Dict[str, Any],
    dest_dir: Path,
    k8s: Any,
    *,
    path: Optional[str] = None,
    ref: Optional[str] = None,
) -> Path:
    """Sparse-checkout a single blueprint directory from a git repository.

    Uses git sparse-checkout to pull only the named blueprint directory rather
    than cloning the entire repo. `ref` (if set on the registry) overrides the
    blueprint version as the git branch/tag/commit; the version is then used
    only for cache keying. `path` (if set) specifies a subdirectory within the
    repo root under which the blueprint package lives.
    Credentials are injected into HTTPS clone URLs directly; SSH keys are
    written to a NamedTemporaryFile and cleaned up in a finally block so key
    material never persists on disk after the clone completes.
    """
    clone_url = url.removeprefix("git+")
    git_ref = ref if ref else version  # registry-level ref overrides version
    sparse_path = f"{path}/{name}" if path else name
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    key_file: Optional[Path] = None

    try:
        if auth.get("type") == "secret" and (secret_ref := auth.get("secretRef")):
            secret_data = _read_secret(secret_ref.get("name", ""), secret_ref.get("namespace", ""), k8s)
            if clone_url.startswith("https://"):
                user = secret_data.get("username", b"git").decode()
                token = (secret_data.get("password") or secret_data.get("token", b"")).decode()
                if token:
                    scheme, rest = clone_url.split("://", 1)
                    clone_url = f"{scheme}://{user}:{token}@{rest}"
            elif ssh_key := secret_data.get("ssh-privatekey"):
                # NamedTemporaryFile with delete=False so we can chmod it before use.
                # Cleaned up unconditionally in the finally block below.
                with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as kf:
                    kf.write(ssh_key)
                    key_file = Path(kf.name)
                key_file.chmod(0o600)
                env["GIT_SSH_COMMAND"] = f"ssh -i {key_file} -o StrictHostKeyChecking=no"

        bp_dir = dest_dir / sparse_path
        if bp_dir.is_dir() and (bp_dir / "main.k").exists():
            return bp_dir
        # dest_dir may exist but be in a bad state (partial/failed clone).
        # Remove it so git clone starts with a clean target.
        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        try:
            subprocess.run(
                ["git", "clone", "--no-checkout", "--depth", "1",
                 "--branch", git_ref, clone_url, str(dest_dir)],
                check=True, env=env, capture_output=True,
            )
            subprocess.run(
                ["git", "-C", str(dest_dir), "sparse-checkout", "init", "--cone"],
                check=True, env=env, capture_output=True,
            )
            subprocess.run(
                ["git", "-C", str(dest_dir), "sparse-checkout", "set", sparse_path],
                check=True, env=env, capture_output=True,
            )
            subprocess.run(
                ["git", "-C", str(dest_dir), "checkout"],
                check=True, env=env, capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            raise BlueprintFetchError(
                f"git clone failed for {url}@{version}: {e.stderr.decode()}"
            ) from e
        if not bp_dir.is_dir():
            raise BlueprintFetchError(
                f"Blueprint '{name}' not found in git repo {url} at ref {git_ref}"
            )
        return bp_dir

    finally:
        if key_file is not None:
            key_file.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# HTTP backend
# ---------------------------------------------------------------------------

def _fetch_http(
    url: str,
    name: str,
    version: str,
    auth: Dict[str, Any],
    dest_dir: Path,
    k8s: Any,
) -> Path:
    """Download a blueprint tarball from an HTTP(S) artifact server.

    Expects the tarball at: {url}/{name}/{version}.tar.gz
    The tarball should extract to a directory named after the blueprint.
    Auth supports Bearer token or Basic auth via a k8s Secret.
    """
    import base64
    import urllib.request

    tarball_url = f"{url.rstrip('/')}/{name}/{version}.tar.gz"
    req = urllib.request.Request(tarball_url)

    if auth.get("type") == "secret" and (ref := auth.get("secretRef")):
        secret_data = _read_secret(ref.get("name", ""), ref.get("namespace", ""), k8s)
        token = (secret_data.get("token") or secret_data.get("password", b"")).decode()
        user = secret_data.get("username", b"").decode()
        if user and token:
            creds = base64.b64encode(f"{user}:{token}".encode()).decode()
            req.add_header("Authorization", f"Basic {creds}")
        elif token:
            req.add_header("Authorization", f"Bearer {token}")

    dest_dir.mkdir(parents=True, exist_ok=True)
    tarball_path = dest_dir / f"{name}-{version}.tar.gz"
    try:
        import urllib.error
        with urllib.request.urlopen(req) as resp:
            tarball_path.write_bytes(resp.read())
    except urllib.error.HTTPError as e:
        raise BlueprintFetchError(
            f"HTTP fetch failed for {tarball_url}: {e.code} {e.reason}"
        ) from e

    with tarfile.open(tarball_path, "r:gz") as tf:
        _safe_extract(tf, dest_dir)
    tarball_path.unlink()

    bp_dir = dest_dir / name
    if not bp_dir.is_dir():
        raise BlueprintFetchError(
            f"Blueprint '{name}' not found in tarball from {tarball_url}"
        )
    return bp_dir


# ---------------------------------------------------------------------------
# S3 backend
# ---------------------------------------------------------------------------

def _fetch_s3(
    url: str,
    name: str,
    version: str,
    auth: Dict[str, Any],
    region: Optional[str],
    dest_dir: Path,
    k8s: Any,
) -> Path:
    """Download a blueprint from S3-compatible object storage.

    Expects the blueprint tarball at: s3://{bucket}/{prefix}/{name}/{version}.tar.gz

    Auth type "serviceAccount" uses the pod's ambient IAM role (IRSA on EKS,
    Workload Identity on GKE). Auth type "secret" reads aws-access-key-id and
    aws-secret-access-key from the referenced Secret.
    """
    import boto3

    path_part = url.removeprefix("s3://")
    bucket, _, prefix = path_part.partition("/")
    key = (
        f"{prefix.rstrip('/')}/{name}/{version}.tar.gz"
        if prefix
        else f"{name}/{version}.tar.gz"
    )

    boto_kwargs: Dict[str, Any] = {}
    if region:
        boto_kwargs["region_name"] = region
    if auth.get("type") == "secret" and (ref := auth.get("secretRef")):
        secret_data = _read_secret(ref.get("name", ""), ref.get("namespace", ""), k8s)
        boto_kwargs["aws_access_key_id"] = secret_data.get("aws-access-key-id", b"").decode()
        boto_kwargs["aws_secret_access_key"] = secret_data.get("aws-secret-access-key", b"").decode()

    s3 = boto3.client("s3", **boto_kwargs)
    dest_dir.mkdir(parents=True, exist_ok=True)
    tarball_path = dest_dir / f"{name}-{version}.tar.gz"
    try:
        s3.download_file(bucket, key, str(tarball_path))
    except Exception as e:
        raise BlueprintFetchError(f"S3 download failed for s3://{bucket}/{key}: {e}") from e

    with tarfile.open(tarball_path, "r:gz") as tf:
        _safe_extract(tf, dest_dir)
    tarball_path.unlink()

    bp_dir = dest_dir / name
    if not bp_dir.is_dir():
        raise BlueprintFetchError(
            f"Blueprint '{name}' not found in S3 object s3://{bucket}/{key}"
        )
    return bp_dir


# ---------------------------------------------------------------------------
# BlueprintFetcher
# ---------------------------------------------------------------------------

class BlueprintFetcher:
    """Resolves blueprint (name, version) pairs to local filesystem paths.

    The fetcher is created once per reconcile from memo["registries"] and the
    operator config. It checks the local filesystem first (zero latency), then
    queries remote registries in registration order.

    Cache: remote blueprints are written to cache_dir/{registry}/{name}/{version}/
    and re-used on subsequent calls. The cache persists for the lifetime of the
    operator process. "latest" versions bypass the cache and are always fetched
    fresh from the remote registry.
    """

    def __init__(
        self,
        registries: Dict[str, Any],
        local_blueprint_dir: Path,
        cache_dir: Path,
        k8s: Any,
    ) -> None:
        self._registries = registries
        self._local_dir = local_blueprint_dir
        self._cache_dir = cache_dir
        self._k8s = k8s

    def _cache_path(self, registry_name: str, name: str, version: str) -> Path:
        return self._cache_dir / registry_name / name / version

    def _is_cached(self, path: Path) -> bool:
        return (path / "main.k").exists()

    def fetch(
        self,
        name: str,
        version: str,
        registry_ref: Optional[str] = None,
    ) -> Path:
        """Return a local path to the blueprint directory, fetching if needed.

        If registry_ref is given, only that registry is tried (and an error is
        raised if it doesn't have the blueprint). Otherwise the local filesystem
        is tried first, then each registered registry in definition order.
        """
        if registry_ref:
            return self._fetch_from_registry(registry_ref, name, version)

        # Local filesystem: fast path, no credentials, no network.
        try:
            return _fetch_filesystem(self._local_dir, name)
        except BlueprintFetchError:
            pass

        # Remote registries in registration order.
        for reg_name in self._registries:
            try:
                return self._fetch_from_registry(reg_name, name, version)
            except BlueprintFetchError as e:
                logger.debug(
                    f"Registry '{reg_name}' could not provide blueprint '{name}': {e}"
                )

        raise BlueprintFetchError(
            f"Blueprint '{name}@{version}' not found in local filesystem "
            f"or any registered registry"
        )

    def _fetch_from_registry(self, registry_name: str, name: str, version: str) -> Path:
        if registry_name not in self._registries:
            raise BlueprintFetchError(f"Unknown registry '{registry_name}'")

        reg = self._registries[registry_name]
        reg_type: str = reg.get("type", "filesystem")
        reg_url: str = reg.get("url", "")
        auth: Dict[str, Any] = reg.get("auth", {})
        region: Optional[str] = reg.get("region")
        reg_path: Optional[str] = reg.get("path")
        reg_ref: Optional[str] = reg.get("ref")

        # Filesystem registries are already local — skip caching.
        if reg_type == "filesystem":
            fs_dir = Path(reg_url) if reg_url else self._local_dir
            if reg_path:
                fs_dir = fs_dir / reg_path
            return _fetch_filesystem(fs_dir, name)

        # Serve from cache for pinned versions (not "latest").
        # Git sparse-checkouts land at cache_path/<sparse_path>/main.k, not
        # cache_path/main.k, so the cache check must account for the path prefix.
        cache_path = self._cache_path(registry_name, name, version)
        if version != "latest":
            if reg_type == "git":
                sparse_path = f"{reg_path}/{name}" if reg_path else name
                cached_bp = cache_path / sparse_path
            else:
                cached_bp = cache_path
            if self._is_cached(cached_bp):
                logger.debug(
                    f"Blueprint '{name}@{version}' from registry '{registry_name}' "
                    "served from cache"
                )
                return cached_bp

        logger.info(
            f"Fetching blueprint '{name}@{version}' from registry '{registry_name}'"
        )

        if reg_type == "oci":
            return _fetch_oci(reg_url, name, version, auth, cache_path, self._k8s)
        elif reg_type == "git":
            return _fetch_git(reg_url, name, version, auth, cache_path, self._k8s, path=reg_path, ref=reg_ref)
        elif reg_type == "http":
            return _fetch_http(reg_url, name, version, auth, cache_path, self._k8s)
        elif reg_type == "s3":
            return _fetch_s3(reg_url, name, version, auth, region, cache_path, self._k8s)
        else:
            raise BlueprintFetchError(f"Unknown registry type '{reg_type}'")
