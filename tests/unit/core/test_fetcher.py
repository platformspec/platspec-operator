"""Tests for core/fetcher.py — blueprint resolution and security."""

import tarfile
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from platspec_operator.core.fetcher import (
    BlueprintFetchError,
    BlueprintFetcher,
    _fetch_filesystem,
    _safe_extract,
)


# ---------------------------------------------------------------------------
# _safe_extract — zip-slip protection
# ---------------------------------------------------------------------------


def _make_tarball(dest: Path, members: list[tuple[str, bytes]]) -> Path:
    """Build a .tar.gz at dest containing the given (name, content) pairs."""
    tarball = dest / "test.tar.gz"
    with tarfile.open(tarball, "w:gz") as tf:
        for name, content in members:
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            import io
            tf.addfile(info, io.BytesIO(content))
    return tarball


def test_safe_extract_normal_file(tmp_path):
    tarball = _make_tarball(tmp_path, [("blueprint/main.k", b"schema = {}")])
    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(tarball, "r:gz") as tf:
        _safe_extract(tf, extract_dir)
    assert (extract_dir / "blueprint" / "main.k").exists()


def test_safe_extract_rejects_path_traversal(tmp_path):
    tarball = _make_tarball(tmp_path, [("../../evil.sh", b"rm -rf /")])
    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(tarball, "r:gz") as tf:
        with pytest.raises(BlueprintFetchError, match="unsafe path traversal"):
            _safe_extract(tf, extract_dir)


def test_safe_extract_rejects_absolute_path(tmp_path):
    """Absolute paths in tarball members should be rejected."""
    tarball = tmp_path / "abs.tar.gz"
    with tarfile.open(tarball, "w:gz") as tf:
        info = tarfile.TarInfo(name="/etc/cron.d/evil")
        info.size = 0
        import io
        tf.addfile(info, io.BytesIO(b""))
    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(tarball, "r:gz") as tf:
        with pytest.raises(BlueprintFetchError, match="unsafe path traversal"):
            _safe_extract(tf, extract_dir)


def test_safe_extract_rejects_nested_traversal(tmp_path):
    """Traversal hidden inside a valid-looking prefix should be caught."""
    tarball = _make_tarball(tmp_path, [("subdir/../../escape.txt", b"escaped")])
    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(tarball, "r:gz") as tf:
        with pytest.raises(BlueprintFetchError, match="unsafe path traversal"):
            _safe_extract(tf, extract_dir)


# ---------------------------------------------------------------------------
# _fetch_filesystem
# ---------------------------------------------------------------------------


def test_fetch_filesystem_direct_match(tmp_path):
    bp_dir = tmp_path / "my-blueprint"
    bp_dir.mkdir()
    (bp_dir / "main.k").write_text("x = 1")
    result = _fetch_filesystem(tmp_path, "my-blueprint")
    assert result == bp_dir


def test_fetch_filesystem_nested_match(tmp_path):
    nested = tmp_path / "components" / "general" / "my-bp"
    nested.mkdir(parents=True)
    (nested / "main.k").write_text("x = 1")
    result = _fetch_filesystem(tmp_path, "my-bp")
    assert result == nested


def test_fetch_filesystem_not_found_raises(tmp_path):
    with pytest.raises(BlueprintFetchError, match="not found"):
        _fetch_filesystem(tmp_path, "missing-blueprint")


def test_fetch_filesystem_dir_without_main_k_not_matched(tmp_path):
    bp_dir = tmp_path / "no-main-k"
    bp_dir.mkdir()
    (bp_dir / "other.k").write_text("x = 1")
    with pytest.raises(BlueprintFetchError, match="not found"):
        _fetch_filesystem(tmp_path, "no-main-k")


# ---------------------------------------------------------------------------
# BlueprintFetcher.fetch — local fast-path and cache
# ---------------------------------------------------------------------------


def _make_fetcher(local_dir: Path, registries: dict, cache_dir: Path) -> BlueprintFetcher:
    return BlueprintFetcher(
        registries=registries,
        local_blueprint_dir=local_dir,
        cache_dir=cache_dir,
        k8s=MagicMock(),
    )


def test_fetcher_returns_local_blueprint(tmp_path):
    bp = tmp_path / "local" / "my-bp"
    bp.mkdir(parents=True)
    (bp / "main.k").write_text("x = 1")
    fetcher = _make_fetcher(tmp_path / "local", {}, tmp_path / "cache")
    result = fetcher.fetch("my-bp", "1.0")
    assert result == bp


def test_fetcher_serves_cached_versioned_blueprint(tmp_path):
    """Cache hit for a pinned version on a remote (OCI) registry avoids network call."""
    cache_dir = tmp_path / "cache"
    cached = cache_dir / "my-reg" / "my-bp" / "1.2.3"
    cached.mkdir(parents=True)
    (cached / "main.k").write_text("cached = True")

    # OCI registry — non-filesystem types go through the cache layer
    registries = {"my-reg": {"type": "oci", "url": "oci://example.com/blueprints"}}
    fetcher = _make_fetcher(tmp_path / "local", registries, cache_dir)

    # Should not call any remote — serves from cache directly
    result = fetcher.fetch("my-bp", "1.2.3", registry_ref="my-reg")
    assert result == cached


def test_fetcher_latest_bypasses_cache(tmp_path):
    """Version 'latest' must always re-fetch, never serve from cache."""
    cache_dir = tmp_path / "cache"
    cached = cache_dir / "my-reg" / "my-bp" / "latest"
    cached.mkdir(parents=True)
    (cached / "main.k").write_text("stale")

    # filesystem registry pointing to a dir that has an updated blueprint
    reg_dir = tmp_path / "reg"
    bp_dir = reg_dir / "my-bp"
    bp_dir.mkdir(parents=True)
    (bp_dir / "main.k").write_text("fresh")

    registries = {"my-reg": {"type": "filesystem", "url": str(reg_dir)}}
    fetcher = _make_fetcher(tmp_path / "local", registries, cache_dir)

    result = fetcher.fetch("my-bp", "latest", registry_ref="my-reg")
    # filesystem backend returns the live path, not the cache
    assert result == bp_dir


def test_fetcher_raises_for_unknown_registry(tmp_path):
    fetcher = _make_fetcher(tmp_path, {}, tmp_path / "cache")
    with pytest.raises(BlueprintFetchError, match="Unknown registry"):
        fetcher.fetch("bp", "1.0", registry_ref="nonexistent")


def test_fetcher_raises_when_not_found_anywhere(tmp_path):
    fetcher = _make_fetcher(tmp_path / "empty", {}, tmp_path / "cache")
    with pytest.raises(BlueprintFetchError, match="not found"):
        fetcher.fetch("missing", "1.0")


def test_fetcher_unknown_registry_type_raises(tmp_path):
    registries = {"bad-reg": {"type": "ftp", "url": "ftp://example.com"}}
    fetcher = _make_fetcher(tmp_path, registries, tmp_path / "cache")
    with pytest.raises(BlueprintFetchError, match="Unknown registry type"):
        fetcher.fetch("bp", "1.0", registry_ref="bad-reg")


# ---------------------------------------------------------------------------
# SSH key tempfile cleanup
# ---------------------------------------------------------------------------


def test_fetch_git_cleans_up_ssh_key_on_success(tmp_path):
    """SSH key tempfile must be deleted after a successful clone."""
    created_key_files: list[Path] = []
    real_ntf = tempfile.NamedTemporaryFile

    def tracking_ntf(**kwargs):
        f = real_ntf(**kwargs)
        created_key_files.append(Path(f.name))
        return f

    with (
        patch("platspec_operator.core.fetcher._read_secret") as mock_secret,
        patch("platspec_operator.core.fetcher.subprocess.run"),
        patch("tempfile.NamedTemporaryFile", side_effect=tracking_ntf),
    ):
        mock_secret.return_value = {
            "ssh-privatekey": b"FAKE_KEY_CONTENT",
        }

        # Plant a fake blueprint directory so the post-clone check passes
        bp_dir = tmp_path / "my-bp"
        bp_dir.mkdir()
        (bp_dir / "main.k").write_text("x = 1")

        from platspec_operator.core.fetcher import _fetch_git
        auth = {"type": "secret", "secretRef": {"name": "s", "namespace": "ns"}}
        try:
            _fetch_git("git+ssh://git@gitlab.com/org/repo.git", "my-bp", "main", auth, tmp_path, MagicMock())
        except Exception:
            pass

    # Every tempfile created for SSH keys must have been deleted
    for kf in created_key_files:
        assert not kf.exists(), f"SSH key tempfile was not cleaned up: {kf}"


# ---------------------------------------------------------------------------
# _fetch_git — path, ref, cache hit, stale dir cleanup
# ---------------------------------------------------------------------------


def _mock_subprocess_success(calls_made: list) -> None:
    """Patch subprocess.run to succeed and record the calls made."""
    import subprocess as sp

    def fake_run(cmd, **kwargs):
        calls_made.append(cmd)
        return sp.CompletedProcess(cmd, 0, b"", b"")

    return fake_run


def _subprocess_creates_bp(dest_dir: Path, sparse_path: str) -> Any:
    """Return a subprocess.run side-effect that records calls and creates bp_dir/main.k on clone."""
    import subprocess as sp

    calls: list = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if "clone" in cmd:
            bp = dest_dir / sparse_path
            bp.mkdir(parents=True, exist_ok=True)
            (bp / "main.k").write_text("x = 1")
        return sp.CompletedProcess(cmd, 0, b"", b"")

    fake_run.calls = calls  # type: ignore[attr-defined]
    return fake_run


def test_fetch_git_uses_ref_instead_of_version(tmp_path):
    """When `ref` is set, git clone --branch uses ref, not the blueprint version."""
    dest = tmp_path / "dest"
    side_effect = _subprocess_creates_bp(dest, "blueprints/my-bp")

    with patch("platspec_operator.core.fetcher.subprocess.run", side_effect=side_effect):
        from platspec_operator.core.fetcher import _fetch_git
        _fetch_git(
            "https://gitlab.com/org/repo.git", "my-bp", "0.1.0",
            {}, dest, MagicMock(), path="blueprints", ref="main",
        )

    clone_cmd = next(c for c in side_effect.calls if "clone" in c)
    assert "--branch" in clone_cmd
    branch_idx = clone_cmd.index("--branch")
    assert clone_cmd[branch_idx + 1] == "main"  # ref, not "0.1.0"


def test_fetch_git_uses_version_when_no_ref(tmp_path):
    """When `ref` is not set, git clone --branch uses the blueprint version."""
    dest = tmp_path / "dest"
    side_effect = _subprocess_creates_bp(dest, "my-bp")

    with patch("platspec_operator.core.fetcher.subprocess.run", side_effect=side_effect):
        from platspec_operator.core.fetcher import _fetch_git
        _fetch_git(
            "https://gitlab.com/org/repo.git", "my-bp", "0.5.0",
            {}, dest, MagicMock(),
        )

    clone_cmd = next(c for c in side_effect.calls if "clone" in c)
    branch_idx = clone_cmd.index("--branch")
    assert clone_cmd[branch_idx + 1] == "0.5.0"


def test_fetch_git_sparse_path_includes_registry_path(tmp_path):
    """sparse-checkout set uses <path>/<name> when registry `path` is configured."""
    dest = tmp_path / "dest"
    side_effect = _subprocess_creates_bp(dest, "components/my-bp")

    with patch("platspec_operator.core.fetcher.subprocess.run", side_effect=side_effect):
        from platspec_operator.core.fetcher import _fetch_git
        _fetch_git(
            "https://gitlab.com/org/repo.git", "my-bp", "1.0",
            {}, dest, MagicMock(), path="components",
        )

    sparse_cmd = next(c for c in side_effect.calls if "sparse-checkout" in c and "set" in c)
    assert sparse_cmd[-1] == "components/my-bp"


def test_fetch_git_cache_hit_skips_clone(tmp_path):
    """If bp_dir/main.k already exists, no git commands are run."""
    bp_dir = tmp_path / "blueprints" / "my-bp"
    bp_dir.mkdir(parents=True)
    (bp_dir / "main.k").write_text("cached = True")

    with patch("platspec_operator.core.fetcher.subprocess.run") as mock_run:
        from platspec_operator.core.fetcher import _fetch_git
        result = _fetch_git(
            "https://gitlab.com/org/repo.git", "my-bp", "1.0",
            {}, tmp_path, MagicMock(), path="blueprints",
        )

    mock_run.assert_not_called()
    assert result == bp_dir


def test_fetch_git_stale_dir_is_removed_before_clone(tmp_path):
    """A dest_dir that exists but lacks bp_dir/main.k is removed before cloning."""
    # Simulate a stale partial clone: dest_dir exists with junk content
    dest_dir = tmp_path / "cache"
    dest_dir.mkdir()
    (dest_dir / "some-leftover-file").write_text("stale")

    calls: list = []

    with patch("platspec_operator.core.fetcher.subprocess.run", side_effect=_mock_subprocess_success(calls)):
        bp_dir = dest_dir / "my-bp"
        # The clone "succeeds" — simulate git having written the blueprint
        # by creating bp_dir/main.k after the clone call would run.
        # We intercept the first clone call to create the expected directory.
        original_fake = _mock_subprocess_success(calls)

        def clone_and_create(cmd, **kwargs):
            result = original_fake(cmd, **kwargs)
            if "clone" in cmd:
                bp_dir.mkdir(parents=True, exist_ok=True)
                (bp_dir / "main.k").write_text("fresh")
            return result

        with patch("platspec_operator.core.fetcher.subprocess.run", side_effect=clone_and_create):
            from platspec_operator.core.fetcher import _fetch_git
            _fetch_git(
                "https://gitlab.com/org/repo.git", "my-bp", "1.0",
                {}, dest_dir, MagicMock(),
            )

    # Leftover file must be gone — dir was wiped and re-created
    assert not (dest_dir / "some-leftover-file").exists()


# ---------------------------------------------------------------------------
# BlueprintFetcher — path/ref forwarded from registry spec
# ---------------------------------------------------------------------------


def test_fetcher_passes_path_and_ref_to_fetch_git(tmp_path):
    """_fetch_from_registry extracts path and ref from the registry spec and passes them."""
    cache_dir = tmp_path / "cache"
    registries = {
        "my-git-reg": {
            "type": "git",
            "url": "https://gitlab.com/org/repo.git",
            "path": "blueprints",
            "ref": "main",
        }
    }
    fetcher = _make_fetcher(tmp_path / "local", registries, cache_dir)

    with patch("platspec_operator.core.fetcher._fetch_git") as mock_fetch_git:
        bp_dir = tmp_path / "result"
        bp_dir.mkdir()
        mock_fetch_git.return_value = bp_dir

        fetcher.fetch("my-bp", "0.1.0", registry_ref="my-git-reg")

    mock_fetch_git.assert_called_once()
    _, kwargs = mock_fetch_git.call_args
    assert kwargs.get("path") == "blueprints"
    assert kwargs.get("ref") == "main"


def test_fetcher_git_registry_without_path_or_ref(tmp_path):
    """Registry spec without path/ref passes None for both."""
    cache_dir = tmp_path / "cache"
    registries = {
        "bare-reg": {
            "type": "git",
            "url": "https://gitlab.com/org/repo.git",
        }
    }
    fetcher = _make_fetcher(tmp_path / "local", registries, cache_dir)

    with patch("platspec_operator.core.fetcher._fetch_git") as mock_fetch_git:
        bp_dir = tmp_path / "result"
        bp_dir.mkdir()
        mock_fetch_git.return_value = bp_dir

        fetcher.fetch("my-bp", "1.0", registry_ref="bare-reg")

    _, kwargs = mock_fetch_git.call_args
    assert kwargs.get("path") is None
    assert kwargs.get("ref") is None


# ---------------------------------------------------------------------------
# _fetch_git — local bare repo (end-to-end, no network)
# ---------------------------------------------------------------------------


@pytest.fixture()
def local_git_repo(tmp_path):
    """Create a local bare git repo with a blueprint at blueprints/test-bp/main.k on branch 'main'."""
    import subprocess as sp

    work = tmp_path / "work"
    work.mkdir()
    bare = tmp_path / "bare.git"

    # Init a working repo, add a blueprint, push to a bare repo
    sp.run(["git", "init", "-b", "main", str(work)], check=True, capture_output=True)
    sp.run(["git", "-C", str(work), "config", "user.email", "test@test.com"], check=True, capture_output=True)
    sp.run(["git", "-C", str(work), "config", "user.name", "Test"], check=True, capture_output=True)

    bp = work / "blueprints" / "test-bp"
    bp.mkdir(parents=True)
    (bp / "main.k").write_text("x = 1")

    sp.run(["git", "-C", str(work), "add", "."], check=True, capture_output=True)
    sp.run(["git", "-C", str(work), "commit", "-m", "init"], check=True, capture_output=True)
    sp.run(["git", "clone", "--bare", str(work), str(bare)], check=True, capture_output=True)

    return bare


def test_fetch_git_local_repo_no_path(tmp_path, local_git_repo):
    """Fetch blueprint directly at repo root (no path prefix)."""
    from platspec_operator.core.fetcher import _fetch_git

    # Blueprint is at blueprints/test-bp — fetch it without path to get the whole tree
    # Use the blueprints dir as the repo root instead
    import subprocess as sp
    work = tmp_path / "work2"
    work.mkdir()
    sp.run(["git", "init", "-b", "main", str(work)], check=True, capture_output=True)
    sp.run(["git", "-C", str(work), "config", "user.email", "t@t.com"], check=True, capture_output=True)
    sp.run(["git", "-C", str(work), "config", "user.name", "T"], check=True, capture_output=True)
    bp = work / "test-bp"
    bp.mkdir()
    (bp / "main.k").write_text("hello = 1")
    sp.run(["git", "-C", str(work), "add", "."], check=True, capture_output=True)
    sp.run(["git", "-C", str(work), "commit", "-m", "init"], check=True, capture_output=True)
    bare2 = tmp_path / "bare2.git"
    sp.run(["git", "clone", "--bare", str(work), str(bare2)], check=True, capture_output=True)

    dest = tmp_path / "cache" / "test-bp" / "main"
    result = _fetch_git(f"file://{bare2}", "test-bp", "main", {}, dest, MagicMock())
    assert (result / "main.k").exists()


def test_fetch_git_local_repo_with_path(tmp_path, local_git_repo):
    """Fetch blueprint at <path>/<name> within the repo using path=."""
    from platspec_operator.core.fetcher import _fetch_git

    dest = tmp_path / "cache" / "test-bp" / "main"
    result = _fetch_git(
        f"file://{local_git_repo}", "test-bp", "main",
        {}, dest, MagicMock(), path="blueprints",
    )
    assert (result / "main.k").exists()
    assert result.name == "test-bp"


def test_fetch_git_local_repo_second_call_uses_cache(tmp_path, local_git_repo):
    """Second fetch of the same blueprint/version returns from cache without cloning."""
    from platspec_operator.core.fetcher import _fetch_git

    dest = tmp_path / "cache" / "test-bp" / "main"
    _fetch_git(
        f"file://{local_git_repo}", "test-bp", "main",
        {}, dest, MagicMock(), path="blueprints",
    )

    # Second call — should not invoke git at all
    with patch("platspec_operator.core.fetcher.subprocess.run") as mock_run:
        result = _fetch_git(
            f"file://{local_git_repo}", "test-bp", "main",
            {}, dest, MagicMock(), path="blueprints",
        )

    mock_run.assert_not_called()
    assert (result / "main.k").exists()


def test_fetch_git_cleans_up_ssh_key_on_error(tmp_path):
    """SSH key tempfile must be deleted even when git clone fails."""
    created_key_files: list[Path] = []
    real_ntf = tempfile.NamedTemporaryFile

    def tracking_ntf(**kwargs):
        f = real_ntf(**kwargs)
        created_key_files.append(Path(f.name))
        return f

    import subprocess as sp
    with (
        patch("platspec_operator.core.fetcher._read_secret") as mock_secret,
        patch("platspec_operator.core.fetcher.subprocess.run",
              side_effect=sp.CalledProcessError(128, "git", b"", b"auth failed")),
        patch("tempfile.NamedTemporaryFile", side_effect=tracking_ntf),
    ):
        mock_secret.return_value = {"ssh-privatekey": b"FAKE_KEY"}
        from platspec_operator.core.fetcher import _fetch_git
        auth = {"type": "secret", "secretRef": {"name": "s", "namespace": "ns"}}
        with pytest.raises(BlueprintFetchError):
            _fetch_git("git+ssh://git@gitlab.com/org/repo.git", "bp", "main", auth, tmp_path, MagicMock())

    for kf in created_key_files:
        assert not kf.exists(), f"SSH key tempfile leaked on error: {kf}"
