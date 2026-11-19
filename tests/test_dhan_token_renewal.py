import json

import pytest
import requests

from scripts import renew_dhan_token as auth

OLD = "old-test-token"
NEW = "replacement-test-token"
CREDENTIALS = {"DHAN_CLIENT_ID": "test-client", "DHAN_ACCESS_TOKEN": OLD}


class Response:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self.payload = payload or {"accessToken": NEW, "expiryTime": "2026-11-20T15:00:00"}

    def json(self):
        return self.payload


class DhanSession:
    def __init__(self, response=None, error=None):
        self.response = response or Response()
        self.error = error
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.error:
            raise self.error
        return self.response


@pytest.fixture
def env_file(tmp_path, monkeypatch):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("GITHUB_ENV", raising=False)
    path = tmp_path / ".env"
    path.write_text(f"# Local config\nOTHER=preserved\nDHAN_ACCESS_TOKEN='{OLD}'\n", encoding="utf-8")
    return path


def test_local_renewal_persists_before_export_and_never_prints_token(env_file, tmp_path, monkeypatch, capsys):
    exported = tmp_path / "github-env"
    monkeypatch.setenv("GITHUB_ENV", str(exported))
    session = DhanSession()
    result = auth.renew(CREDENTIALS, auth.LocalTokenStore(env_file), session)
    assert result == {"status": "renewed", "expiry_time": "2026-11-20T15:00:00"}
    assert f"DHAN_ACCESS_TOKEN={NEW}" in env_file.read_text()
    assert "OTHER=preserved" in env_file.read_text()
    assert exported.read_text() == f"DHAN_ACCESS_TOKEN={NEW}\n"
    assert not env_file.with_name(".env.dhan-recovery").exists()
    assert NEW not in capsys.readouterr().out
    assert session.calls[0][0] == auth.RENEW_URL
    assert session.calls[0][1]["headers"]["dhanClientId"] == "test-client"
    assert session.calls[0][1]["allow_redirects"] is False


def test_failed_local_replace_keeps_replacement_for_restore(env_file, monkeypatch):
    store = auth.LocalTokenStore(env_file)
    store.prepare(OLD)
    write = auth.atomic_write

    def fail_replace(path, text):
        if path == env_file:
            raise PermissionError("locked")
        write(path, text)

    monkeypatch.setattr(auth, "atomic_write", fail_replace)
    with pytest.raises(PermissionError):
        store.save(NEW, None)
    assert OLD in env_file.read_text()
    assert NEW in store.recovery_path.read_text()
    with pytest.raises(RuntimeError, match="Restore"):
        store.prepare(OLD)
    monkeypatch.setattr(auth, "atomic_write", write)
    store.restore()
    assert NEW in env_file.read_text()
    assert not store.recovery_path.exists()


@pytest.mark.parametrize("failure", ["expired", "timeout"])
def test_failure_never_retries_rotation_or_replaces_local_token(env_file, failure):
    session = DhanSession(
        response=Response(401),
        error=requests.Timeout("sensitive provider body") if failure == "timeout" else None,
    )
    with pytest.raises(RuntimeError) as error:
        auth.renew(CREDENTIALS, auth.LocalTokenStore(env_file), session)
    assert len(session.calls) == 1
    assert OLD in env_file.read_text()
    assert "sensitive provider body" not in str(error.value)
    assert NEW not in str(error.value)


def test_local_preflight_stops_before_rotation_for_wrong_input_token(env_file):
    session = DhanSession()
    with pytest.raises(RuntimeError, match="matching"):
        auth.renew(CREDENTIALS | {"DHAN_ACCESS_TOKEN": "different"}, auth.LocalTokenStore(env_file), session)
    assert not session.calls


class GitHubSession:
    def __init__(self, fail_after_preflight=False, fail_preflight=False):
        self.calls = []
        self.fail_after_preflight = fail_after_preflight
        self.fail_preflight = fail_preflight
        self.puts = 0

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if method == "GET":
            return Response(payload={"key": "public-key", "key_id": "key-1"})
        self.puts += 1
        if self.fail_preflight or (self.fail_after_preflight and self.puts > 1):
            return Response(403)
        return Response(204)


def github_store(tmp_path, monkeypatch, session):
    monkeypatch.setattr(auth, "encrypt_secret", lambda value, key: "sealed-old" if value == OLD else "sealed-new")
    return auth.GitHubTokenStore(
        "owner/repo",
        "market-data",
        "github-test-credential",
        session=session,
        recovery_path=tmp_path / "recovery.json",
    )


def test_github_write_preflight_happens_before_dhan_invalidation(tmp_path, monkeypatch):
    session = GitHubSession(fail_preflight=True)
    store = github_store(tmp_path, monkeypatch, session)
    dhan = DhanSession()
    with pytest.raises(RuntimeError, match="403"):
        auth.renew(CREDENTIALS, store, dhan)
    assert not dhan.calls
    assert json.loads(store.recovery_path.read_text())["encrypted_value"] == "sealed-old"


def test_github_persist_failure_leaves_only_ciphertext_and_supports_restore(tmp_path, monkeypatch, env_file):
    session = GitHubSession(fail_after_preflight=True)
    store = github_store(tmp_path, monkeypatch, session)
    dhan = DhanSession()
    with pytest.raises(RuntimeError, match="403"):
        auth.renew(CREDENTIALS, store, dhan)
    text = store.recovery_path.read_text()
    assert json.loads(text) == {"encrypted_value": "sealed-new", "key_id": "key-1"}
    assert OLD not in text and NEW not in text
    assert len(dhan.calls) == 1
    with pytest.raises(RuntimeError, match="Restore"):
        store.prepare(OLD)
    session.fail_after_preflight = False
    store.restore()
    assert not store.recovery_path.exists()
    assert len(dhan.calls) == 1


def test_github_retries_storage_without_reissuing_dhan_token(tmp_path, monkeypatch, env_file):
    session = GitHubSession()
    request = session.request
    failures = []

    def transient_failure(method, url, **kwargs):
        if method == "PUT" and kwargs["json"]["encrypted_value"] == "sealed-new" and len(failures) < 2:
            failures.append(True)
            return Response(503)
        return request(method, url, **kwargs)

    monkeypatch.setattr(session, "request", transient_failure)
    monkeypatch.setattr(auth.time, "sleep", lambda seconds: None)
    store = github_store(tmp_path, monkeypatch, session)
    dhan = DhanSession()
    assert auth.renew(CREDENTIALS, store, dhan)["status"] == "renewed"
    assert len(failures) == 2
    assert len(dhan.calls) == 1
    assert not store.recovery_path.exists()


def test_workflow_uses_fresh_environment_secret_and_serializes_all_slots():
    text = (auth.md.ROOT / ".github/workflows/refresh-screener-parquet.yml").read_text()
    assert "environment: market-data" in text
    assert 'cron: "35 1,7,13,19 * * *"' in text
    assert "group: shared-market-publisher" in text
    download = text.split("- name: Recover target session", 1)[1].split("- name:", 1)[0]
    assert "DHAN_ACCESS_TOKEN: ${{" not in download
    assert "dhan_token_recovery.json" in text
