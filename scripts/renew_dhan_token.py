"""Rotate a Dhan Web token and persist it before exposing it to the publisher."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import market_data as md  # noqa: E402
from scripts.dhan_market_data import load_credentials  # noqa: E402

RENEW_URL = "https://api.dhan.co/v2/RenewToken"
SECRET_NAME = "DHAN_ACCESS_TOKEN"
REPORT_PATH = md.ROOT / "outputs" / "dhan_token_status.json"
RECOVERY_PATH = md.ROOT / "outputs" / "dhan_token_recovery.json"


class TokenRenewalError(RuntimeError):
    """An operator-facing message that contains no credential or response body."""


def atomic_write(path, text):
    """Keep the original file until its complete replacement is ready."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".env.dhan-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


class LocalTokenStore:
    def __init__(self, path):
        self.path = Path(path)
        self.recovery_path = self.path.with_name(".env.dhan-recovery")
        self.contents = None

    def prepare(self, token):
        if self.recovery_path.exists():
            raise TokenRenewalError("Restore the pending local token before renewing again")
        self.contents = self.path.read_text(encoding="utf-8-sig")
        matches = re.findall(r"(?m)^\s*DHAN_ACCESS_TOKEN\s*=([^\r\n]*)", self.contents)
        if len(matches) != 1 or matches[0].strip().strip("\"'") != token:
            raise TokenRenewalError("Local .env must contain exactly one matching DHAN_ACCESS_TOKEN before renewal")
        # Confirm replacement works before invalidating the currently active token.
        atomic_write(self.path, self.contents)

    def save(self, token, expiry):
        updated = re.sub(
            r"(?m)^(\s*DHAN_ACCESS_TOKEN\s*=)[^\r\n]*",
            lambda match: match[1] + token,
            self.contents,
        )
        # This private, ignored recovery file survives a failed .env replacement.
        atomic_write(self.recovery_path, updated)
        atomic_write(self.path, updated)
        self.recovery_path.unlink()

    def restore(self):
        contents = self.recovery_path.read_text(encoding="utf-8")
        atomic_write(self.path, contents)
        self.recovery_path.unlink()


def encrypt_secret(value, public_key):
    # GitHub requires LibSodium sealed boxes; never implement custom encryption.
    from nacl import encoding, public

    key = public.PublicKey(public_key.encode("ascii"), encoding.Base64Encoder())
    return base64.b64encode(public.SealedBox(key).encrypt(value.encode("utf-8"))).decode("ascii")


class GitHubTokenStore:
    def __init__(self, repository, environment, writer_token, session=None, recovery_path=RECOVERY_PATH):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise TokenRenewalError("Set GITHUB_REPOSITORY to owner/repository")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", environment):
            raise TokenRenewalError("Use a simple GitHub environment name, such as market-data")
        if not writer_token:
            raise TokenRenewalError("Configure DHAN_TOKEN_WRITER with repository Environments read/write permission")
        self.url = f"https://api.github.com/repos/{repository}/environments/{environment}/secrets"
        self.headers = {
            "Authorization": f"Bearer {writer_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self.session = session or requests.Session()
        self.recovery_path = Path(recovery_path)
        self.key = None

    def _request(self, method, suffix, payload=None):
        for attempt in range(5):
            try:
                response = self.session.request(
                    method,
                    self.url + suffix,
                    headers=self.headers,
                    json=payload,
                    timeout=30,
                    allow_redirects=False,
                )
            except requests.RequestException:
                if attempt == 4:
                    raise TokenRenewalError("GitHub secret persistence failed after connection retries") from None
            else:
                if response.status_code in {200, 201, 204}:
                    return response
                if response.status_code not in {429, 500, 502, 503, 504} or attempt == 4:
                    raise TokenRenewalError(f"GitHub secret request failed (HTTP {response.status_code})")
            time.sleep(2**attempt)

    def _encrypted(self, token):
        return {"encrypted_value": encrypt_secret(token, self.key["key"]), "key_id": self.key["key_id"]}

    def prepare(self, token):
        if self.recovery_path.exists():
            raise TokenRenewalError("Restore the pending encrypted token before renewing again")
        self.key = self._request("GET", "/public-key").json()
        # Verify encryption, remote write permissions and local recovery storage
        # before asking Dhan to invalidate the old credential.
        encrypted = self._encrypted(token)
        atomic_write(self.recovery_path, json.dumps(encrypted))
        self._request("PUT", "/" + SECRET_NAME, encrypted)
        self.recovery_path.unlink()

    def save(self, token, expiry):
        encrypted = self._encrypted(token)
        atomic_write(self.recovery_path, json.dumps(encrypted))
        self._request("PUT", "/" + SECRET_NAME, encrypted)
        self.recovery_path.unlink()

    def restore(self):
        encrypted = json.loads(self.recovery_path.read_text(encoding="utf-8"))
        if set(encrypted) != {"key_id", "encrypted_value"}:
            raise TokenRenewalError("Invalid encrypted token recovery file")
        key = self._request("GET", "/public-key").json()
        if encrypted["key_id"] != key["key_id"]:
            raise TokenRenewalError("GitHub encryption key changed; generate a fresh Dhan Web token")
        self._request("PUT", "/" + SECRET_NAME, encrypted)
        self.recovery_path.unlink()


def renew(credentials, store, session=None):
    store.prepare(credentials[SECRET_NAME])
    session = session or requests.Session()
    # This endpoint invalidates the old token. A transport timeout is ambiguous,
    # so never automatically repeat the request using that credential.
    try:
        response = session.get(
            RENEW_URL,
            headers={"access-token": credentials[SECRET_NAME], "dhanClientId": credentials["DHAN_CLIENT_ID"]},
            timeout=30,
            allow_redirects=False,
        )
    except requests.RequestException:
        raise TokenRenewalError(
            "Dhan renewal connection failed; token state is uncertain. A fresh Web token may be needed"
        ) from None
    if response.status_code != 200:
        raise TokenRenewalError(
            f"Dhan renewal failed (HTTP {response.status_code}); verify the active Web-generated token"
        )
    try:
        payload = response.json()
    except ValueError:
        raise TokenRenewalError("Dhan returned an invalid renewal response; generate a fresh Web token") from None
    token = payload.get("accessToken") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token or re.search(r"\s", token):
        raise TokenRenewalError("Dhan returned no valid replacement token; generate a fresh Web token")
    if os.getenv("GITHUB_ACTIONS") == "true":
        print(f"::add-mask::{token}", flush=True)
    expiry = payload.get("expiryTime")
    # Persist even if optional expiry metadata is absent or malformed.
    try:
        expiry = datetime.fromisoformat(expiry).isoformat()
    except (TypeError, ValueError):
        expiry = None
    store.save(token, expiry)
    # A step-level secrets expression on the download step would override this.
    if os.getenv("GITHUB_ENV"):
        with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as handle:
            handle.write(f"{SECRET_NAME}={token}\n")
    return {"status": "renewed", "expiry_time": expiry}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--github", action="store_true", help="Persist to a GitHub environment secret")
    parser.add_argument(
        "--restore", action="store_true", help="Retry saving an encrypted recovery file without renewing"
    )
    args = parser.parse_args()
    report = {"checked_at": datetime.now(timezone.utc).isoformat()}
    try:
        with md.publisher_lock():
            if args.github:
                store = GitHubTokenStore(
                    os.getenv("GITHUB_REPOSITORY", ""),
                    os.getenv("DHAN_GITHUB_ENVIRONMENT", "market-data"),
                    os.getenv("DHAN_TOKEN_WRITER", ""),
                )
            else:
                store = LocalTokenStore(md.ROOT / ".env")
            if args.restore:
                store.restore()
                report.update(status="restored")
            else:
                report.update(renew(load_credentials(), store))
    except TokenRenewalError as exc:
        report.update(status="failed", error=str(exc))
    except (RuntimeError, OSError, ValueError, ImportError):
        # Keep third-party exceptions, response bodies and credentials out of logs
        # and public issue reports. Recovery ciphertext may remain for a failed save.
        report.update(
            status="failed",
            error="Token renewal or persistence failed; verify credentials, connectivity and secret-write permissions",
        )
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report))
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
