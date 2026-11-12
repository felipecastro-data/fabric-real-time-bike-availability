"""Fabric API access tokens for the repo's tools.

* **GitHub Actions** (``ACTIONS_ID_TOKEN_REQUEST_URL`` set): request a *fresh* GitHub OIDC token and exchange it with
  Entra ID (client-credentials flow with a federated client assertion) for every renewal. Re-running
  ``az account get-access-token`` is not enough in CI: ``azure/login`` signs the CLI in with a single, short-lived
  federated token, so a renewal during a long pipeline run would fail.
* **Locally**: the signed-in Azure CLI user (``az login``).

Tokens are cached and renewed every 30 minutes (well inside their ~60-minute lifetime).
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.parse
import urllib.request

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
RENEW_AFTER_S = 1800

_cache: dict[str, tuple[str, float]] = {}


def _github_oidc_token() -> str:
    url = (
        os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
        + "&audience="
        + urllib.parse.quote("api://AzureADTokenExchange", safe="")
    )
    request = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {os.environ['ACTIONS_ID_TOKEN_REQUEST_TOKEN']}"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - GitHub-provided URL
        return json.load(response)["value"]


def _entra_token(scope: str) -> str:
    tenant, client = os.environ["AZURE_TENANT_ID"], os.environ["AZURE_CLIENT_ID"]
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client,
            "scope": scope,
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": _github_oidc_token(),
        }
    ).encode()
    url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    with urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=30) as response:  # noqa: S310
        return json.load(response)["access_token"]


def _az_cli_token(scope: str) -> str:
    resource = scope.removesuffix("/.default")
    cmd = ["az", "account", "get-access-token", "--resource", resource, "--query", "accessToken", "-o", "tsv"]
    return subprocess.check_output(cmd, text=True).strip()


def in_github_actions() -> bool:
    return "ACTIONS_ID_TOKEN_REQUEST_URL" in os.environ


def access_token(scope: str = FABRIC_SCOPE) -> str:
    cached = _cache.get(scope)
    if cached is None or time.time() - cached[1] > RENEW_AFTER_S:
        token = _entra_token(scope) if in_github_actions() else _az_cli_token(scope)
        _cache[scope] = (token, time.time())
    return _cache[scope][0]
