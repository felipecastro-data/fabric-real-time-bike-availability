"""GBFS v2.3 client for Divvy station status: discovery, conditional GET, retries and typed events.

Standard library only, so the poller notebook needs nothing beyond the wheel (plus the Event Hubs SDK to send).
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

DISCOVERY_URL = "https://gbfs.lyft.com/gbfs/2.3/chi/gbfs.json"
SCHEMA_VERSION = 1
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

REQUIRED_COUNTS = ("num_bikes_available", "num_docks_available")
OPTIONAL_COUNTS = ("num_ebikes_available", "num_bikes_disabled", "num_docks_disabled")
FLAGS = ("is_installed", "is_renting", "is_returning")


@dataclass(frozen=True)
class HttpResult:
    status: int  # 200, or 304 when the conditional GET matched
    body: dict[str, Any] | None
    etag: str | None
    last_modified: str | None


Fetch = Callable[[str, dict[str, str]], HttpResult]


def urllib_fetch(url: str, headers: dict[str, str], timeout: float = 10.0) -> HttpResult:
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - fixed https host
            return HttpResult(
                response.status,
                json.load(response),
                response.headers.get("ETag"),
                response.headers.get("Last-Modified"),
            )
    except urllib.error.HTTPError as error:
        if error.code == 304:  # urllib raises for 304; it is the normal "not modified" answer
            return HttpResult(304, None, error.headers.get("ETag"), error.headers.get("Last-Modified"))
        raise


def get_with_retry(
    url: str,
    headers: dict[str, str],
    *,
    fetch: Fetch = urllib_fetch,
    attempts: int = 4,
    base_delay: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> HttpResult:
    """Retry network errors, bad JSON, 429 and 5xx with exponential backoff (1 s, 2 s, 4 s); raise anything else."""
    for attempt in range(1, attempts + 1):
        try:
            return fetch(url, headers)
        except urllib.error.HTTPError as error:
            if error.code not in RETRYABLE_STATUS or attempt == attempts:
                raise
        except (OSError, ValueError):  # URLError, timeouts, resets, truncated JSON
            if attempt == attempts:
                raise
        sleep(base_delay * 2 ** (attempt - 1))
    raise AssertionError("unreachable")


def discover_feeds(discovery: dict[str, Any], language: str = "en") -> dict[str, str]:
    """Map feed name → URL from `gbfs.json`."""
    return {feed["name"]: feed["url"] for feed in discovery["data"][language]["feeds"]}


def _iso(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class StationStatusEvent:
    """One station in one poll: the envelope sent to the Eventstream."""

    poll_id: str
    polled_at: str  # ISO-8601 UTC, when the request was made
    feed_last_updated: str  # ISO-8601 UTC, feed `last_updated`
    station_id: str
    num_bikes_available: int
    num_docks_available: int
    num_ebikes_available: int | None
    num_bikes_disabled: int | None
    num_docks_disabled: int | None
    is_installed: bool
    is_renting: bool
    is_returning: bool
    last_reported: str  # ISO-8601 UTC, station `last_reported`
    schema_version: int = SCHEMA_VERSION


@dataclass(frozen=True)
class Rejected:
    station_id: str | None
    reason: str  # e.g. "missing:num_bikes_available", "invalid:last_reported"
    raw: dict[str, Any]


def _count(value: Any) -> bool:
    return type(value) is int and value >= 0


def _parse_station(raw: Any, poll_id: str, polled_at: str, feed_last_updated: str) -> StationStatusEvent | Rejected:
    if not isinstance(raw, dict):
        return Rejected(None, "invalid:station", {"value": raw})
    station_id = raw.get("station_id")
    if not isinstance(station_id, str) or not station_id:
        return Rejected(None, "invalid:station_id", raw)
    for name in (*REQUIRED_COUNTS, *FLAGS, "last_reported"):
        if name not in raw:
            return Rejected(station_id, f"missing:{name}", raw)
    for name in (*REQUIRED_COUNTS, "last_reported"):
        if not _count(raw[name]):
            return Rejected(station_id, f"invalid:{name}", raw)
    for name in OPTIONAL_COUNTS:
        if name in raw and not _count(raw[name]):
            return Rejected(station_id, f"invalid:{name}", raw)
    for name in FLAGS:
        if raw[name] not in (0, 1):  # GBFS 2.3 sends 0/1; True/False compare equal
            return Rejected(station_id, f"invalid:{name}", raw)
    return StationStatusEvent(
        poll_id=poll_id,
        polled_at=polled_at,
        feed_last_updated=feed_last_updated,
        station_id=station_id,
        num_bikes_available=raw["num_bikes_available"],
        num_docks_available=raw["num_docks_available"],
        num_ebikes_available=raw.get("num_ebikes_available"),
        num_bikes_disabled=raw.get("num_bikes_disabled"),
        num_docks_disabled=raw.get("num_docks_disabled"),
        is_installed=bool(raw["is_installed"]),
        is_renting=bool(raw["is_renting"]),
        is_returning=bool(raw["is_returning"]),
        last_reported=_iso(raw["last_reported"]),
    )


def parse_station_status(
    payload: dict[str, Any], *, poll_id: str, polled_at: str
) -> tuple[list[StationStatusEvent], list[Rejected]]:
    """Split a `station_status.json` payload into valid events and rejected stations (one bad station never drops the
    poll). A payload without `last_updated` or `data.stations` raises: that is a feed-level failure."""
    feed_last_updated = _iso(payload["last_updated"])
    events: list[StationStatusEvent] = []
    rejected: list[Rejected] = []
    for raw in payload["data"]["stations"]:
        parsed = _parse_station(raw, poll_id, polled_at, feed_last_updated)
        (events if isinstance(parsed, StationStatusEvent) else rejected).append(parsed)
    return events, rejected


@dataclass
class PollResult:
    poll_id: str
    polled_at: str
    outcome: Literal["new", "not_modified", "unchanged"]
    feed_last_updated: str | None = None
    events: list[StationStatusEvent] = field(default_factory=list)
    rejected: list[Rejected] = field(default_factory=list)


class StationStatusPoller:
    """Polls `station_status.json`, sending If-None-Match/If-Modified-Since and skipping a feed whose `last_updated`
    has not moved, so each feed version is emitted once per poller run."""

    def __init__(
        self,
        url: str,
        *,
        fetch: Fetch = urllib_fetch,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.url = url
        self._fetch = fetch
        self._sleep = sleep
        self._clock = clock
        self.etag: str | None = None
        self.last_modified: str | None = None
        self.last_updated: int | None = None

    def poll(self) -> PollResult:
        poll_id = str(uuid.uuid4())
        polled_at = self._clock().astimezone(UTC).isoformat().replace("+00:00", "Z")
        headers = {}
        if self.etag:
            headers["If-None-Match"] = self.etag
        if self.last_modified:
            headers["If-Modified-Since"] = self.last_modified
        response = get_with_retry(self.url, headers, fetch=self._fetch, sleep=self._sleep)
        if response.status == 304:
            return PollResult(poll_id, polled_at, "not_modified")
        assert response.body is not None
        self.etag, self.last_modified = response.etag, response.last_modified
        last_updated = response.body["last_updated"]
        if last_updated == self.last_updated:
            return PollResult(poll_id, polled_at, "unchanged", _iso(last_updated))
        events, rejected = parse_station_status(response.body, poll_id=poll_id, polled_at=polled_at)
        self.last_updated = last_updated
        return PollResult(poll_id, polled_at, "new", _iso(last_updated), events, rejected)
