"""Send GBFS events to the Eventstream custom endpoint and run the poll loop used by `nb_gbfs_poller`."""

from __future__ import annotations

import base64
import json
import time
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import asdict
from typing import Any, Protocol

from azure.core.credentials import AccessToken
from azure.eventhub import EventData, EventDataBatch

from divvy_rti.gbfs import PollResult, StationStatusEvent

FABRIC_API = "https://api.fabric.microsoft.com/v1"
EVENTHUBS_AUDIENCE = "https://eventhubs.azure.net"
# Measured 2026-11-12: 256 KB and 1 MB batches to the custom endpoint failed with "write operation timed out".
MAX_BATCH_BYTES = 64 * 1024


class CallableTokenCredential:
    """azure-core TokenCredential over a function that returns a raw JWT (e.g. `notebookutils.credentials.getToken`).
    The function is called on every refresh, so it should do its own caching."""

    def __init__(self, get_token: Callable[[], str]) -> None:
        self._get_token = get_token

    def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        token = self._get_token()
        return AccessToken(token, jwt_expiry(token))


def jwt_expiry(token: str) -> int:
    payload = token.split(".")[1]
    return int(json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["exp"])


def fabric_get_json(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - fixed https host
        return json.load(response)


def resolve_custom_endpoint(
    workspace_id: str,
    fabric_token: str,
    *,
    eventstream: str = "es_station_status",
    source: str = "src_gbfs_poller",
    get_json: Callable[[str, str], dict[str, Any]] = fabric_get_json,
) -> tuple[str, str]:
    """Find the Event Hubs namespace and hub behind a custom-endpoint source by item and source name, so the same
    notebook works in dev, test and prod without per-stage parameters."""
    base = f"{FABRIC_API}/workspaces/{workspace_id}/eventstreams"
    items = [i for i in get_json(base, fabric_token)["value"] if i["displayName"] == eventstream]
    if not items:
        raise LookupError(f"Eventstream {eventstream!r} not found in workspace {workspace_id}")
    topology = get_json(f"{base}/{items[0]['id']}/topology", fabric_token)
    sources = [s for s in topology["sources"] if s["name"] == source]
    if not sources:
        raise LookupError(f"Source {source!r} not found in {eventstream!r}")
    connection = get_json(f"{base}/{items[0]['id']}/sources/{sources[0]['id']}/connection", fabric_token)
    return connection["fullyQualifiedNamespace"], connection["eventHubName"]


class Producer(Protocol):
    def create_batch(self, *, max_size_in_bytes: int) -> EventDataBatch: ...
    def send_batch(self, batch: EventDataBatch) -> None: ...


def send_events(
    producer: Producer, events: Iterable[StationStatusEvent], max_batch_bytes: int = MAX_BATCH_BYTES
) -> int:
    """Send events as compact JSON in batches of at most `max_batch_bytes`; return how many were sent."""
    sent = 0
    batch = producer.create_batch(max_size_in_bytes=max_batch_bytes)
    for event in events:
        data = EventData(json.dumps(asdict(event), separators=(",", ":")))
        try:
            batch.add(data)
        except ValueError:  # batch full
            if len(batch) == 0:
                raise  # one event bigger than a batch: never expected for ~400-byte events
            producer.send_batch(batch)
            sent += len(batch)
            batch = producer.create_batch(max_size_in_bytes=max_batch_bytes)
            batch.add(data)
    if len(batch):
        producer.send_batch(batch)
        sent += len(batch)
    return sent


def run_poll_loop(
    poll: Callable[[], PollResult],
    send: Callable[[list[StationStatusEvent]], int],
    *,
    duration_s: float,
    interval_s: float,
    log: Callable[[dict[str, Any]], None],
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Counter[str]:
    """Poll every `interval_s` until `duration_s` is used up. A failed poll or send is logged and the loop goes on:
    the next poll carries a full snapshot, so one lost minute heals itself."""
    deadline = clock() + duration_s
    stats: Counter[str] = Counter()
    while True:
        started = clock()
        try:
            result = poll()
        except Exception as error:  # keep the hourly run alive; logged below
            stats["poll_failed"] += 1
            log({"event": "poll_failed", "error": repr(error)})
        else:
            stats[result.outcome] += 1
            record: dict[str, Any] = {
                "event": "poll",
                "poll_id": result.poll_id,
                "polled_at": result.polled_at,
                "outcome": result.outcome,
                "feed_last_updated": result.feed_last_updated,
                "events": len(result.events),
                "rejected": len(result.rejected),
            }
            if result.rejected:
                stats["rejected"] += len(result.rejected)
                record["reject_reasons"] = dict(Counter(r.reason for r in result.rejected))
            if result.events:
                try:
                    record["sent"] = send(result.events)
                    stats["events_sent"] += record["sent"]
                except Exception as error:  # keep the hourly run alive; logged below
                    stats["send_failed"] += 1
                    record["error"] = repr(error)
            log(record)
        next_tick = started + interval_s
        if next_tick >= deadline:
            return stats
        sleep(max(0.0, next_tick - clock()))
