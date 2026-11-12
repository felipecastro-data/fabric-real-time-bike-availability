import base64
import json
from pathlib import Path

import pytest
from azure.eventhub import EventDataBatch

from divvy_rti.gbfs import PollResult, Rejected, parse_station_status
from divvy_rti.stream import CallableTokenCredential, resolve_custom_endpoint, run_poll_loop, send_events


def load(name):
    return json.loads((Path(__file__).parents[1] / "fixtures" / name).read_text())


def events(n=20):
    found, _ = parse_station_status(load("station_status.json"), poll_id="p", polled_at="2026-11-12T18:00:00Z")
    return (found * (n // len(found) + 1))[:n]


class FakeProducer:
    def __init__(self):
        self.batches = []

    def create_batch(self, *, max_size_in_bytes):
        return EventDataBatch(max_size_in_bytes=max_size_in_bytes)

    def send_batch(self, batch):
        self.batches.append(len(batch))


def test_send_events_splits_batches_under_the_cap():
    producer = FakeProducer()
    assert send_events(producer, events(100), max_batch_bytes=4096) == 100
    assert len(producer.batches) > 1 and sum(producer.batches) == 100


def test_send_events_with_nothing_sends_no_batch():
    producer = FakeProducer()
    assert send_events(producer, []) == 0 and producer.batches == []


def test_send_events_refuses_an_event_bigger_than_a_batch():
    with pytest.raises(ValueError):
        send_events(FakeProducer(), events(1), max_batch_bytes=64)


def test_credential_reads_expiry_from_jwt():
    payload = base64.urlsafe_b64encode(json.dumps({"exp": 1794510000}).encode()).decode().rstrip("=")
    token = f"h.{payload}.s"
    access = CallableTokenCredential(lambda: token).get_token("https://eventhubs.azure.net/.default")
    assert (access.token, access.expires_on) == (token, 1794510000)


def test_resolve_custom_endpoint_by_names():
    base = "https://api.fabric.microsoft.com/v1/workspaces/ws/eventstreams"
    responses = {
        base: {"value": [{"id": "other", "displayName": "es_x"}, {"id": "es1", "displayName": "es_station_status"}]},
        f"{base}/es1/topology": {"sources": [{"id": "s1", "name": "src_gbfs_poller"}]},
        f"{base}/es1/sources/s1/connection": {
            "fullyQualifiedNamespace": "ns.servicebus.windows.net",
            "eventHubName": "h",
        },
    }
    assert resolve_custom_endpoint("ws", "t", get_json=lambda url, token: responses[url]) == (
        "ns.servicebus.windows.net",
        "h",
    )
    with pytest.raises(LookupError):
        resolve_custom_endpoint("ws", "t", eventstream="missing", get_json=lambda url, token: responses[url])


def test_poll_loop_runs_until_deadline_and_survives_failures():
    now = [0.0]
    outcomes = iter(["new", "boom", "not_modified", "new"])
    logs = []

    def poll():
        now[0] += 2  # each poll takes 2 s
        outcome = next(outcomes)
        if outcome == "boom":
            raise TimeoutError("feed down")
        evs = events(3) if outcome == "new" else []
        rejected = [Rejected("x", "missing:last_reported", {})] if outcome == "new" else []
        return PollResult("id", "t", outcome, None, evs, rejected)

    sends = iter([3, RuntimeError("amqp")])

    def send(evs):
        result = next(sends)
        if isinstance(result, Exception):
            raise result
        return result

    stats = run_poll_loop(
        poll,
        send,
        duration_s=240,
        interval_s=60,
        log=logs.append,
        clock=lambda: now[0],
        sleep=lambda s: now.__setitem__(0, now[0] + s),
    )
    assert stats == {"new": 2, "poll_failed": 1, "not_modified": 1, "events_sent": 3, "rejected": 2, "send_failed": 1}
    assert [entry["event"] for entry in logs] == ["poll", "poll_failed", "poll", "poll"]
    assert logs[0]["reject_reasons"] == {"missing:last_reported": 1} and "amqp" in logs[3]["error"]
    assert now[0] == 182  # four ticks at 0, 60, 120, 180 (+2 s for the last poll); next tick 240 hits the deadline
