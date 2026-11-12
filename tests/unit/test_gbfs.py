import copy
import json
import threading
import urllib.error
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from divvy_rti.gbfs import (
    SCHEMA_VERSION,
    HttpResult,
    StationStatusPoller,
    discover_feeds,
    get_with_retry,
    parse_station_status,
    station_information_rows,
    urllib_fetch,
)

FIXTURES = Path(__file__).parents[1] / "fixtures"


def load(name):
    return json.loads((FIXTURES / name).read_text())


def http_error(code):
    return urllib.error.HTTPError("https://example.test", code, "x", {}, None)


def test_discover_feeds_finds_station_feeds():
    feeds = discover_feeds(load("gbfs.json"))
    assert feeds["station_status"] == "https://gbfs.lyft.com/gbfs/2.3/chi/en/station_status.json"
    assert feeds["station_information"].endswith("/en/station_information.json")


def test_parse_recorded_feed():
    events, rejected = parse_station_status(load("station_status.json"), poll_id="p1", polled_at="2026-11-12T18:00:00Z")
    assert len(events) == 20 and rejected == []
    first = events[0]
    assert first.station_id == "2246215988797035830"
    assert (first.num_bikes_available, first.num_docks_available, first.num_ebikes_available) == (2, 3, 1)
    assert first.is_renting is True
    assert first.last_reported == "2026-11-12T17:50:39Z"
    assert first.feed_last_updated == "2026-11-12T17:57:11Z"
    assert first.schema_version == SCHEMA_VERSION and first.poll_id == "p1"


def test_malformed_stations_are_rejected_not_fatal():
    payload = copy.deepcopy(load("station_status.json"))
    stations = payload["data"]["stations"]
    del stations[0]["num_bikes_available"]
    stations[1]["num_docks_available"] = -1
    stations[2]["station_id"] = ""
    stations[3]["is_renting"] = 2
    stations[4]["num_bikes_disabled"] = "3"
    stations.append("not a station")
    events, rejected = parse_station_status(payload, poll_id="p", polled_at="t")
    assert len(events) == 15
    assert [r.reason for r in rejected] == [
        "missing:num_bikes_available",
        "invalid:num_docks_available",
        "invalid:station_id",
        "invalid:is_renting",
        "invalid:num_bikes_disabled",
        "invalid:station",
    ]


def test_retry_backs_off_then_succeeds():
    calls, sleeps = [], []
    ok = HttpResult(200, {}, None, None)

    def fetch(url, headers):
        calls.append(url)
        if len(calls) == 1:
            raise http_error(503)
        if len(calls) == 2:
            raise urllib.error.URLError("reset")
        return ok

    assert get_with_retry("u", {}, fetch=fetch, sleep=sleeps.append) is ok
    assert sleeps == [1.0, 2.0]


def test_retry_gives_up_on_client_error_and_after_last_attempt():
    sleeps = []

    def not_found(url, headers):
        raise http_error(404)

    with pytest.raises(urllib.error.HTTPError):
        get_with_retry("u", {}, fetch=not_found, sleep=sleeps.append)
    assert sleeps == []

    def always_down(url, headers):
        raise http_error(502)

    with pytest.raises(urllib.error.HTTPError):
        get_with_retry("u", {}, fetch=always_down, attempts=3, sleep=sleeps.append)
    assert sleeps == [1.0, 2.0]


def test_poller_skips_not_modified_and_unchanged_feed():
    body = load("station_status.json")
    responses = [
        HttpResult(200, body, '"v1"', "Thu, 12 Nov 2026 17:57:12 GMT"),
        HttpResult(304, None, '"v1"', None),
        HttpResult(200, body, '"v2"', None),  # new ETag, same last_updated
    ]
    seen_headers = []

    def fetch(url, headers):
        seen_headers.append(headers)
        return responses.pop(0)

    poller = StationStatusPoller("u", fetch=fetch, clock=lambda: datetime(2026, 11, 12, 18, tzinfo=UTC))
    first = poller.poll()
    assert first.outcome == "new" and len(first.events) == 20
    assert first.polled_at == "2026-11-12T18:00:00Z" and first.events[0].poll_id == first.poll_id
    assert poller.poll().outcome == "not_modified"
    assert poller.poll().outcome == "unchanged"
    assert seen_headers[0] == {}
    assert seen_headers[1] == {"If-None-Match": '"v1"', "If-Modified-Since": "Thu, 12 Nov 2026 17:57:12 GMT"}


def test_urllib_fetch_returns_304_on_matching_etag():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.headers.get("If-None-Match") == '"abc"':
                self.send_response(304)
                self.send_header("ETag", '"abc"')
                self.end_headers()
                return
            body = b'{"last_updated": 1}'
            self.send_response(200)
            self.send_header("ETag", '"abc"')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/station_status.json"
    try:
        first = urllib_fetch(url, {})
        second = urllib_fetch(url, {"If-None-Match": first.etag})
    finally:
        server.shutdown()
    assert (first.status, first.body, first.etag) == (200, {"last_updated": 1}, '"abc"')
    assert (second.status, second.body) == (304, None)


def test_station_information_rows():
    fetched_at = datetime(2026, 11, 12, 18, tzinfo=UTC)
    snapshot_date, rows = station_information_rows(load("station_information.json"), fetched_at)
    assert snapshot_date == "2026-11-12" and len(rows) == 20
    classic, light = rows[0], rows[-1]
    assert (classic["short_name"], classic["capacity"]) == ("CHI02390", 6)
    assert light["short_name"] is None and light["address"]
    assert json.loads(classic["raw"])["station_id"] == classic["station_id"]
    assert {r["snapshot_date"] for r in rows} == {"2026-11-12"} and classic["fetched_at"] == fetched_at


def test_snapshot_date_is_the_chicago_date():
    payload = {"last_updated": 1794538800, "data": {"stations": []}}  # 2026-11-13 03:00 UTC = 2026-11-12 21:00 CST
    assert station_information_rows(payload, datetime.now(UTC))[0] == "2026-11-12"
