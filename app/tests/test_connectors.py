from datetime import timedelta
from pathlib import Path

from connectors.caha_pdf import CAHAPDFConnector
from connectors.events import EventsConnector
from connectors.reddit import RedditConnector
from connectors.storage import ObjectStore


def test_caha_connector_fetch(tmp_path):
    pdf_path = tmp_path / "caha.pdf"
    pdf_path.write_bytes(b"sample content")
    connector = CAHAPDFConnector(pdf_path=pdf_path)
    sources = connector.fetch(None)
    assert sources[0].channel == "caha_pdf"


def _fixture_bytes(name: str) -> bytes:
    path = Path(__file__).parent / "fixtures" / name
    return path.read_bytes()


def test_reddit_extract_handles_and_urls(tmp_path):
    fetch_calls = 0

    def fetcher(_: str) -> bytes:
        nonlocal fetch_calls
        fetch_calls += 1
        return _fixture_bytes("reddit_thread.html")

    connector = RedditConnector(fetcher=fetcher, object_store=ObjectStore(tmp_path / "reddit"))
    payload = connector.fetch(None)[0]
    candidates = connector.extract(payload)

    assert fetch_calls == 1
    assert any(
        c.metadata.get("handle") == "islandtatts"
        and c.metadata.get("endorsement_score") == 120
        and c.metadata.get("thread_permalink") == "https://www.reddit.com/r/guam/comments/abc123/community_showcase"
        for c in candidates
    )
    assert any(
        c.metadata.get("url") == "https://weaverarts.example"
        and c.metadata.get("context") == "comment"
        and c.metadata.get("endorsement_score") == 15
        for c in candidates
    )


def test_reddit_fetch_respects_since_cache(tmp_path):
    fixture = _fixture_bytes("reddit_thread.html")
    call_count = 0

    def fetcher(_: str) -> bytes:
        nonlocal call_count
        call_count += 1
        return fixture

    connector = RedditConnector(fetcher=fetcher, object_store=ObjectStore(tmp_path / "reddit-cache"))
    first_payload = connector.fetch(None)[0]
    first_path = first_payload.raw_blob_ptr
    assert call_count == 1

    def fail_fetcher(_: str) -> bytes:
        raise AssertionError("fetcher should not be called when cache is fresh")

    connector._fetcher = fail_fetcher
    cached_payload = connector.fetch(first_payload.fetched_at - timedelta(seconds=1))[0]
    assert cached_payload.raw_blob_ptr == first_path



def test_events_extracts_performers_and_vendors(tmp_path):
    connector = EventsConnector(
        fetcher=lambda _: _fixture_bytes("events_calendar.html"),
        object_store=ObjectStore(tmp_path / "events"),
    )
    payload = connector.fetch(None)[0]
    candidates = connector.extract(payload)

    performer = next(c for c in candidates if c.name == "Island Drummers")
    assert performer.metadata["role"] == "performer"
    assert performer.metadata["event_name"] == "Fresku Fest"
    assert performer.metadata["event_date"] == "2024-08-15"
    assert performer.metadata["venue"] == "Chamorro Village"
    assert "https://www.guamtime.net/events/fresku" in performer.metadata["source_url"]

    vendor = next(c for c in candidates if c.name == "Luna Treats")
    assert vendor.metadata["role"] == "vendor"
    assert vendor.metadata["event_date"] == "2024-09-01T18:00:00"

