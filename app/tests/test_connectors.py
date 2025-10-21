from connectors.caha_pdf import CAHAPDFConnector
from connectors.reddit import RedditConnector
from connectors.events import EventsConnector


def test_caha_connector_fetch(tmp_path):
    pdf_path = tmp_path / "caha.pdf"
    pdf_path.write_bytes(b"sample content")
    connector = CAHAPDFConnector(pdf_path=pdf_path)
    sources = connector.fetch(None)
    assert sources[0].channel == "caha_pdf"


def test_reddit_extract_handles(tmp_path):
    html = "@artist1 https://example.com"
    fixture = tmp_path / "reddit.html"
    fixture.write_text(html, encoding="utf-8")
    connector = RedditConnector()
    payload = connector.fetch(None)[0]
    payload.raw_blob_ptr = str(fixture)
    candidates = connector.extract(payload)
    assert any("@artist1" in c.name for c in candidates)


def test_events_extract(tmp_path):
    fixture = tmp_path / "events.txt"
    fixture.write_text("Fresku Fest\nEIF", encoding="utf-8")
    connector = EventsConnector()
    payload = connector.fetch(None)[0]
    payload.raw_blob_ptr = str(fixture)
    candidates = connector.extract(payload)
    assert len(candidates) == 2

