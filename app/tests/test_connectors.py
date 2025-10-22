from connectors.artspace import ArtspaceConnector
from connectors.caha_pdf import CAHAPDFConnector
from connectors.events import EventsConnector
from connectors.guma import GumaConnector
from connectors.reddit import RedditConnector
from connectors.uog import UOGConnector


def test_caha_connector_extracts_structured_metadata():
    connector = CAHAPDFConnector()
    payload = connector.fetch(None)[0]
    candidates = connector.extract(payload)

    names = {candidate.name for candidate in candidates}
    assert names == {
        "Lina Cruz",
        "Mason Reyes",
        "Tia Santos",
        "Sofia Kim",
    }

    lina = next(candidate for candidate in candidates if candidate.name == "Lina Cruz")
    assert lina.metadata["email"] == "lina.cruz@example.com"
    assert lina.metadata["phone_normalized"] == "6715550101"
    assert lina.metadata["institutional_anchor"] is True


def test_guma_connector_deduplicates_contacts():
    connector = GumaConnector()
    payload = connector.fetch(None)[0]
    candidates = connector.extract(payload)

    assert len(candidates) == 2
    programs = {c.metadata["program"] for c in candidates}
    assert "Traditional Arts Incubator" in programs


def test_artspace_connector_extracts_unique_residents():
    connector = ArtspaceConnector()
    payload = connector.fetch(None)[0]
    candidates = connector.extract(payload)

    assert len(candidates) == 2
    assert {c.name for c in candidates} == {"Aria Cepeda", "Benjamin Cruz"}


def test_uog_connector_includes_department_metadata():
    connector = UOGConnector()
    payload = connector.fetch(None)[0]
    candidates = connector.extract(payload)

    assert len(candidates) == 3
    departments = {c.metadata["department"] for c in candidates}
    assert "College of Liberal Arts" in departments


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

