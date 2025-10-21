"""Pytest fixtures."""

import pytest


@pytest.fixture(scope="session")
def sample_caha_pdf(tmp_path_factory):
    path = tmp_path_factory.mktemp("caha") / "sample.pdf"
    path.write_text("Sample CAHA data")
    return path

