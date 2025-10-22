"""Authentication dependencies.

These are placeholders that can be replaced once a real auth system is
available. Routers include them so that flipping on auth requires no API
changes.
"""

from collections.abc import Generator


def require_user() -> Generator[None, None, None]:
    """Placeholder dependency for authenticated routes."""
    # The backend does not yet have authentication. Once auth is available, this
    # dependency can be updated to validate the incoming request and return the
    # authenticated principal.
    yield
