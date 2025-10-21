"""Connector protocol definitions and registry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, Sequence

from pydantic import BaseModel, Field


class SourcePayload(BaseModel):
    channel: str
    url: str | None = None
    kind: str | None = None
    fetched_at: datetime
    content_hash: str | None = None
    raw_blob_ptr: str | None = None
    meta: dict[str, object] = Field(default_factory=dict)


class Candidate(BaseModel):
    name: str
    evidence: str
    channel: str
    metadata: dict[str, object] = Field(default_factory=dict)


class Connector(Protocol):
    name: str
    default_cadence: str

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        ...

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        ...


@dataclass
class RegisteredConnector:
    connector: Connector


class ConnectorRegistry:
    def __init__(self) -> None:
        self._registry: dict[str, RegisteredConnector] = {}

    def register(self, connector: Connector) -> None:
        self._registry[connector.name] = RegisteredConnector(connector)

    def all(self) -> list[Connector]:
        return [entry.connector for entry in self._registry.values()]

    def get(self, name: str) -> Connector:
        return self._registry[name].connector


registry = ConnectorRegistry()

