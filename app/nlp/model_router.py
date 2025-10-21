"""Model router that maps capabilities to configured providers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict

import yaml


CapabilityFn = Callable[..., Any]


@dataclass
class Capability:
    name: str
    run: CapabilityFn


class ModelRouter:
    def __init__(self, config_path: str | None = None) -> None:
        default_path = (
            Path(os.getenv("MODEL_CONFIG_PATH"))
            if os.getenv("MODEL_CONFIG_PATH")
            else Path(__file__).resolve().parents[1] / "infra" / "models.yaml"
        )
        path = Path(config_path) if config_path else default_path
        with path.open("r", encoding="utf-8") as fp:
            self._config: dict[str, Any] = yaml.safe_load(fp)
        self._capabilities: Dict[str, Capability] = {}

    def register(self, capability: str, func: CapabilityFn) -> None:
        self._capabilities[capability] = Capability(name=capability, run=func)

    def capability(self, capability: str) -> Capability:
        if capability not in self._capabilities:
            raise KeyError(f"Capability '{capability}' not registered")
        return self._capabilities[capability]

    @property
    def config(self) -> dict[str, Any]:
        return self._config


router = ModelRouter()

