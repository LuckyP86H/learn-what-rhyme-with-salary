"""Optional, opt-in scenario launcher.

celery_viz is read-only by default. A project that wants a "launch a demo workflow" button supplies a
catalog and enables it explicitly (`--catalog module:attr`). Only scenarios the catalog *names* can be
launched; there is deliberately no way to send an arbitrary task name with arbitrary arguments.

A catalog is any object with:
    describe() -> list[{"id", "title", "description", "params": [{"name", "label", "type", "default", "options"?, "min"?, "max"?}]}]
    launch(scenario_id: str, params: dict) -> dict      # raise ValueError for unknown scenario / bad params
"""
from __future__ import annotations

import importlib
from typing import Any, Protocol


class Catalog(Protocol):
    def describe(self) -> list[dict[str, Any]]: ...

    def launch(self, scenario_id: str, params: dict[str, Any]) -> dict[str, Any]: ...


def load_catalog(spec: str) -> Catalog:
    module_name, _, attr = spec.partition(":")
    if not module_name or not attr:
        raise ValueError(f"catalog must look like 'package.module:object', got {spec!r}")
    catalog = getattr(importlib.import_module(module_name), attr)
    return catalog() if callable(catalog) and not hasattr(catalog, "launch") else catalog


def clean_params(schema: list[dict[str, Any]], given: dict[str, Any]) -> dict[str, Any]:
    """Validate launch parameters against the declared schema: unknown keys are rejected, values coerced and bounded."""
    known = {p["name"]: p for p in schema}
    unknown = set(given) - set(known)
    if unknown:
        raise ValueError(f"unknown parameters: {sorted(unknown)}")
    out: dict[str, Any] = {}
    for name, spec in known.items():
        value = given.get(name, spec.get("default"))
        if spec["type"] == "int":
            value = int(value)
            if not spec.get("min", value) <= value <= spec.get("max", value):
                raise ValueError(f"{name} must be between {spec.get('min')} and {spec.get('max')}")
        elif spec["type"] == "select":
            if value not in spec["options"]:
                raise ValueError(f"{name} must be one of {spec['options']}")
        out[name] = value
    return out
