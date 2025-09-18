"""VismaService entry point."""

from __future__ import annotations

from tap_visma_service.tap import TapVismaService

TapVismaService.cli()
