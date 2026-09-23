"""Installed entry point; companion package is bundled into this plugin directory."""
from .pebble_bridge.plugin import register

__all__ = ['register']
