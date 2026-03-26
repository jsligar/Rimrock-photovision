"""Compatibility helpers for OpenAI CLIP runtime imports."""

import sys
import types


def ensure_pkg_resources_packaging() -> None:
    """Provide the pkg_resources.packaging shim expected by openai-clip."""
    from packaging import version

    try:
        import pkg_resources as pkg_resources_module
    except ModuleNotFoundError:
        pkg_resources_module = types.ModuleType("pkg_resources")
        sys.modules["pkg_resources"] = pkg_resources_module

    if not hasattr(pkg_resources_module, "packaging"):
        pkg_resources_module.packaging = types.SimpleNamespace(version=version)

    sys.modules["pkg_resources"] = pkg_resources_module
