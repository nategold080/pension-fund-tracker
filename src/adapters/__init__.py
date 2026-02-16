"""Pension fund adapter registry.

Maps adapter IDs to their classes for CLI and pipeline use.
"""

from src.adapters.calpers import CalPERSAdapter
from src.adapters.calstrs import CalSTRSAdapter
from src.adapters.wsib import WSIBAdapter
from src.adapters.oregon import OregonAdapter
from src.adapters.ny_common import NYCommonAdapter
from src.adapters.texas_trs import TexasTRSAdapter
from src.adapters.florida_sba import FloridaSBAAdapter

# Adapters that run by default (reliable, fully automated)
DEFAULT_ADAPTERS = ["calpers", "calstrs", "wsib", "oregon", "ny_common"]

# Registry of all available adapters (including those requiring manual steps)
ADAPTER_REGISTRY: dict[str, type] = {
    "calpers": CalPERSAdapter,
    "calstrs": CalSTRSAdapter,
    "wsib": WSIBAdapter,
    "oregon": OregonAdapter,
    "ny_common": NYCommonAdapter,
    "texas_trs": TexasTRSAdapter,
    "florida_sba": FloridaSBAAdapter,
}


def get_adapter(name: str, **kwargs):
    """Get an adapter instance by name.

    Args:
        name: Adapter ID (e.g., "calpers").
        **kwargs: Additional arguments passed to the adapter constructor.

    Returns:
        Adapter instance.

    Raises:
        KeyError: If adapter name is not found.
    """
    if name not in ADAPTER_REGISTRY:
        available = ", ".join(sorted(ADAPTER_REGISTRY.keys()))
        raise KeyError(f"Unknown adapter '{name}'. Available: {available}")
    return ADAPTER_REGISTRY[name](**kwargs)


def get_default_adapters(**kwargs) -> list:
    """Get instances of the default (fully automated) adapters."""
    return [ADAPTER_REGISTRY[name](**kwargs) for name in DEFAULT_ADAPTERS]


def get_all_adapters(**kwargs) -> list:
    """Get instances of all registered adapters."""
    return [cls(**kwargs) for cls in ADAPTER_REGISTRY.values()]
