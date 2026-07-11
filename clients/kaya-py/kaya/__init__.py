"""KayaDB Python client (pure standard library)."""

from .client import InvalidArgument, KayaClient, KayaError, NotFound

__all__ = ["KayaClient", "KayaError", "NotFound", "InvalidArgument"]
__version__ = "0.1.0"
