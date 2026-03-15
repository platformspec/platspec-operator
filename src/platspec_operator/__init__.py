"""
Platspec Operator — open-source Kubernetes operator implementing the
Platform Specification processing layer.
"""

__version__ = "0.1.0"
__author__ = "Josh West <josh@foundation.io>"

from .config import Config
from .logs import setup_logging

__all__ = ["Config", "setup_logging", "__version__"]
