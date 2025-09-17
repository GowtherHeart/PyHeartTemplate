"""Kafka consumer package.

Exports batching and per-message consumer implementations with optional Schema
Registry JSON validation variants (``*RS``).
"""

from .batch import *
from .consumer import *
