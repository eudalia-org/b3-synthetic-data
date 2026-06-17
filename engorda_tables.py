from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

REQUIRED_ENV_VARS = (
    "DATAGEN_RAW_BASE_URI",
    "DATAGEN_SYNTHETIC_BASE_URI",
    "DATAGEN_SPECS_URI",
)
DEFAULT_SCALE_FACTOR = 1.0
DEFAULT_SEED = 42


def table_path_name(table: str) -> str:
    return table.split(".", 1)[1] if "." in table else table


def raw_path(config: dict[str, str], table: str) -> str:
    parts = [config["DATAGEN_RAW_BASE_URI"]]
    if config.get("DATAGEN_RAW_PREFIX"):
        parts.append(config["DATAGEN_RAW_PREFIX"])
    parts.append(table_path_name(table))
    return "/".join(parts)


def synthetic_base_path(config: dict[str, str]) -> str:
    base = config["DATAGEN_SYNTHETIC_BASE_URI"]
    prefix = config.get("DATAGEN_SYNTHETIC_PREFIX")
    return f"{base}/{prefix}" if prefix else base
