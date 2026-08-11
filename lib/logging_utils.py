"""Uniform logging setup so every pipeline's output reads the same."""

import logging
import os
import sys


def setup_logging(level: str | None = None) -> None:
    logging.basicConfig(
        level=getattr(logging, (level or os.environ.get("LOG_LEVEL", "INFO")).upper()),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    # These libraries narrate every HTTP connection at INFO.
    for noisy in ("urllib3", "neo4j", "neo4j.pool", "neo4j.io"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
