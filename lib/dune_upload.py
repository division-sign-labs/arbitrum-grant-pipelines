"""Upload a wallet list to Dune so joins can happen server-side.

Why this exists: the Farcaster verified-address set is ~millions of rows and
some of the tables we join it against (every ARB transfer, every dex trade) are
far larger. Downloading both sides to join in pandas is wasteful; uploading the
small side once and joining in Dune is not.

Privacy note: this account only permits **public** uploads, so anything sent
here is world-readable. That is acceptable for Farcaster verified addresses,
which are already public data published on Farcaster hubs — but it is a real
constraint, so uploading is opt-in per call and never happens implicitly.
"""

from __future__ import annotations

import io
import logging

import pandas as pd
import requests

from config.settings import DUNE_API_BASE, DUNE_API_KEY

logger = logging.getLogger(__name__)

# Dune caps a single upload at 200MB.
MAX_UPLOAD_BYTES = 200 * 1024 * 1024


class DuneUploadError(RuntimeError):
    pass


class DuneUploader:
    def __init__(self, api_key: str | None = None, namespace: str | None = None):
        self.api_key = api_key or DUNE_API_KEY
        if not self.api_key:
            raise RuntimeError("DUNE_API_KEY is not set.")
        self.namespace = namespace

    def upload(self, table_name: str, df: pd.DataFrame, description: str = "") -> str:
        """Replace `table_name` with `df`. Returns the fully-qualified table name.

        Re-uploading the same table_name overwrites it wholesale, which is the
        semantics we want: the wallet list is a snapshot, not an append log.
        """
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        payload = buffer.getvalue()
        size = len(payload.encode())
        if size > MAX_UPLOAD_BYTES:
            raise DuneUploadError(
                f"{table_name} is {size / 1e6:.0f}MB, over Dune's 200MB upload limit. "
                "Narrow the column set or split the join."
            )

        logger.info(
            "uploading %s to Dune (%d rows, %.1fMB) — this table will be PUBLIC",
            table_name,
            len(df),
            size / 1e6,
        )
        response = requests.post(
            f"{DUNE_API_BASE}/uploads/csv",
            headers={"X-DUNE-API-KEY": self.api_key, "Content-Type": "application/json"},
            json={
                "table_name": table_name,
                "data": payload,
                "description": description,
                "is_private": False,
            },
            timeout=900,
        )
        if response.status_code >= 400:
            raise DuneUploadError(
                f"upload of {table_name} failed ({response.status_code}): {response.text[:400]}"
            )
        body = response.json()
        full_name = body.get("full_name")
        if not full_name:
            raise DuneUploadError(f"upload succeeded but returned no table name: {body}")
        self.namespace = full_name.split(".")[1] if full_name.count(".") == 2 else self.namespace
        logger.info("uploaded -> %s", full_name)
        return full_name

    def delete(self, table_name: str, namespace: str | None = None) -> bool:
        namespace = namespace or self.namespace
        if not namespace:
            raise DuneUploadError("namespace required to delete an uploaded table")
        response = requests.delete(
            f"{DUNE_API_BASE}/table/{namespace}/{table_name}",
            headers={"X-DUNE-API-KEY": self.api_key},
            timeout=120,
        )
        return response.status_code < 400
