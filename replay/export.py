"""Export Gold-layer analytics tables to static Parquet/CSV snapshots.

Reads the existing Gold Delta tables from local disk when present
(``storage.config.delta_gold_path``); otherwise generates a realistic
static sample. Run directly with:

    python -m replay.export
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from replay.config import config
from replay.sample_data import (
    generate_funnel_metrics,
    generate_product_performance,
    generate_traffic_analytics,
)
from storage.config import config as storage_config

logger = logging.getLogger(__name__)

_GENERATORS = {
    "funnel_metrics": generate_funnel_metrics,
    "product_performance": generate_product_performance,
    "traffic_analytics": generate_traffic_analytics,
}


def _read_gold_table(name: str) -> pd.DataFrame | None:
    """Read an existing Gold Delta table (as Parquet) if one exists locally."""
    path = Path(storage_config.delta_gold_path) / name
    if not path.exists():
        return None
    try:
        import duckdb

        df = duckdb.sql(f"SELECT * FROM read_parquet('{path}/*.parquet', union_by_name=true)").df()
        return df if not df.empty else None
    except Exception:
        logger.warning("Could not read Gold table %r from %s", name, path)
        return None


def _load_or_generate(name: str) -> pd.DataFrame:
    df = _read_gold_table(name)
    if df is not None:
        logger.info("Loaded %d rows from existing Gold table %r", len(df), name)
        return df

    logger.info("No local Gold table %r found; generating static sample", name)
    return _GENERATORS[name]()


def export_snapshots(output_dir: str | None = None) -> dict[str, Path]:
    """Export each Gold table to a Parquet + CSV snapshot, returning the paths written."""
    out_dir = Path(output_dir or config.snapshot_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: dict[str, Path] = {}
    for name in _GENERATORS:
        df = _load_or_generate(name)

        csv_path = out_dir / f"{name}.csv"
        df.to_csv(csv_path, index=False)

        parquet_path = out_dir / f"{name}.parquet"
        try:
            import duckdb

            duckdb.from_df(df).write_parquet(str(parquet_path))
        except Exception:
            logger.warning("Could not write Parquet for %r (duckdb unavailable?)", name)

        written[name] = csv_path
        logger.info("Wrote %s (%d rows) -> %s", name, len(df), out_dir)

    return written


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    written = export_snapshots()
    for name, path in written.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
