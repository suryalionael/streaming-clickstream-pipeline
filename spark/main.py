"""Spark Streaming pipeline entry point."""

import logging

from spark.streaming import run_pipeline


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_pipeline()


if __name__ == "__main__":
    main()
