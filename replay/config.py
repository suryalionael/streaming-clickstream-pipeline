"""Configuration for the replay/export module."""

import os
from dataclasses import dataclass


@dataclass
class ReplayConfig:
    snapshot_dir: str = os.getenv("REPLAY_SNAPSHOT_DIR", "replay/snapshots")
    sample_hours: int = int(os.getenv("REPLAY_SAMPLE_HOURS", "24"))
    window_minutes: int = int(os.getenv("REPLAY_WINDOW_MINUTES", "5"))
    random_seed: int = int(os.getenv("REPLAY_RANDOM_SEED", "42"))


config = ReplayConfig()
