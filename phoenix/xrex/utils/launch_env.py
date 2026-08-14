# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
import os
from pathlib import Path

from xrex.utils import cluster

IS_LOCAL: bool = "HOST_NODE_ADDR" not in os.environ

DATA_DIR: str = "/tmp/xai_data" if IS_LOCAL else "/data"

XAI_USER: str = cluster.get_user() or "unknown_user"

JOB_NAME: str = cluster.get_job_name() or "local"

CHECKPOINT_DIR: str = os.path.join(DATA_DIR, "checkpoints", XAI_USER, JOB_NAME)

NUM_REPLICAS: int = int(os.environ.get("EXPLORER_COUNT", "1"))
REPLICA_INDEX: int = int(os.environ.get("EXPLORER_INDEX", "0"))

CONFIG: str | None = os.environ.get("EXPLORER_CONFIG")

CLUSTER: str | None = cluster.get_cluster() or None
CLOUD: str | None = cluster.get_cloud() or None

TRAIN_DOCKER_IMAGE: str = os.environ.get("XAI_TRAIN_DOCKER_IMAGE", "")
CPU_DOCKER_IMAGE: str = os.environ.get("XAI_CPU_DOCKER_IMAGE", "")


def run_files_note(run_dir: str | Path) -> str | None:
    del run_dir
    return None
