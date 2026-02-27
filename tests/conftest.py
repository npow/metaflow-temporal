import asyncio
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import pytest_asyncio
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# Make sure the package is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
    MetaflowWorkflow,
    run_compensation,
    run_metaflow_step,
)

FLOWS_DIR = Path(__file__).parent / "flows"


@pytest.fixture(scope="session")
def event_loop():
    """Create a session-scoped event loop."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def temporal_env():
    """Start an in-process Temporal test server (time-skipping)."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


@pytest_asyncio.fixture
async def worker(temporal_env):
    """Worker running the MetaflowWorkflow + run_metaflow_step activity."""
    task_queue = "test-metaflow-%s" % id(temporal_env)
    async with Worker(
        temporal_env.client,
        task_queue=task_queue,
        workflows=[MetaflowWorkflow],
        activities=[run_metaflow_step, run_compensation],
        activity_executor=ThreadPoolExecutor(max_workers=8),
    ):
        yield temporal_env.client, task_queue
