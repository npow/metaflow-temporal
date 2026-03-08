"""
True end-to-end tests for metaflow-temporal.

These tests are intentionally zero-mock: they generate a real worker.py via
`metaflow temporal create`, start it as a real subprocess connected to an
actual Temporal server, trigger runs via the worker's built-in trigger
mechanism, wait for completion, then verify Metaflow artifacts.

Requirements:
  - A real Temporal server running at localhost:7233 (see docker-compose.yml)
  - `pip install -e ".[dev]"`

Run:
  pytest -v -m e2e --timeout=300
"""

import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

import metaflow
import pytest

metaflow.metadata("local")

FLOWS_DIR = Path(__file__).parent / "flows"
TEMPORAL_HOST = "localhost:7233"
WORKER_START_WAIT = 4  # seconds to let the worker connect before triggering


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_worker(
    flow_file: Path,
    tmp_path: Path,
    task_queue: str,
    extra_args: list | None = None,
) -> Path:
    """Run `metaflow temporal create` and return the path to the generated worker."""
    out_file = tmp_path / (f"worker_{uuid.uuid4().hex[:6]}.py")
    cmd = [
        sys.executable,
        str(flow_file),
        "--no-pylint",
        "--metadata=local",
        "--datastore=local",
        "--environment=local",
        "temporal",
        "create",
        "--output",
        str(out_file),
        "--task-queue",
        task_queue,
        "--temporal-host",
        TEMPORAL_HOST,
    ] + (extra_args or [])
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, (
        f"temporal create failed\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )
    return out_file


def _trigger_and_wait(worker_file: Path, params: dict | None = None, timeout: int = 120) -> str:
    """Run `python worker.py trigger [key=value ...]` and return the Metaflow run_id.

    WorkerUtils.trigger() connects to Temporal, starts the workflow, blocks
    until it completes (or raises), then prints:
      "Workflow completed. Run ID: temporal-<id>"
    We parse that line to get the run_id.
    """
    cmd = [sys.executable, str(worker_file), "trigger"]
    for k, v in (params or {}).items():
        cmd.append(f"{k}={v}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    assert result.returncode == 0, (
        f"trigger failed\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )
    m = re.search(r"Run ID:\s+(temporal-\S+)", result.stdout)
    assert m, f"Could not parse run ID from trigger output:\n{result.stdout}"
    return m.group(1)


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def start_worker(tmp_path):
    """Factory fixture: generates a worker.py, starts it as a subprocess.

    Usage in tests::

        def test_foo(start_worker):
            worker_file, task_queue = start_worker(FLOWS_DIR / "my_flow.py", "MyFlow")
            run_id = _trigger_and_wait(worker_file)
            ...
    """
    started = []

    def _start(flow_file: Path, flow_name: str, extra_create_args: list | None = None):
        task_queue = f"e2e-{flow_name.lower()}-{uuid.uuid4().hex[:6]}"
        worker_file = _generate_worker(flow_file, tmp_path, task_queue, extra_create_args)
        proc = subprocess.Popen(
            [sys.executable, str(worker_file)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(WORKER_START_WAIT)
        if proc.poll() is not None:
            stdout = proc.stdout.read().decode() if proc.stdout else ""
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise RuntimeError(
                f"Worker process exited early (rc={proc.returncode})\nSTDOUT: {stdout}\nSTDERR: {stderr}"
            )
        started.append(proc)
        return worker_file, task_queue

    yield _start

    for proc in started:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestE2ELinearFlow:
    def test_linear_flow_artifact(self, start_worker):
        worker_file, _ = start_worker(FLOWS_DIR / "linear_flow.py", "LinearFlow")
        run_id = _trigger_and_wait(worker_file)
        run = metaflow.Run(f"LinearFlow/{run_id}")
        assert run["end"].task.data.result == "hello world"


@pytest.mark.e2e
class TestE2EBranchFlow:
    def test_branch_flow_both_branches_ran(self, start_worker):
        worker_file, _ = start_worker(FLOWS_DIR / "branch_flow.py", "BranchFlow")
        run_id = _trigger_and_wait(worker_file)
        run = metaflow.Run(f"BranchFlow/{run_id}")
        step_names = {s.id for s in run}
        assert "branch_a" in step_names
        assert "branch_b" in step_names


@pytest.mark.e2e
class TestE2EForeachFlow:
    def test_foreach_flow_all_tasks_ran(self, start_worker):
        worker_file, _ = start_worker(FLOWS_DIR / "foreach_flow.py", "ForeachFlow")
        run_id = _trigger_and_wait(worker_file)
        run = metaflow.Run(f"ForeachFlow/{run_id}")
        body_tasks = list(run["body"].tasks())
        assert len(body_tasks) == 3
        results = sorted(t.data.result for t in body_tasks)
        assert results == [
            "processed: alpha",
            "processed: beta",
            "processed: gamma",
        ]


@pytest.mark.e2e
class TestE2EParamFlow:
    def test_param_flow_default(self, start_worker):
        worker_file, _ = start_worker(FLOWS_DIR / "param_flow.py", "ParamFlow")
        run_id = _trigger_and_wait(worker_file)
        run = metaflow.Run(f"ParamFlow/{run_id}")
        assert run["end"].task.data.message == "hello, world"

    def test_param_flow_override(self, start_worker):
        worker_file, _ = start_worker(FLOWS_DIR / "param_flow.py", "ParamFlow")
        run_id = _trigger_and_wait(worker_file, {"greeting": "Temporal"})
        run = metaflow.Run(f"ParamFlow/{run_id}")
        assert run["end"].task.data.message == "hello, Temporal"
