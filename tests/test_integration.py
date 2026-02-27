"""
End-to-end integration tests for metaflow-temporal.

Tier 1 (always runs): Uses temporalio.testing.WorkflowEnvironment (in-process
embedded Temporal server with time-skipping). Activities run real Metaflow
subprocesses against the local datastore.

Tier 2 (docker-compose): marked @pytest.mark.integration — connects to an
external Temporal server at localhost:7233.
"""
import json
import os
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import metaflow
import pytest
import pytest_asyncio
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# Use local datastore/metadata for all tests
metaflow.metadata("local")

# Ensure the test flows are importable
FLOWS_DIR = Path(__file__).parent / "flows"

from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
    MetaflowWorkflow,
    run_metaflow_step,
)


def _build_config(flow_file: Path, flow_name: str, task_queue: str) -> dict:
    """Build a minimal CONFIG dict by running `metaflow temporal create` inline."""
    import subprocess, tempfile, sys

    out_file = tempfile.mktemp(suffix="_worker.py")
    result = subprocess.run(
        [
            sys.executable,
            str(flow_file),
            "--no-pylint",
            "--metadata=local",
            "--datastore=local",
            "--environment=local",
            "temporal",
            "create",
            "--output", out_file,
            "--task-queue", task_queue,
            "--temporal-host", "localhost:7233",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "temporal create failed:\nSTDOUT: %s\nSTDERR: %s" % (result.stdout, result.stderr)
    )

    # Extract CONFIG from the generated file
    ns = {}
    exec(open(out_file).read().split("{{{utils}}}")[0] if "{{{utils}}}" in open(out_file).read() else
         # Parse CONFIG = ... block
         _extract_config_block(out_file), ns)
    os.unlink(out_file)
    return ns.get("CONFIG", {})


def _extract_config_from_worker(worker_file: str) -> dict:
    """Parse CONFIG dict from a generated worker file.
    The worker file contains: CONFIG = json.loads(r'''{ ... }''')
    """
    src = open(worker_file).read()
    # Find CONFIG = json.loads(r\"\"\"...
    marker = 'CONFIG = json.loads(r"""'
    start = src.index(marker) + len(marker)
    end = src.index('""")', start)
    config_str = src[start:end]
    return json.loads(config_str)


async def _run_flow(
    client,
    task_queue: str,
    flow_file: Path,
    flow_name: str,
    params: dict,
) -> str:
    """Compile a flow, then run it via the in-process client."""
    import subprocess, tempfile, uuid

    out_file = tempfile.mktemp(suffix="_worker.py")
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(flow_file),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
                "--task-queue", task_queue,
                "--temporal-host", "localhost:7233",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            "temporal create failed:\nSTDOUT: %s\nSTDERR: %s"
            % (result.stdout, result.stderr)
        )

        config = _extract_config_from_worker(out_file)
    finally:
        try:
            os.unlink(out_file)
        except OSError:
            pass

    workflow_id = "%s-%s" % (flow_name.lower(), uuid.uuid4().hex[:8])
    run_id = await client.execute_workflow(
        MetaflowWorkflow.run,
        {"config": config, "params": params},
        id=workflow_id,
        task_queue=task_queue,
    )
    return run_id


class TestLinearFlow:
    @pytest.mark.asyncio
    async def test_linear_flow_completes(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "linear_flow.py",
            "LinearFlow",
            {},
        )
        assert run_id.startswith("temporal-")

    @pytest.mark.asyncio
    async def test_linear_flow_artifacts(self, worker):
        """Verify that artifacts are stored in the local datastore."""
        import metaflow

        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "linear_flow.py",
            "LinearFlow",
            {},
        )
        # Load the run and check that 'result' artifact exists
        run = metaflow.Run("LinearFlow/%s" % run_id)
        assert run["end"].task.data.result == "hello world"


class TestBranchFlow:
    @pytest.mark.asyncio
    async def test_branch_flow_completes(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "branch_flow.py",
            "BranchFlow",
            {},
        )
        assert run_id.startswith("temporal-")

    @pytest.mark.asyncio
    async def test_branch_flow_both_branches_ran(self, worker):
        import metaflow

        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "branch_flow.py",
            "BranchFlow",
            {},
        )
        run = metaflow.Run("BranchFlow/%s" % run_id)
        step_names = {s.id for s in run}
        assert "branch_a" in step_names
        assert "branch_b" in step_names


class TestForeachFlow:
    @pytest.mark.asyncio
    async def test_foreach_flow_completes(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "foreach_flow.py",
            "ForeachFlow",
            {},
        )
        assert run_id.startswith("temporal-")

    @pytest.mark.asyncio
    async def test_foreach_flow_all_tasks_ran(self, worker):
        import metaflow

        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "foreach_flow.py",
            "ForeachFlow",
            {},
        )
        run = metaflow.Run("ForeachFlow/%s" % run_id)
        body_tasks = list(run["body"].tasks())
        assert len(body_tasks) == 3
        results = [t.data.result for t in body_tasks]
        assert sorted(results) == [
            "processed: alpha",
            "processed: beta",
            "processed: gamma",
        ]


class TestParamFlow:
    @pytest.mark.asyncio
    async def test_param_flow_default(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "param_flow.py",
            "ParamFlow",
            {},
        )
        run = metaflow.Run("ParamFlow/%s" % run_id)
        assert run["end"].task.data.message == "hello, world"

    @pytest.mark.asyncio
    async def test_param_flow_custom_param(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "param_flow.py",
            "ParamFlow",
            {"greeting": "Temporal"},
        )
        run = metaflow.Run("ParamFlow/%s" % run_id)
        assert run["end"].task.data.message == "hello, Temporal"


class TestCompilation:
    """Unit-level tests for the `temporal create` command."""

    def test_create_generates_file(self, tmp_path):
        import subprocess

        out_file = str(tmp_path / "worker.py")
        result = subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "linear_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert os.path.exists(out_file)
        content = open(out_file).read()
        assert "MetaflowWorkflow" in content
        assert "WorkerUtils" in content
        assert "LinearFlow" in content

    def test_create_config_has_steps(self, tmp_path):
        import subprocess

        out_file = str(tmp_path / "worker.py")
        subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "linear_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
            ],
            check=True,
            capture_output=True,
        )
        config = _extract_config_from_worker(out_file)
        assert set(config["steps"].keys()) == {"start", "process", "end"}
        assert config["steps"]["start"]["type"] == "start"
        assert config["steps"]["end"]["type"] == "end"

    def test_create_foreach_config(self, tmp_path):
        import subprocess

        out_file = str(tmp_path / "worker.py")
        subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "foreach_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
            ],
            check=True,
            capture_output=True,
        )
        config = _extract_config_from_worker(out_file)
        assert config["steps"]["start"]["type"] == "foreach"
        assert "body" in config["steps"]
        assert config["steps"]["body"]["type"] == "linear"

    def test_create_tags_in_config(self, tmp_path):
        import subprocess

        out_file = str(tmp_path / "worker.py")
        subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "linear_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
                "--tag", "team:ml",
                "--tag", "env:prod",
            ],
            check=True,
            capture_output=True,
        )
        config = _extract_config_from_worker(out_file)
        assert "tags" in config
        assert "team:ml" in config["tags"]
        assert "env:prod" in config["tags"]

    def test_create_schedule_config(self, tmp_path):
        import subprocess

        out_file = str(tmp_path / "worker.py")
        subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "scheduled_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
            ],
            check=True,
            capture_output=True,
        )
        config = _extract_config_from_worker(out_file)
        assert config.get("schedule") is not None
        assert config["schedule"]["cron"] == "0 * * * ? *"

    def test_no_schedule_when_not_decorated(self, tmp_path):
        import subprocess

        out_file = str(tmp_path / "worker.py")
        subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "linear_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
            ],
            check=True,
            capture_output=True,
        )
        config = _extract_config_from_worker(out_file)
        assert config.get("schedule") is None

    def test_retry_delay_in_config(self, tmp_path):
        import subprocess

        out_file = str(tmp_path / "worker.py")
        subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "retry_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
            ],
            check=True,
            capture_output=True,
        )
        config = _extract_config_from_worker(out_file)
        flaky_step = config["steps"]["flaky"]
        assert flaky_step["retries"] == 2
        assert flaky_step["retry_delay_seconds"] == 60  # 1 minute

    def test_workflow_timeout_in_config(self, tmp_path):
        import subprocess

        out_file = str(tmp_path / "worker.py")
        subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "linear_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
                "--workflow-timeout", "7200",
            ],
            check=True,
            capture_output=True,
        )
        config = _extract_config_from_worker(out_file)
        assert config.get("workflow_timeout_seconds") == 7200

    def test_no_workflow_timeout_by_default(self, tmp_path):
        import subprocess

        out_file = str(tmp_path / "worker.py")
        subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "linear_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
            ],
            check=True,
            capture_output=True,
        )
        config = _extract_config_from_worker(out_file)
        assert config.get("workflow_timeout_seconds") is None

    def test_project_flow_name_in_config(self, tmp_path):
        """@project should produce a project-aware flow_name in CONFIG."""
        import subprocess

        out_file = str(tmp_path / "worker.py")
        result = subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "project_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        config = _extract_config_from_worker(out_file)
        # flow_name should be project-aware: myproject.<branch>.ProjectFlow
        assert config["flow_name"].startswith("myproject.")
        assert config["flow_name"].endswith(".ProjectFlow")
        assert config.get("project") is not None
        assert config["project"]["name"] == "myproject"

    def test_project_production_branch(self, tmp_path):
        """--production should produce branch 'prod'."""
        import subprocess

        out_file = str(tmp_path / "worker.py")
        result = subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "project_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
                "--production",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        config = _extract_config_from_worker(out_file)
        assert config["flow_name"] == "myproject.prod.ProjectFlow"
        assert config["project"]["branch"] == "prod"

    def test_project_custom_branch(self, tmp_path):
        """--branch should produce a test.<branch> branch name."""
        import subprocess

        out_file = str(tmp_path / "worker.py")
        result = subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "project_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
                "--branch", "staging",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        config = _extract_config_from_worker(out_file)
        assert config["flow_name"] == "myproject.test.staging.ProjectFlow"
        assert config["project"]["branch"] == "test.staging"

    def test_project_tags_auto_added(self, tmp_path):
        """@project should auto-add project: and project_branch: tags."""
        import subprocess

        out_file = str(tmp_path / "worker.py")
        result = subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "project_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
                "--production",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        config = _extract_config_from_worker(out_file)
        assert "project:myproject" in config["tags"]
        assert "project_branch:prod" in config["tags"]


class TestConditionalFlow:
    """Tests for split/join (conditional-style branching)."""

    @pytest.mark.asyncio
    async def test_conditional_flow_completes(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "conditional_flow.py",
            "ConditionalFlow",
            {},
        )
        assert run_id.startswith("temporal-")

    @pytest.mark.asyncio
    async def test_conditional_flow_both_paths_ran(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "conditional_flow.py",
            "ConditionalFlow",
            {},
        )
        run = metaflow.Run("ConditionalFlow/%s" % run_id)
        step_names = {s.id for s in run}
        assert "high_path" in step_names
        assert "low_path" in step_names
        assert "join" in step_names
        # Both result strings should be present in join's results list
        results = sorted(run["join"].task.data.results)
        assert len(results) == 2
        assert any("high" in r for r in results)
        assert any("low" in r for r in results)

    @pytest.mark.asyncio
    async def test_conditional_flow_with_param(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "conditional_flow.py",
            "ConditionalFlow",
            {"threshold": "5"},
        )
        run = metaflow.Run("ConditionalFlow/%s" % run_id)
        results = sorted(run["join"].task.data.results)
        # value=15, threshold=5 → high path has "15 >= 5"
        assert any("15 >= 5" in r for r in results)


class TestConfigAndArtifacts:
    """Tests for flow Parameters (config) and artifact persistence."""

    @pytest.mark.asyncio
    async def test_config_defaults(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "config_flow.py",
            "ConfigFlow",
            {},
        )
        run = metaflow.Run("ConfigFlow/%s" % run_id)
        task = run["compute"].task.data
        assert task.total == 30  # [1,2,3,4,5] * 2, sum = 30
        assert task.config_label == "test"

    @pytest.mark.asyncio
    async def test_config_overrides(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "config_flow.py",
            "ConfigFlow",
            {"multiplier": "3", "label": "prod"},
        )
        run = metaflow.Run("ConfigFlow/%s" % run_id)
        task = run["compute"].task.data
        assert task.total == 45  # [1,2,3,4,5] * 3, sum = 45
        assert task.config_label == "prod"

    @pytest.mark.asyncio
    async def test_artifact_types_preserved(self, worker):
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "artifact_flow.py",
            "ArtifactFlow",
            {},
        )
        run = metaflow.Run("ArtifactFlow/%s" % run_id)
        # Check start step artifacts
        start_data = run["start"].task.data
        assert start_data.int_val == 42
        assert abs(start_data.float_val - 3.14) < 0.001
        assert start_data.str_val == "hello"
        assert start_data.list_val == [1, 2, 3]
        assert start_data.dict_val == {"key": "value", "nested": {"a": 1}}
        # Check transform step artifacts
        transform_data = run["transform"].task.data
        assert transform_data.doubled == 84
        assert transform_data.appended == [1, 2, 3, 4, 5]
        assert transform_data.merged["extra"] is True

    @pytest.mark.asyncio
    async def test_foreach_artifacts_per_split(self, worker):
        """Each foreach split should produce its own result artifact."""
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "foreach_flow.py",
            "ForeachFlow",
            {},
        )
        run = metaflow.Run("ForeachFlow/%s" % run_id)
        body_tasks = list(run["body"].tasks())
        assert len(body_tasks) == 3
        results = sorted(t.data.result for t in body_tasks)
        assert results == [
            "processed: alpha",
            "processed: beta",
            "processed: gamma",
        ]

    @pytest.mark.asyncio
    async def test_branch_artifacts_accessible(self, worker):
        """Branch artifacts should be available in the join step."""
        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "branch_flow.py",
            "BranchFlow",
            {},
        )
        run = metaflow.Run("BranchFlow/%s" % run_id)
        # Verify branch artifacts
        branch_a_data = run["branch_a"].task.data
        assert branch_a_data.result_a == "hello from A"
        branch_b_data = run["branch_b"].task.data
        assert branch_b_data.result_b == "hello from B"


# ---------------------------------------------------------------------------
# Saga / Compensation tests
# ---------------------------------------------------------------------------

_SAGA_LOG = "/tmp/saga_test_log.json"


class TestSagaCompilation:
    """Unit tests verifying that @compensate metadata is captured in CONFIG."""

    def _compile_saga(self, tmp_path) -> dict:
        import subprocess

        out_file = str(tmp_path / "saga_worker.py")
        result = subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "saga_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
                "--task-queue", "test",
                "--temporal-host", "localhost:7233",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            "temporal create failed:\nSTDOUT: %s\nSTDERR: %s" % (result.stdout, result.stderr)
        )
        return _extract_config_from_worker(out_file)

    def test_compensations_in_config(self, tmp_path):
        config = self._compile_saga(tmp_path)
        assert "compensations" in config
        assert config["compensations"] == {"book_a": "cancel_a", "book_b": "cancel_b"}

    def test_flow_class_name_in_config(self, tmp_path):
        config = self._compile_saga(tmp_path)
        assert config.get("flow_class_name") == "SagaFlow"

    def test_no_compensations_for_plain_flow(self, tmp_path):
        import subprocess

        out_file = str(tmp_path / "linear_worker.py")
        subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "linear_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "--environment=local",
                "temporal",
                "create",
                "--output", out_file,
            ],
            check=True,
            capture_output=True,
        )
        config = _extract_config_from_worker(out_file)
        assert config.get("compensations") == {}


class TestSagaExecution:
    """End-to-end tests verifying saga compensation runs on failure."""

    @pytest.mark.asyncio
    async def test_compensation_runs_on_failure(self, worker):
        """When fail_step raises, cancel_b then cancel_a should run (LIFO)."""
        import json
        import os

        # Remove any stale log
        if os.path.exists(_SAGA_LOG):
            os.unlink(_SAGA_LOG)

        client, task_queue = worker

        with pytest.raises(Exception):
            await _run_flow(
                client,
                task_queue,
                FLOWS_DIR / "saga_flow.py",
                "SagaFlow",
                {},
            )

        # Both compensations should have run
        assert os.path.exists(_SAGA_LOG), "Saga log not created — compensations did not run"
        with open(_SAGA_LOG) as f:
            log = json.load(f)

        actions = [e["action"] for e in log]
        assert "cancel_a" in actions, "cancel_a compensation did not run"
        assert "cancel_b" in actions, "cancel_b compensation did not run"
        # LIFO order: cancel_b runs before cancel_a
        assert actions.index("cancel_b") < actions.index("cancel_a"), (
            "Compensations did not run in LIFO order: %s" % actions
        )
        # Artifacts were injected correctly
        cancel_b_entry = next(e for e in log if e["action"] == "cancel_b")
        assert cancel_b_entry["booking_id"] == "flight-456"
        cancel_a_entry = next(e for e in log if e["action"] == "cancel_a")
        assert cancel_a_entry["booking_id"] == "hotel-123"

    @pytest.mark.asyncio
    async def test_no_compensation_on_success(self, worker):
        """Successful flows must not write to the compensation log."""
        import os

        if os.path.exists(_SAGA_LOG):
            os.unlink(_SAGA_LOG)

        client, task_queue = worker
        run_id = await _run_flow(
            client,
            task_queue,
            FLOWS_DIR / "linear_flow.py",
            "LinearFlow",
            {},
        )
        assert run_id.startswith("temporal-")
        assert not os.path.exists(_SAGA_LOG), "Compensation log written for successful flow"


# ---------------------------------------------------------------------------
# Tier 2: external Temporal server tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIntegrationExternal:
    """Tests against a real Temporal server (requires docker-compose)."""

    @pytest.fixture(scope="class")
    def ext_client(self):
        import asyncio
        from temporalio.client import Client

        async def _connect():
            return await Client.connect("localhost:7233")

        return asyncio.get_event_loop().run_until_complete(_connect())

    def test_linear_flow_external(self, ext_client, tmp_path):
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        from temporalio.worker import Worker

        async def run():
            task_queue = "integration-test-%s" % id(self)
            async with Worker(
                ext_client,
                task_queue=task_queue,
                workflows=[MetaflowWorkflow],
                activities=[run_metaflow_step],
                activity_executor=ThreadPoolExecutor(max_workers=4),
            ):
                return await _run_flow(
                    ext_client, task_queue, FLOWS_DIR / "linear_flow.py", "LinearFlow", {}
                )

        run_id = asyncio.get_event_loop().run_until_complete(run())
        assert run_id.startswith("temporal-")
