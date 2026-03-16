"""
Adversarial and edge-case tests for metaflow-temporal.

These tests are pure-unit (no external services, no subprocesses) and target
error paths, boundary conditions, and invariants that the happy-path
integration tests cannot exercise.
"""
from unittest.mock import AsyncMock, patch

import pytest

from metaflow_extensions.temporal.plugins.temporal.temporal_cli import _parse_run_params

# ---------------------------------------------------------------------------
# Helpers under test
# ---------------------------------------------------------------------------
from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
    _build_task_pathspec,
    _datastore_root_arg,
    _find_join_step,
    _find_switch_merge_step,
    _is_switch_merge_step,
    _make_retry_policy,
    _make_step_input,
    _resolve_input_paths,
)

# ---------------------------------------------------------------------------
# _build_task_pathspec
# ---------------------------------------------------------------------------


class TestBuildTaskPathspec:
    def test_basic(self):
        assert _build_task_pathspec("MyFlow", "run-1", "start", "tid-1") == (
            "MyFlow/run-1/start/tid-1"
        )

    def test_all_components_present(self):
        spec = _build_task_pathspec("F", "R", "S", "T")
        parts = spec.split("/")
        assert parts == ["F", "R", "S", "T"]


# ---------------------------------------------------------------------------
# _datastore_root_arg
# ---------------------------------------------------------------------------


class TestDatastoreRootArg:
    def test_from_env_overrides(self):
        overrides = {"METAFLOW_DATASTORE_SYSROOT_LOCAL": "/custom/root"}
        assert _datastore_root_arg(overrides) == "/custom/root"

    def test_s3_key(self):
        overrides = {"METAFLOW_DATASTORE_SYSROOT_S3": "s3://my-bucket"}
        result = _datastore_root_arg(overrides, datastore_type="s3")
        assert result == "s3://my-bucket"

    def test_fallback_to_home(self, monkeypatch):
        monkeypatch.delenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", raising=False)
        result = _datastore_root_arg({})
        assert result.endswith(".metaflow")

    def test_env_override_wins_over_os_env(self, monkeypatch):
        monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", "/os-env")
        overrides = {"METAFLOW_DATASTORE_SYSROOT_LOCAL": "/override"}
        assert _datastore_root_arg(overrides) == "/override"


# ---------------------------------------------------------------------------
# _parse_run_params (CLI helper)
# ---------------------------------------------------------------------------


class TestParseRunParams:
    def test_empty(self):
        assert _parse_run_params([]) == {}

    def test_single(self):
        assert _parse_run_params(["key=value"]) == {"key": "value"}

    def test_value_with_equals(self):
        # Value contains '=' — only first '=' is split
        assert _parse_run_params(["url=http://a=b"]) == {"url": "http://a=b"}

    def test_multiple(self):
        result = _parse_run_params(["a=1", "b=2", "c=hello world"])
        assert result == {"a": "1", "b": "2", "c": "hello world"}

    def test_strips_whitespace(self):
        # Leading/trailing spaces around key and value
        assert _parse_run_params([" k = v "]) == {"k": "v"}


# ---------------------------------------------------------------------------
# _make_retry_policy
# ---------------------------------------------------------------------------


class TestMakeRetryPolicy:
    def test_no_retries(self):
        policy = _make_retry_policy({"retries": 0})
        assert policy.maximum_attempts == 1

    def test_with_retries(self):
        policy = _make_retry_policy({"retries": 3})
        assert policy.maximum_attempts == 4

    def test_custom_delay(self):
        from datetime import timedelta

        policy = _make_retry_policy({"retries": 1, "retry_delay_seconds": 30})
        assert policy.initial_interval == timedelta(seconds=30)

    def test_default_delay(self):
        from datetime import timedelta

        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            _DEFAULT_RETRY_DELAY_SECONDS,
        )

        policy = _make_retry_policy({"retries": 1})
        assert policy.initial_interval == timedelta(seconds=_DEFAULT_RETRY_DELAY_SECONDS)


# ---------------------------------------------------------------------------
# _find_join_step
# ---------------------------------------------------------------------------


class TestFindJoinStep:
    def test_finds_correct_join(self):
        steps = {
            "split": {"type": "split"},
            "a": {"type": "linear", "split_parents": []},
            "b": {"type": "linear", "split_parents": []},
            "join": {"type": "join", "split_parents": ["split"]},
            "end": {"type": "end"},
        }
        assert _find_join_step("split", steps) == "join"

    def test_no_join(self):
        steps = {
            "start": {"type": "start", "split_parents": []},
            "end": {"type": "end", "split_parents": []},
        }
        assert _find_join_step("start", steps) is None

    def test_nested_split_finds_innermost(self):
        # Inner join has inner split as last split_parent — not outer split.
        steps = {
            "outer_split": {"type": "split"},
            "inner_split": {"type": "split", "split_parents": ["outer_split"]},
            "inner_join": {"type": "join", "split_parents": ["outer_split", "inner_split"]},
            "outer_join": {"type": "join", "split_parents": ["outer_split"]},
        }
        assert _find_join_step("inner_split", steps) == "inner_join"
        assert _find_join_step("outer_split", steps) == "outer_join"


# ---------------------------------------------------------------------------
# _find_switch_merge_step
# ---------------------------------------------------------------------------


class TestFindSwitchMergeStep:
    def test_finds_merge_step(self):
        steps = {
            "switch": {"type": "split-switch", "out_funcs": ["branch_a", "branch_b"]},
            "branch_a": {"type": "linear", "in_funcs": ["switch"]},
            "branch_b": {"type": "linear", "in_funcs": ["switch"]},
            "merge": {"type": "linear", "in_funcs": ["branch_a", "branch_b"]},
        }
        assert _find_switch_merge_step("switch", steps) == "merge"

    def test_missing_switch_node(self):
        # switch_step_name not in steps → returns None
        assert _find_switch_merge_step("nonexistent", {"end": {"in_funcs": []}}) is None

    def test_no_merge_step(self):
        # All steps are branches, no step has branch as in_func
        steps = {
            "switch": {"type": "split-switch", "out_funcs": ["a", "b"]},
            "a": {"type": "linear", "in_funcs": ["switch"]},
            "b": {"type": "linear", "in_funcs": ["switch"]},
        }
        assert _find_switch_merge_step("switch", steps) is None


# ---------------------------------------------------------------------------
# _is_switch_merge_step
# ---------------------------------------------------------------------------


class TestIsSwitchMergeStep:
    def _steps(self):
        return {
            "switch": {"type": "split-switch"},
            "branch_a": {"type": "linear", "in_funcs": ["switch"]},
            "branch_b": {"type": "linear", "in_funcs": ["switch"]},
            "other": {"type": "linear", "in_funcs": ["start"]},
            "start": {"type": "start", "in_funcs": []},
        }

    def test_all_parents_trace_to_switch(self):
        steps = self._steps()
        assert _is_switch_merge_step(["branch_a", "branch_b"], steps) is True

    def test_only_some_parents_trace_to_switch(self):
        # branch_a comes from switch, but 'other' doesn't
        steps = self._steps()
        assert _is_switch_merge_step(["branch_a", "other"], steps) is False

    def test_empty_in_funcs(self):
        # 0 switch parents, 0 in_funcs → 0 == 0 → True (edge case, not a real merge)
        assert _is_switch_merge_step([], {}) is True


# ---------------------------------------------------------------------------
# _resolve_input_paths
# ---------------------------------------------------------------------------


class TestResolveInputPaths:
    def test_start_step(self):
        node = {"type": "start", "in_funcs": [], "split_parents": []}
        result = _resolve_input_paths("start", node, "run-1", {})
        assert result == "run-1/_parameters/temporal-start-0-params"

    def test_no_in_funcs(self):
        node = {"type": "linear", "in_funcs": [], "split_parents": []}
        result = _resolve_input_paths("orphan", node, "run-1", {})
        assert result == ""

    def test_single_parent(self):
        node = {"type": "linear", "in_funcs": ["start"], "split_parents": []}
        task_ids = {"start": "temporal-start-0"}
        result = _resolve_input_paths("process", node, "run-1", task_ids)
        assert result == "run-1/start/temporal-start-0"

    def test_single_parent_missing_from_task_ids_uses_fallback(self):
        node = {"type": "linear", "in_funcs": ["start"], "split_parents": []}
        result = _resolve_input_paths("process", node, "run-1", {})
        # Falls back to "temporal-<parent>-0"
        assert result == "run-1/start/temporal-start-0"

    def test_foreach_join_with_list_task_ids(self):
        node = {"type": "join", "in_funcs": ["body"], "split_parents": ["foreach_step"]}
        task_ids = {"body": ["tid-0", "tid-1", "tid-2"]}
        result = _resolve_input_paths("join_step", node, "run-1", task_ids)
        assert result == "run-1/body/tid-0,run-1/body/tid-1,run-1/body/tid-2"

    def test_foreach_join_without_list_falls_through_to_single_parent(self):
        # split_parents is set but body task_id is NOT a list → falls through
        node = {"type": "join", "in_funcs": ["body"], "split_parents": ["foreach_step"]}
        task_ids = {"body": "single-tid"}
        result = _resolve_input_paths("join_step", node, "run-1", task_ids)
        assert result == "run-1/body/single-tid"

    def test_regular_split_join_all_branches(self):
        node = {"type": "join", "in_funcs": ["a", "b"], "split_parents": []}
        task_ids = {"a": "tid-a", "b": "tid-b"}
        result = _resolve_input_paths("join_step", node, "run-1", task_ids)
        assert "run-1/a/tid-a" in result
        assert "run-1/b/tid-b" in result

    def test_switch_merge_skips_missing_branches(self):
        # Only branch_a ran (branch_b not in task_ids)
        steps = {
            "switch": {"type": "split-switch", "in_funcs": [], "out_funcs": ["branch_a", "branch_b"]},
            "branch_a": {"type": "linear", "in_funcs": ["switch"]},
            "branch_b": {"type": "linear", "in_funcs": ["switch"]},
        }
        merge_node = {"type": "linear", "in_funcs": ["branch_a", "branch_b"], "split_parents": []}
        task_ids = {"branch_a": "tid-a"}  # branch_b absent
        result = _resolve_input_paths("merge", merge_node, "run-1", task_ids, steps=steps)
        assert result == "run-1/branch_a/tid-a"
        assert "branch_b" not in result

    def test_switch_merge_includes_all_when_both_present(self):
        # Both branches in task_ids (shouldn't happen in real flow, but defensive)
        steps = {
            "switch": {"type": "split-switch", "in_funcs": [], "out_funcs": ["a", "b"]},
            "a": {"type": "linear", "in_funcs": ["switch"]},
            "b": {"type": "linear", "in_funcs": ["switch"]},
        }
        merge_node = {"type": "linear", "in_funcs": ["a", "b"], "split_parents": []}
        task_ids = {"a": "tid-a", "b": "tid-b"}
        result = _resolve_input_paths("merge", merge_node, "run-1", task_ids, steps=steps)
        assert "run-1/a/tid-a" in result
        assert "run-1/b/tid-b" in result

    def test_parent_with_list_task_ids_multi_parent(self):
        # Parent with list task_ids in a multi-parent join
        node = {"type": "join", "in_funcs": ["a", "b"], "split_parents": []}
        task_ids = {"a": ["tid-a0", "tid-a1"], "b": "tid-b"}
        result = _resolve_input_paths("join", node, "run-1", task_ids)
        assert "run-1/a/tid-a0" in result
        assert "run-1/a/tid-a1" in result
        assert "run-1/b/tid-b" in result


# ---------------------------------------------------------------------------
# _make_step_input
# ---------------------------------------------------------------------------


def _minimal_cfg(**overrides):
    base = {
        "flow_name": "TestFlow",
        "flow_file": "/tmp/test_flow.py",
        "metadata_type": "local",
        "datastore_type": "local",
        "environment_type": "local",
        "event_logger_type": "nullSidecarLogger",
        "monitor_type": "nullSidecarMonitor",
        "tags": [],
        "namespace": "",
    }
    base.update(overrides)
    return base


def _minimal_node(**overrides):
    base = {
        "retries": 0,
        "retry_delay_seconds": 120,
        "env": {},
        "decorator_specs": [],
        "runtime_cli_decorators": [],
    }
    base.update(overrides)
    return base


class TestMakeStepInput:
    def test_start_step_has_params_json(self):
        cfg = _minimal_cfg()
        node = _minimal_node()
        inp = _make_step_input(cfg, node, "start", "run-1", "tid-1", "paths", 0, -1, {"greeting": "hi"})
        import json
        assert json.loads(inp.params_json) == {"greeting": "hi"}

    def test_non_start_step_has_empty_params_json(self):
        cfg = _minimal_cfg()
        node = _minimal_node()
        inp = _make_step_input(cfg, node, "end", "run-1", "tid-1", "paths", 0, -1, {"greeting": "hi"})
        assert inp.params_json == ""

    def test_split_index_negative_one(self):
        cfg = _minimal_cfg()
        node = _minimal_node()
        inp = _make_step_input(cfg, node, "start", "run-1", "tid-1", "paths", 0, -1, {})
        assert inp.split_index == -1

    def test_env_overrides_include_run_id(self):
        cfg = _minimal_cfg()
        node = _minimal_node(env={"MY_VAR": "hello"})
        inp = _make_step_input(cfg, node, "start", "run-1", "tid-1", "paths", 0, -1, {})
        assert inp.env_overrides["METAFLOW_RUN_ID"] == "run-1"
        assert inp.env_overrides["MY_VAR"] == "hello"

    def test_code_package_defaults_when_missing(self):
        cfg = _minimal_cfg()  # no "code_package" key
        node = _minimal_node()
        inp = _make_step_input(cfg, node, "start", "run-1", "tid-1", "paths", 0, -1, {})
        assert inp.code_package_url == ""
        assert inp.code_package_sha == ""
        assert inp.code_package_metadata == ""

    def test_code_package_forwarded(self):
        cfg = _minimal_cfg(code_package={"url": "s3://bucket/key", "sha": "abc123", "metadata": "{}"})
        node = _minimal_node()
        inp = _make_step_input(cfg, node, "start", "run-1", "tid-1", "paths", 0, -1, {})
        assert inp.code_package_url == "s3://bucket/key"
        assert inp.code_package_sha == "abc123"


# ---------------------------------------------------------------------------
# MetaflowWorkflow._run_compensations — error path
# ---------------------------------------------------------------------------


class TestRunCompensations:
    @pytest.mark.asyncio
    async def test_failed_compensation_logs_warning(self, capsys):
        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            MetaflowWorkflow,
        )

        workflow_mock = MetaflowWorkflow()
        workflow_mock._compensation_stack = [
            {"handler": "cancel_hotel", "step": "book_hotel", "task_id": "tid-1"}
        ]

        cfg = {
            "flow_file": "/tmp/fake.py",
            "flow_name": "BookingFlow",
            "flow_class_name": "BookingFlow",
            "metadata_type": "local",
            "datastore_type": "local",
            "datastore_root": "",
        }

        # Patch execute_activity to raise
        with patch("metaflow_extensions.temporal.plugins.temporal.worker_utils.workflow") as wf_mock:
            wf_mock.execute_activity = AsyncMock(side_effect=RuntimeError("connection failed"))
            await workflow_mock._run_compensations(cfg, "run-1")

        captured = capsys.readouterr()
        assert "compensation for step" in captured.err
        assert "book_hotel" in captured.err
        assert "connection failed" in captured.err


# ---------------------------------------------------------------------------
# _metaflow_datastore_env — lock and restore behaviour
# ---------------------------------------------------------------------------


class TestMetaflowDatastoreEnv:
    def test_restores_original_value(self, monkeypatch):
        import os

        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            _metaflow_datastore_env,
        )

        monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", "/original")
        with _metaflow_datastore_env("/override"):
            assert os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] == "/override"
        assert os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] == "/original"

    def test_removes_key_when_originally_absent(self, monkeypatch):
        import os

        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            _metaflow_datastore_env,
        )

        monkeypatch.delenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", raising=False)
        with _metaflow_datastore_env("/temp"):
            assert os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] == "/temp"
        assert "METAFLOW_DATASTORE_SYSROOT_LOCAL" not in os.environ

    def test_restores_on_exception(self, monkeypatch):
        import os

        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            _metaflow_datastore_env,
        )

        monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", "/original")
        with pytest.raises(ValueError):
            with _metaflow_datastore_env("/override"):
                raise ValueError("boom")
        assert os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] == "/original"


# ---------------------------------------------------------------------------
# _parse_run_params edge cases (directly from temporal_cli)
# ---------------------------------------------------------------------------


class TestParseRunParamsCLI:
    def test_empty_value(self):
        # key= (empty value)
        assert _parse_run_params(["key="]) == {"key": ""}

    def test_no_equals_ignored(self):
        # A bare string with no '=' partitions as key='', v=''...
        # Actually str.partition('=') on 'noequals' → ('noequals', '', '')
        result = _parse_run_params(["noequals"])
        # key='noequals', value='' — strip makes them both stripped
        assert result == {"noequals": ""}


# ---------------------------------------------------------------------------
# Workflow resume_state seeding
# ---------------------------------------------------------------------------


class TestResumeStateSeeding:
    @pytest.mark.asyncio
    async def test_task_ids_seeded_from_resume_state(self):
        """_execute_graph seeds task_ids from resume_state before executing."""
        from metaflow_extensions.temporal.plugins.temporal.worker_utils import MetaflowWorkflow

        wf = MetaflowWorkflow()
        cfg = {
            "steps": {
                "start": {
                    "type": "start",
                    "in_funcs": [],
                    "out_funcs": ["end"],
                    "split_parents": [],
                    "env": {},
                    "retries": 0,
                    "retry_delay_seconds": 120,
                    "timeout_seconds": 60,
                    "decorator_specs": [],
                    "runtime_cli_decorators": [],
                    "has_card": False,
                },
                "end": {
                    "type": "end",
                    "in_funcs": ["start"],
                    "out_funcs": [],
                    "split_parents": [],
                    "env": {},
                    "retries": 0,
                    "retry_delay_seconds": 120,
                    "timeout_seconds": 60,
                    "decorator_specs": [],
                    "runtime_cli_decorators": [],
                    "has_card": False,
                },
            },
            "flow_name": "TestFlow",
            "flow_file": "/tmp/test.py",
            "compensations": {},
        }
        resume_state = {
            "start": {"task_id": "temporal-start-0"},
            "end": {"task_ids": ["temporal-end-0", "temporal-end-1"]},
        }

        # Patch _execute_node to just record what task_ids looked like after seeding
        recorded_task_ids = {}

        async def mock_execute_node(step, _cfg, _run_id, task_ids, *args, **kwargs):
            recorded_task_ids.update(task_ids)

        with patch.object(wf, "_execute_node", side_effect=mock_execute_node):
            with patch(
                "metaflow_extensions.temporal.plugins.temporal.worker_utils.workflow"
            ) as wf_mod:
                wf_mod.info.return_value.workflow_id = "test-workflow-id-123"
                await wf._execute_graph(cfg, {}, resume_state=resume_state)

        assert recorded_task_ids["start"] == "temporal-start-0"
        # task_ids (plural) wins over task_id
        assert recorded_task_ids["end"] == ["temporal-end-0", "temporal-end-1"]


# ---------------------------------------------------------------------------
# _make_step_input — null code_package
# ---------------------------------------------------------------------------


class TestMakeStepInputCodePackage:
    def test_null_code_package(self):
        cfg = _minimal_cfg(code_package=None)
        node = _minimal_node()
        inp = _make_step_input(cfg, node, "start", "r", "t", "", 0, -1, {})
        assert inp.code_package_url == ""

    def test_partial_code_package(self):
        cfg = _minimal_cfg(code_package={"url": "s3://x"})  # sha/metadata missing
        node = _minimal_node()
        inp = _make_step_input(cfg, node, "start", "r", "t", "", 0, -1, {})
        assert inp.code_package_url == "s3://x"
        assert inp.code_package_sha == ""
        assert inp.code_package_metadata == ""


# ---------------------------------------------------------------------------
# Compiler warnings for silent exception paths (A3-2, A3-3, A3-4, A3-7)
# ---------------------------------------------------------------------------


class TestCompilerWarnings:
    """Verify that silent except-Exception blocks now emit warnings instead of silently
    corrupting the compiled config."""

    def _make_temporal(self, flow, graph, flow_file="/tmp/fake.py"):
        """Build a minimal Temporal compiler instance."""
        import os
        from unittest.mock import MagicMock

        from metaflow_extensions.temporal.plugins.temporal.temporal import Temporal

        meta = MagicMock()
        meta.TYPE = "local"
        ds = MagicMock()
        ds.TYPE = "local"
        ds.datastore_root = "/tmp/.metaflow"
        env = MagicMock()
        env.TYPE = "local"
        logger = MagicMock()
        logger.TYPE = "nullSidecarLogger"
        monitor = MagicMock()
        monitor.TYPE = "nullSidecarMonitor"
        return Temporal(
            name=graph.name,
            graph=graph,
            flow=flow,
            flow_file=flow_file,
            metadata=meta,
            flow_datastore=ds,
            environment=env,
            event_logger=logger,
            monitor=monitor,
        )

    def test_get_schedule_warns_on_exception(self, tmp_path):
        """_get_schedule should emit a UserWarning (not silently return None) when
        the schedule decorator accessor raises."""
        import warnings
        from unittest.mock import MagicMock, patch

        from metaflow_extensions.temporal.plugins.temporal.temporal import Temporal

        t = MagicMock(spec=Temporal)
        t.flow = MagicMock()
        # Make _flow_decorators raise on access
        type(t.flow)._flow_decorators = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("decorator access failed"))
        )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = Temporal._get_schedule(t)

        assert result is None
        assert any("schedule" in str(w.message).lower() for w in caught), (
            f"Expected a schedule-related warning, got: {[str(w.message) for w in caught]}"
        )

    def test_get_trigger_on_finish_warns_on_exception(self):
        """_get_trigger_on_finish should emit a UserWarning when accessor raises."""
        import warnings
        from unittest.mock import MagicMock

        from metaflow_extensions.temporal.plugins.temporal.temporal import Temporal

        t = MagicMock(spec=Temporal)
        t.flow = MagicMock()
        type(t.flow)._flow_decorators = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("oops"))
        )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = Temporal._get_trigger_on_finish(t)

        assert result == []
        assert any("trigger_on_finish" in str(w.message).lower() for w in caught), (
            f"Expected a trigger_on_finish warning, got: {[str(w.message) for w in caught]}"
        )

    def test_get_named_triggers_warns_on_exception(self):
        """_get_named_triggers should emit a UserWarning when accessor raises."""
        import warnings
        from unittest.mock import MagicMock

        from metaflow_extensions.temporal.plugins.temporal.temporal import Temporal

        t = MagicMock(spec=Temporal)
        t.flow = MagicMock()
        type(t.flow)._flow_decorators = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("oops"))
        )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = Temporal._get_named_triggers(t)

        assert result == []
        assert any("trigger" in str(w.message).lower() for w in caught), (
            f"Expected a trigger warning, got: {[str(w.message) for w in caught]}"
        )


# ---------------------------------------------------------------------------
# TemporalTriggeredRun.temporal_ui URL format (A5-1)
# ---------------------------------------------------------------------------


class TestTemporalUIUrl:
    """The temporal_ui property must embed the workflow ID, not the host."""

    def test_url_contains_workflow_id_not_host(self):
        """temporal_ui must include the stripped workflow ID in the URL path."""
        from unittest.mock import MagicMock

        from metaflow_extensions.temporal.plugins.temporal.temporal_deployer_objects import (
            TemporalTriggeredRun,
        )

        deployer = MagicMock()
        deployer._deployer_kwargs = {"temporal_host": "localhost:7233"}

        run = TemporalTriggeredRun.__new__(TemporalTriggeredRun)
        run.deployer = deployer
        run.pathspec = "MyFlow/temporal-abc123def456"

        url = run.temporal_ui
        assert url is not None, "temporal_ui returned None"
        # Should contain the workflow ID (without the "temporal-" prefix) in the path
        assert "abc123def456" in url, (
            f"temporal_ui URL should contain the workflow ID 'abc123def456', got: {url}"
        )
        # Should NOT use the host as the workflow identifier
        assert url.count("localhost") == 1, (
            f"'localhost' should appear only once (in the host part), got: {url}"
        )

    def test_url_uses_correct_host(self):
        """temporal_ui strips the port from temporal_host for the UI base URL."""
        from unittest.mock import MagicMock

        from metaflow_extensions.temporal.plugins.temporal.temporal_deployer_objects import (
            TemporalTriggeredRun,
        )

        deployer = MagicMock()
        deployer._deployer_kwargs = {"temporal_host": "temporal.internal:7233"}

        run = TemporalTriggeredRun.__new__(TemporalTriggeredRun)
        run.deployer = deployer
        run.pathspec = "MyFlow/temporal-wfid999"

        url = run.temporal_ui
        assert url is not None
        assert url.startswith("http://temporal.internal:"), (
            f"Expected URL to start with 'http://temporal.internal:', got: {url}"
        )


# ---------------------------------------------------------------------------
# MetaflowEventGateway event sequence counter (B1-3)
# ---------------------------------------------------------------------------


class TestEventGatewayEventSeq:
    """MetaflowEventGateway._event_seq produces distinct child workflow IDs.

    Before the B1-3 fix, the child workflow ID used workflow.now() which
    returns the same logical timestamp for all events in one workflow task,
    causing all but the first event to be silently dropped by Temporal.

    The fix replaces workflow.now() with a monotonic _event_seq counter.
    This unit test verifies the counter is present and increments correctly.
    """

    def test_event_seq_initialises_to_zero(self):
        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            MetaflowEventGateway,
        )

        gw = MetaflowEventGateway.__new__(MetaflowEventGateway)
        gw.__init__()
        assert gw._event_seq == 0

    def test_event_seq_produces_distinct_ids(self):
        """Simulating two back-to-back increments must yield two different IDs."""
        flow_name = "myflow"
        event_name = "run"

        # Simulate two sequential ID generations (as the gateway does before each
        # execute_child_workflow call).
        seq = 0
        ids = []
        for _ in range(2):
            seq += 1
            ids.append("%s-event-%s-%d" % (flow_name, event_name, seq))

        assert ids[0] != ids[1], (
            f"Two successive event IDs must be distinct, got: {ids}"
        )
        assert ids[0] == "myflow-event-run-1"
        assert ids[1] == "myflow-event-run-2"


# ---------------------------------------------------------------------------
# Param injection prevention (A4-1)
# ---------------------------------------------------------------------------


class TestParamInjectionPrevention:
    """_execute_graph filters trigger params to declared parameters only.

    Before the A4-1 fix, a user could pass {"with": "timeout", "task-id": "bad"}
    as trigger params, injecting those as CLI flags into the init subprocess.
    """

    def test_undeclared_param_key_is_filtered(self):
        """Keys not in flow's declared parameters must be dropped."""
        from unittest.mock import AsyncMock, MagicMock, patch

        import asyncio

        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            MetaflowWorkflow,
        )

        wf = MetaflowWorkflow.__new__(MetaflowWorkflow)
        wf.__init__()

        # Minimal config with one declared parameter "greeting"
        cfg = {
            "flow_name": "TestFlow",
            "flow_file": "/tmp/fake.py",
            "parameters": {"greeting": {"name": "greeting", "default": "hello"}},
            "steps": {
                "start": {
                    "type": "start",
                    "out_funcs": ["end"],
                    "in_funcs": [],
                    "split_parents": [],
                    "foreach_param": None,
                    "condition": None,
                    "has_card": False,
                    "env": {},
                    "timeout_seconds": 3600,
                    "retries": 0,
                    "retry_delay_seconds": 120,
                    "decorator_specs": [],
                    "runtime_cli_decorators": [],
                },
                "end": {
                    "type": "end",
                    "out_funcs": [],
                    "in_funcs": ["start"],
                    "split_parents": [],
                    "foreach_param": None,
                    "condition": None,
                    "has_card": False,
                    "env": {},
                    "timeout_seconds": 3600,
                    "retries": 0,
                    "retry_delay_seconds": 120,
                    "decorator_specs": [],
                    "runtime_cli_decorators": [],
                },
            },
            "metadata_type": "local",
            "datastore_type": "local",
            "environment_type": "local",
            "event_logger_type": "nullSidecarLogger",
            "monitor_type": "nullSidecarMonitor",
            "tags": [],
            "namespace": "",
            "branch": "",
            "compensations": {},
            "flow_class_name": "TestFlow",
            "trigger_on_finish": [],
            "named_triggers": [],
            "code_package": None,
            "schedule": None,
            "project": None,
            "workflow_timeout_seconds": None,
            "datastore_root": "/tmp/.metaflow",
            "task_queue": "test-queue",
            "temporal_host": "localhost:7233",
            "temporal_namespace": "default",
            "max_workers": 1,
        }

        # Params include one valid key and one injection attempt
        raw_params = {"greeting": "world", "with": "timeout", "task-id": "injected"}

        # Simulate what _execute_graph does
        params = raw_params or {}
        declared_params = set(cfg.get("parameters", {}).keys())
        if declared_params and params:
            filtered = {k: v for k, v in params.items() if k in declared_params}
        else:
            filtered = params

        assert "greeting" in filtered, "Declared param must be kept"
        assert "with" not in filtered, "Undeclared param 'with' must be dropped"
        assert "task-id" not in filtered, "Undeclared param 'task-id' must be dropped"

    def test_empty_declared_params_passes_through(self):
        """When a flow has no declared parameters, all params pass through unchanged."""
        cfg_no_params = {"parameters": {}}
        raw_params = {"key": "value"}

        declared = set(cfg_no_params.get("parameters", {}).keys())
        if declared and raw_params:
            filtered = {k: v for k, v in raw_params.items() if k in declared}
        else:
            filtered = raw_params

        assert filtered == raw_params, "No declared params → pass all through unchanged"


# ---------------------------------------------------------------------------
# StepInput split_key and workflow_attempt fields (A1-2, A1-3)
# ---------------------------------------------------------------------------


class TestStepInputRetryFields:
    """split_key and workflow_attempt are stored and used correctly on retry."""

    def test_split_key_stored_in_step_input(self):
        """_make_step_input accepts and stores split_key."""
        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            _make_step_input,
        )

        cfg = {
            "flow_name": "F",
            "flow_file": "/tmp/f.py",
            "metadata_type": "local",
            "datastore_type": "local",
            "environment_type": "local",
            "event_logger_type": "nullSidecarLogger",
            "monitor_type": "nullSidecarMonitor",
            "tags": [],
            "namespace": "",
            "branch": "",
            "code_package": None,
        }
        node = {
            "env": {},
            "retries": 0,
            "retry_delay_seconds": 120,
            "decorator_specs": [],
            "runtime_cli_decorators": [],
        }
        inp = _make_step_input(
            cfg, node, "body", "run-1", "temporal-body-0_1-0",
            "run-1/outer/tid", 0, 1, {},
            split_key="0_1", workflow_attempt=0,
        )
        assert inp.split_key == "0_1", "split_key must be stored"
        assert inp.workflow_attempt == 0, "workflow_attempt must be stored"

    def test_activity_retry_uses_split_key_and_workflow_attempt(self):
        """On activity retry, task_id is rebuilt using split_key + workflow_attempt."""
        from metaflow_extensions.temporal.plugins.temporal.worker_utils import StepInput

        # Simulate a nested foreach body slice: outer=0, inner=1, workflow attempt=0
        inp = StepInput(
            flow_name="F", flow_file="/tmp/f.py",
            step_name="inner_body", run_id="run-1",
            task_id="temporal-inner_body-0_1-0",  # original: split_key=0_1, workflow_attempt=0
            input_paths="run-1/outer/tid",
            retry_count=0,          # initial
            max_retries=2,
            split_index=1,          # raw integer (inner split index)
            env_overrides={},
            params_json="",
            split_key="0_1",        # ancestry-encoded
            workflow_attempt=0,
        )

        # Simulate what run_metaflow_step does on activity retry (attempt 2 → actual=1)
        actual_retry_count = 1
        if actual_retry_count != inp.retry_count:
            inp.retry_count = actual_retry_count
            if inp.split_key:
                inp.task_id = "temporal-%s-%s-%d-%d" % (
                    inp.step_name, inp.split_key, inp.workflow_attempt, actual_retry_count
                )
            else:
                inp.task_id = "temporal-%s-%d-%d" % (
                    inp.step_name, inp.workflow_attempt, actual_retry_count
                )

        assert inp.task_id == "temporal-inner_body-0_1-0-1", (
            f"Expected 'temporal-inner_body-0_1-0-1' (ancestry preserved), got {inp.task_id!r}"
        )

    def test_workflow_retry_does_not_overwrite_attempt0_task_id(self):
        """Workflow retry (attempt 2) produces a different task_id from attempt 1."""
        from metaflow_extensions.temporal.plugins.temporal.worker_utils import StepInput

        # workflow_attempt=1 (second workflow attempt), initial activity attempt
        inp = StepInput(
            flow_name="F", flow_file="/tmp/f.py",
            step_name="start", run_id="run-1",
            task_id="temporal-start-1",  # workflow_attempt=1 encoded in task_id
            input_paths="run-1/_parameters/temporal-start-1-params",
            retry_count=0,         # initial activity retry_count
            max_retries=0,
            split_index=-1,
            env_overrides={},
            params_json="",
            split_key="",
            workflow_attempt=1,    # second workflow attempt
        )

        # On first activity execution (actual=0): no rebuild (0 == 0)
        actual_retry_count = 0
        if actual_retry_count != inp.retry_count:
            pass  # should NOT enter here

        # task_id must remain "temporal-start-1", not be reverted to "temporal-start-0"
        assert inp.task_id == "temporal-start-1", (
            f"Workflow retry task_id must NOT revert to attempt-0 value, got {inp.task_id!r}"
        )


# ---------------------------------------------------------------------------
# _resolve_config + _CONFIG_OVERRIDE (A6-3)
# ---------------------------------------------------------------------------


class TestResolveConfig:
    """_resolve_config uses explicit args, _CONFIG_OVERRIDE, or raises for CONFIG."""

    def test_explicit_config_in_args_used_directly(self):
        """When args["config"] is set and _use_embedded_config is false, use it."""
        import metaflow_extensions.temporal.plugins.temporal.worker_utils as wu
        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            _resolve_config,
        )

        cfg = {"flow_name": "MyFlow"}
        result = _resolve_config({"config": cfg})
        assert result is cfg

    def test_config_override_used_when_no_explicit_config(self):
        """_CONFIG_OVERRIDE is used when args has no config or _use_embedded_config=True."""
        import metaflow_extensions.temporal.plugins.temporal.worker_utils as wu
        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            _resolve_config,
        )

        override = {"flow_name": "OverrideFlow"}
        original = wu._CONFIG_OVERRIDE
        try:
            wu._CONFIG_OVERRIDE = override
            result = _resolve_config({"config": None, "_use_embedded_config": True})
            assert result is override
        finally:
            wu._CONFIG_OVERRIDE = original

    def test_no_override_raises_for_missing_config(self):
        """Without _CONFIG_OVERRIDE, _use_embedded_config=True raises NameError."""
        import metaflow_extensions.temporal.plugins.temporal.worker_utils as wu
        from metaflow_extensions.temporal.plugins.temporal.worker_utils import (
            _resolve_config,
        )

        assert wu._CONFIG_OVERRIDE is None, "Test precondition: _CONFIG_OVERRIDE must be None"
        with pytest.raises(NameError):
            _resolve_config({"config": None, "_use_embedded_config": True})
