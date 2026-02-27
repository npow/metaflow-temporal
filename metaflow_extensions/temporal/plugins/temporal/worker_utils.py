"""
Metaflow Temporal worker runtime utilities.

This file is embedded verbatim into generated worker files.
"""
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker


# ---------------------------------------------------------------------------
# Data types shared between workflow and activities
# ---------------------------------------------------------------------------


@dataclass
class StepInput:
    flow_name: str
    flow_file: str
    step_name: str
    run_id: str
    task_id: str
    input_paths: str
    retry_count: int
    max_retries: int
    split_index: int  # -1 if not a foreach body task
    env_overrides: dict
    params_json: str  # JSON params (non-empty for start step only)
    # Runtime provider types — set from CONFIG at compile time so that the step
    # subprocess uses the same metadata/datastore/environment backend as the flow.
    metadata_type: str = "local"
    datastore_type: str = "local"
    environment_type: str = "local"
    event_logger_type: str = "nullSidecarLogger"
    monitor_type: str = "nullSidecarMonitor"
    # Decorator backend specs forwarded as --with flags (e.g. "kubernetes:cpu=4",
    # "sandbox:backend=daytona", "conda:packages=['numpy']").
    decorator_specs: Optional[List[str]] = None
    # Run tags forwarded as --tag flags to each step subprocess
    tags: Optional[List[str]] = None


@dataclass
class StepOutput:
    task_id: str
    foreach_cardinality: int = 0
    # Public artifact names produced by this step — values are NOT loaded here.
    # See artifact_fetch_hint for a Python snippet to retrieve them.
    artifact_names: Optional[List[str]] = None
    artifact_fetch_hint: Optional[str] = None


@dataclass
class CompensationInput:
    flow_file: str
    flow_name: str          # project-aware name (for artifact pathspec lookup)
    flow_class_name: str    # original Python class name (for importlib)
    handler_name: str       # compensation method name
    forward_step: str       # the step being compensated
    run_id: str
    task_id: str            # task_id of the forward step
    metadata_type: str = "local"
    datastore_type: str = "local"
    datastore_root: str = ""


# ---------------------------------------------------------------------------
# Activity
# ---------------------------------------------------------------------------


def _datastore_root_arg(env_overrides: dict, datastore_type: str = "local") -> str:
    """Return the --datastore-root value for the given datastore type."""
    key = "METAFLOW_DATASTORE_SYSROOT_%s" % datastore_type.upper()
    root = env_overrides.get(key) or os.environ.get(key)
    if root:
        return root
    # Fallback: local default
    return os.path.join(os.path.expanduser("~"), ".metaflow")


def _top_level_args(inp: StepInput) -> list:
    """Build the top-level CLI flags shared by step/init/dump subcommands."""
    args = [
        "--quiet",
        "--no-pylint",
        "--metadata=%s" % inp.metadata_type,
        "--environment=%s" % inp.environment_type,
        "--datastore=%s" % inp.datastore_type,
        "--datastore-root=%s" % _datastore_root_arg(inp.env_overrides, inp.datastore_type),
        "--event-logger=%s" % inp.event_logger_type,
        "--monitor=%s" % inp.monitor_type,
    ]
    # Forward compute/environment backend decorators so that @kubernetes, @batch,
    # @conda, @sandbox, etc. take effect inside the subprocess.
    for spec in (inp.decorator_specs or []):
        args.append("--with=%s" % spec)
    # Forward run tags
    for tag in (inp.tags or []):
        args.append("--tag=%s" % tag)
    return args


def _build_step_cmd(inp: StepInput, input_paths: str) -> list:
    top_level = _top_level_args(inp) + ["--with=temporal_internal"]
    step_args = [
        "step",
        inp.step_name,
        "--run-id", inp.run_id,
        "--task-id", inp.task_id,
        "--retry-count", str(inp.retry_count),
        "--max-user-code-retries", str(inp.max_retries),
        "--input-paths", input_paths,
    ]
    if inp.split_index >= 0:
        step_args += ["--split-index", str(inp.split_index)]
    return [sys.executable, inp.flow_file] + top_level + step_args


def _build_init_cmd(inp: StepInput, params_task_id: str, params: dict) -> list:
    top_level = _top_level_args(inp)
    init_args = [
        "init",
        "--run-id", inp.run_id,
        "--task-id", params_task_id,
    ]
    # Pass user parameters as CLI arguments to the init command
    # (the init command exposes flow Parameters via @add_custom_parameters)
    for k, v in params.items():
        init_args += ["--%s" % k, str(v)]
    return [sys.executable, inp.flow_file] + top_level + init_args


def _build_dump_cmd(inp: StepInput, params_task_id: str) -> list:
    top_level = _top_level_args(inp)
    return [sys.executable, inp.flow_file] + top_level + [
        "dump",
        "--max-value-size=0",
        "%s/_parameters/%s" % (inp.run_id, params_task_id),
    ]


# Lock protecting temporary os.environ mutations in _read_artifact_names.
# Activities run in a ThreadPoolExecutor so concurrent calls are possible.
_artifact_lock = threading.Lock()


def _read_artifact_names(inp: StepInput) -> list:
    """Return the names of public artifacts produced by this step.

    Values are intentionally NOT loaded here — pulling large artifacts
    (numpy arrays, DataFrames, etc.) into the activity result would be
    expensive and wasteful.  The Temporal UI shows the names; fetch values
    via the Metaflow client:

        import metaflow
        task = metaflow.Task("FlowName/run_id/step_name/task_id")
        value = task.data.<artifact_name>
    """
    try:
        import metaflow

        datastore_root = _datastore_root_arg(inp.env_overrides)
        pathspec = "%s/%s/%s/%s" % (
            inp.flow_name,
            inp.run_id,
            inp.step_name,
            inp.task_id,
        )
        with _artifact_lock:
            old = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
            os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = datastore_root
            try:
                metaflow.metadata("local")
                task = metaflow.Task(pathspec)
                # Access only .id — does NOT deserialize the artifact data
                return [a.id for a in task.artifacts if not a.id.startswith("_")]
            finally:
                if old is None:
                    os.environ.pop("METAFLOW_DATASTORE_SYSROOT_LOCAL", None)
                else:
                    os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = old
    except Exception:
        return []


@activity.defn(name="run_metaflow_step")
async def run_metaflow_step(inp: StepInput) -> StepOutput:
    """Execute a single Metaflow step as a Temporal activity."""
    output_fd, output_file = tempfile.mkstemp(suffix=".json")
    os.close(output_fd)

    try:
        env = {
            **os.environ,
            **inp.env_overrides,
            "METAFLOW_TEMPORAL_WORKFLOW_ID": inp.run_id,
            "METAFLOW_TEMPORAL_RUN_ID": inp.run_id,
            "METAFLOW_TEMPORAL_OUTPUT_FILE": output_file,
        }

        input_paths = inp.input_paths
        loop = asyncio.get_event_loop()

        if inp.step_name == "start" and inp.params_json:
            params = json.loads(inp.params_json)
            params_task_id = "%s-params" % inp.task_id
            for k, v in params.items():
                env["METAFLOW_INIT_%s" % k.upper()] = str(v)

            # Check if _parameters task already exists (idempotent on retry)
            check_result = await loop.run_in_executor(
                None,
                lambda: subprocess.run(
                    _build_dump_cmd(inp, params_task_id),
                    env=env,
                    capture_output=True,
                ),
            )
            if check_result.returncode != 0:
                init_result = await loop.run_in_executor(
                    None,
                    lambda: subprocess.run(
                        _build_init_cmd(inp, params_task_id, params),
                        env=env,
                        capture_output=True,
                    ),
                )
                if init_result.returncode != 0:
                    stderr = init_result.stderr.decode(errors="replace")
                    stdout = init_result.stdout.decode(errors="replace")
                    raise ApplicationError(
                        "Parameters init failed: stdout=%s stderr=%s" % (stdout[-1000:], stderr[-1000:])
                    )

            input_paths = "%s/_parameters/%s" % (inp.run_id, params_task_id)

        result = await loop.run_in_executor(
            None,
            lambda: subprocess.run(
                _build_step_cmd(inp, input_paths),
                env=env,
                capture_output=True,
            ),
        )

        if result.returncode != 0:
            stderr = result.stderr.decode(errors="replace")
            stdout = result.stdout.decode(errors="replace")
            raise ApplicationError(
                "Step %s failed (exit %d):\nSTDOUT: %s\nSTDERR: %s"
                % (inp.step_name, result.returncode, stdout[-2000:], stderr[-2000:])
            )

        out = {}
        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            with open(output_file) as f:
                out = json.load(f)

        artifact_names = _read_artifact_names(inp)

        return StepOutput(
            task_id=inp.task_id,
            foreach_cardinality=out.get("foreach_cardinality", 0),
            artifact_names=artifact_names,
            artifact_fetch_hint=(
                "import metaflow; t = metaflow.Task('%s/%s/%s/%s'); t.data.<name>"
                % (inp.flow_name, inp.run_id, inp.step_name, inp.task_id)
            ) if artifact_names else None,
        )
    finally:
        try:
            os.unlink(output_file)
        except OSError:
            pass


@activity.defn(name="run_compensation")
async def run_compensation(inp: CompensationInput) -> None:
    """Execute a saga compensation handler for a previously completed step.

    Loads the forward step's artifacts via the Metaflow client, creates a
    minimal flow instance with those artifacts injected, and calls the
    compensation method.  Best-effort: caller should catch exceptions.
    """
    import importlib.util

    root = inp.datastore_root or _datastore_root_arg({}, inp.datastore_type)
    artifacts = {}

    with _artifact_lock:
        old = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = root
        try:
            import metaflow
            metaflow.metadata(inp.metadata_type)
            pathspec = "%s/%s/%s/%s" % (
                inp.flow_name,
                inp.run_id,
                inp.forward_step,
                inp.task_id,
            )
            task = metaflow.Task(pathspec)
            for a in task.artifacts:
                if not a.id.startswith("_"):
                    try:
                        artifacts[a.id] = getattr(task.data, a.id)
                    except Exception:
                        pass
        finally:
            if old is None:
                os.environ.pop("METAFLOW_DATASTORE_SYSROOT_LOCAL", None)
            else:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = old

    # Load the flow module, create a bare instance, inject artifacts, call handler.
    # We inject directly into __dict__ to bypass FlowSpec.__getattr__/__setattr__
    # which may recurse if internal state (e.g. _datastore) is not initialised.
    spec = importlib.util.spec_from_file_location("_comp_flow", inp.flow_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    flow_class = getattr(module, inp.flow_class_name)
    instance = object.__new__(flow_class)
    # Seed internal FlowSpec state to prevent __getattr__ recursion
    instance.__dict__["_datastore"] = None
    for k, v in artifacts.items():
        instance.__dict__[k] = v
    handler = getattr(instance, inp.handler_name)
    loop = asyncio.get_event_loop()
    if asyncio.iscoroutinefunction(handler):
        await handler()
    else:
        await loop.run_in_executor(None, handler)


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------


@workflow.defn(name="MetaflowWorkflow")
class MetaflowWorkflow:
    """Interprets a CONFIG DAG and runs Metaflow steps as activities.

    Input: {"config": <config dict>, "params": <user params dict>}
    The "config" key holds the compiled flow graph; "params" holds runtime params.
    """

    def __init__(self):
        self._compensation_stack: list = []

    @workflow.run
    async def run(self, args: dict) -> str:
        cfg = args["config"]
        params = args.get("params", {})
        return await self._execute_graph(cfg, params)

    async def _execute_graph(self, cfg: dict, params: dict) -> str:
        run_id = "temporal-%s" % workflow.info().workflow_id[:20]
        task_ids: dict = {}
        try:
            await self._execute_node("start", cfg, run_id, task_ids, params, -1)
            return run_id
        except Exception:
            if cfg.get("compensations") and self._compensation_stack:
                await self._run_compensations(cfg, run_id)
            raise

    async def _run_compensations(self, cfg: dict, run_id: str) -> None:
        """Run all queued compensations in LIFO order (best-effort)."""
        for entry in reversed(self._compensation_stack):
            try:
                await workflow.execute_activity(
                    run_compensation,
                    CompensationInput(
                        flow_file=cfg["flow_file"],
                        flow_name=cfg["flow_name"],
                        flow_class_name=cfg.get("flow_class_name", cfg["flow_name"]),
                        handler_name=entry["handler"],
                        forward_step=entry["step"],
                        run_id=run_id,
                        task_id=entry["task_id"],
                        metadata_type=cfg.get("metadata_type", "local"),
                        datastore_type=cfg.get("datastore_type", "local"),
                        datastore_root=cfg.get("datastore_root", ""),
                    ),
                    start_to_close_timeout=timedelta(seconds=300),
                    retry_policy=RetryPolicy(
                        maximum_attempts=3,
                        initial_interval=timedelta(seconds=5),
                    ),
                )
            except Exception:
                pass  # best-effort: log and continue

    async def _execute_node(
        self,
        step_name: str,
        cfg: dict,
        run_id: str,
        task_ids: dict,
        params: dict,
        split_index: int,
    ):
        steps = cfg["steps"]
        node = steps[step_name]
        node_type = node["type"]

        input_paths = _resolve_input_paths(step_name, node, run_id, task_ids)

        attempt = workflow.info().attempt
        if split_index >= 0:
            task_id = "temporal-%s-%d-%d" % (step_name, split_index, attempt)
        else:
            task_id = "temporal-%s-%d" % (step_name, attempt)

        env_overrides = dict(node.get("env", {}))
        env_overrides["METAFLOW_RUN_ID"] = run_id

        inp = StepInput(
            flow_name=cfg["flow_name"],
            flow_file=cfg["flow_file"],
            step_name=step_name,
            run_id=run_id,
            task_id=task_id,
            input_paths=input_paths,
            retry_count=attempt,
            max_retries=node.get("retries", 0),
            split_index=split_index,
            env_overrides=env_overrides,
            params_json=json.dumps(params) if step_name == "start" else "",
            metadata_type=cfg.get("metadata_type", "local"),
            datastore_type=cfg.get("datastore_type", "local"),
            environment_type=cfg.get("environment_type", "local"),
            event_logger_type=cfg.get("event_logger_type", "nullSidecarLogger"),
            monitor_type=cfg.get("monitor_type", "nullSidecarMonitor"),
            decorator_specs=node.get("decorator_specs", []),
            tags=cfg.get("tags", []),
        )

        retry_policy = RetryPolicy(
            maximum_attempts=node.get("retries", 0) + 1,
            initial_interval=timedelta(seconds=node.get("retry_delay_seconds", 120)),
        )
        timeout_seconds = node.get("timeout_seconds", 3600)

        out: StepOutput = await workflow.execute_activity(
            run_metaflow_step,
            inp,
            start_to_close_timeout=timedelta(seconds=timeout_seconds),
            retry_policy=retry_policy,
        )

        task_ids[step_name] = out.task_id

        # Push to saga compensation stack if this step has a registered compensation
        compensations = cfg.get("compensations", {})
        if step_name in compensations:
            self._compensation_stack.append({
                "handler": compensations[step_name],
                "step": step_name,
                "task_id": out.task_id,
            })

        if step_name == "end":
            return

        if node_type == "foreach":
            out_funcs = node["out_funcs"]
            if len(out_funcs) != 1:
                raise ApplicationError("foreach node must have exactly one out_func")
            body_step = out_funcs[0]
            cardinality = out.foreach_cardinality

            # Run all body steps in parallel
            body_results = await asyncio.gather(
                *[
                    self._execute_foreach_body(body_step, cfg, run_id, params, i, attempt)
                    for i in range(cardinality)
                ]
            )
            split_task_ids = [r.task_id for r in body_results]
            task_ids[body_step] = split_task_ids

            # Continue to join
            body_node = steps[body_step]
            join_step = body_node["out_funcs"][0]
            await self._execute_node(join_step, cfg, run_id, task_ids, params, -1)

        elif node_type == "split":
            out_funcs = node["out_funcs"]
            # Pass a snapshot of the current task_ids to each branch so they
            # can resolve input_paths for the first step (whose parent is this
            # split node, already in task_ids).
            shared_ids = dict(task_ids)
            branch_results = await asyncio.gather(
                *[
                    self._execute_branch_until_join(
                        branch, cfg, run_id, params, attempt, dict(shared_ids)
                    )
                    for branch in out_funcs
                ]
            )
            # Merge per-branch task_ids back
            for branch_task_ids in branch_results:
                task_ids.update(branch_task_ids)

            # Find the join node that corresponds to this split
            join_step = _find_join_step(step_name, steps)
            if join_step:
                await self._execute_node(join_step, cfg, run_id, task_ids, params, -1)

        else:
            for next_step in node["out_funcs"]:
                await self._execute_node(next_step, cfg, run_id, task_ids, params, -1)

    async def _execute_foreach_body(
        self,
        step_name: str,
        cfg: dict,
        run_id: str,
        params: dict,
        split_index: int,
        attempt: int,
    ) -> StepOutput:
        """Execute a single foreach body step."""
        steps = cfg["steps"]
        node = steps[step_name]

        # For foreach body, parent is the foreach step
        parent_step = node["in_funcs"][0]
        parent_task_id = "temporal-%s-%d" % (parent_step, attempt)
        input_paths = "%s/%s/%s" % (run_id, parent_step, parent_task_id)

        task_id = "temporal-%s-%d-%d" % (step_name, split_index, attempt)
        env_overrides = dict(node.get("env", {}))
        env_overrides["METAFLOW_RUN_ID"] = run_id

        inp = StepInput(
            flow_name=cfg["flow_name"],
            flow_file=cfg["flow_file"],
            step_name=step_name,
            run_id=run_id,
            task_id=task_id,
            input_paths=input_paths,
            retry_count=attempt,
            max_retries=node.get("retries", 0),
            split_index=split_index,
            env_overrides=env_overrides,
            params_json="",
            metadata_type=cfg.get("metadata_type", "local"),
            datastore_type=cfg.get("datastore_type", "local"),
            environment_type=cfg.get("environment_type", "local"),
            event_logger_type=cfg.get("event_logger_type", "nullSidecarLogger"),
            monitor_type=cfg.get("monitor_type", "nullSidecarMonitor"),
            decorator_specs=node.get("decorator_specs", []),
            tags=cfg.get("tags", []),
        )

        retry_policy = RetryPolicy(
            maximum_attempts=node.get("retries", 0) + 1,
            initial_interval=timedelta(seconds=node.get("retry_delay_seconds", 120)),
        )
        timeout_seconds = node.get("timeout_seconds", 3600)

        return await workflow.execute_activity(
            run_metaflow_step,
            inp,
            start_to_close_timeout=timedelta(seconds=timeout_seconds),
            retry_policy=retry_policy,
        )

    async def _execute_branch_until_join(
        self,
        step_name: str,
        cfg: dict,
        run_id: str,
        params: dict,
        attempt: int,
        task_ids: dict = None,
    ) -> dict:
        """Execute a branch from step_name until it reaches a join node.
        Returns a dict of step_name -> task_id for steps executed in this branch.
        task_ids is the caller's snapshot (contains the split node's task_id).
        """
        steps = cfg["steps"]
        if task_ids is None:
            task_ids = {}
        current = step_name

        while current is not None:
            node = steps[current]
            node_type = node["type"]

            # Stop at join — let the caller handle it
            if node_type == "join":
                break

            input_paths = _resolve_input_paths(current, node, run_id, task_ids)
            # For the first step after a split, parent is the split step
            # We need to look at in_funcs and find what's already in task_ids
            # _resolve_input_paths handles this via the passed task_ids

            task_id = "temporal-%s-%d" % (current, attempt)
            env_overrides = dict(node.get("env", {}))
            env_overrides["METAFLOW_RUN_ID"] = run_id

            inp = StepInput(
                flow_name=cfg["flow_name"],
                flow_file=cfg["flow_file"],
                step_name=current,
                run_id=run_id,
                task_id=task_id,
                input_paths=input_paths,
                retry_count=attempt,
                max_retries=node.get("retries", 0),
                split_index=-1,
                env_overrides=env_overrides,
                params_json="",
                metadata_type=cfg.get("metadata_type", "local"),
                datastore_type=cfg.get("datastore_type", "local"),
                environment_type=cfg.get("environment_type", "local"),
                event_logger_type=cfg.get("event_logger_type", "nullSidecarLogger"),
                monitor_type=cfg.get("monitor_type", "nullSidecarMonitor"),
                decorator_specs=node.get("decorator_specs", []),
            )

            retry_policy = RetryPolicy(maximum_attempts=node.get("retries", 0) + 1)
            timeout_seconds = node.get("timeout_seconds", 3600)

            out: StepOutput = await workflow.execute_activity(
                run_metaflow_step,
                inp,
                start_to_close_timeout=timedelta(seconds=timeout_seconds),
                retry_policy=retry_policy,
            )
            task_ids[current] = out.task_id

            out_funcs = node["out_funcs"]
            if not out_funcs:
                break
            next_step = out_funcs[0]
            # Check if next is join
            if steps[next_step]["type"] == "join":
                break
            current = next_step

        return task_ids


# ---------------------------------------------------------------------------
# Pure helper functions (usable inside @workflow.defn via sandbox)
# ---------------------------------------------------------------------------


def _resolve_input_paths(
    step_name: str, node: dict, run_id: str, task_ids: dict
) -> str:
    """Build comma-separated Metaflow input path(s) for a step."""
    in_funcs = node["in_funcs"]

    if step_name == "start":
        return "%s/_parameters/temporal-start-0-params" % run_id

    if not in_funcs:
        return ""

    split_parents = node.get("split_parents", [])
    is_foreach_join = False
    if node["type"] == "join" and split_parents:
        # Check if the innermost split parent is a foreach
        # We don't have cfg here, so we check if body_step task_ids is a list
        body_step = in_funcs[0]
        parent_ids = task_ids.get(body_step)
        if isinstance(parent_ids, list):
            is_foreach_join = True

    if is_foreach_join:
        body_step = in_funcs[0]
        split_ids = task_ids.get(body_step, [])
        return ",".join(
            "%s/%s/%s" % (run_id, body_step, tid) for tid in split_ids
        )

    if len(in_funcs) == 1:
        parent = in_funcs[0]
        parent_task_id = task_ids.get(parent, "temporal-%s-0" % parent)
        if isinstance(parent_task_id, list):
            return ",".join(
                "%s/%s/%s" % (run_id, parent, tid) for tid in parent_task_id
            )
        return "%s/%s/%s" % (run_id, parent, parent_task_id)

    # Multiple parents (join after split)
    paths = []
    for parent in in_funcs:
        parent_task_id = task_ids.get(parent, "temporal-%s-0" % parent)
        if isinstance(parent_task_id, list):
            paths.extend(
                "%s/%s/%s" % (run_id, parent, tid) for tid in parent_task_id
            )
        else:
            paths.append("%s/%s/%s" % (run_id, parent, parent_task_id))
    return ",".join(paths)


def _find_join_step(split_step_name: str, steps: dict) -> str | None:
    """Find the join step that corresponds to the given split step."""
    for name, node in steps.items():
        if node["type"] == "join":
            parents = node.get("split_parents", [])
            if parents and parents[-1] == split_step_name:
                return name
    return None


# ---------------------------------------------------------------------------
# WorkerUtils (entry points for generated worker files)
# ---------------------------------------------------------------------------


class WorkerUtils:
    @staticmethod
    async def run_worker(config: dict):
        """Start the Temporal worker for this flow."""
        client = await Client.connect(config["temporal_host"])
        async with Worker(
            client,
            task_queue=config["task_queue"],
            workflows=[MetaflowWorkflow],
            activities=[run_metaflow_step, run_compensation],
            activity_executor=ThreadPoolExecutor(
                max_workers=config.get("max_workers", 10)
            ),
        ):
            print("Worker started. Listening on queue: %s" % config["task_queue"])
            # Register a Temporal Schedule if @schedule decorator is present
            schedule_cfg = config.get("schedule")
            if schedule_cfg and schedule_cfg.get("cron"):
                await WorkerUtils._register_schedule(client, config, schedule_cfg)
            await asyncio.Event().wait()

    @staticmethod
    async def _register_schedule(client: Client, config: dict, schedule_cfg: dict):
        """Register a Temporal Schedule for a @schedule-decorated flow."""
        from temporalio.client import (
            Schedule,
            ScheduleActionStartWorkflow,
            SchedulePolicy,
            ScheduleOverlapPolicy,
            ScheduleSpec,
        )

        flow_name = config["flow_name"]
        schedule_id = "metaflow-%s-schedule" % flow_name.lower()
        cron = schedule_cfg["cron"]
        timezone = schedule_cfg.get("timezone")

        timeout_seconds = config.get("workflow_timeout_seconds")
        execution_timeout = timedelta(seconds=timeout_seconds) if timeout_seconds else None

        try:
            await client.create_schedule(
                schedule_id,
                Schedule(
                    action=ScheduleActionStartWorkflow(
                        MetaflowWorkflow.run,
                        {"config": config, "params": {}},
                        id="%s-scheduled" % flow_name.lower(),
                        task_queue=config["task_queue"],
                        execution_timeout=execution_timeout,
                    ),
                    spec=ScheduleSpec(
                        cron_expressions=[cron],
                        time_zone_name=timezone,
                    ),
                    policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
                ),
            )
            print("Schedule registered: %s (cron: %s)" % (schedule_id, cron))
        except Exception as e:
            # Schedule already exists — log and continue
            print("Note: Schedule '%s' already exists: %s" % (schedule_id, e))

    @staticmethod
    async def trigger(config: dict, params: dict) -> str:
        """Trigger a workflow run and wait for completion."""
        client = await Client.connect(config["temporal_host"])
        workflow_id = "%s-%s" % (config["flow_name"].lower(), uuid.uuid4().hex[:8])
        timeout_seconds = config.get("workflow_timeout_seconds")
        execution_timeout = timedelta(seconds=timeout_seconds) if timeout_seconds else None
        result = await client.execute_workflow(
            MetaflowWorkflow.run,
            {"config": config, "params": params},
            id=workflow_id,
            task_queue=config["task_queue"],
            execution_timeout=execution_timeout,
        )
        print("Workflow completed. Run ID: %s" % result)
        return result
