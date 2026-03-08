"""
Metaflow Temporal worker runtime utilities.

This file is embedded verbatim into generated worker files.
"""
import asyncio
import contextlib
import json
import os
import signal
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
from temporalio.exceptions import ApplicationError, CancelledError as TemporalCancelledError
from temporalio.worker import Worker

# ---------------------------------------------------------------------------
# Tuneable constants
# ---------------------------------------------------------------------------

# How often the heartbeat loop pings Temporal to keep the activity lease alive.
_HEARTBEAT_INTERVAL_SECONDS = 20

# Maximum silence before Temporal considers an activity dead.  Must be greater
# than _HEARTBEAT_INTERVAL_SECONDS so a live activity never times out.
_HEARTBEAT_TIMEOUT_SECONDS = 60

# Default activity timeout when no @timeout decorator is present.
_DEFAULT_STEP_TIMEOUT_SECONDS = 3600  # 1 hour

# Default delay between retries when no @retry(minutes_between_retries=N) is set.
_DEFAULT_RETRY_DELAY_SECONDS = 120  # 2 minutes

# Saga compensation activity: max wall-clock time and retry settings.
_COMPENSATION_TIMEOUT_SECONDS = 300   # 5 minutes per compensation handler
_COMPENSATION_RETRY_ATTEMPTS = 3
_COMPENSATION_RETRY_DELAY_SECONDS = 5


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
    # Uploaded code package metadata (for remote runtime decorators that
    # launch code in external backends).
    code_package_url: str = ""
    code_package_sha: str = ""
    code_package_metadata: str = ""
    # Decorator backend specs forwarded as --with flags (e.g. "kubernetes:cpu=4",
    # "sandbox:backend=daytona", "conda:packages=['numpy']").
    decorator_specs: Optional[List[str]] = None
    # Serialized initialized step decorators (compile-time snapshot) used to
    # invoke runtime_step_cli hooks in this worker process.
    runtime_cli_decorators: Optional[List[dict]] = None
    # Run tags forwarded as --tag flags to each step subprocess
    tags: Optional[List[str]] = None
    # Metaflow namespace (--namespace flag for step subprocess)
    namespace: str = ""


@dataclass
class StepOutput:
    task_id: str
    foreach_cardinality: int = 0
    # Public artifact names produced by this step — values are NOT loaded here.
    # See artifact_fetch_hint for a Python snippet to retrieve them.
    artifact_names: Optional[List[str]] = None
    artifact_fetch_hint: Optional[str] = None
    # For split-switch nodes: the branch name chosen at runtime.
    # Set by run_metaflow_step after reading the _transition artifact from
    # the Metaflow datastore.  None for all non-switch steps.
    chosen_branch: Optional[str] = None


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
    if inp.namespace:
        args.append("--namespace=%s" % inp.namespace)
    # Forward compute/environment backend decorators so that @kubernetes, @batch,
    # @conda, @sandbox, etc. take effect inside the subprocess.
    for spec in (inp.decorator_specs or []):
        args.append("--with=%s" % spec)
    return args


class _RuntimeCLIArgs:
    """Minimal CLIArgs shim for StepDecorator.runtime_step_cli hooks."""

    def __init__(self, inp: StepInput, input_paths: str):
        self.entrypoint = [sys.executable, inp.flow_file]
        self.top_level_options = {
            "quiet": True,
            "pylint": False,
            "metadata": inp.metadata_type,
            "environment": inp.environment_type,
            "datastore": inp.datastore_type,
            "datastore-root": _datastore_root_arg(inp.env_overrides, inp.datastore_type),
            "event-logger": inp.event_logger_type,
            "monitor": inp.monitor_type,
            "with": list(inp.decorator_specs or []) + ["temporal_internal"],
        }
        self.commands = ["step"]
        self.command_args = [inp.step_name]
        self.command_options = {
            "run-id": inp.run_id,
            "task-id": inp.task_id,
            "retry-count": inp.retry_count,
            "max-user-code-retries": inp.max_retries,
            "input-paths": input_paths,
            "split-index": inp.split_index if inp.split_index >= 0 else None,
            "tag": list(inp.tags or []),
        }
        self.env = {}

    def get_args(self) -> list:
        def _options(mapping):
            for k, v in mapping.items():
                if v is None or v is False:
                    continue
                k = k.replace("_", "-")
                values = v if isinstance(v, (list, tuple, set)) else [v]
                for value in values:
                    yield "--%s" % k
                    if not isinstance(value, bool):
                        yield str(value)

        args = list(self.entrypoint)
        args.extend(_options(self.top_level_options))
        args.extend(self.commands)
        args.extend(self.command_args)
        args.extend(_options(self.command_options))
        return args


def _runtime_step_decorators(inp: StepInput) -> list:
    """Resolve StepDecorator instances for runtime_step_cli hooks."""
    try:
        import importlib
        from metaflow.decorators import StepDecorator, extract_step_decorator_from_decospec
    except Exception:
        return []

    # Preferred path: use compile-time initialized decorator snapshots.
    decorators = []
    for snap in (inp.runtime_cli_decorators or []):
        try:
            module_name = snap.get("module")
            class_name = snap.get("class")
            if not module_name or not class_name:
                continue
            module = importlib.import_module(module_name)
            deco_cls = getattr(module, class_name)
            deco = deco_cls()
            if not isinstance(deco, StepDecorator):
                continue
            state = snap.get("state", {})
            if isinstance(state, dict):
                deco.__dict__.update(state)
            # Some decorators expect package pointers from runtime_init/task_created.
            # Hydrate both class-level and instance-level fields when available.
            for attr, value in (
                ("package_url", inp.code_package_url),
                ("package_sha", inp.code_package_sha),
                ("package_metadata", inp.code_package_metadata),
            ):
                if value:
                    try:
                        setattr(deco.__class__, attr, value)
                    except Exception:
                        pass
                    try:
                        if getattr(deco, attr, None) in (None, ""):
                            setattr(deco, attr, value)
                    except Exception:
                        pass
            # Call external_init so decorators that launch remote runtimes can
            # initialize themselves (e.g. resolve credentials, set endpoints).
            try:
                deco.external_init()
            except Exception as exc:
                # Decorator failed to initialize (e.g. missing credentials).
                # Skip it — using a partially-initialized decorator would produce
                # malformed CLI arguments and silent failures.
                deco_name = snap.get("name") or class_name
                print(
                    "Warning: skipping decorator '%s' because external_init() "
                    "raised: %s" % (deco_name, exc),
                    file=sys.stderr,
                )
                continue
            decorators.append(deco)
        except Exception:
            continue
    if decorators:
        return decorators

    # Fallback path: parse raw --with specs.
    for spec in (inp.decorator_specs or []):
        try:
            deco, _ = extract_step_decorator_from_decospec(spec)
            if isinstance(deco, StepDecorator):
                deco.external_init()
                decorators.append(deco)
        except Exception:
            # Unknown or unavailable decorator modules are already represented
            # as --with flags; skip hook invocation and let CLI handle it.
            continue
    return decorators


def _build_step_cmd(inp: StepInput, input_paths: str) -> tuple:
    """Return (argv, extra_env) for a step execution."""
    cli_args = _RuntimeCLIArgs(inp, input_paths)
    for deco in _runtime_step_decorators(inp):
        try:
            deco.runtime_step_cli(
                cli_args,
                inp.retry_count,
                inp.max_retries,
                None,  # ubf_context
            )
        except Exception:
            # Some decorators need additional runtime lifecycle hooks (e.g.
            # runtime_init/runtime_task_created) before runtime_step_cli.
            # Fall back to standard local step execution when unavailable.
            continue
    return cli_args.get_args(), dict(cli_args.env)


def _build_init_cmd(inp: StepInput, params_task_id: str, params: dict) -> list:
    top_level = _top_level_args(inp)
    init_args = [
        "init",
        "--run-id", inp.run_id,
        "--task-id", params_task_id,
    ]
    # Forward run tags (--tag is a subcommand option for init, not top-level)
    for tag in (inp.tags or []):
        init_args += ["--tag", tag]
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


# Lock protecting temporary os.environ mutations in _read_artifact_names,
# _read_transition, and run_compensation.
# Activities run in a ThreadPoolExecutor so concurrent calls are possible.
_artifact_lock = threading.Lock()


def _build_task_pathspec(flow_name: str, run_id: str, step_name: str, task_id: str) -> str:
    """Return the Metaflow task pathspec ``flow/run/step/task``."""
    return "%s/%s/%s/%s" % (flow_name, run_id, step_name, task_id)


@contextlib.contextmanager
def _metaflow_datastore_env(datastore_root: str):
    """Temporarily override METAFLOW_DATASTORE_SYSROOT_LOCAL under _artifact_lock.

    Acquires ``_artifact_lock``, sets the env var, yields, then restores the
    original value — even if an exception is raised inside the ``with`` block.
    """
    key = "METAFLOW_DATASTORE_SYSROOT_LOCAL"
    with _artifact_lock:
        old = os.environ.get(key)
        os.environ[key] = datastore_root
        try:
            yield
        finally:
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old


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
        pathspec = _build_task_pathspec(inp.flow_name, inp.run_id, inp.step_name, inp.task_id)
        with _metaflow_datastore_env(datastore_root):
            metaflow.metadata(inp.metadata_type)
            task = metaflow.Task(pathspec)
            # Access only .id — does NOT deserialize the artifact data
            return [a.id for a in task.artifacts if not a.id.startswith("_")]
    except Exception:
        return []


def _read_transition(inp: StepInput) -> Optional[str]:
    """Read the ``_transition`` artifact written by a split-switch step.

    Metaflow records ``self._transition = ([chosen_step_name], None)`` during
    step execution so the runtime knows which single branch was taken.  We
    retrieve it here using the Metaflow client so the workflow can skip all
    other branches.

    Returns the chosen step name, or ``None`` if the artifact cannot be read
    (e.g. the step is not a split-switch, or reading fails for any reason).
    """
    try:
        import metaflow

        datastore_root = _datastore_root_arg(inp.env_overrides)
        pathspec = _build_task_pathspec(inp.flow_name, inp.run_id, inp.step_name, inp.task_id)
        with _metaflow_datastore_env(datastore_root):
            metaflow.metadata(inp.metadata_type)
            task = metaflow.Task(pathspec)
            transition = task["_transition"].data
            # transition is a tuple: ([step_name], foreach_info_or_None)
            if transition and isinstance(transition, (tuple, list)) and transition[0]:
                return transition[0][0]
    except Exception:
        pass
    return None


async def _run_subprocess(cmd: list, env: dict) -> tuple:
    """Run a subprocess, heartbeating every 20 s so the activity lease stays alive.

    Returns (returncode, stdout_str, stderr_str).
    """
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def _hb():
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL_SECONDS)
            try:
                activity.heartbeat()
            except TemporalCancelledError:
                # Activity was cancelled — kill the subprocess so it doesn't
                # keep running after Temporal has given up on this activity.
                proc.kill()
                raise
            except Exception:
                # Transient heartbeat error (e.g. network blip) — stop
                # heartbeating but let the subprocess finish normally.
                break

    hb_task = asyncio.ensure_future(_hb())
    try:
        stdout_bytes, stderr_bytes = await proc.communicate()
    except BaseException:
        # asyncio.CancelledError or any unexpected error while waiting for the
        # subprocess — kill it to avoid leaving zombie processes.
        proc.kill()
        await proc.wait()
        raise
    finally:
        hb_task.cancel()
        try:
            await hb_task
        except asyncio.CancelledError:
            pass
        # TemporalCancelledError (or other real exceptions) from _hb propagate
        # up naturally — they are NOT caught here.

    return (
        proc.returncode,
        stdout_bytes.decode(errors="replace"),
        stderr_bytes.decode(errors="replace"),
    )


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

        if inp.step_name == "start" and inp.params_json:
            params = json.loads(inp.params_json)
            params_task_id = "%s-params" % inp.task_id
            for k, v in params.items():
                env["METAFLOW_INIT_%s" % k.upper()] = str(v)

            # Check if _parameters task already exists (idempotent on retry)
            check_rc, _, _ = await _run_subprocess(_build_dump_cmd(inp, params_task_id), env)
            if check_rc != 0:
                init_rc, init_stdout, init_stderr = await _run_subprocess(
                    _build_init_cmd(inp, params_task_id, params), env
                )
                if init_rc != 0:
                    raise ApplicationError(
                        "Parameters init failed: stdout=%s stderr=%s"
                        % (init_stdout[-1000:], init_stderr[-1000:])
                    )

            input_paths = "%s/_parameters/%s" % (inp.run_id, params_task_id)

        step_cmd, step_env = _build_step_cmd(inp, input_paths)
        rc, stdout, stderr = await _run_subprocess(step_cmd, {**env, **step_env})

        if rc != 0:
            raise ApplicationError(
                "Step %s failed (exit %d):\nCMD: %s\nSTDOUT: %s\nSTDERR: %s"
                % (
                    inp.step_name,
                    rc,
                    " ".join(str(x) for x in step_cmd),
                    stdout[-2000:],
                    stderr[-2000:],
                )
            )

        out = {}
        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            with open(output_file) as f:
                out = json.load(f)

        artifact_names = _read_artifact_names(inp)

        # For split-switch steps, read _transition to discover which branch was
        # chosen at runtime.  This is done here (in the activity) rather than in
        # workflow code so that we stay within Temporal's deterministic sandbox.
        chosen_branch = _read_transition(inp)

        return StepOutput(
            task_id=inp.task_id,
            foreach_cardinality=out.get("foreach_cardinality", 0),
            artifact_names=artifact_names,
            artifact_fetch_hint=(
                "import metaflow; t = metaflow.Task('%s'); t.data.<name>"
                % _build_task_pathspec(inp.flow_name, inp.run_id, inp.step_name, inp.task_id)
            ) if artifact_names else None,
            chosen_branch=chosen_branch,
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

    with _metaflow_datastore_env(root):
        import metaflow
        metaflow.metadata(inp.metadata_type)
        pathspec = _build_task_pathspec(inp.flow_name, inp.run_id, inp.forward_step, inp.task_id)
        task = metaflow.Task(pathspec)
        for a in task.artifacts:
            if not a.id.startswith("_"):
                try:
                    artifacts[a.id] = getattr(task.data, a.id)
                except Exception:
                    pass

    # Load the flow module, create a bare instance, inject artifacts, call handler.
    # We inject directly into __dict__ to bypass FlowSpec.__getattr__/__setattr__
    # which may recurse if internal state (e.g. _datastore) is not initialised.
    spec = importlib.util.spec_from_file_location("_comp_flow", inp.flow_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    flow_class = getattr(module, inp.flow_class_name)
    instance = object.__new__(flow_class)
    # Seed internal FlowSpec state to prevent __getattr__ recursion.
    # FlowSpec.__getattr__ checks for _datastore; other private attributes
    # may be accessed by helper methods the handler transitively calls.
    for attr, default in [
        ("_datastore", None),
        ("_graph", None),
        ("_flow_decorators", {}),
        ("_success", False),
        ("_task_ok", False),
        ("_ubf_context", None),
        ("_environment", None),
    ]:
        instance.__dict__.setdefault(attr, default)
    for k, v in artifacts.items():
        instance.__dict__[k] = v
    handler = getattr(instance, inp.handler_name)
    if asyncio.iscoroutinefunction(handler):
        await handler()
    else:
        await asyncio.get_running_loop().run_in_executor(None, handler)


# ---------------------------------------------------------------------------
# Workflow helpers
# ---------------------------------------------------------------------------


def _make_step_input(
    cfg: dict,
    node: dict,
    step_name: str,
    run_id: str,
    task_id: str,
    input_paths: str,
    retry_count: int,
    split_index: int,
    params: dict,
) -> StepInput:
    """Build a StepInput from compiled config, node config, and runtime values."""
    code_pkg = cfg.get("code_package") or {}
    env_overrides = dict(node.get("env", {}))
    env_overrides["METAFLOW_RUN_ID"] = run_id
    return StepInput(
        flow_name=cfg["flow_name"],
        flow_file=cfg["flow_file"],
        step_name=step_name,
        run_id=run_id,
        task_id=task_id,
        input_paths=input_paths,
        retry_count=retry_count,
        max_retries=node.get("retries", 0),
        split_index=split_index,
        env_overrides=env_overrides,
        params_json=json.dumps(params) if step_name == "start" else "",
        metadata_type=cfg.get("metadata_type", "local"),
        datastore_type=cfg.get("datastore_type", "local"),
        environment_type=cfg.get("environment_type", "local"),
        event_logger_type=cfg.get("event_logger_type", "nullSidecarLogger"),
        monitor_type=cfg.get("monitor_type", "nullSidecarMonitor"),
        code_package_url=code_pkg.get("url", ""),
        code_package_sha=code_pkg.get("sha", ""),
        code_package_metadata=code_pkg.get("metadata", ""),
        decorator_specs=node.get("decorator_specs", []),
        runtime_cli_decorators=node.get("runtime_cli_decorators", []),
        tags=cfg.get("tags", []),
        namespace=cfg.get("namespace", ""),
    )


def _make_retry_policy(node: dict) -> RetryPolicy:
    """Build a Temporal RetryPolicy from a compiled step node config."""
    return RetryPolicy(
        maximum_attempts=node.get("retries", 0) + 1,
        initial_interval=timedelta(seconds=node.get("retry_delay_seconds", _DEFAULT_RETRY_DELAY_SECONDS)),
    )


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
        cfg = args.get("config")
        if cfg is None or args.get("_use_embedded_config"):
            cfg = CONFIG  # noqa: F821 — defined in generated worker at runtime
        params = args.get("params", {})
        resume_state = args.get("resume_state")  # {step_name: {"task_id": str}, ...}
        run_id_override = args.get("run_id_override")
        return await self._execute_graph(cfg, params, resume_state, run_id_override)

    async def _execute_graph(
        self,
        cfg: dict,
        params: dict,
        resume_state: Optional[dict] = None,
        run_id_override: Optional[str] = None,
    ) -> str:
        run_id = run_id_override or "temporal-%s" % workflow.info().workflow_id
        # Seed task_ids from resume_state so that already-completed steps are
        # skipped and their task_ids are available for input_paths resolution.
        task_ids: dict = {}
        if resume_state:
            for step_name, state in resume_state.items():
                task_ids[step_name] = state.get("task_ids") or state.get("task_id", "")
        try:
            await self._execute_node("start", cfg, run_id, task_ids, params, -1, resume_state)
            return run_id
        except Exception:
            if cfg.get("compensations") and self._compensation_stack:
                await self._run_compensations(cfg, run_id)
            raise

    def _push_compensation(self, cfg: dict, step_name: str, task_id: str) -> None:
        """Push step onto the saga compensation stack if it has a registered handler."""
        handler = cfg.get("compensations", {}).get(step_name)
        if handler:
            self._compensation_stack.append({
                "handler": handler,
                "step": step_name,
                "task_id": task_id,
            })

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
                    start_to_close_timeout=timedelta(seconds=_COMPENSATION_TIMEOUT_SECONDS),
                    retry_policy=RetryPolicy(
                        maximum_attempts=_COMPENSATION_RETRY_ATTEMPTS,
                        initial_interval=timedelta(seconds=_COMPENSATION_RETRY_DELAY_SECONDS),
                    ),
                )
            except Exception as exc:
                print(
                    "Warning: compensation for step %r failed: %s" % (entry["step"], exc),
                    file=sys.stderr,
                )

    async def _execute_node(
        self,
        step_name: str,
        cfg: dict,
        run_id: str,
        task_ids: dict,
        params: dict,
        split_index: int,
        resume_state: Optional[dict] = None,
    ) -> None:
        steps = cfg["steps"]
        node = steps[step_name]
        node_type = node["type"]

        # Resume: skip steps that already completed in a prior run.
        # task_ids are pre-seeded from resume_state in _execute_graph.
        if resume_state and step_name in resume_state:
            if step_name == "end":
                return
            if node_type == "foreach":
                body_step = node["out_funcs"][0]
                body_node = steps[body_step]
                join_step = body_node["out_funcs"][0]
                await self._execute_node(join_step, cfg, run_id, task_ids, params, -1, resume_state)
                return
            for next_step in node["out_funcs"]:
                await self._execute_node(next_step, cfg, run_id, task_ids, params, -1, resume_state)
            return

        input_paths = _resolve_input_paths(step_name, node, run_id, task_ids, steps=steps)

        retry_count = max(0, workflow.info().attempt - 1)
        if split_index >= 0:
            task_id = "temporal-%s-%d-%d" % (step_name, split_index, retry_count)
        else:
            task_id = "temporal-%s-%d" % (step_name, retry_count)

        inp = _make_step_input(
            cfg, node, step_name, run_id, task_id, input_paths,
            retry_count, split_index, params,
        )
        retry_policy = _make_retry_policy(node)
        timeout_seconds = node.get("timeout_seconds", _DEFAULT_STEP_TIMEOUT_SECONDS)

        out: StepOutput = await workflow.execute_activity(
            run_metaflow_step,
            inp,
            start_to_close_timeout=timedelta(seconds=timeout_seconds),
            heartbeat_timeout=timedelta(seconds=_HEARTBEAT_TIMEOUT_SECONDS),
            retry_policy=retry_policy,
        )

        task_ids[step_name] = out.task_id
        self._push_compensation(cfg, step_name, out.task_id)

        if step_name == "end":
            return

        if node_type == "foreach":
            out_funcs = node["out_funcs"]
            if len(out_funcs) != 1:
                raise ApplicationError("foreach node must have exactly one out_func")
            body_step = out_funcs[0]
            cardinality = out.foreach_cardinality

            # Run all body steps in parallel; each slice gets its own task_ids
            # snapshot so nested foreach can accumulate task_ids independently.
            body_task_ids_list = await asyncio.gather(
                *[
                    self._execute_foreach_slice(
                        body_step, cfg, run_id, i, retry_count, dict(task_ids)
                    )
                    for i in range(cardinality)
                ]
            )

            # Flatten each slice's body_step task_id into a single list so the
            # join step receives all parallel input paths.  body_task_ids_list[i]
            # is the accumulated task_ids dict for slice i.
            flat_split_task_ids = []
            for slice_ids in body_task_ids_list:
                tid = slice_ids.get(body_step)
                if isinstance(tid, list):
                    flat_split_task_ids.extend(tid)
                else:
                    flat_split_task_ids.append(tid)
            task_ids[body_step] = flat_split_task_ids

            # Continue to join
            body_node = steps[body_step]
            join_step = body_node["out_funcs"][0]
            await self._execute_node(join_step, cfg, run_id, task_ids, params, -1, resume_state)

        elif node_type == "split-switch":
            # Only one branch runs at runtime.  The run_metaflow_step activity
            # already read _transition from the datastore and put the chosen
            # branch name in out.chosen_branch.
            chosen_branch = out.chosen_branch
            if chosen_branch is None:
                # Fallback: if _transition could not be read, run the first branch.
                chosen_branch = node["out_funcs"][0] if node["out_funcs"] else None

            merge_step = _find_switch_merge_step(step_name, steps)

            if chosen_branch:
                branch_task_ids = await self._execute_branch_until_switch_merge(
                    chosen_branch, cfg, run_id, retry_count,
                    dict(task_ids), merge_step,
                )
                task_ids.update(branch_task_ids)

            if merge_step:
                await self._execute_node(merge_step, cfg, run_id, task_ids, params, -1, resume_state)

        elif node_type == "split":
            shared_ids = dict(task_ids)
            branch_results = await asyncio.gather(
                *[
                    self._execute_branch_until_join(
                        branch, cfg, run_id, retry_count, dict(shared_ids)
                    )
                    for branch in node["out_funcs"]
                ]
            )
            for branch_task_ids in branch_results:
                task_ids.update(branch_task_ids)

            join_step = _find_join_step(step_name, steps)
            if join_step:
                await self._execute_node(join_step, cfg, run_id, task_ids, params, -1, resume_state)

        else:
            for next_step in node["out_funcs"]:
                await self._execute_node(next_step, cfg, run_id, task_ids, params, -1, resume_state)

    async def _execute_foreach_slice(
        self,
        step_name: str,
        cfg: dict,
        run_id: str,
        split_index: int,
        retry_count: int,
        task_ids: dict,
    ) -> dict:
        """Execute one slice of a foreach body, handling arbitrary nesting depth.

        Runs ``step_name`` as a Temporal activity with the given ``split_index``,
        then — if that step is itself a foreach — recursively fans out its body
        steps in parallel.  Returns the accumulated ``task_ids`` dict for this
        slice so the caller can build the join's input_paths.
        """
        steps = cfg["steps"]
        node = steps[step_name]
        node_type = node["type"]

        # Resolve input_paths for this slice.  The parent foreach step's
        # task_id is already in task_ids (single string, not a list).
        parent_step = node["in_funcs"][0]
        parent_task_id = task_ids.get(parent_step, "temporal-%s-%d" % (parent_step, retry_count))
        if isinstance(parent_task_id, list):
            # Should not happen at the first body level, but handle gracefully
            input_paths = ",".join(
                "%s/%s/%s" % (run_id, parent_step, tid) for tid in parent_task_id
            )
        else:
            input_paths = "%s/%s/%s" % (run_id, parent_step, parent_task_id)

        task_id = "temporal-%s-%d-%d" % (step_name, split_index, retry_count)
        inp = _make_step_input(cfg, node, step_name, run_id, task_id, input_paths, retry_count, split_index, {})
        retry_policy = _make_retry_policy(node)
        timeout_seconds = node.get("timeout_seconds", _DEFAULT_STEP_TIMEOUT_SECONDS)

        out: StepOutput = await workflow.execute_activity(
            run_metaflow_step,
            inp,
            start_to_close_timeout=timedelta(seconds=timeout_seconds),
            heartbeat_timeout=timedelta(seconds=_HEARTBEAT_TIMEOUT_SECONDS),
            retry_policy=retry_policy,
        )

        task_ids[step_name] = out.task_id
        self._push_compensation(cfg, step_name, out.task_id)

        if node_type == "foreach":
            # Nested foreach: fan out the inner body steps in parallel
            inner_out_funcs = node["out_funcs"]
            if len(inner_out_funcs) != 1:
                raise ApplicationError("foreach node must have exactly one out_func")
            inner_body_step = inner_out_funcs[0]
            inner_cardinality = out.foreach_cardinality

            inner_task_ids_list = await asyncio.gather(
                *[
                    self._execute_foreach_slice(
                        inner_body_step, cfg, run_id,
                        inner_split_index, retry_count, dict(task_ids),
                    )
                    for inner_split_index in range(inner_cardinality)
                ]
            )

            # Aggregate inner body task_ids for the inner join
            flat_inner_task_ids = []
            for inner_slice_ids in inner_task_ids_list:
                tid = inner_slice_ids.get(inner_body_step)
                if isinstance(tid, list):
                    flat_inner_task_ids.extend(tid)
                else:
                    flat_inner_task_ids.append(tid)
            task_ids[inner_body_step] = flat_inner_task_ids

            # Execute the inner join step
            inner_body_node = steps[inner_body_step]
            inner_join_step = inner_body_node["out_funcs"][0]
            await self._execute_node(inner_join_step, cfg, run_id, task_ids, {}, -1)

        return task_ids

    async def _dispatch_activity(
        self,
        step_name: str,
        cfg: dict,
        run_id: str,
        retry_count: int,
        task_ids: dict,
    ) -> StepOutput:
        """Run a single branch step as a Temporal activity and record its task_id.

        Shared by both branch traversal methods to avoid duplicating the
        activity dispatch boilerplate.
        """
        steps = cfg["steps"]
        node = steps[step_name]
        input_paths = _resolve_input_paths(step_name, node, run_id, task_ids, steps=steps)
        task_id = "temporal-%s-%d" % (step_name, retry_count)
        inp = _make_step_input(cfg, node, step_name, run_id, task_id, input_paths, retry_count, -1, {})
        timeout_seconds = node.get("timeout_seconds", _DEFAULT_STEP_TIMEOUT_SECONDS)

        out: StepOutput = await workflow.execute_activity(
            run_metaflow_step,
            inp,
            start_to_close_timeout=timedelta(seconds=timeout_seconds),
            heartbeat_timeout=timedelta(seconds=_HEARTBEAT_TIMEOUT_SECONDS),
            retry_policy=_make_retry_policy(node),
        )
        task_ids[step_name] = out.task_id
        self._push_compensation(cfg, step_name, out.task_id)
        return out

    async def _execute_branch_until_join(
        self,
        step_name: str,
        cfg: dict,
        run_id: str,
        retry_count: int,
        task_ids: Optional[dict] = None,
    ) -> dict:
        """Execute a branch from step_name until it reaches a join node.

        The join node itself is NOT executed — the caller handles that after
        all parallel branches have finished.  Returns the accumulated
        task_ids dict (caller's snapshot plus steps executed here).
        """
        steps = cfg["steps"]
        if task_ids is None:
            task_ids = {}
        current = step_name

        while current is not None:
            node = steps[current]
            if node["type"] == "join":
                break  # let the caller handle the join

            await self._dispatch_activity(current, cfg, run_id, retry_count, task_ids)

            out_funcs = node["out_funcs"]
            current = out_funcs[0] if out_funcs else None

        return task_ids

    async def _execute_branch_until_switch_merge(
        self,
        step_name: str,
        cfg: dict,
        run_id: str,
        retry_count: int,
        task_ids: dict,
        merge_step: Optional[str],
    ) -> dict:
        """Execute one branch of a split-switch until it reaches the merge step.

        Unlike ``_execute_branch_until_join``, the stop condition is the named
        ``merge_step`` (a regular linear step, not a ``join`` type).  The merge
        step itself is NOT executed here — the caller handles it after the
        single chosen branch finishes.

        Returns the accumulated ``task_ids`` dict for this branch.
        """
        steps = cfg["steps"]
        current = step_name

        while current is not None:
            if current == merge_step:
                break  # let the caller execute the merge step

            node = steps[current]
            # Defensive: stop at any join node (shouldn't occur in a split-switch branch)
            if node["type"] == "join":
                break

            await self._dispatch_activity(current, cfg, run_id, retry_count, task_ids)

            out_funcs = node["out_funcs"]
            current = out_funcs[0] if out_funcs and out_funcs[0] != merge_step else None

        return task_ids


# ---------------------------------------------------------------------------
# Pure helper functions (usable inside @workflow.defn via sandbox)
# ---------------------------------------------------------------------------


def _resolve_input_paths(
    step_name: str, node: dict, run_id: str, task_ids: dict, steps: Optional[dict] = None
) -> str:
    """Build comma-separated Metaflow input path(s) for a step.

    Parameters
    ----------
    step_name:
        The step whose input paths are being constructed.
    node:
        The compiled step config dict for ``step_name``.
    run_id:
        The Temporal run ID (also used as the Metaflow run ID).
    task_ids:
        Mapping of step_name -> task_id (or list of task_ids for foreach).
        Only steps that actually executed will be present.
    steps:
        The full compiled steps dict from CONFIG.  When provided, used to
        detect ``split-switch`` parents so that un-executed branches are
        excluded from the join's input paths.
    """
    in_funcs = node["in_funcs"]

    if step_name == "start":
        return "%s/_parameters/temporal-start-0-params" % run_id

    if not in_funcs:
        return ""

    # Foreach join: the body step's task_ids is a list of per-slice task_ids.
    # Detect this by checking whether the first parent's recorded task_ids is a list
    # (set by _execute_node after gathering all slices).
    if node["type"] == "join" and node.get("split_parents"):
        body_step = in_funcs[0]
        split_ids = task_ids.get(body_step)
        if isinstance(split_ids, list):
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

    # Multiple parents (join after split, or merge after split-switch).
    # For split-switch merges, only ONE branch ran so we must skip parents
    # that are absent from task_ids (the unchosen branches).
    # For regular splits all branches always ran so we keep the fallback.
    is_switch_merge = (
        steps is not None
        and len(in_funcs) > 1
        and _is_switch_merge_step(in_funcs, steps)
    )

    paths = []
    for parent in in_funcs:
        if is_switch_merge and parent not in task_ids:
            # This branch was not chosen by the split-switch — skip it.
            continue
        parent_task_id = task_ids.get(parent, "temporal-%s-0" % parent)
        if isinstance(parent_task_id, list):
            paths.extend(
                "%s/%s/%s" % (run_id, parent, tid) for tid in parent_task_id
            )
        else:
            paths.append("%s/%s/%s" % (run_id, parent, parent_task_id))
    return ",".join(paths)


def _is_switch_merge_step(in_funcs: list, steps: dict) -> bool:
    """Return True when every parent of a step traces back to a split-switch node.

    Used to detect merge steps after conditional branches so that un-executed
    branch steps are excluded from the join's input paths.
    """
    switch_parents = {
        src
        for src in in_funcs
        if any(
            steps.get(grandparent, {}).get("type") == "split-switch"
            for grandparent in (steps.get(src, {}).get("in_funcs") or [])
        )
    }
    return len(switch_parents) == len(in_funcs)


def _find_join_step(split_step_name: str, steps: dict) -> str | None:
    """Find the join step (type="join") that corresponds to a regular split step."""
    for name, node in steps.items():
        if node["type"] == "join":
            parents = node.get("split_parents", [])
            if parents and parents[-1] == split_step_name:
                return name
    return None


def _find_switch_merge_step(switch_step_name: str, steps: dict) -> str | None:
    """Find the merge step that follows a split-switch node.

    For a split-switch, branches converge at a regular linear step (not a join
    type).  This is the first step whose ``in_funcs`` contains at least one of
    the switch branches as a direct parent.
    """
    switch_node = steps.get(switch_step_name)
    if not switch_node:
        return None
    branch_names = set(switch_node.get("out_funcs", []))
    for name, node in steps.items():
        if name in branch_names:
            continue
        in_funcs = node.get("in_funcs", [])
        if branch_names & set(in_funcs):
            return name
    return None


# ---------------------------------------------------------------------------
# Event gateway workflow — handles @trigger(event="foo") signals
# ---------------------------------------------------------------------------


@workflow.defn(name="MetaflowEventGateway")
class MetaflowEventGateway:
    """Long-running workflow that accepts named signals and starts MetaflowWorkflow runs.

    One gateway workflow per flow is kept alive while the worker is running.
    Sending a Temporal signal whose name matches a registered ``@trigger(event=...)``
    event causes a new ``MetaflowWorkflow`` run to start.

    Signal payload format (JSON-serialisable dict, optional):
        {"<event_field>": <value>, ...}

    Parameter mapping (from @trigger(event={"name": "foo", "parameters": {...}})):
        {"<flow_param>": "<event_field>"}  — copied from the trigger config in CONFIG.
    """

    def __init__(self):
        self._pending: list = []   # [(event_name, payload_dict), ...]
        self._stop: bool = False

    @workflow.signal
    def receive_event(self, payload: dict) -> None:
        """General signal handler; payload must include ``_event_name`` key."""
        event_name = payload.get("_event_name", "")
        self._pending.append((event_name, payload))

    @workflow.signal
    def stop(self) -> None:
        """Gracefully stop the gateway workflow."""
        self._stop = True

    @workflow.run
    async def run(self, args: dict) -> None:
        """Process incoming event signals until stopped."""
        cfg = args.get("config")
        if cfg is None or args.get("_use_embedded_config"):
            cfg = CONFIG  # noqa: F821 — defined in generated worker at runtime
        named_triggers = cfg.get("named_triggers", [])
        # Build lookup: event_name -> parameter_map dict
        trigger_map = {}
        for t in named_triggers:
            evt = t.get("event")
            if evt:
                trigger_map[evt] = t.get("parameters") or {}

        while not self._stop:
            # Block until a signal arrives or a stop is requested.
            await workflow.wait_condition(lambda: bool(self._pending) or self._stop)
            # Drain the pending queue
            while self._pending:
                event_name, payload = self._pending.pop(0)
                param_map = trigger_map.get(event_name, {})
                # Apply parameter mapping: flow_param = payload[event_field]
                params = {}
                for flow_param, event_field in param_map.items():
                    if event_field in payload:
                        params[flow_param] = payload[event_field]
                # Start a child workflow so Temporal tracks the causal relationship.
                workflow.logger.info(
                    "Event gateway: received event '%s', starting workflow with params %r"
                    % (event_name, params)
                )
                await workflow.execute_child_workflow(
                    MetaflowWorkflow.run,
                    {
                        "config": None,
                        "params": params,
                        "_use_embedded_config": True,
                    },
                    id="%s-event-%s-%s"
                    % (
                        cfg.get("flow_name", "flow").lower(),
                        event_name,
                        workflow.now().strftime("%Y%m%d%H%M%S%f"),
                    ),
                    task_queue=cfg.get("task_queue", ""),
                )


# ---------------------------------------------------------------------------
# WorkerUtils (entry points for generated worker files)
# ---------------------------------------------------------------------------


class WorkerUtils:
    @staticmethod
    async def run_worker(config: dict):
        """Start the Temporal worker for this flow."""
        client = await Client.connect(
            config["temporal_host"],
            namespace=config.get("temporal_namespace", "default"),
        )

        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, shutdown_event.set)
            except (NotImplementedError, RuntimeError):
                # signal handlers are not available on all platforms (e.g. Windows)
                pass

        named_triggers = config.get("named_triggers", [])
        workflow_types = [MetaflowWorkflow]
        if named_triggers:
            workflow_types.append(MetaflowEventGateway)

        async with Worker(
            client,
            task_queue=config["task_queue"],
            workflows=workflow_types,
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

            # Start trigger_on_finish polling if upstream flows are configured
            triggers = config.get("trigger_on_finish", [])
            trigger_task = None
            if triggers:
                trigger_task = asyncio.ensure_future(
                    WorkerUtils._poll_trigger_on_finish(client, config, triggers)
                )

            # Start the event gateway workflow if @trigger(event=...) is configured.
            # The gateway is a long-lived workflow that accepts named signals and
            # starts a new MetaflowWorkflow run for each received event.
            if named_triggers:
                await WorkerUtils._ensure_event_gateway(client, config, named_triggers)

            await shutdown_event.wait()
            print("Worker shutting down gracefully.")

            if trigger_task is not None:
                trigger_task.cancel()
                try:
                    await trigger_task
                except asyncio.CancelledError:
                    pass

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
    async def _poll_trigger_on_finish(client: Client, config: dict, triggers: list):
        """Poll for completed runs of upstream flows and auto-trigger this flow.

        Checks each upstream flow listed in ``triggers`` every 30 seconds.
        When a new successful run is detected (one we haven't seen before),
        triggers a new run of this flow.

        Upstream flows are identified by the Temporal workflow ID prefix
        ``<flow_name_lower>-``.  Only workflows that completed successfully
        since the last check are used as triggers.
        """
        seen_run_ids: set = set()

        while True:
            await asyncio.sleep(30)
            for trigger in triggers:
                upstream_name = trigger.get("flow", "")
                if not upstream_name:
                    continue
                prefix = "%s-" % upstream_name.lower()
                try:
                    async for wf in await client.list_workflows(
                        query='WorkflowType="MetaflowWorkflow" AND ExecutionStatus="Completed"'
                    ):
                        wf_id = wf.id
                        if not wf_id.startswith(prefix):
                            continue
                        if wf_id in seen_run_ids:
                            continue
                        seen_run_ids.add(wf_id)
                        new_id = "%s-%s" % (config["flow_name"].lower(), uuid.uuid4().hex[:8])
                        try:
                            result = await client.execute_workflow(
                                MetaflowWorkflow.run,
                                {"config": config, "params": {}},
                                id=new_id,
                                task_queue=config["task_queue"],
                            )
                            print(
                                "trigger_on_finish: triggered by %s -> run ID: %s"
                                % (wf_id, result)
                            )
                        except Exception as trigger_err:
                            print(
                                "trigger_on_finish: failed to trigger for %s: %s"
                                % (wf_id, trigger_err)
                            )
                except asyncio.CancelledError:
                    raise
                except Exception as poll_err:
                    print(
                        "trigger_on_finish: poll error for %s: %s" % (upstream_name, poll_err)
                    )

    @staticmethod
    async def _ensure_event_gateway(client: Client, config: dict, named_triggers: list):
        """Start (or reuse) the long-lived MetaflowEventGateway workflow.

        The gateway is keyed by flow name so there is exactly one per compiled flow.
        If the gateway is already running (e.g. from a previous worker restart) this
        is a no-op (USE_EXISTING conflict policy).  Prints the event names that are
        now wired to the flow.
        """
        from temporalio.common import WorkflowIDConflictPolicy

        flow_name = config["flow_name"]
        gateway_id = "metaflow-%s-event-gateway" % flow_name.lower().replace(".", "-")
        event_names = [t["event"] for t in named_triggers if t.get("event")]
        try:
            await client.start_workflow(
                "MetaflowEventGateway",
                {
                    "config": config,
                    "_use_embedded_config": False,
                },
                id=gateway_id,
                task_queue=config["task_queue"],
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            )
            print(
                "Event gateway ready (workflow ID: %s). Listening for events: %s"
                % (gateway_id, ", ".join(event_names))
            )
        except Exception as exc:
            print(
                "Warning: could not start event gateway for events %s: %s"
                % (event_names, exc)
            )

    @staticmethod
    async def trigger(config: dict, params: dict) -> str:
        """Trigger a workflow run and wait for completion."""
        client = await Client.connect(
            config["temporal_host"],
            namespace=config.get("temporal_namespace", "default"),
        )
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
