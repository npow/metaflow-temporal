import inspect
import json
import os
import warnings
from datetime import datetime

from metaflow.decorators import StepDecorator

try:
    from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
except ImportError:
    get_run_time_limit_for_task = None

from . import worker_utils
from .exception import TemporalException

WORKER_TEMPLATE_FILE = os.path.join(os.path.dirname(__file__), "worker_template.mustache")

# Decorators that must NOT be forwarded as --with specs to step subprocesses.
# These decorators either have no meaning inside a step subprocess (schedule,
# project, trigger) or are already handled via runtime_step_cli hooks (sandbox,
# daytona, e2b, boxlite) — forwarding them would cause double step_init.
# NOTE: @resources is intentionally excluded here; resource hints are emitted
# as a structured "resources" entry in the per-step config and forwarded to
# compute backends (e.g. @kubernetes) via the decorator_specs list.
_EXCLUDED_DECORATOR_SPECS = frozenset({
    "temporal_internal",
    "retry", "timeout", "environment",
    "project", "trigger", "trigger_on_finish",
    "schedule", "card", "catch",
    "resources",
    "sandbox", "daytona", "e2b", "boxlite",
})

# Default values for compiled step config (also used in worker_utils constants).
_DEFAULT_STEP_TIMEOUT_SECONDS = 3600   # 1 hour
_DEFAULT_RETRY_DELAY_SECONDS = 120     # 2 minutes


class Temporal:
    def __init__(
        self,
        name,
        graph,
        flow,
        flow_file,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        tags=None,
        namespace=None,
        username=None,
        max_workers=10,
        task_queue=None,
        temporal_host="localhost:7233",
        temporal_namespace="default",
        branch=None,
        production=False,
        workflow_timeout_seconds=None,
    ):
        self.name = name
        self.graph = graph
        self.flow = flow
        self.flow_file = flow_file
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags or []
        self.namespace = namespace
        self.username = username
        self.max_workers = max_workers
        self.temporal_host = temporal_host
        self.temporal_namespace = temporal_namespace
        self.branch = branch
        self.production = production
        self.workflow_timeout_seconds = workflow_timeout_seconds

        # Compute @project info and derive the effective flow name.
        # Must happen after all self.* assignments above.
        self._project_info = self._get_project()
        # Sanitize dots to hyphens for the task queue name.
        self.task_queue = task_queue or (
            "metaflow-{}".format(self._effective_flow_name.lower().replace(".", "-"))
        )
        self._code_package_info = None

    @property
    def _effective_flow_name(self) -> str:
        """The flow name to use for datastore paths and the task queue.

        Returns the project-aware name (e.g. ``myproject.prod.TrainFlow``) when
        the flow carries ``@project``, otherwise the plain class name.
        """
        return self._project_info["flow_name"] if self._project_info else self.name

    def compile(self) -> str:
        self._validate()
        config = self._build_config()
        return self._render_template(config)

    def _validate(self) -> None:
        """Validate the graph before compilation, raising errors for unsupported patterns."""
        # @resources: resource hints are extracted and forwarded as decorator specs
        # to the step subprocess so that compute backends (e.g. @kubernetes) can
        # enforce them.  No warning is emitted — this is fully supported.

    def _build_config(self) -> dict:
        datastore_root = getattr(self.flow_datastore, "datastore_root", None) or ""
        # Use project-aware flow name if @project is present
        flow_name = self._effective_flow_name
        # Merge user tags with auto-added project tags
        tags = list(self.tags)
        if self._project_info:
            tags = tags + [
                "project:{}".format(self._project_info["name"]),
                "project_branch:{}".format(self._project_info["branch"]),
            ]
        return {
            "flow_name": flow_name,
            "flow_file": self.flow_file,
            "task_queue": self.task_queue,
            "temporal_host": self.temporal_host,
            "temporal_namespace": self.temporal_namespace,
            "max_workers": self.max_workers,
            # Runtime provider types — forwarded as CLI flags to each step subprocess.
            "metadata_type": self.metadata.TYPE,
            "datastore_type": self.flow_datastore.TYPE,
            "datastore_root": datastore_root,
            "environment_type": self.environment.TYPE,
            "event_logger_type": self.event_logger.TYPE,
            "monitor_type": self.monitor.TYPE,
            "parameters": self._process_parameters(),
            "steps": self._build_steps(),
            # Tags forwarded as --tag flags to each step subprocess
            "tags": tags,
            # @schedule decorator config — used to register a Temporal Schedule
            "schedule": self._get_schedule(),
            # @project info — for display and diagnostics
            "project": self._project_info,
            # Workflow execution timeout in seconds (None = no limit)
            "workflow_timeout_seconds": self.workflow_timeout_seconds,
            # Metaflow namespace (forwarded to step subprocesses via --namespace)
            "namespace": self.namespace or "",
            # @project branch name (forwarded to step subprocesses via --branch)
            "branch": self.branch or "",
            # Saga compensation map: {"step_name": "handler_method_name", ...}
            "compensations": self._build_compensations(),
            # Original Python class name (needed by run_compensation to importlib-load the class)
            "flow_class_name": self.flow.__class__.__name__,
            # @trigger_on_finish: upstream flows whose completion auto-triggers this flow.
            # Each entry: {"flow": "<FlowName>"}.
            "trigger_on_finish": self._get_trigger_on_finish(),
            # @trigger(event="foo"): named events that start this workflow via a Temporal signal.
            # Each entry: {"event": "<event_name>", "parameters": {<flow_param>: <event_field>}}.
            "named_triggers": self._get_named_triggers(),
            # Uploaded code package metadata for remote runtime decorators
            # (e.g. sandbox/daytona/e2b, batch, kubernetes).
            "code_package": self._get_code_package_info(),
        }

    def _get_code_package_info(self) -> dict | None:
        if self._code_package_info is not None:
            return self._code_package_info
        try:
            from metaflow.package import MetaflowPackage

            package = MetaflowPackage(
                self.flow,
                self.environment,
                echo=lambda *args, **kwargs: None,
                flow_datastore=self.flow_datastore,
            )
            package_url, package_sha = self.flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]
            self._code_package_info = {
                "url": package_url,
                "sha": package_sha,
                "metadata": package.package_metadata,
            }
        except Exception:
            self._code_package_info = None
        return self._code_package_info

    def _process_parameters(self) -> dict:
        """Extract flow parameters."""
        params = {}
        for var, param in self.flow._get_parameters():
            params[var] = {
                "name": param.name,
                "default": None,
            }
            try:
                default = param.kwargs.get("default")
                if default is None:
                    pass
                elif callable(default):
                    warnings.warn(
                        f"Parameter '{var}' has a callable default. Its value cannot be "
                        "baked into the worker file — you must supply it explicitly "
                        f"at trigger time (e.g. trigger {var}=<value>).",
                        UserWarning,
                        stacklevel=2,
                    )
                else:
                    params[var]["default"] = default
            except Exception:
                pass
        return params

    def _build_steps(self) -> dict:
        steps = {}
        for node in self.graph:
            steps[node.name] = {
                "type": node.type,
                "out_funcs": list(node.out_funcs),
                "in_funcs": list(node.in_funcs),
                "split_parents": list(node.split_parents),
                "foreach_param": getattr(node, "foreach_param", None),
                # For split-switch nodes: the condition variable name (string).
                # The worker reads _transition from the datastore after the step runs
                # to determine which single branch was chosen at runtime.
                "condition": getattr(node, "condition", None),
                "has_card": any(d.name == "card" for d in node.decorators),
                "env": self._step_env(node),
                "timeout_seconds": self._get_timeout(node),
                "retries": self._get_retries(node),
                "retry_delay_seconds": self._get_retry_delay(node),
                # Decorator backend specs, e.g. ["kubernetes:image=python:3.11,cpu=2"].
                # Forwarded as --with=<spec> flags to the step subprocess.
                "decorator_specs": self._get_decorator_specs(node),
                # Serialized initialized decorator state used to invoke
                # runtime_step_cli hooks in the worker process.
                "runtime_cli_decorators": self._get_runtime_cli_decorators(node),
            }
        return steps

    def _iter_step_decorators(self, node) -> list:
        step_obj = getattr(self.flow, node.name, None)
        all_decorators = list(getattr(node, "decorators", []) or [])
        if step_obj is not None:
            all_decorators.extend(getattr(step_obj, "wrappers", []) or [])
            all_decorators.extend(getattr(step_obj, "config_decorators", []) or [])
        return all_decorators

    def _get_decorator_specs(self, node) -> list:
        """Return --with-compatible spec strings for user-defined step decorators.

        @resources is handled specially: cpu/memory/gpu hints are emitted as a
        ``resources:cpu=N,memory=M,gpu=G`` spec so that Metaflow compute backends
        (e.g. @kubernetes, @batch) receive the resource constraints at step runtime.
        """
        specs = []
        seen = set()
        for d in self._iter_step_decorators(node):
            name = getattr(d, "name", None)
            if not name:
                continue
            if name in _EXCLUDED_DECORATOR_SPECS:
                # @resources is in _EXCLUDED_DECORATOR_SPECS to skip make_decorator_spec()
                # (which would emit a bare "resources:..." spec that some backends reject).
                # Instead we build a clean spec from the known attributes below.
                if name == "resources":
                    attrs = getattr(d, "attributes", {})
                    resource_parts = []
                    cpu = attrs.get("cpu")
                    memory = attrs.get("memory")
                    gpu = attrs.get("gpu")
                    if cpu is not None:
                        resource_parts.append(f"cpu={cpu}")
                    if memory is not None:
                        resource_parts.append(f"memory={memory}")
                    if gpu is not None:
                        resource_parts.append(f"gpu={gpu}")
                    if resource_parts:
                        spec = "resources:{}".format(",".join(resource_parts))
                        if spec not in seen:
                            seen.add(spec)
                            specs.append(spec)
                continue
            try:
                spec = d.make_decorator_spec()
                if spec and spec not in seen:
                    seen.add(spec)
                    specs.append(spec)
            except Exception:
                pass
        return specs

    def _get_runtime_cli_decorators(self, node) -> list:
        """Serialize initialized decorator objects that override runtime_step_cli."""
        snapshots = []
        seen = set()
        skip_state_keys = {
            "flow",
            "graph",
            "package",
            "metadata",
            "task_datastore",
            "flow_datastore",
            "logger",
            "echo",
        }
        for d in self._iter_step_decorators(node):
            name = getattr(d, "name", None)
            if not name or name == "temporal_internal":
                continue
            try:
                if d.__class__.runtime_step_cli is StepDecorator.runtime_step_cli:
                    continue
            except Exception:
                continue

            key = (d.__class__.__module__, d.__class__.__name__, name)
            if key in seen:
                continue
            seen.add(key)

            state = {}
            for k, v in getattr(d, "__dict__", {}).items():
                if k in skip_state_keys:
                    continue
                try:
                    json.dumps(v)
                except Exception:
                    warnings.warn(
                        f"Decorator '{name}' field '{k}' is not JSON-serializable and "
                        "will not be forwarded to the worker. Runtime behavior "
                        "may differ if this field is required by runtime_step_cli.",
                        UserWarning,
                        stacklevel=2,
                    )
                    continue
                state[k] = v

            snapshots.append(
                {
                    "name": name,
                    "module": d.__class__.__module__,
                    "class": d.__class__.__name__,
                    "state": state,
                }
            )
        return snapshots

    def _step_env(self, node) -> dict:
        """Build METAFLOW_* env vars needed at step execution time."""
        env = {}

        # Pull in any @environment decorator vars
        env_deco = [d for d in node.decorators if d.name == "environment"]
        if env_deco:
            env.update(env_deco[0].attributes.get("vars", {}))

        # Use project-aware flow name if @project is present
        flow_name = self._effective_flow_name
        env["METAFLOW_FLOW_NAME"] = flow_name
        env["METAFLOW_STEP_NAME"] = node.name
        env["METAFLOW_OWNER"] = self.username or ""
        env["METAFLOW_DEFAULT_DATASTORE"] = self.flow_datastore.TYPE
        env["METAFLOW_DEFAULT_METADATA"] = self.metadata.TYPE

        # Datastore root
        datastore_root = getattr(self.flow_datastore, "datastore_root", None)
        if datastore_root:
            env[f"METAFLOW_DATASTORE_SYSROOT_{self.flow_datastore.TYPE.upper()}"] = datastore_root

        # Metadata service URL — needed when using the service metadata provider.
        try:
            from metaflow import metaflow_config as mfc
            for var in ("SERVICE_URL", "SERVICE_INTERNAL_URL", "SERVICE_AUTH_KEY"):
                val = getattr(mfc, var, None)
                if val:
                    env[f"METAFLOW_{var}"] = val
        except Exception:
            pass

        # Pass flow config values (--config-value overrides) to each step subprocess
        # so that config_expr / FlowMutator / @project decorators evaluate correctly
        # at task runtime.  Mirrors the same fix applied to the Airflow deployer.
        try:
            from metaflow.flowspec import FlowStateItems

            flow_configs = self.flow._flow_state[FlowStateItems.CONFIGS]
            config_env = {
                name: value
                for name, (value, _is_plain) in flow_configs.items()
                if value is not None
            }
            if config_env:
                env["METAFLOW_FLOW_CONFIG_VALUE"] = json.dumps(config_env)
        except Exception:
            pass

        return env

    def _get_timeout(self, node) -> int:
        try:
            if get_run_time_limit_for_task is not None:
                limit = get_run_time_limit_for_task(node.decorators)
                if limit:
                    return limit
        except Exception:
            pass
        return _DEFAULT_STEP_TIMEOUT_SECONDS

    def _get_retries(self, node) -> int:
        for deco in node.decorators:
            if deco.name == "retry":
                return int(deco.attributes.get("times", 0))
        return 0

    def _get_retry_delay(self, node) -> int:
        """Return retry delay in seconds from @retry(minutes_between_retries=N)."""
        for deco in node.decorators:
            if deco.name == "retry":
                minutes = float(deco.attributes.get("minutes_between_retries", 2))
                return int(minutes * 60)
        return _DEFAULT_RETRY_DELAY_SECONDS

    def _get_schedule(self) -> dict | None:
        """Extract @schedule decorator config from flow-level decorators."""
        try:
            flow_decos = getattr(self.flow, "_flow_decorators", {})
            schedule_list = flow_decos.get("schedule", [])
            if not schedule_list:
                return None
            d = schedule_list[0]
            raw = getattr(d, "schedule", None)
            timezone = getattr(d, "timezone", None)
            # Some Metaflow builds store schedule as a dict {"cron": ..., "timezone": ...}
            if isinstance(raw, dict):
                cron = raw.get("cron")
                if timezone is None:
                    timezone = raw.get("timezone")
            else:
                cron = raw
            if cron:
                return {"cron": cron, "timezone": timezone}
        except Exception:
            pass
        return None

    def _get_project(self) -> dict | None:
        """Extract @project decorator config and compute the project-aware flow name."""
        try:
            from metaflow.plugins.project_decorator import format_name

            flow_decos = getattr(self.flow, "_flow_decorators", {})
            project_list = flow_decos.get("project", [])
            if not project_list:
                return None
            d = project_list[0]
            project_name = d.attributes.get("name")
            if not project_name:
                return None
            project_flow_name, branch_name = format_name(
                self.name,
                project_name,
                self.production,
                self.branch,
                self.username or "",
            )
            return {
                "name": project_name,
                "flow_name": project_flow_name,
                "branch": branch_name,
            }
        except Exception:
            return None

    def _get_trigger_on_finish(self) -> list:
        """Extract @trigger_on_finish decorator config from flow-level decorators.

        Uses the decorator's ``.triggers`` property (set during ``init_flow_decorator``)
        which contains fully-resolved ``{"flow": "<name>", ...}`` dicts, rather than
        reading raw attributes directly.  This mirrors the approach used by metaflow-prefect.
        """
        results = []
        try:
            flow_decos = getattr(self.flow, "_flow_decorators", {})
            for d in flow_decos.get("trigger_on_finish", []):
                raw_triggers = getattr(d, "triggers", None)
                if raw_triggers is None:
                    # Fallback: read raw attributes for older Metaflow builds
                    attrs = getattr(d, "attributes", {})
                    flow_ref = attrs.get("flow") or attrs.get("flows")
                    if not flow_ref:
                        continue
                    if isinstance(flow_ref, str):
                        results.append({"flow": flow_ref})
                    elif isinstance(flow_ref, (list, tuple)):
                        for f in flow_ref:
                            name = f if isinstance(f, str) else getattr(f, "__name__", None)
                            if name:
                                results.append({"flow": name})
                    continue
                for t in raw_triggers:
                    if not isinstance(t, dict):
                        continue
                    flow_name = t.get("flow") or t.get("fq_name")
                    if not flow_name or not isinstance(flow_name, str):
                        warnings.warn(
                            f"@trigger_on_finish entry has a missing or non-string flow name {flow_name!r} — "
                            "skipping this trigger.",
                            UserWarning,
                            stacklevel=2,
                        )
                        continue
                    results.append({"flow": flow_name})
        except Exception:
            pass
        return results

    def _get_named_triggers(self) -> list:
        """Extract @trigger(event="foo") entries from flow-level decorators.

        Returns a list of dicts: [{"event": "<event_name>", "parameters": {...}}, ...].
        In the generated worker these are wired as Temporal signal handlers — sending a
        signal named ``<event_name>`` to the workflow causes it to start a new run.
        Uses the decorator's ``.triggers`` property (set during ``init_flow_decorator``).
        """
        results = []
        try:
            flow_decos = getattr(self.flow, "_flow_decorators", {})
            for d in flow_decos.get("trigger", []):
                raw_triggers = getattr(d, "triggers", None)
                if raw_triggers is None:
                    continue
                for t in raw_triggers:
                    if not isinstance(t, dict):
                        continue
                    event_name = t.get("name")
                    if not event_name or not isinstance(event_name, str):
                        warnings.warn(
                            f"@trigger entry has a missing or non-string event name {event_name!r} — "
                            "skipping this trigger.  Evaluate the event name before deploying.",
                            UserWarning,
                            stacklevel=2,
                        )
                        continue
                    # parameters: dict mapping flow_param -> event_field
                    raw_params = t.get("parameters") or {}
                    param_map = dict(raw_params) if isinstance(raw_params, dict) else {}
                    results.append({"event": event_name, "parameters": param_map})
        except Exception:
            pass
        return results

    def _build_compensations(self) -> dict:
        """Scan the flow class for @compensate-decorated methods and return the mapping."""
        compensations = {}
        for name, method in inspect.getmembers(self.flow.__class__, predicate=inspect.isfunction):
            if hasattr(method, "_compensate_for_step"):
                compensations[method._compensate_for_step] = name
        return compensations

    def _render_template(self, config: dict) -> str:
        try:
            import chevron
        except ImportError:
            raise TemporalException(
                "chevron is required for template rendering. Install with: pip install chevron"
            )

        with open(WORKER_TEMPLATE_FILE) as f:
            template = f.read()

        with open(worker_utils.__file__) as f:
            utils_src = f.read()

        rendered = chevron.render(
            template,
            {
                "config": json.dumps(config, indent=2),
                "utils": utils_src,
                "deployed_on": str(datetime.now()),
                "flow_name": self.name,
                "flow_file": os.path.basename(self.flow_file),
            },
        )
        return rendered
