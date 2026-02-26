import json
import os
import sys
from datetime import datetime
from typing import Optional

from metaflow.exception import MetaflowException

try:
    from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
except ImportError:
    get_run_time_limit_for_task = None

from . import worker_utils
from .exception import TemporalException

WORKER_TEMPLATE_FILE = os.path.join(os.path.dirname(__file__), "worker_template.mustache")


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
        self.task_queue = task_queue or "metaflow-%s" % name.lower()
        self.temporal_host = temporal_host

    def compile(self) -> str:
        config = self._build_config()
        return self._render_template(config)

    def _build_config(self) -> dict:
        datastore_root = getattr(self.flow_datastore, "datastore_root", None) or ""
        return {
            "flow_name": self.name,
            "flow_file": self.flow_file,
            "task_queue": self.task_queue,
            "temporal_host": self.temporal_host,
            "max_workers": self.max_workers,
            # Runtime provider types — forwarded as CLI flags to each step subprocess.
            # These reflect whatever backend the user has configured (service metadata,
            # S3/GCS datastore, conda environment, etc.).
            "metadata_type": self.metadata.TYPE,
            "datastore_type": self.flow_datastore.TYPE,
            "datastore_root": datastore_root,
            "environment_type": self.environment.TYPE,
            "event_logger_type": self.event_logger.TYPE,
            "monitor_type": self.monitor.TYPE,
            "parameters": self._process_parameters(),
            "steps": self._build_steps(),
            # Tags forwarded as --tag flags to each step subprocess
            "tags": self.tags,
            # @schedule decorator config — used to register a Temporal Schedule
            "schedule": self._get_schedule(),
        }

    def _process_parameters(self) -> dict:
        """Extract flow parameters."""
        params = {}
        for var, param in self.flow._get_parameters():
            params[var] = {
                "name": param.name,
                "default": None,
            }
            # Try to get the default value
            try:
                default = param.kwargs.get("default")
                if default is not None and not callable(default):
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
                "env": self._step_env(node),
                "timeout_seconds": self._get_timeout(node),
                "retries": self._get_retries(node),
                "retry_delay_seconds": self._get_retry_delay(node),
                # Decorator backend specs, e.g. ["kubernetes:image=python:3.11,cpu=2",
                # "conda:packages=['numpy']", "sandbox:backend=daytona"].
                # Forwarded as --with=<spec> flags to the step subprocess so that
                # compute backends (@kubernetes, @batch, @sandbox, etc.) take effect.
                "decorator_specs": self._get_decorator_specs(node),
            }
        return steps

    def _get_decorator_specs(self, node) -> list:
        """Return --with-compatible spec strings for user-defined step decorators."""
        specs = []
        for d in node.decorators:
            # Skip injected decorators (e.g. environment, retry added by Metaflow
            # internals) and our own temporal_internal decorator.
            if d.name in ("temporal_internal",):
                continue
            # Only forward decorators that are compute/environment backends —
            # skip ones that are purely metadata/config (e.g. retry, timeout,
            # environment which we handle separately).
            if d.name in ("retry", "timeout", "environment", "project", "trigger",
                          "trigger_on_finish", "schedule", "card"):
                continue
            try:
                spec = d.make_decorator_spec()
                if spec:
                    specs.append(spec)
            except Exception:
                pass
        return specs

    def _step_env(self, node) -> dict:
        """Build METAFLOW_* env vars needed at step execution time."""
        env = {}

        # Pull in any @environment decorator vars
        env_deco = [d for d in node.decorators if d.name == "environment"]
        if env_deco:
            env.update(env_deco[0].attributes.get("vars", {}))

        env["METAFLOW_FLOW_NAME"] = self.flow.name
        env["METAFLOW_STEP_NAME"] = node.name
        env["METAFLOW_OWNER"] = self.username or ""
        env["METAFLOW_DEFAULT_DATASTORE"] = self.flow_datastore.TYPE
        env["METAFLOW_DEFAULT_METADATA"] = self.metadata.TYPE

        # Datastore root
        datastore_root = getattr(self.flow_datastore, "datastore_root", None)
        if datastore_root:
            env["METAFLOW_DATASTORE_SYSROOT_%s" % self.flow_datastore.TYPE.upper()] = datastore_root

        # Metadata service URL — needed when using the service metadata provider.
        # These are already in the worker's os.environ if configured, but we
        # explicitly capture them so they survive in generated worker files that
        # may be deployed to a different machine.
        try:
            from metaflow import metaflow_config as mfc
            for var in ("SERVICE_URL", "SERVICE_INTERNAL_URL", "SERVICE_AUTH_KEY"):
                val = getattr(mfc, var, None)
                if val:
                    env["METAFLOW_%s" % var] = val
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
        return 3600  # 1-hour default

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
        return 120  # 2-minute default, matching Metaflow's default

    def _get_schedule(self) -> Optional[dict]:
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
