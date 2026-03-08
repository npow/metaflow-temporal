"""DeployedFlow and TriggeredRun objects for the Temporal Deployer plugin."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from typing import TYPE_CHECKING, ClassVar

from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

# Default port for the Temporal UI web server.
_TEMPORAL_UI_PORT = 8080

# Seconds to wait after spawning the worker subprocess before submitting workflows.
# The worker needs time to connect to the Temporal server and register its task queue.
_WORKER_STARTUP_WAIT_SECONDS = 3

if TYPE_CHECKING:
    pass


class TemporalTriggeredRun(TriggeredRun):
    """A Temporal workflow run that was triggered via the Deployer API.

    Inherits ``.run`` from :class:`~metaflow.runner.deployer.TriggeredRun`, which polls
    Metaflow until the run with ``pathspec`` (``FlowName/temporal-<id>``) appears.
    """

    @property
    def run(self):
        """Retrieve the Run object, applying deployer env vars so local metadata works."""
        import metaflow
        from metaflow.exception import MetaflowNotFound

        # Apply any env overrides configured on the deployer (e.g. METAFLOW_DEFAULT_METADATA=local)
        # so that metaflow.Run() can find the run regardless of the test process's default config.
        env_vars = getattr(self.deployer, "env_vars", {}) or {}
        meta_type = env_vars.get("METAFLOW_DEFAULT_METADATA")
        sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")

        old_meta = os.environ.get("METAFLOW_DEFAULT_METADATA")
        old_sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        try:
            if meta_type:
                os.environ["METAFLOW_DEFAULT_METADATA"] = meta_type
                metaflow.metadata(meta_type)
            if meta_type == "local" and sysroot is None:
                sysroot = os.path.expanduser("~")
            if sysroot:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
            return metaflow.Run(self.pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None
        except Exception:
            return None
        finally:
            if old_meta is None:
                os.environ.pop("METAFLOW_DEFAULT_METADATA", None)
            else:
                os.environ["METAFLOW_DEFAULT_METADATA"] = old_meta
            if old_sysroot is None:
                os.environ.pop("METAFLOW_DATASTORE_SYSROOT_LOCAL", None)
            else:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = old_sysroot

    @property
    def temporal_ui(self) -> str | None:
        """URL to the Temporal UI for this workflow run, if available."""
        try:
            _, run_id = self.pathspec.split("/")
            if run_id.startswith("temporal-"):
                run_id[len("temporal-"):]
                temporal_host = getattr(self.deployer, "_deployer_kwargs", {}).get(
                    "temporal_host", "localhost:7233"
                )
                # Derive UI base URL from temporal_host (strip port, use default UI port 8080)
                host = temporal_host.split(":")[0]
                return f"http://{host}:{_TEMPORAL_UI_PORT}/namespaces/default/workflows/{host}"
        except Exception:
            pass
        return None

    @property
    def status(self) -> str | None:
        """Return a simple status string based on the underlying Metaflow run."""
        run = self.run
        if run is None:
            return "PENDING"
        if run.successful:
            return "SUCCEEDED"
        if run.finished:
            return "FAILED"
        return "RUNNING"


class TemporalDeployedFlow(DeployedFlow):
    """A Metaflow flow compiled as a Temporal worker and ready to trigger."""

    TYPE: ClassVar[str | None] = "temporal"

    @property
    def id(self) -> str:
        """Deployment identifier encoding all info needed for ``from_deployment``."""
        import json
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        return json.dumps({
            "name": self.name,
            "flow_name": self.flow_name,
            "flow_file": getattr(self.deployer, "flow_file", None),
            **additional_info,
        })

    def _ensure_worker_running(self) -> None:
        """Start the Temporal worker in the background if it is not already running.

        The worker file path is read from ``deployer.additional_info["worker_file"]``.
        If that key is absent or the file does not exist, this is a no-op (the caller
        is responsible for having a worker running externally).
        """
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        worker_file = additional_info.get("worker_file")
        if not worker_file or not os.path.isfile(worker_file):
            return

        # Reuse an existing healthy worker process attached to this deployer.
        proc = getattr(self.deployer, "_worker_process", None)
        if proc is not None and proc.poll() is None:
            return  # still running

        env = dict(os.environ)
        # Forward any env overrides from the deployer (e.g. METAFLOW_DEFAULT_METADATA).
        env.update(getattr(self.deployer, "env_vars", {}) or {})

        proc = subprocess.Popen(
            [sys.executable, worker_file],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.deployer._worker_process = proc
        # Give the worker time to connect to Temporal and register its task queue.
        time.sleep(_WORKER_STARTUP_WAIT_SECONDS)

    def run(self, **kwargs) -> TemporalTriggeredRun:
        """Trigger a new run of this deployed flow.

        Parameters
        ----------
        **kwargs : Any
            Flow parameters as keyword arguments (e.g. ``greeting="hello"``).

        Returns
        -------
        TemporalTriggeredRun
        """
        self._ensure_worker_running()

        # Convert kwargs to "key=value" strings for --run-param.
        # Use a list (not tuple) so MetaflowAPI.execute() emits separate
        # --run-param flags for each entry rather than stringifying the tuple.
        run_params = [f"{k}={v}" for k, v in kwargs.items()]

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            trigger_kwargs = {"name": self.name, "deployer_attribute_file": attribute_file_path}
            if run_params:
                trigger_kwargs["run_params"] = run_params
            additional_info = getattr(self.deployer, "additional_info", {}) or {}
            for key in ("temporal_host", "task_queue", "temporal_namespace"):
                val = additional_info.get(key)
                if val:
                    trigger_kwargs[key] = val
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(**trigger_kwargs)

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                attribute_file_fd, command_obj, self.deployer.file_read_timeout
            )
            command_obj.sync_wait()
            if command_obj.process.returncode == 0:
                return TemporalTriggeredRun(deployer=self.deployer, content=content)

        raise RuntimeError(
            f"Error triggering Temporal workflow for flow {self.deployer.flow_file!r}"
        )

    trigger = run

    @classmethod
    def from_deployment(cls, identifier: str, metadata: str | None = None) -> TemporalDeployedFlow:
        """Recover a TemporalDeployedFlow from a deployment identifier.

        ``identifier`` may be either:
        - A JSON string written by :py:prop:`id` containing ``name``, ``flow_name``,
          ``flow_file``, and optional ``additional_info`` keys.
        - A plain flow name string (e.g. ``"HelloWorld"``), as returned by
          ``deployed_flow.deployer.name``.  In this case the local deployment
          metadata written by ``temporal create`` is consulted to recover the
          flow file path and connection details.
        """
        import json
        import tempfile

        from metaflow.runner.deployer import generate_fake_flow_file_contents

        from .temporal_cli import _read_deployment_metadata
        from .temporal_deployer import TemporalDeployer

        try:
            info = json.loads(identifier)
        except (ValueError, TypeError):
            # Plain name string — look up local deployment metadata.
            stored = _read_deployment_metadata(identifier) or {}
            info = {
                "name": identifier,
                "flow_name": stored.get("flow_name", identifier),
                "flow_file": stored.get("flow_file"),
                "task_queue": stored.get("task_queue"),
                "temporal_host": stored.get("temporal_host"),
                "temporal_namespace": stored.get("temporal_namespace"),
                "worker_file": stored.get("worker_file"),
                "effective_flow_name": stored.get("effective_flow_name"),
            }
            # Remove None values so they don't pollute additional_info.
            info = {k: v for k, v in info.items() if v is not None}

        flow_file = info.get("flow_file")
        flow_name = info.get("flow_name", info.get("name", identifier))

        # If the original flow file is unavailable, generate a minimal fake one
        # so that the Metaflow CLI can be initialised (required by TemporalDeployer).
        # The fake file only needs to satisfy the CLI's graph-parsing step; the
        # actual flow code runs inside the worker, not in this process.
        if not flow_file or not os.path.isfile(flow_file):
            fake_contents = generate_fake_flow_file_contents(
                flow_name=flow_name, param_info={}
            )
            tmp = tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w")
            tmp.write(fake_contents)
            tmp.close()
            flow_file = tmp.name

        deployer = TemporalDeployer(flow_file=flow_file, deployer_kwargs={})
        deployer.name = info.get("name", identifier)
        deployer.flow_name = flow_name
        deployer.metadata = metadata or "{}"
        deployer.additional_info = {
            k: v for k, v in info.items()
            if k not in ("name", "flow_name", "flow_file")
        }
        return cls(deployer=deployer)
