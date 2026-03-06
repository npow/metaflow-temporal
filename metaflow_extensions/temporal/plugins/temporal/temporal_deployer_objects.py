"""DeployedFlow and TriggeredRun objects for the Temporal Deployer plugin."""

from __future__ import annotations

import os
import sys
import time
import subprocess
from typing import TYPE_CHECKING, ClassVar, Optional

from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

if TYPE_CHECKING:
    import metaflow
    import metaflow.runner.deployer_impl


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
    def temporal_ui(self) -> Optional[str]:
        """URL to the Temporal UI for this workflow run, if available."""
        try:
            _, run_id = self.pathspec.split("/")
            if run_id.startswith("temporal-"):
                workflow_id = run_id[len("temporal-"):]
                temporal_host = getattr(self.deployer, "_deployer_kwargs", {}).get(
                    "temporal_host", "localhost:7233"
                )
                # Derive UI base URL from temporal_host (strip port, use default UI port 8080)
                host = temporal_host.split(":")[0]
                return "http://%s:8080/namespaces/default/workflows/%s" % (host, workflow_id)
        except Exception:
            pass
        return None

    @property
    def status(self) -> Optional[str]:
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

    TYPE: ClassVar[Optional[str]] = "temporal"

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
        # Give the worker a moment to connect to the Temporal server.
        time.sleep(3)

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
        run_params = tuple("%s=%s" % (k, v) for k, v in kwargs.items())

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            trigger_kwargs = dict(name=self.name, deployer_attribute_file=attribute_file_path)
            if run_params:
                trigger_kwargs["run_params"] = run_params
            additional_info = getattr(self.deployer, "additional_info", {}) or {}
            for key in ("temporal_host", "task_queue"):
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
            "Error triggering Temporal workflow for flow %r"
            % self.deployer.flow_file
        )

    trigger = run

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None) -> "TemporalDeployedFlow":
        """Recover a TemporalDeployedFlow from a deployment identifier."""
        import json
        from .temporal_deployer import TemporalDeployer

        info = json.loads(identifier)
        deployer = TemporalDeployer(flow_file=info["flow_file"], deployer_kwargs={})
        deployer.name = info["name"]
        deployer.flow_name = info["flow_name"]
        deployer.metadata = metadata or "{}"
        deployer.additional_info = {
            k: v for k, v in info.items()
            if k not in ("name", "flow_name", "flow_file")
        }
        return cls(deployer=deployer)
