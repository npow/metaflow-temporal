"""DeployedFlow and TriggeredRun objects for the Temporal Deployer plugin."""

from __future__ import annotations

import sys
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
        # Convert kwargs to "key=value" strings for --run-param.
        run_params = tuple("%s=%s" % (k, v) for k, v in kwargs.items())

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            trigger_kwargs = dict(name=self.name, deployer_attribute_file=attribute_file_path)
            if run_params:
                trigger_kwargs["run_params"] = run_params
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
