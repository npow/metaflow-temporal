"""Metaflow Deployer plugin for Temporal.

Registers ``TYPE = "temporal"`` so that ``Deployer(flow_file).temporal(...)``
is available and the UX test suite can parametrise ``--scheduler-type=temporal``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    from metaflow_extensions.temporal.plugins.temporal.temporal_deployer_objects import (
        TemporalDeployedFlow,
    )


class TemporalDeployer(DeployerImpl):
    """Deployer implementation for Temporal.

    Parameters
    ----------
    task_queue : str, optional
        Temporal task queue name.  Defaults to ``metaflow-<flowname>``.
    temporal_host : str, optional
        Temporal server host:port (default ``localhost:7233``).
    max_workers : int, optional
        Maximum concurrent activity workers (default 10).
    """

    TYPE: ClassVar[str | None] = "temporal"

    def __init__(self, deployer_kwargs: dict[str, str], **kwargs) -> None:
        self._deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    @property
    def deployer_kwargs(self) -> dict[str, str]:
        return self._deployer_kwargs

    @staticmethod
    def deployed_flow_type() -> type[TemporalDeployedFlow]:
        from .temporal_deployer_objects import TemporalDeployedFlow

        return TemporalDeployedFlow

    def create(self, **kwargs) -> TemporalDeployedFlow:
        """Compile and register this flow as a Temporal worker.

        Parameters
        ----------
        task_queue : str, optional
            Temporal task queue name.
        temporal_host : str, optional
            Temporal server host:port.
        max_workers : int, optional
            Maximum concurrent activity workers.
        tags : list[str], optional
            Tags to attach to all runs.
        branch : str, optional
            @project branch name. If not provided, falls back to the value
            from the Deployer top-level kwargs (e.g. when ``Deployer(..., branch=b)``
            is called by the UX test suite).
        production : bool, optional
            Deploy to the @project production branch.
        deployer_attribute_file : str, optional
            Write deployment info JSON here (Metaflow Deployer API internal).

        Returns
        -------
        TemporalDeployedFlow
        """
        from .temporal_deployer_objects import TemporalDeployedFlow

        # Forward branch/production from top_level_kwargs when not explicitly
        # given as create() kwargs.  This handles the UX test pattern:
        #   Deployer(flow_file, branch=b).temporal(...).create()
        # where branch is a recognised top-level @project CLI option and ends up
        # in self.top_level_kwargs rather than in deployer_kwargs.
        for key in ("branch", "production"):
            if key not in kwargs and key in self.top_level_kwargs:
                kwargs[key] = self.top_level_kwargs[key]

        return self._create(TemporalDeployedFlow, **kwargs)
