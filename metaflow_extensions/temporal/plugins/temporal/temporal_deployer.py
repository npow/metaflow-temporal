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
        deployer_attribute_file : str, optional
            Write deployment info JSON here (Metaflow Deployer API internal).

        Returns
        -------
        TemporalDeployedFlow
        """
        from .temporal_deployer_objects import TemporalDeployedFlow

        return self._create(TemporalDeployedFlow, **kwargs)
