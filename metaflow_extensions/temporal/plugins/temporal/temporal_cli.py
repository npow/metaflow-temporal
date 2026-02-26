import os
import sys

from metaflow._vendor import click
from metaflow.util import get_username

from .temporal import Temporal


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Temporal orchestration.")
@click.pass_obj
def temporal(obj):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)


@temporal.command(help="Compile a Metaflow flow to a self-contained Temporal worker file.")
@click.pass_obj
@click.option("--output", "-o", default=None, help="Output file path")
@click.option("--task-queue", default=None, help="Temporal task queue name")
@click.option(
    "--temporal-host",
    default="localhost:7233",
    show_default=True,
    help="Temporal server host:port",
)
@click.option(
    "--max-workers",
    default=10,
    show_default=True,
    type=int,
    help="Max concurrent activity workers",
)
@click.option("--tag", "tags", multiple=True, help="Tags to attach to all runs")
@click.option("--namespace", default=None, help="Metaflow namespace")
@click.option(
    "--branch",
    default=None,
    help="@project branch name (default: user.<username>)",
)
@click.option(
    "--production",
    is_flag=True,
    default=False,
    help="Deploy to the @project production branch",
)
@click.option(
    "--workflow-timeout",
    default=None,
    type=int,
    help="Maximum seconds a workflow execution may run (default: no limit)",
)
def create(
    obj,
    output,
    task_queue,
    temporal_host,
    max_workers,
    tags,
    namespace,
    branch,
    production,
    workflow_timeout,
):
    flow_name = obj.graph.name
    if output is None:
        output = "%s_temporal_worker.py" % flow_name.lower()

    t = Temporal(
        name=flow_name,
        graph=obj.graph,
        flow=obj.flow,
        flow_file=os.path.abspath(sys.argv[0]),
        metadata=obj.metadata,
        flow_datastore=obj.flow_datastore,
        environment=obj.environment,
        event_logger=obj.event_logger,
        monitor=obj.monitor,
        tags=list(tags),
        namespace=namespace,
        username=get_username(),
        max_workers=max_workers,
        task_queue=task_queue,
        temporal_host=temporal_host,
        branch=branch,
        production=production,
        workflow_timeout_seconds=workflow_timeout,
    )

    worker_code = t.compile()

    with open(output, "w") as f:
        f.write(worker_code)

    click.echo("Worker written to: %s" % output)
    if t._project_info:
        click.echo(
            "Project: %s, Branch: %s"
            % (t._project_info["name"], t._project_info["branch"])
        )
        click.echo("Flow name (in datastore): %s" % t._project_info["flow_name"])
    click.echo("Start worker:  python %s" % output)
    click.echo("Trigger run:   python %s trigger [key=value ...]" % output)
