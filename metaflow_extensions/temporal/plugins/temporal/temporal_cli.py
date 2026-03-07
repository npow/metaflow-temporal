import asyncio
import json
import os
import sys
import uuid
from datetime import timedelta

from metaflow._vendor import click
from metaflow.util import get_username

from .temporal import Temporal


def _resolve_task_queue(obj, task_queue, branch=None, production=False):
    """Return the effective Temporal task queue name.

    Mirrors the logic in ``Temporal.__init__`` so that ``trigger`` and
    ``resume`` use the same queue as the compiled worker.  For @project flows
    the queue name includes the project-aware flow name; for plain flows it is
    simply ``metaflow-<flowname>``.
    """
    if task_queue is not None:
        return task_queue
    flow_name = obj.graph.name
    try:
        from metaflow.plugins.project_decorator import format_name

        flow_decos = getattr(obj.flow, "_flow_decorators", {})
        project_list = flow_decos.get("project", [])
        if project_list:
            d = project_list[0]
            project_name = d.attributes.get("name")
            if project_name:
                project_flow_name, _ = format_name(
                    flow_name,
                    project_name,
                    production,
                    branch,
                    get_username() or "",
                )
                flow_name = project_flow_name
    except Exception:
        pass
    return "metaflow-%s" % flow_name.lower().replace(".", "-")


@click.group()
def cli():
    pass


def _parse_run_params(run_params) -> dict:
    """Convert ['key=value', ...] tuples from --run-param flags into a dict."""
    params = {}
    for kv in run_params:
        k, _, v = kv.partition("=")
        params[k.strip()] = v.strip()
    return params


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
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write deployment info JSON here (used by Metaflow Deployer API).",
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
    deployer_attribute_file,
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

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": flow_name,
                    "flow_name": flow_name,
                    "metadata": "{}",
                    "additional_info": {
                        "worker_file": os.path.abspath(output),
                        "task_queue": t.task_queue,
                        "temporal_host": temporal_host,
                    },
                },
                f,
            )


async def _do_trigger(temporal_host, flow_name, task_queue, params, workflow_timeout):
    """Submit a MetaflowWorkflow to Temporal and return (run_id, workflow_id).

    The worker must already be running; this only submits the workflow for
    execution.  The worker uses its own embedded CONFIG (``_use_embedded_config``
    sentinel), so no config needs to be transmitted here.
    """
    from temporalio.client import Client

    client = await Client.connect(temporal_host)
    workflow_id = "%s-%s" % (flow_name.lower(), uuid.uuid4().hex[:8])
    execution_timeout = timedelta(seconds=workflow_timeout) if workflow_timeout else None
    await client.start_workflow(
        "MetaflowWorkflow",
        {"config": None, "params": params, "_use_embedded_config": True},
        id=workflow_id,
        task_queue=task_queue,
        execution_timeout=execution_timeout,
    )
    return "temporal-%s" % workflow_id, workflow_id


@temporal.command(help="Trigger a run for a previously compiled Temporal worker.")
@click.pass_obj
@click.option("--name", default=None, help="Flow name (defaults to graph name)")
@click.option(
    "--task-queue", default=None, help="Temporal task queue name"
)
@click.option(
    "--temporal-host",
    default="localhost:7233",
    show_default=True,
    help="Temporal server host:port",
)
@click.option(
    "--workflow-timeout",
    default=None,
    type=int,
    help="Maximum seconds a workflow execution may run (default: no limit)",
)
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write triggered-run info JSON here (used by Metaflow Deployer API).",
)
@click.option(
    "--run-param",
    "run_params",
    multiple=True,
    default=None,
    help="Flow parameter as key=value (repeatable).",
)
@click.option(
    "--branch",
    default=None,
    help="@project branch name (must match the compiled worker's branch).",
)
@click.option(
    "--production",
    is_flag=True,
    default=False,
    help="Target the @project production branch (must match the compiled worker).",
)
def trigger(
    obj,
    name,
    task_queue,
    temporal_host,
    workflow_timeout,
    deployer_attribute_file,
    run_params,
    branch,
    production,
):
    """Trigger a Temporal workflow run and write run info for the Deployer API."""
    flow_name = name or obj.graph.name
    task_queue = _resolve_task_queue(obj, task_queue, branch=branch, production=production)
    params = _parse_run_params(run_params)

    run_id, workflow_id = asyncio.run(_do_trigger(temporal_host, flow_name, task_queue, params, workflow_timeout))
    pathspec = "%s/%s" % (flow_name, run_id)

    click.echo("Triggered Temporal workflow: %s (pathspec: %s)" % (workflow_id, pathspec))

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "pathspec": pathspec,
                    "name": flow_name,
                    "workflow_id": workflow_id,
                    "metadata": "{}",
                },
                f,
            )


async def _do_resume(temporal_host, flow_name, run_id, task_queue, params, workflow_timeout):
    """Resume a previously started Temporal workflow and wait for completion.

    Inspects the Metaflow datastore to find which steps already completed,
    then starts a new workflow execution that skips those steps.
    """
    import metaflow
    from temporalio.client import Client

    # Discover which steps completed by inspecting the Metaflow datastore.
    try:
        mf_run = metaflow.Run("%s/%s" % (flow_name, run_id))
        resume_state = {}
        for step in mf_run:
            tasks = list(step.tasks())
            if tasks and all(t.successful for t in tasks):
                if len(tasks) == 1:
                    resume_state[step.id] = {"task_id": tasks[0].id}
                else:
                    resume_state[step.id] = {"task_ids": [t.id for t in tasks]}
    except Exception as e:
        click.echo("Warning: could not read prior run state: %s" % e, err=True)
        resume_state = {}

    client = await Client.connect(temporal_host)
    new_workflow_id = "%s-resume-%s" % (flow_name.lower(), uuid.uuid4().hex[:8])
    execution_timeout = timedelta(seconds=workflow_timeout) if workflow_timeout else None

    handle = await client.start_workflow(
        "MetaflowWorkflow",
        {
            "config": None,
            "params": params,
            "_use_embedded_config": True,
            "resume_state": resume_state,
            "run_id_override": run_id,
        },
        id=new_workflow_id,
        task_queue=task_queue,
        execution_timeout=execution_timeout,
    )
    click.echo("Resumed workflow: %s (original run ID: %s)" % (new_workflow_id, run_id))
    click.echo("Waiting for completion...")
    result = await handle.result()
    click.echo("Workflow completed. Run ID: %s" % result)
    return result


@temporal.command(help="Resume a previously failed or incomplete Temporal workflow run.")
@click.pass_obj
@click.argument("run_id")
@click.option("--name", default=None, help="Flow name (defaults to graph name)")
@click.option("--task-queue", default=None, help="Temporal task queue name")
@click.option(
    "--temporal-host",
    default="localhost:7233",
    show_default=True,
    help="Temporal server host:port",
)
@click.option(
    "--workflow-timeout",
    default=None,
    type=int,
    help="Maximum seconds a workflow execution may run (default: no limit)",
)
@click.option(
    "--run-param",
    "run_params",
    multiple=True,
    default=None,
    help="Flow parameter as key=value (override for the resumed run, repeatable).",
)
@click.option(
    "--branch",
    default=None,
    help="@project branch name (must match the compiled worker's branch).",
)
@click.option(
    "--production",
    is_flag=True,
    default=False,
    help="Target the @project production branch (must match the compiled worker).",
)
def resume(obj, run_id, name, task_queue, temporal_host, workflow_timeout, run_params, branch, production):
    """Resume a previously failed or incomplete Temporal workflow run.

    RUN_ID is the Metaflow run ID of the run to resume (e.g. temporal-myflow-abc123).
    Completed steps are skipped; only steps that did not finish will re-execute.
    """
    flow_name = name or obj.graph.name
    task_queue = _resolve_task_queue(obj, task_queue, branch=branch, production=production)
    params = _parse_run_params(run_params)

    asyncio.run(_do_resume(temporal_host, flow_name, run_id, task_queue, params, workflow_timeout))
