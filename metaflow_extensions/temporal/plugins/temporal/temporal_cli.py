import asyncio
import json
import os
import sys
import uuid
from datetime import timedelta

from metaflow._vendor import click
from metaflow.util import get_username

from .temporal import Temporal


def _resolve_project_flow_name(obj, branch=None, production=False):
    """Return the project-aware flow name for @project flows, or the plain graph name.

    Mirrors the logic in ``Temporal._get_project()`` so that ``trigger`` and
    ``resume`` derive the same effective flow name as the compiled worker.
    """
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
                return project_flow_name
    except Exception:
        pass
    return flow_name


def _resolve_task_queue(obj, task_queue, branch=None, production=False):
    """Return the effective Temporal task queue name.

    Mirrors the logic in ``Temporal.__init__`` so that ``trigger`` and
    ``resume`` use the same queue as the compiled worker.  For @project flows
    the queue name includes the project-aware flow name; for plain flows it is
    simply ``metaflow-<flowname>``.
    """
    if task_queue is not None:
        return task_queue
    flow_name = _resolve_project_flow_name(obj, branch=branch, production=production)
    return "metaflow-{}".format(flow_name.lower().replace(".", "-"))


def _deployment_metadata_path(name: str) -> str:
    """Return the path to the local deployment metadata file for *name*."""
    deploy_dir = os.path.join(os.path.expanduser("~"), ".metaflow", "temporal_deployments")
    os.makedirs(deploy_dir, exist_ok=True)
    return os.path.join(deploy_dir, f"{name}.json")


def _write_deployment_metadata(
    name: str,
    flow_file: str,
    flow_name: str,
    effective_flow_name: str,
    task_queue: str,
    temporal_host: str,
    temporal_namespace: str,
    worker_file: str,
) -> None:
    """Persist deployment metadata so that from_deployment() can recover it."""
    path = _deployment_metadata_path(name)
    with open(path, "w") as f:
        json.dump(
            {
                "name": name,
                "flow_file": flow_file,
                "flow_name": flow_name,
                "effective_flow_name": effective_flow_name,
                "task_queue": task_queue,
                "temporal_host": temporal_host,
                "temporal_namespace": temporal_namespace,
                "worker_file": worker_file,
            },
            f,
            indent=2,
        )


def _read_deployment_metadata(name: str) -> dict | None:
    """Read deployment metadata written by _write_deployment_metadata, or None."""
    path = _deployment_metadata_path(name)
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


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
    "--temporal-namespace",
    default="default",
    show_default=True,
    help="Temporal server namespace",
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
    temporal_namespace,
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
        output = f"{flow_name.lower()}_temporal_worker.py"

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
        temporal_namespace=temporal_namespace,
        branch=branch,
        production=production,
        workflow_timeout_seconds=workflow_timeout,
    )

    worker_code = t.compile()

    with open(output, "w") as f:
        f.write(worker_code)

    click.echo(f"Worker written to: {output}")
    if t._project_info:
        click.echo(
            "Project: {}, Branch: {}".format(t._project_info["name"], t._project_info["branch"])
        )
        click.echo("Flow name (in datastore): {}".format(t._project_info["flow_name"]))
    click.echo(f"Start worker:  python {output}")
    click.echo(f"Trigger run:   python {output} trigger [key=value ...]")

    # Write a local metadata file so that from_deployment() can recover
    # the flow file path, task queue, and temporal host from just the flow name.
    _write_deployment_metadata(
        name=flow_name,
        flow_file=os.path.abspath(sys.argv[0]),
        flow_name=flow_name,
        effective_flow_name=t._effective_flow_name,
        task_queue=t.task_queue,
        temporal_host=temporal_host,
        temporal_namespace=temporal_namespace,
        worker_file=os.path.abspath(output),
    )

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
                        "temporal_namespace": temporal_namespace,
                    },
                },
                f,
            )


async def _do_trigger(temporal_host, temporal_namespace, flow_name, task_queue, params, workflow_timeout):
    """Submit a MetaflowWorkflow to Temporal and return (run_id, workflow_id).

    The worker must already be running; this only submits the workflow for
    execution.  The worker uses its own embedded CONFIG (``_use_embedded_config``
    sentinel), so no config needs to be transmitted here.
    """
    from temporalio.client import Client

    client = await Client.connect(temporal_host, namespace=temporal_namespace)
    workflow_id = f"{flow_name.lower()}-{uuid.uuid4().hex[:8]}"
    execution_timeout = timedelta(seconds=workflow_timeout) if workflow_timeout else None
    await client.start_workflow(
        "MetaflowWorkflow",
        {"config": None, "params": params, "_use_embedded_config": True},
        id=workflow_id,
        task_queue=task_queue,
        execution_timeout=execution_timeout,
    )
    return f"temporal-{workflow_id}", workflow_id


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
    "--temporal-namespace",
    default="default",
    show_default=True,
    help="Temporal server namespace",
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
    temporal_namespace,
    workflow_timeout,
    deployer_attribute_file,
    run_params,
    branch,
    production,
):
    """Trigger a Temporal workflow run and write run info for the Deployer API."""
    # Use plain name if explicitly given, otherwise derive project-aware name so
    # the pathspec matches the flow name used by the worker when storing runs.
    if name:
        flow_name = name
    else:
        flow_name = _resolve_project_flow_name(obj, branch=branch, production=production)
    task_queue = _resolve_task_queue(obj, task_queue, branch=branch, production=production)
    params = _parse_run_params(run_params)

    run_id, workflow_id = asyncio.run(_do_trigger(temporal_host, temporal_namespace, flow_name, task_queue, params, workflow_timeout))
    pathspec = f"{flow_name}/{run_id}"

    click.echo(f"Triggered Temporal workflow: {workflow_id} (pathspec: {pathspec})")

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


async def _do_resume(temporal_host, temporal_namespace, flow_name, run_id, task_queue, params, workflow_timeout):
    """Resume a previously started Temporal workflow and wait for completion.

    Inspects the Metaflow datastore to find which steps already completed,
    then starts a new workflow execution that skips those steps.
    """
    import metaflow
    from temporalio.client import Client

    # Discover which steps completed by inspecting the Metaflow datastore.
    try:
        mf_run = metaflow.Run(f"{flow_name}/{run_id}")
        resume_state = {}
        for step in mf_run:
            tasks = list(step.tasks())
            if tasks and all(t.successful for t in tasks):
                if len(tasks) == 1:
                    resume_state[step.id] = {"task_id": tasks[0].id}
                else:
                    resume_state[step.id] = {"task_ids": [t.id for t in tasks]}
    except Exception as e:
        click.echo(f"Warning: could not read prior run state: {e}", err=True)
        resume_state = {}

    client = await Client.connect(temporal_host, namespace=temporal_namespace)
    new_workflow_id = f"{flow_name.lower()}-resume-{uuid.uuid4().hex[:8]}"
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
    click.echo(f"Resumed workflow: {new_workflow_id} (original run ID: {run_id})")
    click.echo("Waiting for completion...")
    result = await handle.result()
    click.echo(f"Workflow completed. Run ID: {result}")
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
    "--temporal-namespace",
    default="default",
    show_default=True,
    help="Temporal server namespace",
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
def resume(obj, run_id, name, task_queue, temporal_host, temporal_namespace, workflow_timeout, run_params, branch, production):
    """Resume a previously failed or incomplete Temporal workflow run.

    RUN_ID is the Metaflow run ID of the run to resume (e.g. temporal-myflow-abc123).
    Completed steps are skipped; only steps that did not finish will re-execute.
    """
    flow_name = name or obj.graph.name
    task_queue = _resolve_task_queue(obj, task_queue, branch=branch, production=production)
    params = _parse_run_params(run_params)

    asyncio.run(_do_resume(temporal_host, temporal_namespace, flow_name, run_id, task_queue, params, workflow_timeout))
