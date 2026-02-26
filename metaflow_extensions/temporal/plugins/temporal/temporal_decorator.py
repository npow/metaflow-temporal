import json
import os

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum


class TemporalInternalDecorator(StepDecorator):
    name = "temporal_internal"

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        entries = []
        if "METAFLOW_TEMPORAL_WORKFLOW_ID" in os.environ:
            entries.append(
                MetaDatum(
                    field="temporal-workflow-id",
                    value=os.environ["METAFLOW_TEMPORAL_WORKFLOW_ID"],
                    type="temporal-workflow-id",
                    tags=["attempt_id:{0}".format(retry_count)],
                )
            )
        if "METAFLOW_TEMPORAL_RUN_ID" in os.environ:
            entries.append(
                MetaDatum(
                    field="temporal-run-id",
                    value=os.environ["METAFLOW_TEMPORAL_RUN_ID"],
                    type="temporal-run-id",
                    tags=["attempt_id:{0}".format(retry_count)],
                )
            )
        if entries:
            metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        output = {"task_ok": is_task_ok}
        if graph[step_name].type == "foreach":
            output["foreach_cardinality"] = flow._foreach_num_splits
        output_file = os.environ.get("METAFLOW_TEMPORAL_OUTPUT_FILE")
        if output_file:
            with open(output_file, "w") as f:
                json.dump(output, f)
