"""Foreach flow with saga compensations for testing compensation-in-foreach."""
import json
import os

from metaflow import FlowSpec

from metaflow_extensions.temporal.plugins.temporal import step

_LOG_FILE = "/tmp/foreach_saga_test_log.json"


class ForeachSagaFlow(FlowSpec):
    """
    start -> body (foreach) -> join -> fail_step -> end

    Each body slice has a compensation handler.  fail_step always raises,
    triggering compensations for all completed body slices.
    """

    @step
    def start(self):
        self.items = ["item-a", "item-b"]
        self.next(self.body, foreach="items")

    @step
    def body(self):
        self.item_value = self.input
        self.next(self.join)

    @body.compensate
    def cancel_body(self):
        _append_log({"action": "cancel_body", "item": self.item_value})

    @step
    def join(self, inputs):
        self.item_values = [inp.item_value for inp in inputs]
        self.next(self.fail_step)

    @step
    def fail_step(self):
        raise RuntimeError("Intentional failure to trigger foreach compensations")
        self.next(self.end)  # noqa: unreachable — required by Metaflow validator

    @step
    def end(self):
        pass


def _append_log(entry: dict):
    existing = []
    if os.path.exists(_LOG_FILE):
        try:
            with open(_LOG_FILE) as f:
                existing = json.load(f)
        except (OSError, json.JSONDecodeError):
            pass
    existing.append(entry)
    with open(_LOG_FILE, "w") as f:
        json.dump(existing, f)


if __name__ == "__main__":
    ForeachSagaFlow()
