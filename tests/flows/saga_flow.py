import json
import os

from metaflow import FlowSpec, step
from metaflow_extensions.temporal.plugins.temporal import compensate

_LOG_FILE = "/tmp/saga_test_log.json"


class SagaFlow(FlowSpec):
    """A flow demonstrating the Saga pattern.

    start -> book_a -> book_b -> fail_step -> end

    book_a and book_b have compensation handlers that write to a log file.
    fail_step always raises, triggering both compensations in reverse (LIFO) order.
    """

    @step
    def start(self):
        self.next(self.book_a)

    @step
    def book_a(self):
        self.booking_a_id = "hotel-123"
        self.next(self.book_b)

    @step
    def book_b(self):
        self.booking_b_id = "flight-456"
        self.next(self.fail_step)

    @step
    def fail_step(self):
        raise RuntimeError("Intentional failure to trigger compensations")
        self.next(self.end)  # noqa: unreachable — required by Metaflow validator

    @step
    def end(self):
        pass

    @compensate("book_a")
    def cancel_a(self):
        _append_log({"action": "cancel_a", "booking_id": self.booking_a_id})

    @compensate("book_b")
    def cancel_b(self):
        _append_log({"action": "cancel_b", "booking_id": self.booking_b_id})


def _append_log(entry: dict):
    """Append a JSON entry to the saga test log file."""
    existing = []
    if os.path.exists(_LOG_FILE):
        try:
            with open(_LOG_FILE) as f:
                existing = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    existing.append(entry)
    with open(_LOG_FILE, "w") as f:
        json.dump(existing, f)


if __name__ == "__main__":
    SagaFlow()
