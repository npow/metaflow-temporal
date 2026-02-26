from metaflow import FlowSpec, Parameter, step


class ConditionalFlow(FlowSpec):
    """A flow with a split/join (conditional-style branching)."""

    threshold = Parameter("threshold", default=10, type=int, help="Threshold for branching")

    @step
    def start(self):
        self.value = 15
        self.next(self.high_path, self.low_path)

    @step
    def high_path(self):
        """Path for values >= threshold."""
        self.result = "high: %s >= %s" % (self.value, self.threshold)
        self.next(self.join)

    @step
    def low_path(self):
        """Path for values < threshold."""
        self.result = "low: %s < %s" % (self.value, self.threshold)
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = sorted([inp.result for inp in inputs])
        self.merge_artifacts(inputs, exclude=["result"])
        self.next(self.end)

    @step
    def end(self):
        print("Results:", self.results)


if __name__ == "__main__":
    ConditionalFlow()
