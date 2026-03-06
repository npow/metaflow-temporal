from metaflow import FlowSpec, Parameter, step


class SwitchFlow(FlowSpec):
    """A flow with a split-switch (condition=) that runs exactly one branch at runtime.

    The start step sets ``self.category`` and routes to either ``high`` or ``low``
    based on the value.  Only the chosen branch runs; the merge step receives a
    single input path from the executed branch.  This is different from a regular
    split where all branches always run.
    """

    value = Parameter("value", default=10, type=int, help="Integer value to route on")

    @step
    def start(self):
        # category is the condition variable used by self.next()
        if self.value > 5:
            self.category = "high"
        else:
            self.category = "low"
        self.next(
            {"high": self.high, "low": self.low},
            condition="category",
        )

    @step
    def high(self):
        self.result = "high: %d > 5" % self.value
        self.next(self.merge)

    @step
    def low(self):
        self.result = "low: %d <= 5" % self.value
        self.next(self.merge)

    @step
    def merge(self):
        # Regular linear step — NOT a join.  Only one of high/low actually ran,
        # so self.result comes from whichever branch was executed.
        print("Result:", self.result)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    SwitchFlow()
