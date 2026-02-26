from metaflow.exception import MetaflowException


class TemporalException(MetaflowException):
    headline = "Temporal error"


class NotSupportedException(MetaflowException):
    headline = "Not supported"
