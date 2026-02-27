def step(fn):
    """Drop-in replacement for metaflow.step that adds optional .compensate support.

    Use exactly like metaflow's @step. To declare a compensation handler, decorate
    a plain method immediately after with @<step_name>.compensate:

        from metaflow import FlowSpec
        from metaflow_extensions.temporal.plugins.temporal import step

        class BookingFlow(FlowSpec):

            @step
            def book_hotel(self):
                self.hotel_id = reserve_hotel()
                self.next(self.book_flight)

            @book_hotel.compensate
            def cancel_hotel(self):
                cancel_reservation(self.hotel_id)
    """
    from metaflow import step as _metaflow_step
    wrapped = _metaflow_step(fn)
    _step_name = fn.__name__

    def compensate(handler_fn):
        """Register handler_fn as the compensation for this step."""
        handler_fn._compensate_for_step = _step_name
        return handler_fn

    wrapped.compensate = compensate
    return wrapped
