def compensate(step: str):
    """Decorator marking a FlowSpec method as a saga compensation for `step`.

    The decorated method receives the forward step's artifacts as self.<attr>.
    Compensation methods are plain Python methods (NOT @step-decorated) and
    must not call self.next().

    Example::

        @compensate("book_hotel")
        def cancel_hotel(self):
            cancel_reservation(self.hotel_booking_id)
    """
    def decorator(fn):
        fn._compensate_for_step = step
        return fn
    return decorator
