import time
from enum import Enum, auto


class event(Enum):
    on_creation = auto()
    on_insert_ack = auto()
    on_insert_nack = auto()
    on_cancel = auto()
    on_fill = auto()
    on_full_fill = auto()
    on_cancel_ack = auto()
    on_cancel_nack = auto()
    on_amend = auto()
    on_amend_ack = auto()
    on_amend_partial_ack = auto()
    on_amend_nack = auto()


class state:
    def on_event(self, event):
        pass

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.__class__.__name__


class inactive(state):
    def on_event(self, event):
        if event == event.on_creation:
            return insert_pending()
        raise StateException("invalid event for the current state. event = {}, state = {}".format(event, self.__class__.__name__))
        return self


class insert_pending(state):
    def on_event(self, event):
        if event == event.on_insert_nack:
            return insert_failed()
        elif event == event.on_cancel:
            return cancel_pending()
        elif event == event.on_insert_ack:
            return active()
        elif event == event.on_cancel_ack:
            return cancelled()
        elif event == event.on_fill:
            return fill()
        elif event == event.on_full_fill:
            return full_fill()
        return self


class active(state):
    def on_event(self, event):
        if event == event.on_fill:
            return fill()
        elif event == event.on_insert_nack:
            return insert_failed()
        elif event == event.on_cancel:
            return cancel_pending()
        elif event == event.on_amend:
            return amend_pending()
        elif event == event.on_fill:
            return fill()
        elif event == event.on_full_fill:
            return full_fill()
        elif event == event.on_amend_nack:
            return inactive()
        return self


class amend_pending(state):
    def on_event(self, event):
        if event == event.on_cancel:
            return cancel_pending()
        elif event == event.on_amend_ack:
            return active()
        elif event == event.on_amend_partial_ack:
            return active()
        elif event == event.on_amend_nack:
            return inactive()
        elif event == event.on_fill:
            return fill()
        elif event == event.on_full_fill:
            return full_fill()
        elif event == event.on_cancel_ack:
            return cancelled()
        return self


class cancelled(state):
    def on_event(self, event):
        if event == event.on_creation:
            return insert_pending()
        return self


class insert_failed(state):
    def on_event(self, event):
        return self


class fill(state):
    def on_event(self, event):
        if event == event.on_full_fill:
            return full_fill()
        elif event == event.on_cancel:
            return cancel_pending()
        elif event == event.on_amend:
            return amend_pending()
        elif event == event.on_cancel:
            return cancel_pending()
        elif event == event.on_cancel_ack:
            return cancelled()
        return self


class full_fill(state):
    def on_event(self, event):
        if event == event.on_cancel:
            return cancel_pending()
        elif event == event.on_fill:
            return fill()
        elif event == event.on_amend_partial_ack:
            return fill()
        elif event == event.on_creation:
            return insert_pending()
        return self


class cancel_pending(state):
    def on_event(self, event):
        if event == event.on_fill:
            return fill()
        elif event == event.on_cancel_ack:
            return cancelled()
        elif event == event.on_cancel_nack:
            return cancel_failed()
        return self


class cancel_failed(state):
    def on_event(self, event):
        if event == event.on_fill:
            return fill()
        elif event == event.on_full_fill:
            return full_fill()
        return self


class order_state:
    def __init__(self):
        self.order_id = None
        self.state = inactive()
        self.last_update_timestamp = None

    def on_event(self, event):
        self.state = self.state.on_event(event)
        self.last_update_timestamp = time.time()
