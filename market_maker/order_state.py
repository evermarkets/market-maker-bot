import time
from enum import Enum, auto


class Event(Enum):
    on_creation = auto()
    on_insert_ack = auto()
    on_insert_rejection = auto()
    on_cancel = auto()
    on_fill = auto()
    on_full_fill = auto()
    on_cancel_ack = auto()
    on_cancel_rejection = auto()
    on_amend = auto()
    on_amend_ack = auto()
    on_amend_partial_ack = auto()
    on_amend_rejection = auto()


class State:
    def on_event(self, event):
        pass

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.__class__.__name__


class Inactive(State):
    def on_event(self, event):
        if event == event.on_creation:
            return InsertPending()
        return self


class InsertPending(State):
    def on_event(self, event):
        if event == event.on_insert_rejection:
            return InsertFailed()
        elif event == event.on_cancel:
            return CancelPending()
        elif event == event.on_insert_ack:
            return Active()
        elif event == event.on_cancel_ack:
            return Cancelled()
        elif event == event.on_fill:
            return Fill()
        elif event == event.on_full_fill:
            return FullFill()
        return self


class Active(State):
    def on_event(self, event):
        if event == event.on_fill:
            return Fill()
        elif event == event.on_insert_rejection:
            return InsertFailed()
        elif event == event.on_cancel:
            return CancelPending()
        elif event == event.on_amend:
            return AmendPending()
        elif event == event.on_fill:
            return Fill()
        elif event == event.on_full_fill:
            return FullFill()
        elif event == event.on_amend_rejection:
            return Inactive()
        return self


class AmendPending(State):
    def on_event(self, event):
        if event == event.on_cancel:
            return CancelPending()
        elif event == event.on_amend_ack:
            return Active()
        elif event == event.on_amend_partial_ack:
            return Active()
        elif event == event.on_amend_rejection:
            return Inactive()
        elif event == event.on_fill:
            return Fill()
        elif event == event.on_full_fill:
            return FullFill()
        elif event == event.on_cancel_ack:
            return Cancelled()
        return self


class Cancelled(State):
    def on_event(self, event):
        if event == event.on_creation:
            return InsertPending()
        return self


class InsertFailed(State):
    def on_event(self, event):
        return self


class Fill(State):
    def on_event(self, event):
        if event == event.on_full_fill:
            return FullFill()
        elif event == event.on_cancel:
            return CancelPending()
        elif event == event.on_amend:
            return AmendPending()
        elif event == event.on_cancel:
            return CancelPending()
        elif event == event.on_cancel_ack:
            return Cancelled()
        return self


class FullFill(State):
    def on_event(self, event):
        if event == event.on_cancel:
            return CancelPending()
        elif event == event.on_fill:
            return Fill()
        elif event == event.on_amend_partial_ack:
            return Fill()
        elif event == event.on_creation:
            return InsertPending()
        return self


class CancelPending(State):
    def on_event(self, event):
        if event == event.on_fill:
            return Fill()
        elif event == event.on_cancel_ack:
            return Cancelled()
        elif event == event.on_cancel_rejection:
            return CancelFailed()
        return self


class CancelFailed(State):
    def on_event(self, event):
        if event == event.on_fill:
            return Fill()
        elif event == event.on_full_fill:
            return FullFill()
        return self


class OrderState:
    def __init__(self):
        self.order_id = None
        self.state = Inactive()
        self.last_update_timestamp = None

    def on_event(self, event):
        self.state = self.state.on_event(event)
        self.last_update_timestamp = time.time()
