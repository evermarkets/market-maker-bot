import datetime
from enum import Enum


class order_side(Enum):
    unknown = 0
    buy = 1
    sell = 2


class order_type(Enum):
    unknown = 0
    mkt = 1
    limit = 2
    stop = 3


class api_result:
    def __init__(self):
        self.success = False
        self.msg = ""


class order_request:
    def __init__(self):
        self.instrument_name = ""
        self.quantity = 0.0
        self.price = 0.0
        self.side = order_side.unknown
        self.type = order_type.unknown
        self.order_id = ""
        self.timestamp = datetime.datetime.now().timestamp()


class exchange_order:
    def __init__(self):
        self.instrument_name = ""
        self.quantity = 0.0
        self.filled_quantity = 0.0
        self.price = 0.0
        self.side = order_side.unknown
        self.type = order_type.unknown
        self.exchange_order_id = ""


class exchange_orders:
    def __init__(self):
        self.exchange = ""
        self.instrument = ""
        self.bids = []  # exchange_order is expected
        self.asks = []  # exchange_order is expected


class fill:
    def __init__(self):
        self.exchange = ""
        self.instrument = ""
        self.order_id = ""
        self.fill_id = ""
        self.fill_price = 0.0
        self.qty = 0.0
        self.timestamp = ""
        self.fees = 0.0
        self.is_funding = False
        self.type = order_type.unknown


class tob:
    def __init__(self):
        self.exchange = ""
        self.product = ""
        self.best_bid_price = None
        self.best_bid_qty = None
        self.best_ask_price = None
        self.best_ask_qty = None
        self.timestamp = 0.0


class new_order_ack:
    def __init__(self):
        self.order_id = ""
        self.instrument_name = ""
        self.quantity = 0.0
        self.price = 0.0
        self.side = order_side.unknown
        self.type = order_type.unknown
        self.timestamp = datetime.datetime.now().timestamp()


class new_order_rejection:
    def __init__(self):
        self.order_id = ""
        self.exchange_order_id = ""
        self.rejection_reason = ""
        # self.timestamp = None


class order_elim_ack:
    def __init__(self):
        self.order_id = ""
        # self.timestamp = None


class order_elim_rejection:
    def __init__(self):
        self.order_id = ""
        self.rejection_reason = ""
        # self.timestamp = None


class order_fill_ack:
    def __init__(self):
        self.exchange = ""
        self.instrument = ""
        self.order_id = ""
        self.exchange_id = ""
        self.fill_id = ""
        self.side = ""
        self.order_type = ""
        self.order_qty = 0.0
        self.price = 0.0
        self.fill_price = 0.0
        self.running_fill_qty = 0.0
        self.incremental_fill_qty = 0.0
        self.timestamp = ""
        self.fee = 0.0


class order_full_fill_ack:
    def __init__(self):
        self.exchange = ""
        self.instrument = ""
        self.order_id = ""
        self.exchange_id = ""
        self.fill_id = ""
        self.side = ""
        self.order_type = ""
        self.order_qty = 0.0
        self.price = 0.0
        self.fill_price = 0.0
        self.running_fill_qty = 0.0
        self.incremental_fill_qty = 0.0
        self.timestamp = ""
        self.fee = 0.0


class amend_ack_on_partial:
    def __init__(self):
        self.exchange = ""
        self.instrument = ""
        self.order_id = ""
        self.exchange_id = ""
        self.fill_id = ""
        self.side = ""
        self.order_type = ""
        self.order_qty = 0.0
        self.price = 0.0
        self.running_fill_qty = 0.0
        self.timestamp = ""
        self.fee = 0.0


class amend_ack:
    def __init__(self):
        self.order_id = ""
        self.instrument_name = ""
        self.quantity = 0.0
        self.price = 0.0
        self.side = order_side.unknown
        self.type = order_type.unknown
        self.timestamp = datetime.datetime.now().timestamp()


class amend_rejection:
    def __init__(self):
        self.order_id = ""
        self.rejection_reason = ""
