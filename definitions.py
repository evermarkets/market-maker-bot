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
        self.orderid = ""
        self.timestamp = datetime.datetime.now().timestamp()


class exchange_order:
    def __init__(self):
        self.instrument_name = ""
        self.quantity = 0.0
        self.filled_quantity = 0.0
        self.price = 0.0
        self.side = order_side.unknown
        self.type = order_type.unknown
        self.exchange_orderid = ""


class exchange_orders:
    def __init__(self):
        self.exchange = ""
        self.instrument = ""
        self.bids = [] # exchange_order is expected
        self.asks = []  # exchange_order is expected



class fill:
    def __init__(self):
        self.exchange = ""
        self.instrument = ""
        self.orderid = ""
        self.fillid = ""
        self.fill_price = 0.0
        self.qty = 0.0
        self.timestamp = ""
        self.fees = 0.0
        self.is_funding = False
        self.type = order_type.unknown
