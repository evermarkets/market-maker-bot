import datetime
from enum import Enum


class OrderSide(Enum):
    unknown = 0
    buy = 1
    sell = 2


class OrderType(Enum):
    unknown = 0
    mkt = 1
    limit = 2
    stop = 3


class ApiResult:
    def __init__(self):
        self.success = False
        self.msg = ""


class OrderRequest:
    def __init__(self):
        self.instrument_name = ""
        self.quantity = 0.0
        self.price = 0.0
        self.side = OrderSide.unknown
        self.type = OrderType.unknown
        self.order_id = ""
        self.timestamp = datetime.datetime.now().timestamp()


class ExchangeOrder:
    def __init__(self):
        self.instrument_name = ""
        self.quantity = 0.0
        self.filled_quantity = 0.0
        self.price = 0.0
        self.side = OrderSide.unknown
        self.type = OrderType.unknown
        self.exchange_order_id = ""


class ExchangeOrders:
    def __init__(self):
        self.exchange = ""
        self.instrument = ""
        self.bids = []  # exchange_order is expected
        self.asks = []  # exchange_order is expected


class Fill:
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
        self.type = OrderType.unknown


class TopOfBook:
    def __init__(self):
        self.exchange = ""
        self.product = ""
        self.best_bid_price = None
        self.best_bid_qty = None
        self.best_ask_price = None
        self.best_ask_qty = None
        self.timestamp = 0.0


class NewOrderAcknowledgement:
    def __init__(self):
        self.order_id = ""
        self.instrument_name = ""
        self.quantity = 0.0
        self.price = 0.0
        self.side = OrderSide.unknown
        self.type = OrderType.unknown
        self.timestamp = datetime.datetime.now().timestamp()


class NewOrderRejection:
    def __init__(self):
        self.order_id = ""
        self.exchange_order_id = ""
        self.rejection_reason = ""
        # self.timestamp = None


class OrderEliminationAcknowledgement:
    def __init__(self):
        self.order_id = ""
        # self.timestamp = None


class OrderEliminationRejection:
    def __init__(self):
        self.order_id = ""
        self.rejection_reason = ""
        # self.timestamp = None


class OrderFillAcknowledgement:
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


class OrderFullFillAcknowledgement:
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


class AmendAcknowledgementPartial:
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


class AmendAcknowledgement:
    def __init__(self):
        self.order_id = ""
        self.instrument_name = ""
        self.quantity = 0.0
        self.price = 0.0
        self.side = OrderSide.unknown
        self.type = OrderType.unknown
        self.timestamp = datetime.datetime.now().timestamp()


class AmendRejection:
    def __init__(self):
        self.order_id = ""
        self.rejection_reason = ""

class Position:
    def __init__(self):
        self.exchange = ""
        self.instrument = ""
        self.position = None
