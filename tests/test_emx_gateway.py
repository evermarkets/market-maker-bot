import json
import pytest
import datetime
from munch import DefaultMunch

from gateways.emx import streaming, execution
from gateways.emx.shared_storage import SharedStorage

from definitions import (
    NewOrderAcknowledgement,
    NewOrderRejection,
    OrderEliminationAcknowledgement,
    OrderEliminationRejection,
    OrderFillAcknowledgement,
    OrderFullFillAcknowledgement,
)


@pytest.fixture
def cfg_fixture():
    b = DefaultMunch()
    b.url = "test_url"
    b.api_key = "test_key"
    b.api_secret = "test_secret"

    b.name = "market_maker"
    b.instrument_name = "TEST-PERP"
    b.tick_size = 1
    return b


class Callback:
    def __init__(self):
        self.success = False
        self.type_to_check = None

    async def msg_callback(self, msg):
        if isinstance(msg, self.type_to_check) is False:
            self.success = False
        else:
            self.success = True


@pytest.mark.asyncio
async def test_streaming_fill(cfg_fixture):
    order_msg = {
        "channel": "orders",
        "type": "update",
        "action": "filled",
        "data": {
            "status": "accepted",
            "timestamp": "2019-02-22T10:03:21.000Z",
            "contract_code": "BTCG19",
            "order_id": "475cc533-7248-4266-87ab-3cb82b64b4c7",
            "order_type": "limit",
            "side": "sell",
            "size": "345.9343",
            "size_filled": "1.5258",
            "fill_fees": "-1.77",
            "price": "3925.50",
            "stop_price": None,
            "epoch_timestamp": "2019-02-22T10:03:20.899Z",
            "fill_price": "3925.50",
            "average_fill_price": "3925.18",
            "size_filled_delta": "1.0000",
            "fill_fees_delta": "-1.18",
            "fee_type": "maker",
            "auction_code": "BTCG19-2019-02-22T10:03:21.000Z"
        }
    }

    sub_msg = {
        "type": "subscriptions",
    }

    strg = SharedStorage()
    strg.eid_to_uid["475cc533-7248-4266-87ab-3cb82b64b4c7"] = "test_uid"
    adapter = streaming.StreamingAdapter(cfg_fixture, None, strg)

    clb = Callback()
    clb.type_to_check = OrderFillAcknowledgement

    await adapter.process(sub_msg, clb.msg_callback)
    await adapter.process(order_msg, clb.msg_callback)
    assert clb.success


@pytest.mark.asyncio
async def test_streaming_multiple_fills(cfg_fixture):
    order_msg = {
        "channel": "orders",
        "type": "update",
        "action": "filled",
        "data": {
            "status": "accepted",
            "timestamp": "2019-02-22T10:03:21.000Z",
            "contract_code": "BTCG19",
            "order_id": "475cc533-7248-4266-87ab-3cb82b64b4c7",
            "order_type": "limit",
            "side": "sell",
            "size": "345.9343",
            "size_filled": "1.5258",
            "fill_fees": "-1.77",
            "price": "3925.50",
            "stop_price": None,
            "epoch_timestamp": "2019-02-22T10:03:20.899Z",
            "fill_price": "3925.50",
            "average_fill_price": "3925.18",
            "size_filled_delta": "1.0000",
            "fill_fees_delta": "-1.18",
            "fee_type": "maker",
            "auction_code": "BTCG19-2019-02-22T10:03:21.000Z"
        }
    }

    sub_msg = {
        "type": "subscriptions",
    }

    strg = SharedStorage()
    strg.eid_to_uid["475cc533-7248-4266-87ab-3cb82b64b4c7"] = "test_uid"
    adapter = streaming.StreamingAdapter(cfg_fixture, None, strg)

    clb = Callback()
    clb.type_to_check = OrderFillAcknowledgement

    await adapter.process(sub_msg, clb.msg_callback)
    await adapter.process(order_msg, clb.msg_callback)

    order_msg_second = {
        "channel": "orders",
        "type": "update",
        "action": "filled",
        "data": {
            "status": "accepted",
            "timestamp": "2019-02-22T10:03:21.000Z",
            "contract_code": "BTCG19",
            "order_id": "475cc533-7248-4266-87ab-3cb82b64b4c7",
            "order_type": "limit",
            "side": "buy",
            "size": "345.9343",
            "size_filled": "1.5258",
            "fill_fees": "-1.77",
            "price": "3925.50",
            "stop_price": None,
            "epoch_timestamp": "2019-02-22T10:03:20.899Z",
            "fill_price": "3925.50",
            "average_fill_price": "3925.18",
            "size_filled_delta": "0.5",
            "fill_fees_delta": "-1.18",
            "fee_type": "maker",
            "auction_code": "BTCG19-2019-02-22T10:03:21.000Z"
        }
    }

    await adapter.process(order_msg_second, clb.msg_callback)
    assert clb.success


@pytest.mark.asyncio
async def test_streaming_full_fill(cfg_fixture):
    order_msg = {
        "channel": "orders",
        "type": "update",
        "action": "filled",
        "data": {
            "status": "done",
            "timestamp": "2019-02-22T10:03:21.000Z",
            "contract_code": "BTCG19",
            "order_id": "475cc533-7248-4266-87ab-3cb82b64b4c7",
            "order_type": "limit",
            "side": "sell",
            "size": "345.9343",
            "size_filled": "345.9343",
            "fill_fees": "-1.77",
            "price": "3925.50",
            "stop_price": None,
            "epoch_timestamp": "2019-02-22T10:03:20.899Z",
            "fill_price": "3925.50",
            "average_fill_price": "3925.18",
            "size_filled_delta": "345.9343",
            "fill_fees_delta": "-1.18",
            "fee_type": "maker",
            "auction_code": "BTCG19-2019-02-22T10:03:21.000Z"
        }
    }

    sub_msg = {
        "type": "subscriptions",
    }

    strg = SharedStorage()
    strg.eid_to_uid["475cc533-7248-4266-87ab-3cb82b64b4c7"] = "test_uid"
    adapter = streaming.StreamingAdapter(cfg_fixture, None, strg)

    clb = Callback()
    clb.type_to_check = OrderFullFillAcknowledgement

    await adapter.process(sub_msg, clb.msg_callback)
    await adapter.process(order_msg, clb.msg_callback)
    assert clb.success
