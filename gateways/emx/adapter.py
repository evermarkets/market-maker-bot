import asyncio

from ws_client import websocket_client

from gateways.gateway_interface import gateway_interface
from gateways.emx.execution import execution_adapter
from gateways.emx.streaming import streaming_adapter
from gateways.emx.shared_storage import shared_storage
from gateways.emx.auth import auth


class emx_adapter(gateway_interface):
    def __init__(self, config):
        super().__init__()

        self.config = config
        self.storage = shared_storage()
        self.ws = websocket_client()
        self.auth = auth(self.config)

        self.execution = execution_adapter(self.config.execution, self.auth, self.ws, self.storage)
        self.streaming = streaming_adapter(self.config.streaming, self.auth, self.storage)

    def set_order_update_callback(self, msg_callback):
        self.msg_callback = msg_callback

    def reset(self):
        self.started = False
        self.storage.reset()
        self.streaming.reset()
        self.ready_to_listen.clear()

    async def amend_order(self, new, old):
        return await self.execution.amend_order(new, old)

    async def amend_orders(self, new, old):
        return await self.execution.amend_orders(new, old)

    async def send_order(self, order):
        return await self.execution.send_order(order)

    async def send_orders(self, orders):
        return await self.execution.send_orders(orders)

    async def cancel_order(self, orderid):
        return await self.execution.cancel_order(orderid)

    async def cancel_orders(self, ordersids):
        raise Exception("cancel_orders method is not implemented yet")

    async def cancel_active_orders(self):
        return await self.execution.cancel_active_orders()

    async def request_orders(self):
        return await self.execution.request_orders()

    async def start(self):
        self.started = False

        url = self.streaming.get_sub_url()
        sub_params = self.streaming.get_sub_params()

        if sub_params is None or url is None:
            self.logger.error("{} Params for creating the connection was not created".format(self.config.name))
            raise Exception("{} Params for creating the connection was not created".format(self.config.name))

        try:
            await self.ws.create_session_and_connection(url, sub_params)
        except Exception as err:
            raise Exception("{} create_session_and_connection failed on {}".format(self.config.name, err))

        self.ready_to_listen.set()

        count = 1
        while self.streaming.subscribed is False:
            if count > 50:
                raise Exception("{} Subscription ack was not received".format(self.config.name))
            count += 1
            await asyncio.sleep(0.2)

        if self.cancel_orders_on_start is True:
            res = await self.cancel_active_orders()
            if res.success is False:
                raise Exception("{} Existing orders were not cancelled".format(self.config.name))
        else:
            self.logger.info("Cancellation request won't be sent")

        await asyncio.sleep(2.0)
        self.started = True

    def is_ready(self):
        return self.stop is False and self.started and self.streaming.is_ready() is True and self.reconnecting is False
