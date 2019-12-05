import asyncio

from websocket_client import WebsocketClient

from gateways.gateway_interface import GatewayInterface
from gateways.emx.execution import ExecutionAdapter
from gateways.emx.streaming import StreamingAdapter
from gateways.emx.shared_storage import SharedStorage
from gateways.emx.authentication import Authentication


class EmxAdapter(GatewayInterface):
    def __init__(self, config):
        super().__init__()

        self.config = config
        self.storage = SharedStorage()
        self.websocket = WebsocketClient()
        self.auth = Authentication(self.config)

        self.execution = ExecutionAdapter(self.config.execution, self.auth,
                                          self.websocket, self.storage)
        self.streaming = StreamingAdapter(self.config.streaming, self.auth, self.storage)

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

    async def cancel_order(self, order_id):
        return await self.execution.cancel_order(order_id)

    async def cancel_orders(self, orders_ids):
        raise Exception('cancel_orders method is not implemented yet')

    async def cancel_active_orders(self):
        return await self.execution.cancel_active_orders()

    async def request_orders(self):
        return await self.execution.request_orders()

    async def start(self):
        self.started = False

        url = self.streaming.get_sub_url()
        sub_params = self.streaming.get_sub_params()

        if sub_params is None or url is None:
            self.logger.error(
                f'{self.config.name} Params for creating the connection was not created')
            raise Exception(
                f'{self.config.name} Params for creating the connection was not created')

        try:
            await self.websocket.create_session_and_connection(url, sub_params)
        except Exception as err:
            raise Exception(f'{self.config.name} create_session_and_connection failed on {err}')

        self.ready_to_listen.set()

        count = 1
        while self.streaming.subscribed is False:
            if count > 50:
                raise Exception(f'{self.config.name} Subscription ack was not received')
            count += 1
            await asyncio.sleep(0.2)

        if self.cancel_orders_on_start:
            res = await self.cancel_active_orders()
            if res.success is False:
                raise Exception(f'{self.config.name} Existing orders were not cancelled')
        else:
            self.logger.info('Cancellation request will not be sent')

        await asyncio.sleep(2.0)
        self.started = True

    def is_ready(self):
        return self.stop is False and self.started and \
               self.streaming.is_ready() is True and self.reconnecting is False
