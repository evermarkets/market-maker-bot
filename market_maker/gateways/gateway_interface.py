import abc
import time
import json
import aiohttp
import asyncio

from market_maker.definitions import ApiResult
from market_maker.logger import logging


class GatewayInterface(abc.ABC):
    def __init__(self):
        self.msg_callback = None
        self.stop = False
        self.last_hb_time = time.time()
        self.cancel_orders_on_start = False
        self.send_post_only_orders = True

        self.started = False
        self.reconnecting = False
        self.ready_to_listen = asyncio.Event()

        self.logger = logging.getLogger()

    @abc.abstractmethod
    async def set_order_update_callback(self, msg_callback):
        pass

    async def request_orders(self) -> ApiResult:
        pass

    @abc.abstractmethod
    async def send_order(self, order) -> ApiResult:
        pass

    @abc.abstractmethod
    async def send_orders(self, orders) -> ApiResult:
        pass

    @abc.abstractmethod
    async def cancel_order(self, order) -> ApiResult:
        pass

    @abc.abstractmethod
    async def cancel_orders(self, orders) -> ApiResult:
        pass

    @abc.abstractmethod
    async def cancel_active_orders(self) -> ApiResult:
        pass

    @abc.abstractmethod
    async def start(self):
        pass

    @abc.abstractmethod
    def is_ready(self):
        pass

    async def listen(self):
        while not self.stop:
            # await self.ready_to_listen.wait()
            if self.ready_to_listen.is_set() is False:
                await asyncio.sleep(0.1)
                continue

            try:
                if time.time() - self.last_hb_time >= 30:
                    try:
                        await self.websocket.ping('keepalive')
                    except Exception as err:
                        self.logger.info(f'Ping failed: {err}')
                        await asyncio.sleep(0.1)
                        continue
                    self.last_hb_time = time.time()

                try:
                    msg = await self.websocket.receive()
                    if msg is None:
                        continue
                except Exception as err:
                    self.logger.info(f'Receive failed: {err}')
                    if self.reconnecting is False:
                        self.logger.error(f'Will be reconnected. Receive failed: {err}')
                        raise Exception('Failed to get ws msg')
                    await asyncio.sleep(0.1)
                    continue

                self.logger.debug(f'websocket msg received: {msg}')
                if msg.type == aiohttp.WSMsgType.closed:
                    if self.reconnecting is False:
                        self.logger.warning('Connection msg closed was received')
                        raise Exception('Connection msg closed was received')
                    continue
                elif msg.type == aiohttp.WSMsgType.closing:
                    self.logger.warning('Connection is closing')
                    continue
                elif msg.type == aiohttp.WSMsgType.close:
                    self.logger.warning('Connection is closed')
                    continue
                elif msg.type == aiohttp.WSMsgType.error:
                    self.logger.warning('Connection msg error was received')
                    raise Exception('Connection msg error was received')
            except Exception as e:
                self.logger.warning(f'Exception raised: {e}')
                raise Exception(f'Exception raised: {e}')
            if msg.type == aiohttp.WSMsgType.text:
                try:
                    msg = json.loads(msg.data)
                except ValueError:
                    self.logger.warning(f'Unable to load the msg. Msg = {msg}')
                    raise Exception(f'Unable to load the msg. Msg = {msg}')
                try:
                    await self.streaming.process(msg, self.msg_callback)
                except Exception as e:
                    self.logger.warning(f'Exception raised during processing: {e}')
                    raise Exception(f'Exception raised during processing: {e}')
            else:
                raise Exception(f'Unknown msg type was received. {msg}')

    async def reconnect(self):
        if self.reconnecting:
            return

        self.logger.warning('Gateway will be reconnected')

        self.reconnecting = True

        await self.websocket.close()

        self.logger.warning('Connection is closed before new attempt')

        try:
            await self.start()
        except Exception as err:
            self.reconnecting = False
            raise Exception(f'Restart failed. Reason: {err}')
        except Exception as err:
            self.reconnecting = False
            raise Exception(f'Catch Exception. Restart failed. Reason: {err}')

        self.reconnecting = False
        self.logger.warning('Connection was established')
