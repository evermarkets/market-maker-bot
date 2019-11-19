import abc
import time
import json
import aiohttp
import asyncio

from definitions import api_result
from logger import logging


class gateway_interface(abc.ABC):
    def __init__(self):
        self.msg_callback = None
        self.stop = False
        self.last_hb_time = time.time()
        self.request_positions_flag = False
        self.request_fills_flag = False
        self.cancel_orders_on_start = False

        self.max_num_of_orders = None
        self.qty_multiplier = None

        self.started = False
        self.reconnecting = False
        self.ready_to_listen = asyncio.Event()

        self.logger = logging.getLogger()

    @abc.abstractmethod
    async def set_order_update_callback(self, msg_callback):
        pass

    async def request_orders(self) -> api_result:
        pass

    @abc.abstractmethod
    async def send_order(self, order) -> api_result:
        pass

    @abc.abstractmethod
    async def send_orders(self, orders) -> api_result:
        pass

    @abc.abstractmethod
    async def cancel_order(self, order) -> api_result:
        pass

    @abc.abstractmethod
    async def cancel_orders(self, orders) -> api_result:
        pass

    @abc.abstractmethod
    async def cancel_active_orders(self) -> api_result:
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
                        await self.ws.ping("keepalive")
                    except Exception as err:
                        self.logger.info("Ping failed: {}".format(err))
                        await asyncio.sleep(0.1)
                        continue
                    self.last_hb_time = time.time()

                try:
                    msg = await self.ws.receive()
                    if msg is None: continue
                except Exception as err:
                    self.logger.info("Receive failed: {}".format(err))
                    if self.reconnecting is False:
                        self.logger.error("Will be reconnected. Receive failed: {}".format(err))
                        raise Exception("Failed to get ws msg")
                    await asyncio.sleep(0.1)
                    continue

                self.logger.debug("WS msg received: {}".format(msg))
                if msg.type == aiohttp.WSMsgType.closed:
                    if self.reconnecting is False:
                        self.logger.warning("Connection msg closed was received")
                        raise Exception("Connection msg closed was received")
                    continue
                elif msg.type == aiohttp.WSMsgType.closing:
                    self.logger.warning("Connection is closing")
                    continue
                elif msg.type == aiohttp.WSMsgType.close:
                    self.logger.warning("Connection is closed")
                    continue
                elif msg.type == aiohttp.WSMsgType.error:
                    self.logger.warning("Connection msg error was received")
                    raise Exception("Connection msg error was received")
            except Exception as e:
                self.logger.warning("Exception raised: {}".format(e))
                raise Exception("Exception raised: {}".format(e))
            if msg.type == aiohttp.WSMsgType.text:
                try:
                    msg = json.loads(msg.data)
                except ValueError:
                    self.logger.warning("Unable to load the msg. Msg = {}".format(msg))
                    raise Exception("Unable to load the msg. Msg = {}".format(msg))
                try:
                    await self.streaming.process(msg, self.msg_callback)
                except Exception as e:
                    self.logger.warning("Exception raised during processing: {}".format(e))
                    raise Exception("Exception raised during processing: {}".format(e))
            else:
                raise Exception("Unknown msg type was received. Msg = {}".format(msg))

    async def reconnect(self):
        if self.reconnecting:
            return

        self.logger.warning("Gateway will be reconnected")

        self.reconnecting = True

        await self.ws.close()

        self.logger.warning("Connection is closed before new attempt")

        try:
            await self.start()
        except Exception as err:
            self.reconnecting = False
            raise Exception("Restart failed. Reason: {}".format(err))
        except Exception as err:
            self.reconnecting = False
            raise Exception("Catch Exception. Restart failed. Reason: {}".format(err))

        self.reconnecting = False
        self.logger.warning("Connection was established")
