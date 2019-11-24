import time
import json
import asyncio
import aiohttp

from logger import logging


class websocket_client():
    def __init__(self, exchange_name=None):
        self.ws = None
        self.session = None
        self.exchange_name = exchange_name

        self.logger = logging.getLogger()

    def create_client_session(self):
        try:
            self.session = aiohttp.ClientSession()
        except Exception as err:
            raise Exception("Failed to create a client session. Uknown exception: {}".format(err))

    async def connect(self, url):
        try:
            self.ws = await self.session.ws_connect(url)
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise Exception("{} Failed to connect. Reason = {}".format(url, str(err)))
        except aiohttp.client_exceptions.WSServerHandshakeError as err:
            raise Exception("{} Failed to connect. Reason = {}".format(url, str(err)))
        except Exception as err:
            raise Exception("{} Failed to connect. Uknown exception: {}".format(url, err))

    async def create_session_and_connection(self, url, auth_params=None):
        while True:
            try:
                await self._create_session_and_connection(url, auth_params)
            except Exception as err:
                self.logger.exception("connection attempt failed on {}".format(err))
            else:
                return

            try:
                await self.close()
            except Exception as err:
                self.logger.warning("self.close failed on {}".format(err))
                raise Exception("Failed to connect, sice close didn't work")

    async def create_connection(self, url, auth_params):
        try:
            await self.connect(url)
        except Exception as err:
            self.logger.exception("connect raise: {}".format(err))
            raise

        if auth_params:
            await self.send_ws_msg(auth_params)

    async def send_ws_msg(self, msg):
        await self.ws.send_str(json.dumps(msg))

    async def _create_session_and_connection(self, url, auth_params=None):
        self.create_client_session()

        try:
            await self.connect(url)
        except Exception as err:
            self.logger.exception("{} ws failed to connect: {}".format(self.exchange_name, err))
            raise

        if auth_params:
            if isinstance(auth_params, list):
                for param in auth_params:
                    await self.send_ws_msg(param)
            else:
                await self.send_ws_msg(auth_params)

    async def send(self, params):
        await self.ws.send_str(json.dumps(params))

    async def receive(self):
        try:
            msg = await asyncio.wait_for(self.ws.receive(), timeout=0.1)
        except asyncio.TimeoutError:
            self.logger.debug("no incoming msgs websocket_client")
            return None
        except Exception as err:
            self.logger.error("Failed to get ws msg: {}".format(err))
            raise
        self.logger.debug("websocket_client received: %s", msg)
        return msg

    async def ping(self, msg):
        await self.ws.ping(msg)

    async def close(self):
        try:
            await self.ws.close()
        except Exception as err:
            self.logger.exception("Failed to close ws connection")

        try:
            await self.session.close()
        except Exception as err:
            self.logger.exception("Failed to close ws session")
