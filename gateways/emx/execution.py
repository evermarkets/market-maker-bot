import time
import json
import aiohttp
import datetime

from definitions import api_result, order_type, order_side, exchange_orders, exchange_order

from logger import logging


class execution_adapter():
    ROUNDING_QTY = 4

    def __init__(self, config, auth, ws, shared_storage):
        self.logger = logging.getLogger()

        self.config = config
        self.ws = ws
        self.shared_storage = shared_storage
        self.auth = auth

        self.headers = {
                        'content-type': 'application/json'
                       }

    def _get_timestamp(self):
        return int(round(time.time()))

    async def request_orders(self):
        body = {}
        endpoint = "/v1/orders?contract_code={}".format(self.config.symbol)
        url = self.config.url + endpoint

        timestamp = self._get_timestamp()
        signature = self.auth.generate_signature(timestamp, "GET", endpoint, body)

        self.headers['EMX-ACCESS-KEY'] = self.auth.api_key
        self.headers['EMX-ACCESS-SIG'] = signature.decode().strip()
        self.headers['EMX-ACCESS-TIMESTAMP'] = str(timestamp)

        self.logger.info("{} Requesting orders, Info = {}, Data: {}".format(self.config.exchange_name, self.headers, body))

        try:
            resp = await self.ws.session.get(url, json=body, headers=self.headers)
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise Exception(str(err))
        msg = await resp.text()

        self.logger.info("{} Orders received: {}".format(self.config.exchange_name, msg))

        if resp.status != 200:
            raise Exception("Failed to request positions. Reason: {}".format(msg))

        try:
            msg_json = json.loads(msg)
        except Exception as e:
            self.logger.exception("{} Exception raised during json parsing: {}. Msg: {}".format(self.config.exchange_name, str(e), msg))
            raise Exception("{} Failed to parse the response {}".format(self.config.exchange_name, msg))

        res = exchange_orders()

        res.exchange = self.config.exchange_name
        res.instrument = self.config.symbol

        for order_dict in msg_json['orders']:
            order = exchange_order()

            order.quantity = order_dict['size']
            order.price = float(order_dict['price'])

            if order_dict['type'] == 'limit':
                order.type = order_type.limit
            elif order_dict['type'] == 'market':
                order.type = order_type.limit

            order.exchange_orderid = order_dict['order_id']

            if order_dict['side'] == 'buy':
                order.side = order_side.buy
                res.bids.append(order)
            else:
                order.side = order_side.sell
                res.asks.append(order)
        return res

    async def send_orders(self, orders):
        res = api_result()

        data = []
        for order in orders:
            side = ""
            if order.side == order_side.buy:
                side = "buy"
            elif order.side == order_side.sell:
                side = "sell"
            else:
                raise Exception("Unknown order side. Type = {}".format(order.side))

            ord_type = ""
            if order.type == order_type.mkt:
                ord_type = "market"
            elif order.type == order_type.limit:
                ord_type = "limit"
            else:
                raise Exception("Unknown order type. Type = {}".format(order.type))

            body = {
              "client_id": order.orderid,
              "contract_code": self.config.symbol,
              "type": ord_type,
              "side": side,
              "size": str(round(order.quantity, self.ROUNDING_QTY))
            }
            if order.type == order_type.limit:
                body["price"] = str(order.price)
            data.append(body)
            self.shared_storage.orders[order.orderid] = order

        final_data = {
                        "channel": "trading",
                        "type": "request",
                        "action": "create-order",
                        "data": data
                     }

        self.logger.info("EMX sending new order request. Data: {}".format(final_data))
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def amend_order(self, new_order, old_order):
        res = api_result()

        try:
            eid = self.shared_storage.uid_to_eid[old_order.orderid]
        except KeyError:
            self.logger.warning("Order id was not found for amend. Order id = {}".format(old_order.orderid))
            return res

        ord_type = ""
        if new_order.type == order_type.mkt:
            ord_type = "market"
        elif new_order.type == order_type.limit:
            ord_type = "limit"
        else:
            raise Exception("Unknown order type. Type = {}".format(new_order.type))

        if new_order.side != old_order.side:
            raise Exception("Wrong order side. Side = {}".format(new_order.side))

        side = ""
        if new_order.side == order_side.buy:
            side = "buy"
        elif new_order.side == order_side.sell:
            side = "sell"
        else:
            raise Exception("Unknown order side. Type = {}".format(new_order.side))

        body = {
          "type": ord_type,
          "side": side,
          "order_id": eid,
          "size": str(round(new_order.quantity, self.ROUNDING_QTY))
        }
        if new_order.type == order_type.limit:
            body["price"] = str(new_order.price)

        final_data = {
                        "channel": "trading",
                        "type": "request",
                        "action": "modify-order",
                        "data": body
                     }

        self.logger.info("Sending amend request. New orderId = {}, old orderid = {}, Data: {}".format(
            new_order.orderid,
            old_order.orderid,
            final_data))

        self.shared_storage.eid_to_uid[eid] = new_order.orderid
        self.shared_storage.uid_to_eid[new_order.orderid] = eid
        self.shared_storage.eid_to_uid_amend[eid] = old_order.orderid
        self.shared_storage.eids_to_amend[eid] = True
        self.shared_storage.orders[new_order.orderid] = new_order

        self.logger.debug("Ids mapped during amend, eid = {}, uid = {}".format(eid, new_order.orderid))
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def amend_orders(self, new_orders, old_orders):
        res = api_result()

        data = []
        for new_order, old_order in zip(new_orders, old_orders):
            try:
                eid = self.shared_storage.uid_to_eid[old_order.orderid]
            except KeyError:
                self.logger.warning("Order id was not found for amend. Order id = {}".format(old_order.orderid))
                return res

            ord_type = ""
            if new_order.type == order_type.mkt:
                ord_type = "market"
            elif new_order.type == order_type.limit:
                ord_type = "limit"
            else:
                raise Exception("Unknown order type. Type = {}".format(new_order.type))

            if new_order.side != old_order.side:
                raise Exception("Wrong order side. Side = {}".format(new_order.side))

            side = ""
            if new_order.side == order_side.buy:
                side = "buy"
            elif new_order.side == order_side.sell:
                side = "sell"
            else:
                raise Exception("Unknown order side. Type = {}".format(new_order.side))

            body = {
              "type": ord_type,
              "side": side,
              "order_id": eid,
              "size": str(round(new_order.quantity, self.ROUNDING_QTY))
            }
            if new_order.type == order_type.limit:
                body["price"] = str(new_order.price)
            data.append(body)

            self.shared_storage.eid_to_uid[eid] = new_order.orderid
            self.shared_storage.uid_to_eid[new_order.orderid] = eid
            self.shared_storage.eid_to_uid_amend[eid] = old_order.orderid
            self.shared_storage.eids_to_amend[eid] = True
            self.shared_storage.orders[new_order.orderid] = new_order
            self.logger.debug("Ids mapped during amend, eid = {}, uid = {}".format(eid, new_order.orderid))

        final_data = {
                        "channel": "trading",
                        "type": "request",
                        "action": "modify-order",
                        "data": data
                     }

        self.logger.info("Sending bulk amend request. Data: {}".format(final_data))
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def send_order(self, order):
        res = api_result()

        side = ""
        if order.side == order_side.buy:
            side = "buy"
        elif order.side == order_side.sell:
            side = "sell"
        else:
            raise Exception("Unknown order side. Type = {}".format(order.side))

        ord_type = ""
        if order.type == order_type.mkt:
            ord_type = "market"
        elif order.type == order_type.limit:
            ord_type = "limit"
        else:
            raise Exception("Unknown order type. Type = {}".format(order.type))

        body = {
          "client_id": order.orderid,
          "contract_code": self.config.symbol if self.config.symbol else order.instrument_name,
          "type": ord_type,
          "side": side,
          "size": str(round(order.quantity, self.ROUNDING_QTY))
        }
        if order.type == order_type.limit:
            body["price"] = str(order.price)

        final_data = {
                        "channel": "trading",
                        "type": "request",
                        "action": "create-order",
                        "data": body
                     }

        self.shared_storage.orders[order.orderid] = order

        self.logger.info("EMX sending new order request. OrderId = {}, Data: {}".format(order.orderid, final_data))
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def cancel_order(self, orderid):
        res = api_result()
        try:
            eid = self.shared_storage.uid_to_eid[orderid]
        except KeyError:
            # Order elimination msg was received on the Streaming side
            self.logger.warning("Order was already removed. Order id = {}".format(orderid))
            res.success = True
            return res

        body = {
          "order_id": eid
        }

        final_data = {
                        "channel": "trading",
                        "type": "request",
                        "action": "cancel-order",
                        "data": body
                     }

        self.logger.info("Sending cancellation request. eid = {}, data: {}".format(eid, final_data))
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            self.logger.warning("ConnectionError will be raised")
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def cancel_active_orders(self):
        res = api_result()

        body = {
          "contract_code": self.config.symbol
        }

        final_data = {
                        "channel": "trading",
                        "type": "request",
                        "action": "cancel-all-orders",
                        "data": body
                     }

        self.logger.info("{}: Sending cancel all request".format(self.config.exchange_name))
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def cancel_orders(self, ordersids):
        raise Exception("Multiple orders cancellation is not supported")
