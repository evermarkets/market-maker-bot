import time
import json
import aiohttp

from definitions import ApiResult, OrderType, OrderSide, ExchangeOrders, ExchangeOrder

from logger import logging


def get_timestamp():
    return int(round(time.time()))


class ExecutionAdapter:
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

    async def request_orders(self):
        body = {}
        endpoint = f'/v1/orders?contract_code={self.config.symbol}'
        url = self.config.url + endpoint

        timestamp = get_timestamp()
        signature = self.auth.generate_signature(timestamp, 'GET', endpoint, body)

        self.headers['EMX-ACCESS-KEY'] = self.auth.api_key
        self.headers['EMX-ACCESS-SIG'] = signature.decode().strip()
        self.headers['EMX-ACCESS-TIMESTAMP'] = str(timestamp)

        self.logger.info(
            f'{self.config.exchange_name} Requesting orders, Info = {self.headers}, Data: {body}')

        try:
            resp = await self.ws.session.get(url, json=body, headers=self.headers)
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise Exception(str(err))
        msg = await resp.text()

        self.logger.info(f'{self.config.exchange_name} Orders received: {msg}')

        if resp.status != 200:
            raise Exception(f'Failed to request positions. Reason: {msg}')

        try:
            msg_json = json.loads(msg)
        except Exception as err:
            self.logger.exception(
                f'{self.config.exchange_name} Exception raised '
                f'during json parsing: {err}. Msg: {msg}')
            raise Exception(
                f'{self.config.exchange_name} Failed to parse the response {msg}')

        res = ExchangeOrders()

        res.exchange = self.config.exchange_name
        res.instrument = self.config.symbol

        for order_dict in msg_json['orders']:
            order = ExchangeOrder()

            order.quantity = order_dict['size']
            order.price = float(order_dict['price'])

            if order_dict['type'] == 'limit':
                order.type = OrderType.limit
            elif order_dict['type'] == 'market':
                order.type = OrderType.limit

            order.exchange_order_id = order_dict['order_id']

            if order_dict['side'] == 'buy':
                order.side = OrderSide.buy
                res.bids.append(order)
            else:
                order.side = OrderSide.sell
                res.asks.append(order)
        return res

    async def send_orders(self, orders):
        res = ApiResult()

        data = []
        for order in orders:
            side = ''
            if order.side == OrderSide.buy:
                side = 'buy'
            elif order.side == OrderSide.sell:
                side = 'sell'
            else:
                raise Exception(f'Unknown order side. Type = {order.side}')

            ord_type = ''
            if order.type == OrderType.mkt:
                ord_type = 'market'
            elif order.type == OrderType.limit:
                ord_type = 'limit'
            else:
                raise Exception(f'Unknown order type. Type = {order.type}')

            body = {
                'client_id': order.order_id,
                'contract_code': self.config.symbol,
                'type': ord_type,
                'side': side,
                'size': str(round(order.quantity, self.ROUNDING_QTY))
            }
            if order.type == OrderType.limit:
                body['price'] = str(order.price)
                body['post_only'] = True
            data.append(body)

        final_data = {
            'channel': 'trading',
            'type': 'request',
            'action': 'create-order',
            'data': data
        }

        self.logger.info(f'EMX sending new order request. Data: {final_data}')
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def amend_order(self, new_order, old_order):
        res = ApiResult()

        try:
            eid = self.shared_storage.uid_to_eid[old_order.order_id]
        except KeyError:
            self.logger.warning(
                f'Order id was not found for amend. Order id = {old_order.order_id}')
            return res

        ord_type = ''
        if new_order.type == OrderType.mkt:
            ord_type = 'market'
        elif new_order.type == OrderType.limit:
            ord_type = 'limit'
        else:
            raise Exception(f'Unknown order type. Type = {new_order.type}')

        if new_order.side != old_order.side:
            raise Exception(f'Wrong order side. Side = {new_order.side}')

        side = ''
        if new_order.side == OrderSide.buy:
            side = 'buy'
        elif new_order.side == OrderSide.sell:
            side = 'sell'
        else:
            raise Exception(f'Unknown order side. Type = {new_order.side}')

        body = {
            'type': ord_type,
            'side': side,
            'order_id': eid,
            'size': str(round(new_order.quantity, self.ROUNDING_QTY))
        }
        if new_order.type == OrderType.limit:
            body['price'] = str(new_order.price)

        final_data = {
            'channel': 'trading',
            'type': 'request',
            'action': 'modify-order',
            'data': body
        }

        self.logger.info(
            f'Sending amend request. New orderId = {new_order.order_id},'
            f' old order_id = {old_order.order_id}, Data: {final_data}')

        self.shared_storage.eid_to_uid[eid] = new_order.order_id
        self.shared_storage.uid_to_eid[new_order.order_id] = eid
        self.shared_storage.eids_to_amend[eid] = True

        self.logger.debug(f'Ids mapped during amend, eid = {eid}, uid = {new_order.order_id}')
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def amend_orders(self, new_orders, old_orders):
        res = ApiResult()

        data = []
        for new_order, old_order in zip(new_orders, old_orders):
            try:
                eid = self.shared_storage.uid_to_eid[old_order.order_id]
            except KeyError:
                self.logger.warning(
                    f'Order id was not found for amend. Order id = {old_order.order_id}')
                return res

            ord_type = ''
            if new_order.type == OrderType.mkt:
                ord_type = 'market'
            elif new_order.type == OrderType.limit:
                ord_type = 'limit'
            else:
                raise Exception(f'Unknown order type. Type = {new_order.type}')

            if new_order.side != old_order.side:
                raise Exception(f'Wrong order side. Side = {new_order.side}')

            side = ''
            if new_order.side == OrderSide.buy:
                side = 'buy'
            elif new_order.side == OrderSide.sell:
                side = 'sell'
            else:
                raise Exception(f'Unknown order side. Type = {new_order.side}')

            body = {
                'type': ord_type,
                'side': side,
                'order_id': eid,
                'size': str(round(new_order.quantity, self.ROUNDING_QTY))
            }
            if new_order.type == OrderType.limit:
                body['price'] = str(new_order.price)
            data.append(body)

            self.shared_storage.eid_to_uid[eid] = new_order.order_id
            self.shared_storage.uid_to_eid[new_order.order_id] = eid
            self.shared_storage.eids_to_amend[eid] = True
            self.logger.debug(f'Ids mapped during amend, eid = {eid}, uid = {new_order.order_id}')

        final_data = {
            'channel': 'trading',
            'type': 'request',
            'action': 'modify-order',
            'data': data
        }

        self.logger.info(f'Sending bulk amend request. Data: {final_data}')
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def send_order(self, order):
        res = ApiResult()

        side = ''
        if order.side == OrderSide.buy:
            side = 'buy'
        elif order.side == OrderSide.sell:
            side = 'sell'
        else:
            raise Exception(f'Unknown order side. Type = {order.side}')

        ord_type = ''
        if order.type == OrderType.mkt:
            ord_type = 'market'
        elif order.type == OrderType.limit:
            ord_type = 'limit'
        else:
            raise Exception(f'Unknown order type. Type = {order.type}')

        body = {
            'client_id': order.order_id,
            'contract_code': self.config.symbol if self.config.symbol else order.instrument_name,
            'type': ord_type,
            'side': side,
            'size': str(round(order.quantity, self.ROUNDING_QTY))
        }
        if order.type == OrderType.limit:
            body['price'] = str(order.price)

        final_data = {
            'channel': 'trading',
            'type': 'request',
            'action': 'create-order',
            'data': body
        }

        self.logger.info(
            f'EMX sending new order request. OrderId = {order.order_id}, Data: {final_data}')
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def cancel_order(self, order_id):
        res = ApiResult()
        try:
            eid = self.shared_storage.uid_to_eid[order_id]
        except KeyError:
            # Order elimination msg was received on the Streaming side
            self.logger.warning(f'Order was already removed. Order id = {order_id}')
            res.success = True
            return res

        body = {
            'order_id': eid
        }

        final_data = {
            'channel': 'trading',
            'type': 'request',
            'action': 'cancel-order',
            'data': body
        }

        self.logger.info(f'Sending cancellation request. eid = {eid}, data: {final_data}')
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            self.logger.warning('ConnectionError will be raised')
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def cancel_active_orders(self):
        res = ApiResult()

        body = {
            'contract_code': self.config.symbol
        }

        final_data = {
            'channel': 'trading',
            'type': 'request',
            'action': 'cancel-all-orders',
            'data': body
        }

        self.logger.info('Sending cancel all request')
        try:
            await self.ws.ws.send_str(json.dumps(final_data))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise ConnectionError(str(err))

        res.success = True
        return res

    async def cancel_orders(self, orders_ids):
        raise Exception('Multiple orders cancellation is not supported')
