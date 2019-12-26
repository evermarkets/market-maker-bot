import time
import datetime

from market_maker.logger import logging
from market_maker.definitions import (
    TopOfBook,
    ExchangeOrders,
    ExchangeOrder,
    OrderType,
    OrderSide,
    NewOrderAcknowledgement,
    NewOrderRejection,
    OrderEliminationAcknowledgement,
    OrderEliminationRejection,
    OrderFillAcknowledgement,
    OrderFullFillAcknowledgement,
    AmendAcknowledgement,
    AmendRejection,
    AmendAcknowledgementPartial,
    Position
)


class StreamingAdapter:
    def __init__(self, config, auth, shared_storage):
        self.logger = logging.getLogger()

        self.config = config
        self.shared_storage = shared_storage

        self.symbol = self.config.symbol

        self.auth = auth

        self.subscribed = False
        self.orders_received = False

        self.events = {
            'order-received': self.process_new_received,
            'modify-received': self.process_amend_received,
            'cancel-received': self.process_cancel_received,
            'accepted': self.process_accept,
            'rejected': self.process_new_rejection,
            'modify-rejected': self.process_amend_rejection,
            'canceled': self.process_elimination,
            'cancel-rejected': self.process_elimination_reject,
            'filled': self.process_fill,
        }

        if self.symbol is None:
            self.config.symbol = None
        elif type(self.config.symbol) is not list:
            self.config.symbol = [self.config.symbol]

    def is_ready(self):
        return self.subscribed is True and self.orders_received is True

    def reset(self):
        # self.subscribed = False
        self.orders_received = False
        return

    def get_sub_url(self):
        return self.config.url

    def get_sub_params(self):
        timestamp = str(round(time.time()))
        endpoint = '/v1/user/verify'
        signature = self.auth.generate_signature(timestamp, 'GET', endpoint, None)

        msg = {
            'type': 'subscribe',
            'channels': ['orders', 'trading', 'ticker', 'positions'],
            'key': self.auth.api_key,
            'sig': signature.decode().strip(),
            'timestamp': timestamp
        }
        if self.config.symbol:
            msg['contract_codes'] = self.config.symbol
        else:
            msg['contract_codes'] = []
        return [msg]

    async def process(self, msg, msg_callback):
        self.logger.info(f'Emx streaming got a msg: {msg}')

        if msg.get('type') == 'subscriptions':
            self.subscribed = True
            self.logger.info(f'{self.config.exchange_name}: Successfully subscribed')
            return

        if msg.get('type') == 'snapshot' and msg.get('channel') == 'orders':
            try:
                msg = msg['data']
            except KeyError:
                raise Exception('Unable to parse data')

            try:
                res = self.process_active_orders(msg)
            except Exception as err:
                raise Exception(f'process_event raised an exception. Reason: {err}')

            if res:
                if msg_callback is None:
                    return
                await msg_callback(res)

            return

        if self.subscribed is False:
            self.logger.warning('emx app was not subscribed to order updates')
            return

        if msg.get('channel') == 'positions':
            res = self.process_position_update(msg)
            if msg_callback is None or res is None:
                return

            await msg_callback(res)
            return

        if msg.get('channel') == 'ticker':
            try:
                res = self.process_tick(msg)
            except KeyError as error:
                raise Exception(f'{self.config.exchange_name} Ticker processing failed: {error}')
            if res is None:
                self.logger.warning('process_tick returned None')
                return

            try:
                self.positions_mngr.set_mark_price(self.config.exchange_name,
                                                   msg['data']['contract_code'],
                                                   float(res.mark_price))
            except Exception:
                pass

            if msg_callback is None:
                return
            await msg_callback(res)
            return

        if msg.get('channel') != 'orders':
            self.logger.debug('Message is not about orders updates')
            return

        if msg.get('type') != 'update':
            self.logger.info('No need to process since it is not an update')
            return

        action = msg.get('action')
        if action is None:
            self.logger.warning('No action was found')
            return

        process_event = self.events.get(action)
        if process_event is None:
            self.logger.warning(f'No {action} callback found related to order updates')
            return

        try:
            msg = msg['data']
        except KeyError:
            raise Exception('Unable to parse data')

        try:
            res = process_event(msg)
        except Exception as err:
            raise Exception(f'process_event raised an exception. Reason: {err}')

        if res:
            if msg_callback is None:
                self.logger.warning('No msg callback found for EMX adapter')
                return
            await msg_callback(res)

    def process_active_orders(self, msg):
        self.logger.info(f'process_active_orders started. Msg = {msg}')
        self.orders_received = True

        orders_msg = ExchangeOrders()
        orders_msg.exchange = 'emx'
        orders_msg.instrument = ''

        for elem in msg:
            exchange_order_ = ExchangeOrder()
            exchange_order_.instrument_name = elem['contract_code']
            exchange_order_.quantity = float(elem['size'])
            exchange_order_.filled_quantity = float(elem['size_filled'])
            exchange_order_.price = float(elem['price'])

            if elem['order_type'] != 'market' and elem['order_type'] != 'limit':
                raise Exception('Unable to get order type')
            if elem['order_type'] == 'market':
                exchange_order_.type = OrderType.mkt
            else:
                exchange_order_.type = OrderType.limit
            exchange_order_.exchange_order_id = elem['order_id']

            if elem['side'] == 'sell':
                exchange_order_.side = OrderSide.sell
                orders_msg.asks.append(exchange_order_)
            else:
                exchange_order_.side = OrderSide.buy
                orders_msg.bids.append(exchange_order_)

        return orders_msg

    def process_position_update(self, msg):
        _pos = None
        if msg['type'] == 'snapshot':
            for elem in msg['data']:
                if elem['contract_code'] == self.symbol:
                    _pos = elem['quantity']
                    break
        else:
            if msg['data']['contract_code'] != self.symbol:
                return None
            _pos = msg['data']['quantity']

        if _pos is None:
            self.logger.info(f'Were not able to parse position update {msg}')
            return None

        pos = Position()
        pos.exchange = self.config.exchange_name
        pos.instrument_name = self.symbol
        pos.position = float(_pos)
        return pos

    def process_tick(self, msg):
        data = msg['data']
        tb = TopOfBook()
        tb.exchange = 'emx'
        tb.product = data['contract_code']
        tb.best_bid_price = float(data['quote']['bid'])
        tb.best_bid_qty = float(data['quote']['bid_size'])
        tb.best_ask_price = float(data['quote']['ask'])
        tb.best_ask_qty = float(data['quote']['ask_size'])

        tb.timestamp = datetime.datetime.utcnow()
        return tb

    def process_new_received(self, msg):
        try:
            eid = msg['order_id']
        except KeyError:
            raise Exception('Unable to parse eid')

        uid = '0'  # this is checked in strela/orders_manager
        try:
            uid = msg['client_id']
        except KeyError:
            self.logger.warning(f'Got new_received, but unable to find uid for {msg["order_id"]}')

        self.shared_storage.eid_to_uid[eid] = uid
        self.shared_storage.uid_to_eid[uid] = eid
        return None

    def process_amend_received(self, msg):
        pass

    def process_cancel_received(self, msg):
        self.logger.info('process_cancel_received is started')
        return None

    def process_accept(self, msg):
        if self.config.symbol and msg['contract_code'] not in self.config.symbol:
            self.logger.warning(f'emx msg for the wrong instrument. {msg}')
            return

        try:
            eid = msg['order_id']
        except KeyError:
            raise Exception('Unable to parse eid')

        uid = '0'
        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.debug(f'Got order process ack, but unable to find uid for {eid}')

        if self.shared_storage.eids_to_amend.get(eid):
            if float(msg['size_filled']) > 0:
                self.logger.info(f'amend_ack_on_partial will be created. Msg = {eid}')

                ack = AmendAcknowledgementPartial()
                ack.exchange = 'emx'
                ack.instrument = msg['contract_code']
                ack.order_id = uid
                ack.exchange_id = eid
                ack.fill_id = ''
                ack.order_type = msg['order_type']
                ack.side = msg['side']
                ack.order_qty = float(msg['size'])
                ack.price = float(msg['price'])
                ack.running_fill_qty = float(msg['size_filled'])
                ack.average_fill_price = float(msg['average_fill_price'])
                ack.timestamp = msg['timestamp']
                ack.fee = float(msg['fill_fees'])
                return ack

            ack = AmendAcknowledgement()
            del self.shared_storage.eids_to_amend[eid]
        else:
            ack = NewOrderAcknowledgement()

        if msg['side'] == 'buy':
            ack.side = OrderSide.buy
        elif msg['side'] == 'sell':
            ack.side = OrderSide.sell
        else:
            raise Exception('Unable to find an order side')

        if msg['order_type'] == 'limit':
            ack.type = OrderType.limit
        elif msg['order_type'] == 'market':
            ack.type = OrderType.mkt
        else:
            raise Exception('Unable to find an order type')

        ack.instrument_name = msg['contract_code']
        ack.quantity = float(msg['size'])
        if ack.type == OrderType.limit:
            ack.price = float(msg['price'])
        ack.order_id = uid
        return ack

    def process_new_rejection(self, msg):
        if self.config.symbol and msg['contract_code'] not in self.config.symbol:
            self.logger.warning(f'emx msg for the wrong instrument. {msg}')
            return

        self.logger.info(f'Emx received new rejection: {msg}')
        try:
            eid = msg['order_id']
        except KeyError:
            raise Exception('Unable to parse eid')

        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.warning(f'Got new order rejection, but unable to find uid for {eid}')
            return

        rejection = NewOrderRejection()
        rejection.order_id = uid
        rejection.exchange_order_id = eid
        rejection.rejection_reason = msg.get('message')
        return rejection

    def process_amend_rejection(self, msg):
        if self.config.symbol and msg['contract_code'] not in self.config.symbol:
            self.logger.warning(f'emx msg for the wrong instrument. {msg}')
            return

        self.logger.info(f'Emx received amend rejection: {msg}')
        try:
            eid = msg['order_id']
        except KeyError:
            raise Exception('f{self.config.exchange_name} Unable to parse eid')

        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.warning(
                f'{self.config.exchange_name} Got amend reject, but unable to find uid for {eid}')
            return

        del self.shared_storage.eids_to_amend[eid]

        rejection = AmendRejection()
        rejection.order_id = uid
        rejection.rejection_reason = msg.get('message')
        return rejection

    def process_elimination(self, msg):
        if self.config.symbol and msg['contract_code'] not in self.config.symbol:
            self.logger.warning(f'emx msg for the wrong instrument. {msg}')
            return None

        self.logger.info(f'Emx received elimination: {msg}')
        try:
            eid = msg['order_id']
        except KeyError:
            raise Exception('Unable to parse eid')
        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.warning(f'Got elim ack, but unable to find uid for {eid}')
            return None

        if float(msg['size']) == float(msg['size_filled']):
            self.logger.warning(f'Got full filled order elimination')
            return None

        ack = OrderEliminationAcknowledgement()
        ack.order_id = uid
        return ack

    def process_elimination_reject(self, msg):
        if self.config.symbol and msg['contract_code'] not in self.config.symbol:
            self.logger.warning(f'emx msg for the wrong instrument. {msg}')
            return

        self.logger.info(f'Emx received elim rejection: {msg}')
        try:
            eid = msg['order_id']
        except KeyError:
            raise Exception('Unable to parse eid')
        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.warning(f'Got elim reject, but unable to find uid for {msg["order_id"]}')
            return

        rejection = OrderEliminationRejection()
        rejection.order_id = uid
        rejection.rejection_reason = msg.get('message')
        return rejection

    def process_fill(self, msg):
        if self.config.symbol and msg['contract_code'] not in self.config.symbol:
            self.logger.warning(f'emx msg for the wrong instrument. {msg}')
            return

        self.logger.debug(f'Emx streaming got a fill: {msg}')

        try:
            status = msg['status']
        except KeyError:
            raise Exception('Unable to parse message status')

        if status == 'canceled':
            return

        try:
            eid = msg['order_id']
        except KeyError:
            raise Exception('Unable to parse eid')

        uid = '0'
        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.warning(f'Got a fill, but unable to find uid for {msg["order_id"]}')

        timestamp_obj = datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        if status == 'done':
            ack = OrderFullFillAcknowledgement()
            ack.exchange = 'emx'
            ack.instrument = msg['contract_code']
            ack.order_id = uid
            ack.exchange_id = eid
            ack.fill_id = ack.exchange_id + '_' + msg['auction_code']
            ack.order_type = msg['order_type']
            ack.side = msg['side']
            ack.order_qty = float(msg['size'])
            ack.price = float(msg['price'])
            ack.fill_price = float(msg['fill_price'])
            ack.running_fill_qty = float(msg['size_filled'])
            ack.incremental_fill_qty = float(msg['size_filled_delta'])  # incremental fill quantity
            ack.average_fill_price = float(msg['average_fill_price'])
            ack.timestamp = msg['timestamp']
            ack.fee = float(msg['fill_fees_delta'])
        else:
            ack = OrderFillAcknowledgement()
            ack.exchange = 'emx'
            ack.instrument = msg['contract_code']
            ack.order_id = uid
            ack.exchange_id = eid
            ack.fill_id = ack.exchange_id + '_' + msg['auction_code']
            ack.order_type = msg['order_type']
            ack.side = msg['side']
            ack.order_qty = float(msg['size'])
            ack.price = float(msg['price'])
            ack.fill_price = float(msg['fill_price'])
            ack.running_fill_qty = float(msg['size_filled'])
            ack.incremental_fill_qty = float(msg['size_filled_delta'])  # incremental fill quantity
            ack.average_fill_price = float(msg['average_fill_price'])
            ack.timestamp = msg['timestamp']
            ack.fee = float(msg['fill_fees_delta'])

        t_diff = datetime.datetime.utcnow() - timestamp_obj
        self.logger.info(f'Took {t_diff.total_seconds()} to receive a fill')
        return ack
