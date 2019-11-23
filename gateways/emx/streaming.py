import time
import datetime

from logger import logging
from definitions import (
    tob, 
    exchange_orders,
    exchange_order,
    order_type,
    order_side,
    new_order_ack,
    new_order_nack,
    order_elim_ack,
    order_elim_nack,
    order_fill_ack,
    order_full_fill_ack,
    amend_ack,
    amend_nack,
    amend_ack_on_partial

)


class streaming_adapter():
    def __init__(self, config, auth, shared_storage):
        self.logger = logging.getLogger()

        self.config = config
        self.shared_storage = shared_storage

        self.symbol = self.config.symbol

        self.auth = auth

        self.subscribed = False
        self.orders_received = False

        self.events = {
            "order-received": self.process_new_received,
            "modify-received": self.process_amend_received,
            "cancel-received": self.process_cancel_received,
            "accepted": self.process_accept,
            "rejected": self.process_new_rejection,
            "modify-rejected": self.process_amend_rejection,
            "canceled": self.process_elim,
            "cancel-rejected": self.process_elim_reject,
            "filled":   self.process_fill,
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
        endpoint = "/v1/user/verify"
        signature = self.auth.generate_signature(timestamp, "GET", endpoint, None)

        msg = {
                "type": "subscribe",
                "channels": ["orders", "trading", "ticker"],
                "key": self.auth.api_key,
                "sig": signature.decode().strip(),
                "timestamp": timestamp
            }
        if self.config.symbol:
            msg["contract_codes"] = self.config.symbol
        else:
            msg["contract_codes"] = []
        return [msg]

    async def process(self, msg, msg_callback):
        self.logger.info("Emx streaming got a msg: {}".format(msg))

        if msg.get("type") == "subscriptions":
            self.subscribed = True
            self.logger.info("{}: Successfully subscribed".format(self.config.exchange_name))
            return

        if msg.get("type") == "snapshot" and msg.get("channel") == "orders":
            try:
                msg = msg["data"]
            except KeyError:
                raise Exception("Unable to parse data")

            try:
                res = self.process_active_orders(msg)
            except Exception as err:
                raise Exception("process_event raised an exception. Reason: {}".format(str(err)))

            if res:
                if msg_callback is None:
                    return
                await msg_callback(res)

            return

        if self.subscribed is False:
            self.logger.warning("emx app was not subscribed to order updates")
            return

        if msg.get("channel") == "positions":
            res = self.process_position_update(msg)
            if msg_callback is None or res is None:
                return

            await msg_callback(res)
            return
            

        if msg.get("channel") == "ticker":
            try:
                res = self.process_tick(msg)
            except KeyError as err:
                raise Exception("{} Ticker processing failed: {}".format(self.config.exchange_name, err))
            if res is None:
                self.logger.warning("process_tick returned None")
                return

            try:
                self.positions_mngr.set_mark_price(self.config.exchange_name, msg['data']['contract_code'], float(res.mark_price))
            except:
                pass

            if msg_callback is None:
                return
            await msg_callback(res)
            return

        if msg.get("channel") != "orders":
            self.logger.debug("Message is not about orders updates")
            return

        if msg.get("type") != "update":
            self.logger.info("No need to process since it is not an update")
            return

        action = msg.get("action")
        if action is None:
            self.logger.warning("No action was found")
            return

        process_event = self.events.get(action)
        if process_event is None:
            self.logger.warning("No {} callback found related to order updates".format(action))
            return

        try:
            msg = msg["data"]
        except KeyError:
            raise Exception("Unable to parse data")

        try:
            res = process_event(msg)
        except Exception as err:
            raise Exception("process_event raised an exception. Reason: {}".format(str(err)))

        if res:
            if msg_callback is None:
                self.logger.warning("No msg callback found for EMX adapter")
                return
            await msg_callback(res)

    def process_active_orders(self, msg):
        self.logger.info("process_active_orders started. Msg = {}".format(msg))
        self.orders_received = True

        orders_msg = exchange_orders()
        orders_msg.exchange = "emx"
        orders_msg.instrument = ""

        for elem in msg:
            exch_ord = exchange_order()
            exch_ord.instrument_name = elem["contract_code"]
            exch_ord.quantity = float(elem["size"])
            exch_ord.filled_quantity = float(elem["size_filled"])
            exch_ord.price = float(elem["price"])

            if elem["order_type"] != "market" and elem["order_type"] != "limit":
                raise Exception("Unable to get order type")
            exch_ord.type = order_type.mkt if elem["order_type"] == "market" else order_type.limit
            exch_ord.exchange_orderid = elem["order_id"]

            if elem["side"] == "sell":
                exch_ord.side = order_side.sell
                orders_msg.asks.append(exch_ord)
            else:
                exch_ord.side = order_side.buy
                orders_msg.bids.append(exch_ord)

        return orders_msg

    def process_position_update(self, msg):
        _pos = None
        if msg["type"] == "snapshot":
            for elem in msg["data"]:
                if elem["contract_code"] == self.symbol:
                    _pos = elem["quantity"]
                    break
        else:
            if msg["data"]["contract_code"] != self.symbol:
                return None
            _pos = msg["data"]["quantity"]

        if _pos is None:
            pos = position()
            pos.exchange = self.config.exchange_name
            pos.instrument_name = self.symbol
            pos.position = float(0)

            return pos

            # return None # TODO: Uncomment

        pos = position()
        pos.exchange = self.config.exchange_name
        pos.instrument_name = self.symbol
        pos.position = float(_pos)
        return pos

    def process_tick(self, msg):
        data = msg["data"]
        tb = tob()
        tb.exchange = "emx"
        tb.product = data["contract_code"]
        tb.best_bid_price = float(data["quote"]["bid"])
        tb.best_bid_qty = float(data["quote"]["bid_size"])
        tb.best_ask_price = float(data["quote"]["ask"])
        tb.best_ask_qty = float(data["quote"]["ask_size"])

        tb.timestamp = datetime.datetime.utcnow()
        return tb

    def process_new_received(self, msg):
        try:
            eid = msg["order_id"]
        except KeyError:
            raise Exception("Unable to parse eid")

        uid = "0"  # this is checked in strela/orders_manager
        try:
            uid = msg["client_id"]
        except KeyError:
            self.logger.warning("Got new_received, but unable to find uid for {}".format(msg["order_id"]))

        self.shared_storage.eid_to_uid[eid] = uid
        self.shared_storage.uid_to_eid[uid] = eid
        return None

    def process_amend_received(self, msg):
        pass

    def process_cancel_received(self, msg):
        self.logger.info("process_cancel_received is started")
        return None

    def process_accept(self, msg):
        if self.config.symbol and msg["contract_code"] not in self.config.symbol:
            self.logger.warning("emx msg for the wrong instrument. {}".format(msg))
            return

        try:
            eid = msg["order_id"]
        except KeyError:
            raise Exception("Unable to parse eid")

        uid = "0"
        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.debug("Got order process ack, but unable to find uid for {}".format(eid))

        if self.shared_storage.eids_to_amend.get(eid):
            if float(msg["size_filled"]) > 0:
                self.logger.info("amend_ack_on_partial will be created. Msg = {}".format(msg))

                ack = amend_ack_on_partial()
                ack.exchange = "emx"
                ack.instrument = msg["contract_code"]
                ack.orderid = uid
                ack.exchangeid = eid
                ack.fillid =  ""
                ack.order_type = msg['order_type']
                ack.side = msg['side']
                ack.order_qty = float(msg['size'])
                ack.price = float(msg["price"])
                ack.running_fill_qty = float(msg["size_filled"])
                ack.average_fill_price = float(msg['average_fill_price'])
                ack.timestamp = msg["timestamp"]
                ack.fee = float(msg['fill_fees'])
                return ack

            ack = amend_ack()
            del self.shared_storage.eids_to_amend[eid]
        else:
            ack = new_order_ack()

        if msg["side"] == "buy":
            ack.side = order_side.buy
        elif msg["side"] == "sell":
            ack.side = order_side.sell
        else:
            raise Exception("Unable to find an order side")

        if msg["order_type"] == "limit":
            ack.type = order_type.limit
        elif msg["order_type"] == "market":
            ack.type = order_type.mkt
        else:
            raise Exception("Unable to find an order type")

        ack.instrument_name = msg["contract_code"]
        ack.quantity = float(msg["size"])
        if ack.type == order_type.limit:
            ack.price = float(msg["price"])
        ack.orderid = uid
        # ack.timestamp = dateutil.parser.parse(msg["timestamp"]).timestamp()
        return ack

    def process_new_rejection(self, msg):
        if self.config.symbol and msg["contract_code"] not in self.config.symbol:
            self.logger.warning("emx msg for the wrong instrument. {}".format(msg))
            return

        self.logger.info("Emx received new rejection: {}".format(msg))
        try:
            eid = msg["order_id"]
        except KeyError:
            raise Exception("Unable to parse eid")

        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.warning("Got new order rejection, but unable to find uid for {}".format(eid))
            return

        nack = new_order_nack()
        nack.orderid = uid
        nack.exchange_orderid = eid
        nack.rejection_reason = msg.get("message")
        # nack.timestamp = dateutil.parser.parse(msg["timestamp"]).timestamp()
        return nack

    def process_amend_rejection(self, msg):
        if self.config.symbol and msg["contract_code"] not in self.config.symbol:
            self.logger.warning("emx msg for the wrong instrument. {}".format(msg))
            return

        self.logger.info("Emx received amend rejection: {}".format(msg))
        try:
            eid = msg["order_id"]
        except KeyError:
            raise Exception("{} Unable to parse eid".formate(self.config.exchange_name))

        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.warning("{} Got amend reject, but unable to find uid for {}".format(self.config.exchange_name, eid))
            return

        del self.shared_storage.eids_to_amend[eid]

        nack = amend_nack()
        nack.orderid = uid
        nack.rejection_reason = msg.get("message")
        # nack.timestamp = dateutil.parser.parse(msg["timestamp"]).timestamp()
        return nack

    def process_elim(self, msg):
        if self.config.symbol and msg["contract_code"] not in self.config.symbol:
            self.logger.warning("emx msg for the wrong instrument. {}".format(msg))
            return

        self.logger.info("Emx received elimination: {}".format(msg))
        try:
            eid = msg["order_id"]
        except KeyError:
            raise Exception("Unable to parse eid")
        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.warning("Got elim ack, but unable to find uid for {}".format(eid))
            return

        ack = order_elim_ack()
        ack.orderid = uid
        # ack.timestamp = dateutil.parser.parse(msg["timestamp"]).timestamp()
        return ack

    def process_elim_reject(self, msg):
        if self.config.symbol and msg["contract_code"] not in self.config.symbol:
            self.logger.warning("emx msg for the wrong instrument. {}".format(msg))
            return

        self.logger.info("Emx received elim rejection: {}".format(msg))
        try:
            eid = msg["order_id"]
        except KeyError:
            raise Exception("Unable to parse eid")
        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.warning("Got elim reject, but unable to find uid for {}".format(msg["order_id"]))
            return

        nack = order_elim_nack()
        nack.orderid = uid
        nack.rejection_reason = msg.get("message")
        # nack.timestamp = dateutil.parser.parse(msg["timestamp"]).timestamp()
        return nack

    def process_fill(self, msg):
        if self.config.symbol and msg["contract_code"] not in self.config.symbol:
            self.logger.warning("emx msg for the wrong instrument. {}".format(msg))
            return

        self.logger.debug("Emx streaming got a fill: {}".format(msg))

        try:
            status = msg["status"]
        except KeyError:
            raise Exception("Unable to parse message status")

        if status == "canceled":
            return

        try:
            eid = msg["order_id"]
        except KeyError:
            raise Exception("Unable to parse eid")

        uid = "0"
        try:
            uid = self.shared_storage.eid_to_uid[eid]
        except KeyError:
            self.logger.warning("Got a fill, but unable to find uid for {}".format(msg["order_id"]))

        timestamp_obj = datetime.datetime.strptime(msg["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
        if status == "done":
            ack = order_full_fill_ack()
            ack.exchange = "emx"
            ack.instrument = msg["contract_code"]
            ack.orderid = uid
            ack.exchangeid = eid
            ack.fillid = ack.exchangeid + '_' + msg["auction_code"]
            ack.order_type = msg['order_type']
            ack.side = msg['side']
            ack.order_qty = float(msg['size'])
            ack.price = float(msg["price"])
            ack.fill_price = float(msg["fill_price"])
            ack.running_fill_qty = float(msg["size_filled"])
            ack.incremental_fill_qty = float(msg["size_filled_delta"])  # incremental fill quantity
            ack.average_fill_price = float(msg['average_fill_price'])
            ack.timestamp = msg["timestamp"]
            ack.fee = float(msg['fill_fees_delta'])
        else:
            ack = order_fill_ack()
            ack.exchange = "emx"
            ack.instrument = msg["contract_code"]
            ack.orderid = uid
            ack.exchangeid = eid
            ack.fillid = ack.exchangeid + '_' + msg["auction_code"]
            ack.order_type = msg['order_type']
            ack.side = msg['side']
            ack.order_qty = float(msg['size'])
            ack.price = float(msg["price"])
            ack.fill_price = float(msg["fill_price"])
            ack.running_fill_qty = float(msg["size_filled"])
            ack.incremental_fill_qty = float(msg["size_filled_delta"])  # incremental fill quantity
            ack.average_fill_price = float(msg['average_fill_price'])
            ack.timestamp = msg["timestamp"]
            ack.fee = float(msg['fill_fees_delta'])

        t_diff = datetime.datetime.utcnow() - timestamp_obj
        self.logger.info("Took {} to receive a fill".format(t_diff.total_seconds()))
        return ack
