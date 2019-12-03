import uuid

from order_state import event
from order_state import (
    active,
    fill,
    full_fill,
    cancelled,
    inactive,
    insert_pending,
    amend_pending,
    cancel_pending,
    order_state,
    cancel_failed,
)

from definitions import (
    order_side,
    order_request,
    exchange_orders,
    new_order_ack,
    new_order_rejection,
    order_elim_ack,
    order_elim_rejection,
    order_fill_ack,
    order_full_fill_ack,
    amend_ack,
    amend_rejection,
    amend_ack_on_partial
)

from logger import logging


def generate_id():
    return str(uuid.uuid4())


def sort_orders(orders):
    return sorted(orders, key=lambda order: order.price)


class OrdersManager:
    MIN_VALUE = 10e-5
    PRICE_DIFF = 10e-5
    ORDERS_QTY_DIFF = 10e-10

    def __init__(self, exchange_adapter):
        self.exchange_adapter = exchange_adapter

        try:
            self.exchange_name = self.exchange_adapter.config.name
        except AttributeError:
            self.exchange_name = ""

        self.orders = {}
        self.live_orders_ids = []
        self.orders_states = {}
        self.order_id_to_order_id_map = {}
        self.order_to_event = {}
        self.order_to_strategy_type = {}
        self.ids_to_fills = {}

        self.update_type_to_state = {
            new_order_ack: event.on_insert_ack,
            new_order_rejection: event.on_insert_rejection,
            order_elim_ack: event.on_cancel_ack,
            order_elim_rejection: event.on_cancel_rejection,
            order_fill_ack: event.on_fill,
            order_full_fill_ack: event.on_full_fill,
            amend_ack: event.on_amend_ack,
            amend_ack_on_partial: event.on_amend_partial_ack,
            amend_rejection: event.on_amend_rejection,
        }

        self.logger = logging.getLogger()

    def reset(self):
        self.logger.info("{} orders manager will be reset".format(self.exchange_name))

        self.orders = {}
        self.live_orders_ids = []
        self.orders_states = {}

    async def place_order(self, order):
        if not order.order_id: order.order_id = generate_id()
        self.orders[order.order_id] = order
        self.update_order_state(order.order_id, event.on_creation)

        try:
            res = await self.exchange_adapter.send_order(order)
        except Exception as err:
            self.logger.error("Order placement failed, err {}".format(err))
            raise
        if res.success is False:
            self.logger.error("Order placement failed, err {}".format(res.msg))
            raise Exception("Orders placement failed, err {}".format(res.msg))
        self.live_orders_ids.append(order.order_id)

    async def place_orders(self, orders):
        if len(orders) == 0:
            self.logger.debug("No orders to place")
            return

        for elem in orders:
            if not elem.order_id:
                elem.order_id = generate_id()

        for elem in orders:
            self.orders[elem.order_id] = elem
            self.live_orders_ids.append(elem.order_id)
            self.update_order_state(elem.order_id, event.on_creation)

        try:
            res = await self.exchange_adapter.send_orders(orders)
        except Exception as err:
            self.logger.error("Bulk orders placement failed, err {}".format(err))
            raise
        if res.success is False:
            self.logger.error("Bulk orders placement failed, err {}".format(res.msg))
            raise Exception("Bulk orders placement failed, err {}".format(res.msg))
        self.logger.info("Multiple orders were placed")

    async def amend_order(self, new, existing):
        self.orders[new.order_id] = new

        self.update_order_state(new.order_id, event.on_creation)
        self.update_order_state(new.order_id, event.on_insert_ack)
        self.update_order_state(new.order_id, event.on_amend)

        try:
            res = await self.exchange_adapter.amend_order(new, existing)
        except Exception as err:
            self.logger.error("Order amend failed, err {}".format(err))
            raise
        if res.success is False:
            self.logger.error("Order amend failed, msg={}".format(res.msg))
            raise GatewayError("Orders amend failed, msg={}".format(res.msg))
        self.live_orders_ids.remove(existing.order_id)
        self.live_orders_ids.append(new.order_id)

    async def amend_active_orders(self, new_orders):
        try:
            existing_orders = [self.orders[oid] for oid in self.live_orders_ids]
        except KeyError as err:
            raise Exception("Failed to grad existing orders".format(err))
        await self.amend_orders(new_orders, existing_orders)

    async def _amend_orders(self, new_orders, existing_orders):
        if len(new_orders) == 0:
            self.logger.debug("No need to send a bulk amend, no orders to be amended")
            return

        new_place = sort_orders(new_orders)
        existing_orders = sort_orders(existing_orders)
        if len([i for i, k in zip(new_orders, existing_orders) if i.side != k.side]) > 0:
            self.logger.error("Invalid orders for the amend")
            raise Exception("Invalid orders for the amend")
        elif len(new_orders) != len(existing_orders):
            self.logger.error("Invalid orders for the amend; not matching sizes")
            raise Exception("Invalid orders for the amend; not matching sizes")
        for i in range(1, len(new_orders)):
            if new_orders[i].side is order_side.buy and new_orders[i - 1].side is order_side.sell:
                raise Exception("Self crossing orders detected")

        try:
            new_bid = [order.price for order in new_orders if order.side is order_side.buy][-1]
            existing_ask = [order.price for order in existing_orders if order.side is order_side.sell][0]
            if new_bid > existing_ask:
                new_orders.reverse()
                existing_orders.reverse()
        except IndexError:
            pass

        for new, existing in zip(new_orders, existing_orders):
            new.order_id = existing.order_id
            self.orders[new.order_id] = new

            try:
                res = await self.exchange_adapter.amend_orders(new_orders, existing_orders)
            except Exception as err:
                self.logger.error("Orders amend failed, err {}".format(err))
                raise

        for order in existing_orders: self.live_orders_ids.remove(order.order_id)
        for order in new_orders:
            self.live_orders_ids.append(order.order_id)
            self.orders[order.order_id] = order
            self.update_order_state(order.order_id, event.on_amend)

    async def amend_orders(self, new_orders, existing_orders):
        for elem in new_orders:
            if not elem.order_id:
                # note that order_id can be regenerated later in the method
                elem.order_id = generate_id()

        new_orders = sort_orders(new_orders)
        existing_orders = sort_orders(existing_orders)

        pairs_to_amend = {}
        orders_to_place = []
        orders_ids_to_cancel = []

        if len(existing_orders) == 0:
            orders_to_place = new_orders

        for new, existing in zip(new_orders, existing_orders):
            if new.side != existing.side:
                self.logger.warning("Order sides are not the same. Orderid {}".format(existing.order_id))

            existing_state = None
            try:
                existing_state = self.orders_states[existing.order_id].state
            except KeyError:
                self.logger.debug("Order status was not found. Orderid {}".format(existing.order_id))

            if isinstance(existing_state, fill):
                orders_to_place.append(new)
                orders_ids_to_cancel.append(existing.order_id)
            elif isinstance(existing_state, cancelled) or isinstance(existing_state, full_fill):
                self.live_orders_ids.remove(existing.order_id)
                try:
                    del self.ids_to_fills[existing.order_id]
                except KeyError:
                    self.logger.info("Order {} was not found in ids_to_fills".format(existing.order_id))
                except Exception:
                    self.logger.info("Order {} failed to be removed from ids_to_fills".format(existing.order_id))

                orders_to_place.append(new)
            elif isinstance(existing_state, cancelled) or isinstance(existing_state, full_fill):
                self.live_orders_ids.remove(existing.order_id)
                orders_to_place.append(new)
            elif isinstance(existing_state, active):
                if abs(new.quantity - existing.quantity) < self.ORDERS_QTY_DIFF and \
                        abs(new.price - existing.price) < self.ORDERS_QTY_DIFF:
                    self.logger.debug("Order {} will be ignored, no need to amend".format(new.order_id))
                    self.live_orders_ids = [oid for oid in self.live_orders_ids if oid != existing.order_id]

                    new.order_id = existing.order_id
                    self.live_orders_ids.append(new.order_id)
                    self.orders[new.order_id] = new
                else:
                    pairs_to_amend[new] = existing

        try:
            await self.cancel_orders(orders_ids_to_cancel)
            await self.place_orders(orders_to_place)
            await self._amend_orders(list(pairs_to_amend.keys()), list(pairs_to_amend.values()))
        except Exception as err:
            self.logger.error("Amend logic failed {}".format(err))
            raise

    async def cancel_order(self, order_id):
        self.update_order_state(order_id, event.on_cancel)

        try:
            res = await self.exchange_adapter.cancel_order(order_id)
        except Exception as err:
            self.logger.error("Order cancellation failed, err {}".format(err))
            raise
        if res.success is False:
            self.logger.error("Orders cancellation failed, msg={}".format(res.msg))
            raise GatewayError("Orders cancellation failed, msg={}".format(res.msg))
        try:
            self.live_orders_ids.remove(order_id)
        except ValueError:
            pass
        self.logger.debug("Order was cancelled. Orderid = {}".format(order_id))

    async def cancel_orders(self, order_ids):
        try:
            order_ids = [oid for oid in order_ids if isinstance(self.orders_states[oid], full_fill) is False]
        except KeyError as err:
            self.logger.error("Failed to find an order for the cancelation {}".format(err))
            return

        if len(order_ids) == 0:
            return

        self.live_orders_ids = [oid for oid in self.live_orders_ids if oid not in order_ids]
        for oid in order_ids:
            try:
                res = await self.cancel_order(oid)
            except Exception as err:
                self.logger.error("Order cancellation failed, err {}".format(err))
                raise

    async def cancel_active_orders(self):
        try:
            res = await self.exchange_adapter.cancel_active_orders()
        except Exception as err:
            self.logger.error("Orders active orders cancellation failed, err {}".format(err))
            raise

    def is_ready_for_amend(self, order_id):
        return not isinstance(self.orders_states[order_id].state,
                              (
                                  inactive,
                                  insert_pending,
                                  amend_pending,
                                  cancel_failed,
                                  cancel_pending
                              )
                              )

    def update_order_state(self, order_id, upd_event):
        if isinstance(upd_event, event) is False:
            _upd_event = self.update_type_to_state[upd_event.__class__]
        else:
            _upd_event = upd_event

        if _upd_event is event.on_creation:
            curr_state = self.orders_states[order_id] = order_state()
        else:
            try:
                curr_state = self.orders_states[order_id]
            except KeyError:
                self.logger.warning("{} Order state was not found for order_id = {}".format(
                    self.exchange_name, order_id))
                return

        if _upd_event == event.on_full_fill:
            _order = self.orders.get(upd_event.order_id)
            if _order:
                if _order.quantity > upd_event.running_fill_qty:
                    self.logger.warning(
                        "Inflight partial fill was detected. Recorded order {}, full_fill {}".format(_order, upd_event))
                    _upd_event = event.on_fill
        elif _upd_event == event.on_fill:
            try:
                self.ids_to_fills[upd_event.order_id] = upd_event
            except AttributeError:
                pass

        try:
            curr_state.on_event(_upd_event)
        except StateException as err:
            self.logger.exception("{} Invalid state. Order id = {}".format(self.exchange_name, order_id))
            raise Exception(
                "{}. Invalid state. Order id = {}. Reason = {}".format(
                    self.exchange_name, order_id, str(err))
            )

    def activate_orders(self, orders_msg):
        self.logger.info("activate_orders started, orders_msg: {}".format(orders_msg))

        if isinstance(orders_msg, exchange_orders):
            exch_orders = sort_orders(orders_msg.bids + orders_msg.asks)
        else:
            exch_orders = orders_msg

        orders = []
        for exch_order in exch_orders:
            order = order_request()
            order.side = exch_order.side
            order.type = exch_order.type
            order.price = exch_order.price
            order.quantity = exch_order.quantity
            orders.append(order)

        for elem, exch_elem in zip(orders, exch_orders):
            elem.order_id = generate_id()
            self.exchange_adapter.storage.uid_to_eid[elem.order_id] = exch_elem.exchange_order_id
            self.exchange_adapter.storage.eid_to_uid[exch_elem.exchange_order_id] = elem.order_id

            self.orders[elem.order_id] = elem
            self.live_orders_ids.append(elem.order_id)

            self.update_order_state(elem.order_id, event.on_creation)
            self.update_order_state(elem.order_id, event.on_insert_ack)
            self.update_order_state(elem.order_id, event.on_amend)
            self.update_order_state(elem.order_id, event.on_amend_ack)

            if exch_elem.filled_quantity > 0.0:
                _fill = order_fill_ack()
                _fill.instrument = ""
                _fill.order_id = elem.order_id
                _fill.exchange_id = exch_elem.exchange_order_id
                _fill.running_fill_qty = exch_elem.filled_quantity
                _fill.incremental_fill_qty = exch_elem.filled_quantity
                _fill.order_qty = exch_elem.quantity

                self.ids_to_fills[elem.order_id] = _fill
                self.update_order_state(elem.order_id, event.on_fill)

        return orders

    def active_orders_ids(self):
        return [oid for oid in self.live_orders_ids if
                isinstance(self.orders_states[oid].state, active) or isinstance(self.orders_states[oid].state, fill)]

    def get_number_of_active_orders(self):
        return len(self.active_orders_ids())

    def get_exch_order_id(self, client_order_id):
        return self.exchange_adapter.get_exch_order_id(client_order_id)

    def get_number_of_ready_for_amend(self):
        return len([oid for oid in self.live_orders_ids if self.is_ready_for_amend(oid) is True])

    def get_live_orders(self):
        return [self.orders[oid] for oid in self.live_orders_ids]

    def connect_orders(self, order_id1, order_id2):
        self.order_id_to_order_id_map[order_id1] = order_id2
        self.order_id_to_order_id_map[order_id2] = order_id1

    def get_mapped_order(self, order_id):
        return self.order_id_to_order_id_map[order_id]
