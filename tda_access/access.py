"""
this module contains functionality related to account information.
Pulls account data which can be used for creating orders or
analyzing order/balance/position history

TODO order history?
"""

from __future__ import annotations
import asyncio
import datetime
import authlib
from authlib.integrations.base_client import OAuthError
from tda.orders.common import OrderType, Duration, Session
# from tda.orders.generic import OrderBuilder
import abstract_broker.abstract as abstract_access
# import credentials
import tda
import selenium.webdriver
import pandas as pd
import tda_access.tdargs as tdargs
from dataclasses import dataclass, field
import typing as t

import tda.orders.equities as toe
import json

import tda_access.utils as utils

OrderStatus = tda.client.Client.Order.Status

# decode price history interval to appropriate function call


def _handle_raw_price_history(resp, symbol) -> pd.DataFrame:
    """
    attempt to translate price history response to a dataframe
    try to raise meaningful exception if there is an issue
    """
    try:
        history = resp.json()
    except json.decoder.JSONDecodeError:
        raise utils.EmptyDataError

    if history.get("candles", None) is None:
        error = history.get("error", None)
        if error is None and history.get("fault", None) is not None:
            raise utils.FaultReceivedError(
                f"tda responded with fault at {symbol}: {error}"
            )
        if error == "Not Found":
            print(f"td api could not find symbol {symbol}")
            raise utils.TickerNotFoundError(
                f"td api could not find symbol {symbol}"
            )
        elif error is None:
            raise Exception
        else:
            raise utils.EmptyDataError(
                f"No data received for symbol {symbol}"
            )

    if history["empty"] is True:
        raise utils.EmptyDataError(f"No data received for symbol {symbol}")

    df = pd.DataFrame(history["candles"])

    # datetime given in ms, convert to readable date
    df.datetime = pd.to_datetime(df.datetime, unit="ms")

    # rename datetime to time for finplot compatibility
    df = df.rename(columns={"datetime": "time"})
    df.index = df.time

    df = df[
        ["open", "high", "close", "low"]
    ]

    return df


def easy_get_price_history(client, symbol: str, interval: int):
    """
    Note: must be pure function otherwise subprocess will error out
    get price history with simplified inputs, max data retrieved if no start/end specified
    :param client:
    :param symbol: ticker symbol
    :param interval: minute interval: 1, 5, 10, 15, 30
    :return:
    """
    price_interval_lookup = {
        1: client.get_price_history_every_minute,
        5: client.get_price_history_every_five_minutes,
        10: client.get_price_history_every_ten_minutes,
        15: client.get_price_history_every_fifteen_minutes,
        30: client.get_price_history_every_thirty_minutes,
        # '1d': self._client.get_price_history_every_day,
        # '1w': self._client.get_price_history_every_week,
    }

    try:
        # not sure why argument is unexpected
        resp = price_interval_lookup[interval](symbol)
    except OAuthError:
        raise utils.EmptyDataError
    return _handle_raw_price_history(resp, symbol)


def parse_orders(orders: t.List[t.Dict]) -> t.Dict[int, t.Dict]:
    return {order["orderId"]: order for order in orders}


def configure_stream(
    stream_client: tda.streaming.StreamClient,
    add_book_handler: t.Callable,
    book_subs,
    symbols: t.List[str],
):
    """
    :param stream_client:
    :param add_book_handler:
    :param book_subs:
    :param symbols:
    :return:
    """

    async def _initiate_stream(handlers: t.List[t.Callable[[t.Dict], None]]):
        await stream_client.login()
        await stream_client.quality_of_service(
            tda.streaming.StreamClient.QOSLevel.EXPRESS
        )

        for handler in handlers:
            add_book_handler(handler)

        await book_subs(symbols)

        while True:
            await stream_client.handle_message()

    return _initiate_stream


def give_attribute(new_attr: str, value: t.Any) -> t.Callable:
    """
    gives function an attribute upon declaration
    :param new_attr: attribute name to add to function attributes
    :param value: value to initialize attribute to
    :return: decorator that will give the decorated function an attribute named
    after the input new_attr
    """

    def decorator(function: t.Callable) -> t.Callable:
        setattr(function, new_attr, value)
        return function

    return decorator


@dataclass
class OrderData:
    OPEN_ORDER = {
        utils.Side.LONG: lambda sym, qty, _: toe.equity_buy_market(sym, qty),
        utils.Side.SHORT: lambda sym, qty, _: toe.equity_sell_short_market(
            sym, qty
        ),
    }

    CLOSE_ORDER = {
        utils.Side.LONG: lambda sym, qty, _: toe.equity_sell_market(sym, qty),
        utils.Side.SHORT: lambda sym, qty, _: toe.equity_buy_to_cover_market(
            sym, qty
        ),
    }

    OPEN_STOP = {
        utils.Side.LONG: lambda sym, qty, stop_price: (
            toe.equity_sell_market(sym, qty)
            .set_order_type(OrderType.STOP)
            .set_stop_price(stop_price)
            .set_duration(Duration.GOOD_TILL_CANCEL)
            .set_session(Session.SEAMLESS)
        ),
        utils.Side.SHORT: lambda sym, qty, stop_price: (
            toe.equity_buy_to_cover_market(sym, qty)
            .set_order_type(OrderType.STOP)
            .set_stop_price(stop_price)
            .set_duration(Duration.GOOD_TILL_CANCEL)
            .set_session(Session.SEAMLESS)
        ),
    }
    NEW_OPEN_STOP = {
        utils.Side.LONG: lambda sym, qty, stop_price, stop_type: (
            toe.equity_sell_market(sym, qty)
            .set_order_type(stop_type)
            .set_stop_price(stop_price)
            .set_duration(Duration.GOOD_TILL_CANCEL)
            .set_session(Session.SEAMLESS)
        ),
        utils.Side.SHORT: lambda sym, qty, stop_price, stop_type: (
            toe.equity_buy_to_cover_market(sym, qty)
            .set_order_type(stop_type)
            .set_stop_price(stop_price)
            .set_duration(Duration.GOOD_TILL_CANCEL)
            .set_session(Session.SEAMLESS)
        ),
    }

    ORDER_DICT = t.Dict[utils.Side, t.Callable]

    name: str
    direction: utils.Side
    quantity: int
    stop_loss: t.Union[float, None] = field(default=None)
    status: OrderStatus = field(default=None)
    size_remaining_pct: float = field(default=1)

    def __post_init__(self):
        self.quantity = max(self.quantity, 0)


class Position(abstract_access.AbstractPosition):
    @classmethod
    def init_existing_position(cls, raw_position):
        """
        initialize Position from data retrieved via api call
        :param raw_position:
        :return:
        """
        short_qty = raw_position["shortQuantity"]
        long_qty = raw_position["longQuantity"]
        if short_qty > 0:
            qty = short_qty
            side = utils.Side.SHORT
        else:
            qty = long_qty
            side = utils.Side.LONG
        new_position = cls(
            symbol=raw_position["instrument"]["symbol"],
            qty=qty,
            side=side,
            raw_position=raw_position,
        )
        new_position.value = raw_position["marketValue"]

    def _stop_order(self):
        return OrderData.NEW_OPEN_STOP[self.side](
            self._symbol, self.qty, self._stop_value, self._stop_type
        )

    def _open(self, quantity):
        return OrderData.OPEN_ORDER[self._side](self._symbol, quantity, None)

    def _close(self, quantity):
        return OrderData.CLOSE_ORDER[self._side](self._symbol, quantity, None)


@dataclass
class AccountInfo:
    acct_data_raw: t.Dict
    equity: float = field(init=False)
    liquid_funds: float = field(init=False)
    buy_power: float = field(init=False)
    _positions: t.Dict[str, Position] = field(init=False)
    _pending_orders: t.Dict[int, t.Dict] = field(init=False)

    def __post_init__(self):
        cur_balance = self.acct_data_raw["securitiesAccount"]["currentBalances"]
        self.equity = cur_balance["equity"]
        self.liquid_funds = cur_balance["moneyMarketFund"] + cur_balance["cashBalance"]
        self.buy_power = cur_balance["buyingPower"]

        raw_positions = self.acct_data_raw["securitiesAccount"].get("positions", dict())
        self._positions = {}
        for pos in raw_positions:
            # don't add position if it is money_market
            if pos["instrument"]["cusip"] != "9ZZZFD104":
                short_qty = pos["shortQuantity"]
                long_qty = pos["longQuantity"]
                if short_qty > 0:
                    qty = short_qty
                    side = utils.Side.SHORT
                else:
                    qty = long_qty
                    side = utils.Side.LONG

                self._positions[pos["instrument"]["symbol"]] = Position(
                    raw_position=pos,
                    symbol=pos["instrument"]["symbol"],
                    qty=qty,
                    side=utils.Side(side),
                )
        # self._pending_orders = self._parse_order_statuses()

    @property
    def positions(self) -> t.Dict[str, Position]:
        return self._positions

    @property
    def raw_orders(self):
        return self.acct_data_raw["securitiesAccount"]["orderStrategies"]

    # @property
    # def orders(self) -> t.Dict[int, t.Dict]:
    #     return self._pending_orders

    def get_position_info(self, symbol: str) -> t.Union[Position, None]:
        return self._positions.get(symbol, None)

    def get_symbols(self) -> t.List:
        return [symbol for symbol, _ in self._positions.items()]

    # def _parse_order_statuses(self) -> t.Dict[int, t.Dict]:
    #     """for convenient lookup of order status"""
    #     raw_orders = self.acct_data_raw['securitiesAccount']['orderStrategies']
    #     return parse_orders(raw_orders)


# class _LocalClientMeta(type):
#     _cached_account_info: t.Union[None, AccountInfo] = None
#     _cached_orders: t.List[t.Dict] = None
#
#     TDA_CLIENT: tda.client.Client = tda.auth.easy_client(
#         webdriver_func=selenium.webdriver.Firefox, **credentials.CLIENT_PARAMS
#     )
#     STREAM_CLIENT: tda.streaming.StreamClient = tda.streaming.StreamClient(
#         client=TDA_CLIENT, account_id=_ACCOUNT_ID
#     )
#
#     _stream_data = []
#
#     def account_info(cls, cached=False) -> AccountInfo:
#         if cached is False or cls._cached_account_info is None:
#
#             # dump account data to txt for reference
#             while True:
#                 try:
#                     resp = LocalClient.TDA_CLIENT.get_account(
#                         account_id=_ACCOUNT_ID,
#                         fields=[
#                             tda.client.Client.Account.Fields.ORDERS,
#                             tda.client.Client.Account.Fields.POSITIONS,
#                         ],
#                     )
#                 except authlib.integrations.base_client.errors.OAuthError as e:
#                     print(e)
#                     continue
#
#                 account_info_raw = resp.json()
#                 try:
#                     cls._cached_account_info = AccountInfo(account_info_raw)
#                 except (KeyError, json.decoder.JSONDecodeError):
#                     pass
#                 else:
#                     with open("account_data.json", "w") as outfile:
#                         json.dump(account_info_raw, outfile, indent=4)
#                     break
#         return cls._cached_account_info
#
#     def orders(cls, status: OrderStatus = None, cached=False):
#         if cached is False or cls._cached_orders is None:
#             cls._cached_orders = cls.TDA_CLIENT.get_orders_by_path(
#                 account_id=_ACCOUNT_ID,
#                 from_entered_datetime=datetime.datetime.utcnow()
#                 - datetime.timedelta(days=59),
#                 status=status,
#             ).json()
#         return cls._cached_orders
#
#     def orders_by_id(cls, status: OrderStatus = None, cached=False):
#         """returns orders where key is the order id"""
#         return parse_orders(cls.orders(status=status, cached=cached))
#
#     def get_order_data(cls, order_id, cached=False) -> OrderData:
#         """TODO call in debugger to get location of symbol name"""
#         order = cls.orders_by_id(cached=cached)[order_id]
#         return OrderData(
#             name=order["orderLegCollection"][0]["instrument"]["symbol"],
#             direction=utils.Side(
#                 order["orderLegCollection"][0]["instruction"]
#             ),
#             quantity=order["filledQuantity"],
#             stop_loss=None,
#             status=OrderStatus(order["status"]),
#         )
#
#     def get_queued_stop_order(
#         cls, symbol: str
#     ) -> t.Union[t.Tuple[OrderData, int], t.Tuple[None, None]]:
#         """"""
#         orders = cls.orders()
#         stop_order_data = None
#         stop_order_id = None
#         for order in orders:
#             order_leg_collection = order["orderLegCollection"][0]
#             if (
#                 order_leg_collection["instrument"]["symbol"] == symbol
#                 and order["orderType"] == "STOP"
#                 and order_leg_collection["positionEffect"] == "CLOSING"
#                 and order["status"] == "QUEUED"
#             ):
#                 # direction should match the OPEN position direction
#                 direction = utils.Side.LONG
#                 if order_leg_collection["instruction"] == "BUY_TO_COVER":
#                     direction = utils.Side.SHORT
#
#                 stop_order_data = OrderData(
#                     name=symbol,
#                     direction=direction,
#                     quantity=order["quantity"],
#                     stop_loss=order["stopPrice"],
#                 )
#                 stop_order_id = order["orderId"]
#                 break
#         return stop_order_data, stop_order_id
#
#     def place_order_spec(cls, order_spec) -> t.Tuple[int, str]:
#         """place order with tda-api order spec, return order id"""
#         cls.TDA_CLIENT.place_order(account_id=_ACCOUNT_ID, order_spec=order_spec)
#         order_data = cls.orders()[0]
#         return order_data["orderId"], order_data["status"]
#
#     def init_equity_stream(cls, symbols: t.List[str]):
#         """
#         stream price data of the given symbols every 500ms
#         use this code to execute function: asyncio.run(LocalClient.initiate_stream(<enter symbols here>)
#         """
#         return configure_stream(
#             stream_client=cls.STREAM_CLIENT,
#             add_book_handler=cls.STREAM_CLIENT.add_chart_equity_handler,
#             symbols=symbols,
#             book_subs=cls.STREAM_CLIENT.chart_equity_subs,
#         )
#
#     def init_futures_stream(cls, symbols: t.List[str]) -> t.Callable:
#         # TODO add handler converting stream to ohlc bars + writes to csv
#         return configure_stream(
#             stream_client=cls.STREAM_CLIENT,
#             add_book_handler=cls.STREAM_CLIENT.add_chart_futures_handler,
#             symbols=symbols,
#             book_subs=cls.STREAM_CLIENT.chart_futures_subs,
#         )
#
#     def market_is_open(cls, market_type: tda.client.Client.Markets) -> bool:
#         """
#         TODO move to MarketData class
#         """
#         resp = cls.TDA_CLIENT.get_hours_for_single_market(
#             market_type, datetime.datetime.now()
#         )
#         resp = resp.json()
#         return resp["equity"]["EQ"]["isOpen"]
#
#     def market_was_open(
#         cls, market_type: tda.client.Client.Markets, time_ago: datetime.timedelta
#     ):
#         resp = cls.TDA_CLIENT.get_hours_for_single_market(
#             market_type, datetime.datetime.now()
#         )
#         resp = resp.json()
#         market_end = resp["equity"]["EQ"]["sessionHours"]["regularMarket"][0]["end"][
#             :-6
#         ]
#         market_end = datetime.datetime.strptime(market_end, "%Y-%m-%dT%H:%M:%S")
#         return datetime.datetime.now() - market_end <= time_ago
#
#     def cancel_order(cls, order_id: int):
#         cls.TDA_CLIENT.cancel_order(order_id=order_id, account_id=_ACCOUNT_ID)


# # create td client
# class LocalClient(metaclass=_LocalClientMeta):
#     OrderStatus = tda.client.Client.Order.Status
#     cached_account_info: AccountInfo = None
#
#     @classmethod
#     def price_history(
#         cls, symbol: str, freq_range: tdargs.FreqRangeArgs,
#     ) -> pd.DataFrame:
#         """
#         :param symbol:
#         :param freq_range:
#         :return:
#         """
#         # get historical data, store as dataframe, convert datetime (ms) to y-m-d-etc
#         try:
#             resp = cls.TDA_CLIENT.get_price_history(
#                 symbol,
#                 period_type=freq_range.range.period.type,
#                 period=freq_range.range.period.val,
#                 frequency_type=freq_range.freq.type,
#                 frequency=freq_range.freq.val,
#                 start_datetime=freq_range.range.start,
#                 end_datetime=freq_range.range.end,
#                 need_extended_hours_data=False,
#             )
#         except OAuthError:
#             raise utils.EmptyDataError
#
#         try:
#             history = resp.json()
#         except json.decoder.JSONDecodeError:
#             raise utils.EmptyDataError
#
#         if history.get("candles", None) is None:
#             error = history.get("error", None)
#             if error is None and history.get("fault", None) is not None:
#                 raise utils.FaultReceivedError(
#                     f"tda responded with fault at {symbol}: {error}"
#                 )
#             if error == "Not Found":
#                 print(f"td api could not find symbol {symbol}")
#                 raise utils.TickerNotFoundError(
#                     f"td api could not find symbol {symbol}"
#                 )
#             elif error is None:
#                 raise Exception
#             else:
#                 raise utils.EmptyDataError(
#                     f"No data received for symbol {symbol}"
#                 )
#
#         df = pd.DataFrame(history["candles"])
#
#         if history["empty"] is True:
#             raise utils.EmptyDataError(f"No data received for symbol {symbol}")
#
#         # datetime given in ms, convert to readable date
#         df.datetime = pd.to_datetime(df.datetime, unit="ms")
#
#         # for truncating to date only (not hours/minutes/seconds)
#         # df.datetime = df.datetime.dt.date
#
#         # rename datetime to time for finplot compatibility
#         df = df.rename(columns={"datetime": "time"})
#         df.index = df.time
#         # drop columns other than those mentioned (maybe want to save volume)
#         df["b_high"] = df.high
#         df["b_low"] = df.low
#         df["b_close"] = df.close
#
#         df = df[
#             ["b_high", "b_low", "b_close", "open", "high", "close", "low", "volume"]
#         ]
#
#         return df
#
#     @classmethod
#     def price_history_stream(
#         cls, symbol: str, freq_range: tdargs.FreqRangeArgs,
#     ) -> t.Tuple[pd.DataFrame, t.Any]:
#         return (
#             cls.price_history(symbol=symbol, freq_range=freq_range),
#             datetime.timedelta(seconds=5),
#         )
#
#     @classmethod
#     def init_position(
#         cls, symbol, quantity, side, stop_value=None, data_row=None
#     ) -> Position:
#         return Position(symbol, quantity, side, stop_value=stop_value, data_row=None)


def hof_init_td_client(credentials):
    def _init_td_client():
        return TdBrokerClient(credentials)
    return _init_td_client


class TdBrokerAccount(abstract_access.AbstractBrokerAccount):
    def __init__(self, account_client):
        self._account_client = account_client

    @property
    def positions(self) -> t.Dict[str, t.Type[AbstractPosition]]:
        pass

    @property
    def equity(self):
        pass

    def get_symbols(self) -> t.List[str]:
        pass



class TdBrokerClient(abstract_access.AbstractBrokerClient):
    _client: tda.client.Client

    def __init__(self, credentials: t.Dict):
        self._client_credentials = credentials['client']
        self._account_id = credentials['account_id']
        super().__init__(self._client_credentials)
        self._cached_orders = None

    @staticmethod
    def _get_broker_client(credentials) -> tda.client.Client:
        return tda.auth.easy_client(
            webdriver_func=selenium.webdriver.Firefox,
            **credentials
        )

    def account_info(self, *args, **kwargs):
        account_info = self.client.get_account(self._account_id, fields=tda.client.Client.Account.Fields.POSITIONS)
        return account_info

    def get_transactions(self):
        """todo add to AbstractBrokerClient as abstract method"""
        return self.client.get_transactions(self._account_id)

    def price_history(self, symbol, freq_range: tdargs.FreqRangeArgs) -> pd.DataFrame:
        """
        attempt to get price history, catch and raise meaningful exceptions
        when specific issues arise
        :param symbol:
        :param freq_range:
        :return:
        """
        # get historical data, store as dataframe, convert datetime (ms) to y-m-d-etc
        try:
            resp = self._client.get_price_history(
                symbol,
                period_type=freq_range.range.period.type,
                period=freq_range.range.period.val,
                frequency_type=freq_range.freq.type,
                frequency=freq_range.freq.val,
                start_datetime=freq_range.range.start,
                end_datetime=freq_range.range.end,
                need_extended_hours_data=False,
            )
        except OAuthError:
            raise utils.EmptyDataError

        return _handle_raw_price_history(resp, symbol)

    def easy_get_price_history(self, symbol: str, interval: int):
        """
        get price history with simplified inputs, max data retrieved if no start/end specified
        :param symbol: ticker symbol
        :param interval: minute interval: 1, 5, 10, 15, 30
        :return:
        """
        return easy_get_price_history(self.client, symbol, interval)

    def place_order_spec(self, order_spec) -> t.Tuple[int, str]:
        """
        place order with tda-api order spec, return order id and order status
        """
        self._client.place_order(account_id=self._account_id, order_spec=order_spec)
        order_data = self.get_orders()[0]
        return order_data["orderId"], order_data["status"]

    def get_orders(self, status: OrderStatus = None, cached=False):
        """
        get all orders in the last X 59 days (limit is 60)
        """
        if cached is False or self._cached_orders is None:
            self._cached_orders = self._client.get_orders_by_path(
                account_id=self._account_id,
                from_entered_datetime=datetime.datetime.utcnow()
                - datetime.timedelta(days=59),
                status=status,
            ).json()
        return self._cached_orders

    def orders_by_id(self, status: OrderStatus = None, cached=False):
        """returns orders where key is the order id"""
        return parse_orders(self.get_orders(status=status, cached=cached))

    def get_order_data(self, order_id, cached=False) -> OrderData:
        """
        get order status given order id
        TODO call in debugger to get location of symbol name
        """
        order = self.orders_by_id(cached=cached)[order_id]
        return OrderData(
            name=order["orderLegCollection"][0]["instrument"]["symbol"],
            direction=utils.Side(
                order["orderLegCollection"][0]["instruction"]
            ),
            quantity=order["filledQuantity"],
            stop_loss=None,
            status=OrderStatus(order["status"]),
        )

    def init_position(self, symbol, quantity, side) -> Position:
        return Position(symbol, quantity, side, data_row=None)

    def init_stream(
            self,
            live_quote_fp: str,
            price_history_fp: str,
            interval: int,
            # fetch_data_params: t.Optional[t.Dict] = None,
            # fetch_price_data: t.Optional[abstract_access.DATA_FETCH_FUNCTION] = None,

    ) -> TdTickerStream:
        """
        Initialize a ticker stream object
        :param live_quote_fp: file path to write json file containing live quotes
        :param price_history_fp: file path to write OHLC data to csv by given interval
                                up to the most recent close time
        :param interval: minute interval to write OHLC data to price_history_fp
                        1, 5, 10, 15, 30
        :return:
        """
        return TdTickerStream(
            self.client,
            account_id=self._account_id,
            stream_parser=TdStreamParser,
            quote_file_path=live_quote_fp,
            history_path=price_history_fp,
            fetch_price_data=self.easy_get_price_history,
            interval=interval
        )


class TdTickerStream(abstract_access.AbstractTickerStream):
    def __init__(
            self,
            broker_client,
            account_id,
            stream_parser,
            quote_file_path,
            history_path,
            fetch_price_data: abstract_access.DATA_FETCH_FUNCTION,
            interval
    ):
        super().__init__(stream_parser, quote_file_path, history_path, fetch_price_data, interval)
        self._account_id = account_id
        self._current_quotes = {}
        self._broker_client = broker_client

    def run_stream(self, writer_send_conn, symbols: t.List[str]):
        """configure stream, create write subprocess, then run stream with handler piping data to write subprocesses"""
        _stream_client = tda.streaming.StreamClient(
            client=self._broker_client,
            account_id=self._account_id
        )
        stream = configure_stream(
            _stream_client,
            add_book_handler=_stream_client.add_chart_equity_handler,
            book_subs=_stream_client.chart_equity_subs,
            symbols=symbols
        )
        self._current_quotes = {symbol: None for symbol in symbols}
        data_send_conn = self._init_processes(writer_send_conn)
        stream_start = datetime.datetime.utcnow()
        self._init_stream_parsers(symbols, stream_start)
        fetch_time = self.get_fetch_time(stream_start, None)
        last = datetime.datetime.utcnow()
        while (now := datetime.datetime.utcnow()) < fetch_time:
            if now > last + datetime.timedelta(seconds=1):
                print(f'Waiting: {fetch_time - now}', end='\r')
                last = now
        history_data = self.get_all_symbol_data(symbols, self._interval)
        history_data.to_csv(self._history_path)
        print('streaming start')
        asyncio.run(
            stream(
                [
                    lambda msg: self.handle_stream(msg, data_send_conn),
                    # lambda msg: print(json.dumps(msg, indent=4))
                ]
            )
        )

    def handle_stream(self, msg, send_conn):
        parsed_data = self.__class__.get_symbol(msg)
        time_stamp = datetime.datetime.utcnow()
        for symbol, data in parsed_data.items():
            ohlc_data = self._stream_parsers[symbol].update_ohlc(data, time_stamp)
            if ohlc_data != self._current_quotes[symbol]:
                self._current_quotes[symbol] = ohlc_data
        if len(self._current_quotes) > 0:
            send_conn.send(self._current_quotes)

    @staticmethod
    def get_symbol(msg) -> t.Dict[str, t.Dict]:
        return {content['key']: content for content in msg['content']}


class TdStreamParser(abstract_access.AbstractStreamParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrieve_ohlc(self, data: dict):
        """get prices from ticker stream, expects content to be passed in"""
        return (
            data["OPEN_PRICE"],
            data["HIGH_PRICE"],
            data["LOW_PRICE"],
            data["CLOSE_PRICE"],
        )
