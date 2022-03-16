"""
utilities for code development of back test platform and
trading modules
"""

from enum import Enum, auto

import numpy as np
import pandas as pd


class EmptyDataError(Exception):
    """A dictionary with no data was received from request"""


class TickerNotFoundError(Exception):
    """error response from td api is Not Found"""


class FaultReceivedError(Exception):
    """received a fault from response to an API request"""


class Side(Enum):
    """Used to express signals and trade direction in natural language"""

    LONG = 1
    SHORT = -1
    CLOSE = 0

    @classmethod
    def _missing_(cls, value):
        """
        provides additional mappings of enum values
        """
        enum_map = {
            "BUY": cls.LONG,
            "SELL_SHORT": cls.SHORT,
        }
        if (res := enum_map.get(value, None)) is None:
            if np.isnan(value):
                res = cls.CLOSE
            else:
                res = super()._missing_(value)
        return res


class SignalStatus(Enum):
    NEW_LONG = auto()
    NEW_SHORT = auto()
    NEW_CLOSE = auto()
    LONG = auto()
    SHORT = auto()
    CLOSE = auto()
    # NEW_LONG_SHORT = auto()  # is it possible skip close?
    # NEW_SHORT_LONG = auto()

    @classmethod
    def _missing_(cls, value):
        signal_status_selector = {
            (Side.CLOSE, Side.LONG): SignalStatus.NEW_LONG,
            (Side.CLOSE, Side.SHORT): SignalStatus.NEW_SHORT,
            (Side.LONG, Side.CLOSE): SignalStatus.NEW_CLOSE,
            (Side.SHORT, Side.CLOSE): SignalStatus.NEW_CLOSE,
            (Side.CLOSE, Side.CLOSE): SignalStatus.CLOSE,
            (Side.LONG, Side.LONG): SignalStatus.LONG,
            (Side.SHORT, Side.SHORT): SignalStatus.SHORT,
        }
        if (res := signal_status_selector.get(value, None)) is None:
            res = super()._missing_(value)
        return res
