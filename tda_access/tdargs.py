"""
Defines associations of type and value for frequency and period parameters
so they don't have to be passed individually when attempting to fetch data
via tda-api

Restricts the input options
"""
import pandas as pd
from tda.client import Client
from datetime import datetime, timedelta
from typing import Union, Callable, Any
from dataclasses import dataclass

_tda_freq = Client.PriceHistory.Frequency
_tda_freq_type = Client.PriceHistory.FrequencyType
_tda_period = Client.PriceHistory.Period
_tda_period_type = Client.PriceHistory.PeriodType


@dataclass
class LabeledData:
    symbol: str
    data: pd.DataFrame


@dataclass
class _TypeMap:
    """bind tda frequency types to frequency value"""

    type: Any
    val: Any


@dataclass
class _TimeSliceArgs:
    """
    # the last parameter should be None upon initialization. they are included for duck typing
    """

    start: Any
    end: Any
    period: Any


@dataclass
class FreqRangeArgs:
    freq: Any
    range: Any


def time_slice(left_bound: datetime, right_bound: datetime):
    assert type(left_bound) == datetime
    return _TimeSliceArgs(left_bound, right_bound, _TypeMap(None, None))


@dataclass
class _TimePeriodArgs:
    period: Any
    end: Any
    start: Any


def time_period(left_bound: _TypeMap, right_bound: datetime):
    assert type(left_bound) == _TypeMap
    return _TimePeriodArgs(left_bound, right_bound, None)


@dataclass
class _PeriodMap:
    """create bindings for period type and period"""

    y20: _TypeMap
    y15: _TypeMap
    y10: _TypeMap
    y5: _TypeMap
    y3: _TypeMap
    y2: _TypeMap
    y1: _TypeMap
    m6: _TypeMap
    m3: _TypeMap
    m2: _TypeMap
    m1: _TypeMap
    d10: _TypeMap
    d5: _TypeMap
    d4: _TypeMap
    d3: _TypeMap
    d2: _TypeMap
    d1: _TypeMap


class _PHistoryArgs:
    """
    TODO:
        - add param defining minimum/maximum ranges to pass in for valid data
            -(FUTURE) create a module for automatically obtaining the min/max ranges
    """

    def __init__(
        self,
        freq_type_map: _TypeMap,
        time_range_init_func: Callable[
            [Union[datetime, _TypeMap], datetime],
            Union[_TimeSliceArgs, _TimePeriodArgs],
        ],
    ):
        self.freq = freq_type_map
        self.period = None
        self._tr_init = time_range_init_func

    def range(
        self, left_bound: Union[_PeriodMap, datetime], right_bound=datetime.now()
    ) -> FreqRangeArgs:
        """
        :param left_bound:
        :param right_bound:
        :return: named tuple to use with price history args
        """
        time_range = self._tr_init(left_bound, right_bound)
        return FreqRangeArgs(freq=self.freq, range=time_range)


@dataclass
class _FrequencyMap:
    """contains tda param mappings for all bar types"""

    month: _PHistoryArgs
    week: _PHistoryArgs
    day: _PHistoryArgs
    m30: _PHistoryArgs
    m15: _PHistoryArgs
    m10: _PHistoryArgs
    m5: _PHistoryArgs
    m1: _PHistoryArgs


def time_ago(
    days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0
):
    """
    produce a date time distanced from datetime.now()
    """
    return datetime.now() - timedelta(**locals())


freqs = _FrequencyMap(
    month=_PHistoryArgs(
        _TypeMap(_tda_freq_type.MONTHLY, _tda_freq.MONTHLY), time_period
    ),
    week=_PHistoryArgs(_TypeMap(_tda_freq.WEEKLY, _tda_freq.WEEKLY), time_period),
    day=_PHistoryArgs(_TypeMap(_tda_freq_type.DAILY, _tda_freq.DAILY), time_period),
    m30=_PHistoryArgs(
        _TypeMap(_tda_freq_type.MINUTE, _tda_freq.EVERY_THIRTY_MINUTES), time_slice
    ),
    m15=_PHistoryArgs(
        _TypeMap(_tda_freq_type.MINUTE, _tda_freq.EVERY_FIFTEEN_MINUTES), time_slice
    ),
    m10=_PHistoryArgs(
        _TypeMap(_tda_freq_type.MINUTE, _tda_freq.EVERY_TEN_MINUTES), time_slice
    ),
    m5=_PHistoryArgs(
        _TypeMap(_tda_freq_type.MINUTE, _tda_freq.EVERY_FIVE_MINUTES), time_slice
    ),
    m1=_PHistoryArgs(
        _TypeMap(_tda_freq_type.MINUTE, _tda_freq.EVERY_MINUTE), time_slice
    ),
)

periods = _PeriodMap(
    y20=_TypeMap(_tda_period_type.YEAR, _tda_period.TWENTY_YEARS),
    y15=_TypeMap(_tda_period_type.YEAR, _tda_period.FIFTEEN_YEARS),
    y10=_TypeMap(_tda_period_type.YEAR, _tda_period.TEN_YEARS),
    y5=_TypeMap(_tda_period_type.YEAR, _tda_period.FIVE_YEARS),
    y3=_TypeMap(_tda_period_type.YEAR, _tda_period.THREE_YEARS),
    y2=_TypeMap(_tda_period_type.YEAR, _tda_period.TWO_YEARS),
    y1=_TypeMap(_tda_period_type.YEAR, _tda_period.ONE_YEAR),
    m6=_TypeMap(_tda_period_type.MONTH, _tda_period.SIX_MONTHS),
    m3=_TypeMap(_tda_period_type.MONTH, _tda_period.THREE_MONTHS),
    m2=_TypeMap(_tda_period_type.MONTH, _tda_period.TWO_MONTHS),
    m1=_TypeMap(_tda_period_type.MONTH, _tda_period.ONE_MONTH),
    d10=_TypeMap(_tda_period_type.DAY, _tda_period.TEN_DAYS),
    d5=_TypeMap(_tda_period_type.DAY, _tda_period.FIVE_DAYS),
    d4=_TypeMap(_tda_period_type.DAY, _tda_period.FOUR_DAYS),
    d3=_TypeMap(_tda_period_type.DAY, _tda_period.THREE_DAYS),
    d2=_TypeMap(_tda_period_type.DAY, _tda_period.TWO_DAYS),
    d1=_TypeMap(_tda_period_type.DAY, _tda_period.ONE_DAY),
)
