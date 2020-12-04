
from datetime import time
from itertools import chain
from pandas.tseries.holiday import AbstractHolidayCalendar, GoodFriday, USLaborDay

from dateutil.relativedelta import (MO, TH)
from pandas import (DateOffset, Timestamp, date_range)
from pandas.tseries.holiday import (Holiday, nearest_workday, sunday_to_monday)
from pandas.tseries.offsets import Day
from abc import ABC

MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY = range(7)

AbstractHolidayCalendar.start_date = '1992-06-15'

USNewYearsDay = Holiday(
    'New Years Day',
    month=1,
    day=1,
    # When Jan 1 is a Sunday, US markets observe the subsequent Monday.
    # When Jan 1 is a Saturday (as in 2005 and 2011), no holiday is observed.
    observance=sunday_to_monday
)

USMartinLutherKingJrAfter1998 = Holiday(
    'Dr. Martin Luther King Jr. Day',
    month=1,
    day=1,
    # The US markets didn't observe MLK day as a holiday until 1998.
    start_date=Timestamp('1998-01-01'),
    offset=DateOffset(weekday=MO(3)),
)

USPresidentsDay = Holiday('President''s Day',
                          start_date=Timestamp('1971-01-01'),
                          month=2, day=1,
                          offset=DateOffset(weekday=MO(3)))

USMemorialDay = Holiday(
    'Memorial Day',
    month=5,
    day=25,
    start_date=Timestamp('1971-01-01'),
    offset=DateOffset(weekday=MO(1)),
)

USIndependenceDay = Holiday(
    'July 4th',
    month=7,
    day=4,
    start_date=Timestamp('1954-01-01'),
    observance=nearest_workday,
)

USThanksgivingDay = Holiday('Thanksgiving',
                            start_date=Timestamp('1942-01-01'),
                            month=11, day=1,
                            offset=DateOffset(weekday=TH(4)))

ChristmasEveInOrAfter1993 = Holiday(
    'Christmas Eve',
    month=12,
    day=24,
    start_date=Timestamp('1993-01-01'),
    # When Christmas is a Saturday, the 24th is a full holiday.
    days_of_week=(MONDAY, TUESDAY, WEDNESDAY, THURSDAY),
)

Christmas = Holiday(
    'Christmas',
    month=12,
    day=25,
    start_date=Timestamp('1954-01-01'),
    observance=nearest_workday,
)

September11Closings = [
    Timestamp("2001-09-11", tz='UTC'),
    Timestamp("2001-09-12", tz='UTC'),
    Timestamp("2001-09-13", tz='UTC'),
    Timestamp("2001-09-14", tz='UTC'),
]

HurricaneSandyClosings = date_range(
    '2012-10-29',
    '2012-10-30',
    tz='UTC'
)

# National Days of Mourning
# - President John F. Kennedy - November 25, 1963
# - Martin Luther King - April 9, 1968
# - President Dwight D. Eisenhower - March 31, 1969
# - President Harry S. Truman - December 28, 1972
# - President Lyndon B. Johnson - January 25, 1973
# - President Richard Nixon - April 27, 1994
# - President Ronald W. Reagan - June 11, 2004
# - President Gerald R. Ford - Jan 2, 2007
# - President George H.W. Bush - Dec 5, 2018
USNationalDaysofMourning = [
    Timestamp('1963-11-25', tz='UTC'),
    Timestamp('1968-04-09', tz='UTC'),
    Timestamp('1969-03-31', tz='UTC'),
    Timestamp('1972-12-28', tz='UTC'),
    Timestamp('1973-01-25', tz='UTC'),
    Timestamp('1994-04-27', tz='UTC'),
    Timestamp('2004-06-11', tz='UTC'),
    Timestamp('2007-01-02', tz='UTC'),
    Timestamp('2018-12-05', tz='UTC'),
]

class USTradingCalendar(ABC):
    """
    Trading calendar for US
    Regularly-Observed Holidays:
    - New Years Day (observed on monday when Jan 1 is a Sunday)
    - Martin Luther King Jr. Day (3rd Monday in January, only after 1998)
    - Washington's Birthday (aka President's Day, 3rd Monday in February,
      after 1970)
    - Good Friday (two days before Easter Sunday)
    - Memorial Day (last Monday in May, after 1970)
    - Independence Day (observed on the nearest weekday to July 4th, after
      1953)
    - Labor Day (first Monday in September)
    - Thanksgiving (fourth Thursday in November, after 1941)
    - Christmas (observed on nearest weekday to December 25, after 1953)

    Additional Irregularities:
    - Closed on 4/27/1994 due to Richard Nixon's death.
    - Closed from 9/11/2001 to 9/16/2001 due to terrorist attacks in NYC.
    - Closed on 6/11/2004 due to Ronald Reagan's death.
    - Closed on 1/2/2007 due to Gerald Ford's death.
    - Closed on 10/29/2012 and 10/30/2012 due to Hurricane Sandy.
    - Closed on 12/5/2018 due to George H.W. Bush's death.
    - Christmas Eve (on Fridays, when the exchange is closed entirely)!! NOT IMPLEMENTED
    """

    @property
    def name(self):
        return "US"

#     @property
#     def tz(self):
#         return timezone('America/New_York')

#     @property
#     def open_time_default(self):
#         return time(9, 30, tzinfo=self.tz)

#     @property
#     def close_time_default(self):
#         return time(16, tzinfo=self.tz)

    @property
    def regular_holidays(self):
        return AbstractHolidayCalendar(rules=[
            USNewYearsDay,
            USMartinLutherKingJrAfter1998,
            USPresidentsDay,
            GoodFriday,
            USMemorialDay,
            USIndependenceDay,
            USLaborDay,
            USThanksgivingDay,
            Christmas,
        ])

    @property
    def adhoc_holidays(self):
        return list(chain(
            September11Closings,
            HurricaneSandyClosings,
            USNationalDaysofMourning,
        ))
