# -*- coding: utf-8 -*-

import pandas_market_calendars as mcal

nyse = mcal.get_calendar('NYSE')

#########
#Examples
#########
# note Christmas Eve is closed if it is a friday, else it's half-day
dec16 = nyse.schedule(start_date='2016-12-01', end_date='2016-12-31') 
dec18 = nyse.schedule(start_date='2018-12-01', end_date='2018-12-31')

# get list of dates
list_dec16 = list(dec16.index)