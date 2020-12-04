# -*- coding: utf-8 -*-
import pandas as pd
import urllib.request
import time

df_tickers = pd.read_csv('FinnhubID.csv')

# sort tickers with most recent trading dates first
df_sorted = df_tickers.sort_values(by=['end'],ascending = False).reset_index(drop = True)

symbol_list = df_sorted.symbol

key= 'demo'  # CHANGE it to YOUR OWN requested API key from Alphavantage

y = range(1,3)
m = range(1,13)
slices = []
for i in y:
    for j in m:
        slices.append('year'+str(i)+'month'+str(j))
        
interval = '1min'

############
# CSV Download - change the symbol to desired ticker
##################
symbol = symbol_list[0]  

for s_slice in slices:
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=' + symbol + '&interval=' + interval + '&slice=' + s_slice + '&apikey=' + key + '&datatype=csv'
    urllib.request.urlretrieve(url, symbol+'_'+ s_slice+'.csv')
    time.sleep(3)  # to comply with 5 requests/minute constraint - adjust this sleep time if needed

