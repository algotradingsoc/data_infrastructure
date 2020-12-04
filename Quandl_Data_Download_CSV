import quandl
import pandas as pd

name_stock_ls= pd.read_csv('FinnhubID.csv')
name_stock_ls = name_stock_ls.symbol
auth_key = "RSJU_9Zsy_ryxzT2UN5G"
name_head = 'FNSQ'


def data_to_csv_from_quandl(name_head,name_stock_ls,auth_key):
# This funnction returns CSVs from the FINRA, using ticker list and name_head provided
## the name_stock_ls is a list of tickers, such as QQQ, which needs to be provided.
## name_head is the name such as FNSQ
    for i in range(len(name_stock_ls)):
        name_stock = name_stock_ls[i]
        nof = "FINRA/{name1}_{name2}".format(name1 = name_head, name2 = name_stock)#name of file
        data_frame = quandl.get(nof, authtoken=auth_key) # getting data using my token
        data_frame.to_csv (r'{name3}.csv'.format(name3=name_stock), header=True) #converts to csv file

data_to_csv_from_quandl(name_head,name_stock_ls,auth_key)
