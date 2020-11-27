from datetime import datetime
from dataloader import Data_Loader_CSV
import numpy as np


#data_directory = "test_data"  # "path of data"
#tickers = ["DIS", "GE", "AAPL"]
#features = []
#start = datetime(2019, 1, 4)
#end = datetime(2019, 11, 7)
# these could be used to test if the volume_ratio function works

def volume_ratio_calc(data_directory, tickers, features, start, end):
    # return a list, where individual variables are dataframes with volume ratio added as an extra column at the end
    # all dataframes in the list with <25 past days has been eliminated to avoid calculation errors

    data_loader_csv = Data_Loader_CSV(data_directory, tickers, features, start, end)
    data = data_loader_csv.load_data()  # loaded as a dict type
    data_value = data.values()  # extract dictionary values
    data_df = list(data_value)  # convert these values into a list which can be used to extract stocks
    returned_data_df = []  # create an empty list which is avaliable for adding dataframe later in for loops

    for x in range(len(data_df)):
        volume_ratio = []  # create an empty list of volume ratio that will be added to dataframe later
        stock = data_df[x]  # pull out the stock's dataframe
        stock = stock.sort_index(ascending=False,
                                 axis=0)  # reverse the dataframe to get today's values at the top of the dataframe
        volatility_25 = [] # create list for volatility

        for i in range(len(stock) - 24):
            TV = stock.iloc[i, 7]  # today's volume
            V_past25 = stock.iloc[i:i + 25, 7]  # past 25 days' volumes
            volatility_25.append(np.std(TV/V_past25)) # volatility of past 25 day's volume ratio
            volume_ratio.append(TV / np.average(V_past25))  # this will be cleared by the end of each external for loops

        returned_stock = stock.drop(stock.index[-24:])  # drop the last 24 days of the stock after calculations above
        returned_stock['Volume Ratio'] = volume_ratio  # add column to the data frame
        returned_stock['Vol_VR'] = volatility_25 # add volatility back to the column
        returned_stock = returned_stock.sort_index(ascending=True, axis=0)  # reverse dataframe back to original look
        # for easier interpretation
        returned_data_df.append(returned_stock)  # add dataframe to the list#

    return returned_data_df
