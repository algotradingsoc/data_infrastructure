import pandas as pd

#Assuming we have csv files arranged by tickers (finnhub_id)
def ListingDelisting(ticker_range):
    listing_days = []
    delisting_days = []
    for i in range(len(ticker_range)):
        ticker = ticker_range[i]
        data = pd.read_csv(ticker+'.csv')
        listing_days[i] = data['day'][0]
        delisting_days[i] = data['day'][-1]
    return listing_days,delisting_days