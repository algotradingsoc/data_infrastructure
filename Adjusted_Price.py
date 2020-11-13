import pandas as pd

#Assuming we have csv files arranged by tickers (finnhub_id)
#reference : https://joshschertz.com/2016/08/27/Vectorizing-Adjusted-Close-with-Python/

def Adjusted_Price(ticker_range):
    adjusted_price= []
    for i in range(len(ticker_range)):
        ticker = ticker_range[i]
        data = pd.read_csv(ticker+'.csv')
        adjusted_price[i] = calculate_adjusted_prices_iterative(data, 'close')
    return adjusted_price

def calculate_adjusted_prices_iterative(df, column):
    """ Iteratively calculates the adjusted prices for the specified column in
    the provided DataFrame. This creates a new column called 'adj_<column name>'
    with the adjusted prices. This function requires that the DataFrame have
    columns with dividend and split_ratio values.

    :param df: DataFrame with raw prices along with dividend and split_ratio
        values
    :param column: String of which price column should have adjusted prices
        created for it
    :return: DataFrame with the addition of the adjusted price column
    """
    adj_column = 'adj_' + column

    # Set default values for adjusted price column to 0
    df[adj_column] = 0

    # Get a list of the index dates, in reverse order (most recent to oldest)
    dates = list(df.index)[:-1]
    dates.reverse()
    final_date = list(df.index)[-1]

    # Put the column's last price as the last adj_<column>'s value
    df.loc[final_date, adj_column] = df.loc[final_date, column]

    # Iterate through each DataFrame row from bottom to top (newest to oldest)
    for date in dates:
        if dates.index(date) - 1 == -1:
            # If this is the first date in the index, use the final date
            #   variable, which is the newest date
            preceding_date = final_date
        else:
            # Otherwise, the next date variable will be the date preceding the
            #   current one (the next calendar day)
            preceding_date = dates[dates.index(date) - 1]

        current_val = df.loc[date, column]
        precede_val = df.loc[preceding_date, column]
        precede_adj = df.loc[preceding_date, adj_column]

        # Both the split ratio and dividend need to use the preceding date's
        #   values (the next calendar day's values)
        split_ratio = df.loc[preceding_date, 'split_ratio']
        dividend = df.loc[preceding_date, 'div']

        adj_price = (precede_adj + precede_adj *
                     (((current_val * split_ratio) - precede_val - dividend) /
                      precede_val))

        # Add the adjusted price to the adj_<column> in the DataFrame
        df.loc[date, adj_column] = round(adj_price, 4)

    return df
