import pymongo
import pandas as pd
import numpy as np

import typing
from abc import ABC, abstractmethod
from datetime import datetime


class DataLoader(ABC):
    def __init__(
        self,
        datasource: str,
        tickers: typing.List[str],
        features: typing.List[str],
        start: datetime,
        end: datetime,
    ):
        self.datasource = datasource
        self.tickers = tickers
        self.features = features
        self.start = start
        self.end = end
        super().__init__()

    # load raw data to self.rawdata which is a list of dataframes
    @abstractmethod
    def load_rawdata(self):
        pass

    def process_data(self):
        self.featuredata = list()
        for pricedata in self.rawdata:
            # adjust for dividends and splits
            # original data fields: open, high, low, close, volume, dividend and splits ratios with bid and ask
            # created adj_close as the adjusted close price for research
            # TODO: create adj_open, adj_high and adj_low also?
            for feature in self.features:
                if feature in ["return"]:
                    pricedata[feature] = pricedata["adj_close"].apply(np.log).diff(1)
