import os
import csv
import pymongo
import pandas as pd
import numpy as np
import scipy.stats

import typing
from pymongo import MongoClient
from abc import ABC, abstractmethod
from datetime import datetime

# =============================================================================
# Data Loader Abstract Class
# =============================================================================


class Data_Loader(ABC):
    """
    Abstract Class for data loading
    """

    def __init__(
        self,
        datasource: str,
        tickers: typing.List[str],
        features: typing.List[str],
        start: datetime,
        end: datetime,
    ):
        """
        :param datasource: where is the dataset located
        :param tickers: symbols/tickers of the stocks you want to load
        :param features: features you want to extract
        :param start: start date
        :param end: end date (includes ending date)
        """

        if end < start:
            raise TimeInvalid("The end date cannot be before the start date")

        self.datasource = datasource
        self.tickers = list(set(tickers))
        self.features = list(set(features))
        self.start = start
        self.end = end
        super().__init__()

    @abstractmethod
    def load_data(self) -> typing.Dict[str, pd.DataFrame]:
        """
        :return: List of Dataframes (each df represents the time series for a particular stock)
        """
        pass


# =============================================================================
# CSV Data Loader
# =============================================================================


class Data_Loader_CSV(Data_Loader):
    """
    Data Loader for CSV files
    """

    def __init__(
        self,
        datasource: str,
        tickers: typing.List[str],
        features: typing.List[str],
        start: datetime,
        end: datetime,
    ):
        """
        :param datasource: relative (from root) or absolute path of folder containing all csv files
        :param tickers: symbols/tickers of the stocks you want to load
        :param features: features you want to extract
        :param start: start date
        :param end: end date ( result includes ending date)
        """
        super().__init__(datasource, tickers, features, start, end)
        self._file_names = os.listdir(datasource)
        self._features_list = self.return_features()
        self.__datetime_filename_HashMap = {
            datetime.strptime(filename[:-4], "%Y%m%d"): filename
            for filename in self._file_names
        }

    def load_data(self) -> typing.Dict[str, pd.DataFrame]:
        """
        :return: List of Dataframes (each df represents the time series for a particular stock)
        :raise FeaturesMismatchException if feature does not exist in dataset
        """

        filenames = self._extract_filesnames_from_date()

        if len(self.features) == 0:
            columns = self._features_list
        else:
            if len(set(self.features).intersection(set(self._features_list))) == len(
                self.features
            ):
                features = set(self.features)
                features.add("symbol")
                columns = list(features)
            else:
                raise Exception(
                    "FeaturesMismatchException: Some input features not present in dataset"
                )

        data_dict = {}
        for filename in filenames:
            date_time = datetime.strptime(filename[:-4], "%Y%m%d")
            df = pd.read_csv(
                self.datasource + "/" + filename, usecols=columns, index_col="symbol"
            )

            for ticker in self.tickers:
                df_row = None
                try:
                    row = df.loc[ticker]
                    df_row = row.to_frame().transpose()
                    df_row["datetime"] = date_time
                    df_row = df_row.set_index("datetime")
                    df_row.insert(loc=0, column="symbol", value=ticker)
                except:
                    pass  # TODO: Pass for now, implement method for checking missing data later
                try:
                    data_dict[ticker] = data_dict[ticker].append(
                        df_row, verify_integrity=True
                    )
                except KeyError:
                    if not df_row is None:
                        data_dict[ticker] = df_row
        # TODO: A mapping between stock ticker and price data needs to be there
        return data_dict

    def return_features(self) -> typing.List[str]:
        """
        :return: List of features found in dataset (column names)
        """
        file = open(self.datasource + "/" + self._file_names[0])
        return next(csv.reader(file))

    def _extract_filesnames_from_date(self) -> typing.List[str]:
        """
        :return: List of filenames from start date to end date
        :raise DateNoInvalidException if date does not exist in dataset
        """
        dt = sorted(list(self.__datetime_filename_HashMap))

        # TODO: How to handle weekend and trading holidays?
        try:
            start_index = dt.index(self.start)
        except:
            raise Exception(
                f"DateNoInvalidException: {self.start.date()} is not in the dataset"
            )

        try:
            end_index = dt.index(self.end)
        except:
            raise Exception(
                f"DateNoInvalidException: {self.end.date()} is not in the dataset"
            )

        filenames = []
        for index in dt[start_index : end_index + 1]:
            filenames.append(self.__datetime_filename_HashMap[index])

        return filenames

    def compute_features(
        self, features: typing.List[str]
    ) -> typing.Dict[str, pd.DataFrame]:
        raw_data_dict = self.load_data()
        data_dict = dict()
        feature_map = {
            "return": np.sum,
            "volatility": np.std,
            "skewness": scipy.stats.skew,
            "kurtosis": scipy.stats.kurtosis,
        }

        def compute_split_ratio(entry):
            if not np.isnan(entry):
                previous_ratio = np.float(entry.split(":")[0])
                after_ratio = np.float(entry.split(":")[1])
                return previous_ratio / after_ratio
            else:
                return 1.0

        for ticker, raw_df in raw_data_dict.items():
            # compute adjusted_close with roll forward method,
            # which add the dividend back to the price
            raw_df["div"] = raw_df["div"].fillna(0)
            raw_df["adjust_cum"] = (
                raw_df["adjustment"].apply(compute_split_ratio).cumprod()
            )
            raw_df["adjust_div"] = raw_df["div"] * raw_df["adjust_cum"]
            raw_df["adjust_close"] = (
                raw_df["close"] * raw_df["adjust_cum"] + raw_df["adjust_div"].cumsum()
            )
            # compute t-cost and return
            raw_df["return"] = raw_df["adjust_close"].apply(lambda x: np.log(x)).diff(1)
            raw_df["tcost"] = (raw_df["ask"] - raw_df["bid"]) / (
                raw_df["ask"] + raw_df["bid"]
            )

            for f in features:
                f_funcstr = f.split("_")[0]
                f_lookback = np.int(f.split("_")[1])
                raw_df[f] = (
                    raw_df["return"].rolling(f_lookback).apply(feature_map[f_funcstr])
                )

            selected_features = ["return", "tcost"] + features
            data_dict[ticker] = raw_df[selected_features]

        return data_dict


# =============================================================================
# MongoDB Data Loader
# =============================================================================


class Data_Loader_mongo(Data_Loader):
    """
    Data Loader from mongoDB
    """

    def __init__(
        self,
        datasource: str,
        tickers: typing.List[str],
        features: typing.List[str],
        start: datetime,
        end: datetime,
    ):
        """
        :param datasource: name of database in mongoDB
        :param tickers: symbols/tickers of the stocks you want to load
        :param features: features you want to extract
        :param start: start date
        :param end: end date ( result includes ending date)
        """
        super().__init__(datasource, tickers, features, start, end)
        client = MongoClient()
        self._db = client[datasource]
        if len(self._db.list_collection_names()) == 0:
            raise EmptyDatabase(f"{datasource} is an empty database")
        self._features_list = self.return_features()

    def load_data(self) -> typing.Dict[str, pd.DataFrame]:
        """
        :return: List of Dataframes (each df represents the time series for a particular stock)
        :raise FeaturesMismatchException if feature does not exist in dataset
        """
        if len(self.features) == 0:
            columns = self._features_list
        else:
            if len(set(self.features).intersection(set(self._features_list))) == len(
                self.features
            ):
                features = set(self.features)
                features.add("symbol")
                features.add("datetime")
                columns = list(set(features))
            else:
                raise Exception(
                    "FeaturesMismatchException: Some input features not present in dataset"
                )

        columns_dict = {column: 1 for column in columns}
        columns_dict["_id"] = 0

        data_dict = {}
        for ticker in self.tickers:
            collection = self._db[ticker]
            if collection.count_documents({}) == 0:
                raise Exception(f"{ticker} collection is empty (check ticker name)")

            range_query_statement = {"datetime": {"$gte": self.start, "$lte": self.end}}
            query_result = pd.DataFrame(
                collection.find(range_query_statement, columns_dict).sort("datetime")
            )
            query_result = query_result.set_index("datetime")

            try:
                data_dict[ticker] = data_dict[ticker].append(
                    query_result, verify_integrity=True
                )
            except KeyError:
                data_dict[ticker] = query_result

        return data_dict

    def return_features(self) -> typing.List[str]:
        """
        :return: List of features found in dataset (column names)
        """
        collection = self._db[self._db.list_collection_names()[0]]
        features = list(collection.find_one())
        features.remove("_id")
        return features

    def compute_features(
        self, features: typing.List[str]
    ) -> typing.Dict[str, pd.DataFrame]:
        raw_data_dict = self.load_data()
        data_dict = dict()
        feature_map = {
            "return": np.sum,
            "volatility": np.std,
            "skewness": scipy.stats.skew,
            "kurtosis": scipy.stats.kurtosis,
        }

        def compute_split_ratio(entry):
            try:
                previous_ratio = np.float(entry.split(":")[0])
                after_ratio = np.float(entry.split(":")[1])
                return previous_ratio / after_ratio
            except:
                return 1.0

        for ticker, raw_df in raw_data_dict.items():
            # compute adjusted_close with roll forward method,
            # which add the dividend back to the price
            raw_df = raw_df.astype(np.float, errors="ignore")
            raw_df["div"] = raw_df["div"].replace("", 0.0).astype(np.float)
            raw_df["adjust_cum"] = (
                raw_df["adjustment"].apply(compute_split_ratio).cumprod()
            )

            raw_df["adjust_div"] = raw_df["div"] * raw_df["adjust_cum"]
            raw_df["adjust_close"] = (
                raw_df["close"].astype(np.float) * raw_df["adjust_cum"]
                + raw_df["adjust_div"].cumsum()
            )
            # compute t-cost and return
            raw_df["return"] = raw_df["adjust_close"].apply(lambda x: np.log(x)).diff(1)
            raw_df["ask"] = raw_df["ask"].replace("", 0.0)
            raw_df["bid"] = raw_df["bid"].replace("", 0.0)
            raw_df["tcost"] = (
                raw_df["ask"].astype(np.float) - raw_df["bid"].astype(np.float)
            ) / (raw_df["ask"].astype(np.float) + raw_df["bid"].astype(np.float))

            for f in features:
                f_funcstr = f.split("_")[0]
                f_lookback = np.int(f.split("_")[1])
                raw_df[f] = (
                    raw_df["return"].rolling(f_lookback).apply(feature_map[f_funcstr])
                )

            selected_features = ["return", "tcost", "adjust_close"] + features
            data_dict[ticker] = raw_df[selected_features]

        return data_dict


# =============================================================================
# MongoDB Data Loader V2 (Finnhub IDs are the collection name)
# =============================================================================


class Data_Loader_mongo_V2(Data_Loader):
    """
    Data Loader from mongoDB
    """

    def __init__(
        self,
        datasource: str,
        tickers: typing.List[str],
        features: typing.List[str],
        start: datetime,
        end: datetime,
    ):
        """
        :param datasource: name of database in mongoDB
        :param tickers: symbols/tickers of the stocks you want to load
        :param features: features you want to extract
        :param start: start date
        :param end: end date ( result includes ending date)
        """
        super().__init__(datasource, tickers, features, start, end)
        client = MongoClient()
        self._db = client[datasource]
        if len(self._db.list_collection_names()) == 0:
            raise EmptyDatabase(f"{datasource} is an empty database")
        self._features_list = self.return_features()

    def load_data(self) -> typing.Dict[str, pd.DataFrame]:
        """
        :return: List of Dataframes (each df represents the time series for a particular stock)
        :raise FeaturesMismatchException if feature does not exist in dataset
        """
        if len(self.features) == 0:
            columns = self._features_list
        else:
            if len(set(self.features).intersection(set(self._features_list))) == len(
                self.features
            ):
                features = set(self.features)
                features.add("symbol")
                features.add("datetime")
                columns = list(set(features))
            else:
                raise Exception(
                    "FeaturesMismatchException: Some input features not present in dataset"
                )

        columns_dict = {column: 1 for column in columns}
        columns_dict["_id"] = 0

        tickers_new = self.__match_ticker_finnhub_id()

        data_dict = {}
        for ticker_class, values in tickers_new.items():
            for id_start_end in values:
                collection = self._db[id_start_end[0]]

                if collection.count_documents({}) == 0:
                    raise Exception(
                        f"{ticker_class} collection is empty (check ticker name)"
                    )

                range_query_statement = {
                    "datetime": {"$gte": id_start_end[1], "$lte": id_start_end[2]}
                }
                query_result = pd.DataFrame(
                    collection.find(range_query_statement, columns_dict).sort(
                        "datetime"
                    )
                )
                query_result = query_result.set_index("datetime")

                try:
                    data_dict[ticker_class] = data_dict[ticker_class].append(
                        query_result, verify_integrity=True
                    )
                except KeyError:
                    data_dict[ticker_class] = query_result

        return data_dict

    def return_features(self) -> typing.List[str]:
        """
        :return: List of features found in dataset (column names)
        """
        collection = self._db[self._db.list_collection_names()[0]]
        features = list(collection.find_one())
        features.remove("_id")
        return features

    def compute_features(
        self, features: typing.List[str]
    ) -> typing.Dict[str, pd.DataFrame]:
        raw_data_dict = self.load_data()
        data_dict = dict()
        feature_map = {
            "return": np.sum,
            "volatility": np.std,
            "skewness": scipy.stats.skew,
            "kurtosis": scipy.stats.kurtosis,
        }

        def compute_split_ratio(entry):
            try:
                previous_ratio = np.float(entry.split(":")[0])
                after_ratio = np.float(entry.split(":")[1])
                return previous_ratio / after_ratio
            except:
                return 1.0

        for ticker, raw_df in raw_data_dict.items():
            # compute adjusted_close with roll forward method,
            # which add the dividend back to the price
            raw_df = raw_df.astype(np.float, errors="ignore")
            raw_df["div"] = raw_df["div"].replace("", 0.0).astype(np.float)
            raw_df["adjust_cum"] = (
                raw_df["adjustment"].apply(compute_split_ratio).cumprod()
            )
            raw_df["adjvolume"] = (
                raw_df["volume"].astype(np.float) / raw_df["adjust_cum"]
            )
            raw_df["adjust_div"] = raw_df["div"] * raw_df["adjust_cum"]
            raw_df["adjclose"] = (
                raw_df["close"].astype(np.float) * raw_df["adjust_cum"]
                + raw_df["adjust_div"].cumsum()
            )
            # compute t-cost and return
            raw_df["ask"] = raw_df["ask"].replace("", 0.0)
            raw_df["bid"] = raw_df["bid"].replace("", 0.0)
            raw_df["return"] = raw_df["adjclose"].apply(lambda x: np.log(x)).diff(1)
            raw_df["tcost"] = (
                raw_df["ask"].astype(np.float) - raw_df["bid"].astype(np.float)
            ) / (raw_df["ask"].astype(np.float) + raw_df["bid"].astype(np.float))

            for f in features:
                f_field = f.split("_")[0]
                f_funcstr = f.split("_")[1]
                f_lookback = np.int(f.split("_")[2])
                raw_df[f] = (
                    raw_df[f_field].rolling(f_lookback).apply(feature_map[f_funcstr])
                )

            selected_features = [
                "return",
                "tcost",
                "adjclose",
                "adjvolume",
            ] + features
            data_dict[ticker] = raw_df[selected_features]

        return data_dict

    def __match_ticker_finnhub_id(
        self,
    ) -> typing.Dict[str, typing.List[typing.Tuple[str, datetime, datetime]]]:
        """
        :return: A dictionary with the key being the ticker + class and the value being the finnhub id and start and end dates
        """

        collection = self._db["ticker_id_meta_data"]

        tickers_new = {}
        for ticker in self.tickers:
            query_statement_ticker = {"symbol": ticker}

            tickers_all_class = collection.find(query_statement_ticker, {"_id": 0})

            for ticker_one_class in tickers_all_class:
                ticker = ticker_one_class["symbol"]
                class_of_ticker = ticker_one_class["class"]

                query_statement = {
                    "symbol": ticker,
                    "class": class_of_ticker,
                    # "start": {"$lte": self.start},
                    "end": {"$gte": self.end},
                }

                result = collection.find_one(query_statement, {"_id": 0})
                try:
                    tickers_new[result["symbol"] + "_" + result["class"]] = [
                        (result["finnhub_id"], self.start, self.end)
                    ]
                except:
                    pass

        return tickers_new

    def get_date_range(self):
        raw_data_dict = self.load_data()
        tickers = raw_data_dict.keys()
        date_range = {}
        for one_ticker in tickers:
            df = raw_data_dict[one_ticker]
            listing = df.index[0]
            delisting = df.index[-1]
            date_range[one_ticker] = [(listing, delisting)]

        return date_range


# =============================================================================
# Exceptions
# =============================================================================


class TimeInvalid(Exception):
    pass


class EmptyDatabase(Exception):
    pass


# =============================================================================
# Test
# =============================================================================

if __name__ == "__main__":

    # Test MongoDB Loader
    data_loader_mongo = Data_Loader_mongo_V2(
        "kaggle_US_Equity_daily",
        [
            "T",
            "GS",
            "GE",
            "AAPL",
        ],
        [],
        datetime(1992, 1, 2),
        datetime(2019, 12, 30),
    )

    features = data_loader_mongo.compute_features(
        [
            "return_volatility_20",
            "return_skewness_20",
            "return_kurtosis_20",
            "adjvolume_volatility_20",
        ]
    )
    for key, df in features.items():
        df = df.reset_index().dropna()
        print(key, df)
        df.to_csv("data/{}.csv".format(key), index=False)
