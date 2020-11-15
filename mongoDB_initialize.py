import os
import csv
import pymongo
from pymongo import MongoClient

from datetime import datetime

import pandas as pd

client = MongoClient()

# =============================================================================
# Create Raw Dataset database
# =============================================================================

# Create New Databse (One collection for each day)
# The basically uploads csv to mongoDB without changing format
def create_database(db_name, data_directory):
    """
    :param db_name: name of database
    :param data_directory: path of directory where data csv is stored
    """

    db = client[db_name]

    file_names = os.listdir(data_directory)

    for name in file_names:
        collection = db[name[:-4]]
        file = open(data_directory + "/" + name)
        csv_file = csv.DictReader(file)

        for row in csv_file:
            collection.insert_one(row)


# Create New Database (Collection name == ticker)
# This changes the format so that each ticker will be the collection
# Use this method whening using "Data_Loader_mongo" in data_loader
def create_database_ticker(db_name, data_directory):
    """
    :param db_name: name of database
    :param data_directory: path of directory where data csv is stored
    """

    db = client[db_name]

    file_names = os.listdir(data_directory)

    ticker_id_HashMap = {}
    for name in file_names:
        file = open(data_directory + "/" + name)
        csv_file = csv.DictReader(file)
        dt = datetime.strptime(name[:-4], "%Y%m%d")

        for row in csv_file:
            collection = db[row["symbol"]]
            row["datetime"] = dt
            collection.insert_one(row)

            # try:
            #     ticker_id_HashMap[row["symbol"]].append(row["finnhub_id"])
            # except:
            #     ticker_id_HashMap[row["symbol"]] = [row["finnhub_id"]]

    # ticker_id_meta_data = db["ticker_id_meta_data"]
    # ticker_id_meta_data.insert_many(ticker_id_HashMap)

    # WORK IN PROGRESS: ticker to finnhub id meta loader


# Create New Database (Collection name == finnhub ID)
# This changes the format so that each ticker will be the collection
# Use this method whening using "Data_Loader_mongo_v2" in data_loader
# Also creates a collection mapping tickers, class, start and end time to the correct finnhub ID
def create_database_id(db_name, data_directory):
    """
    :param db_name: name of database
    :param data_directory: path of directory where data csv is stored
    """

    db = client[db_name]

    file_names = os.listdir(data_directory)

    id_ticker_HashMap = {}
    for name in file_names:
        file = open(data_directory + "/" + name)
        csv_file = csv.DictReader(file)
        dt = datetime.strptime(name[:-4], "%Y%m%d")

        for row in csv_file:
            collection = db[row["finnhub_id"]]
            collection.create_index([("datetime", pymongo.ASCENDING)], unique=True)
            row["datetime"] = dt
            try:
                collection.insert_one(row)
                try:
                    id_ticker_HashMap[row["finnhub_id"]][
                        (row["symbol"], row["class"])
                    ].append(dt)
                except:
                    id_ticker_HashMap[row["finnhub_id"]] = {
                        (row["symbol"], row["class"]): [dt]
                    }
            except:
                pass  # Pass for now (Should really look into the data that are causing duplicates and see if it is a data problem)

    ticker_id_HashMap = {}
    for finnhub_id, ticker_class_dates in id_ticker_HashMap.items():
        for ticker_class, dates in ticker_class_dates.items():
            ticker_id_HashMap[
                (ticker_class[0], ticker_class[1], dates[0], dates[-1])
            ] = finnhub_id

    ticker_id_meta_data = db["ticker_id_meta_data"]
    ticker_id_meta_data.create_index(
        [
            ("ticker", pymongo.ASCENDING),
            ("class", pymongo.ASCENDING),
            ("start", pymongo.ASCENDING),
            ("end", pymongo.ASCENDING),
        ],
        unique=True,
    )
    for key, value in ticker_id_HashMap.items():
        row = {
            "ticker": key[0],
            "class": key[1],
            "start": key[2],
            "end": key[3],
            "finnhub_id": value,
        }
        ticker_id_meta_data.insert_one(row)


def create_ticker_id_map(db_name):

    db = client[db_name]
    ticker_id_meta_data = db["ticker_id_meta_data"]
    ticker_id_meta_data.create_index(
        [
            ("ticker", pymongo.ASCENDING),
            ("class", pymongo.ASCENDING),
            ("start", pymongo.ASCENDING),
            ("end", pymongo.ASCENDING),
        ],
        unique=True,
    )

    collection_list = db.list_collection_names()
    for cname in collection_list:
        print(cname)
        df = pd.DataFrame(db[cname].find()).sort_values("datetime")
        start_symbol = df.groupby(["symbol", "class"]).first()
        end_symbol = df.groupby(["symbol", "class"]).last()
        symbol = start_symbol[["finnhub_id"]]
        symbol["start"] = start_symbol["datetime"]
        symbol["end"] = end_symbol["datetime"]
        symbol.reset_index(inplace=True)
        print(symbol)
        try:
            ticker_id_meta_data.insert_many(symbol.to_dict("records"))
        except pymongo.errors.BulkWriteError as e:
            print(e.details["writeErrors"])


if __name__ == "__main__":

    create_ticker_id_map(
        "kaggle_US_Equity_daily",
    )

    # Test Create Database
    # create_database_id("kaggle_US_Equity_daily", "../data/kaggle_us_eod")

    """
    Change "kaggle_db" to any name you want the database to be called
    Change "kaggle_data" to the path your csv files are located in.
    """

    """
    NOTE: THIS OPERATION WILL TAKE A LONG TIME TO RUN
    Run this function once after you install mongoDB.
    Once the database is created you do not need to run this function again.
    (Even if you close mongoDB and open it again)
    """

    pass
