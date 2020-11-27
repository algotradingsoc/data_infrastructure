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


# Create New Database (Collection name == finnhub ID)
# This changes the format so that each ticker will be the collection
# Use this method whening using "Data_Loader_mongo_v2" in data_loader
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
            except:
                pass  # Pass for now (Should really look into the data that are causing duplicates and see if it is a data problem)


# Create a collection for symbol to id meta data.
# The collection is for mapping the symbol, class, start and end time to the correct finnhub ID
def create_ticker_id_map(db_name):

    db = client[db_name]
    ticker_id_meta_data = db["ticker_id_meta_data"]
    ticker_id_meta_data.create_index(
        [
            ("symbol", pymongo.ASCENDING),
            ("class", pymongo.ASCENDING),
            ("start", pymongo.ASCENDING),
            ("end", pymongo.ASCENDING),
        ],
        unique=True,
    )

    collection_list = db.list_collection_names()
    collection_list.remove("ticker_id_meta_data")
    for cname in collection_list:
        df = pd.DataFrame(db[cname].find()).sort_values("datetime")
        start_symbol = df.groupby(["symbol", "class"]).first()
        end_symbol = df.groupby(["symbol", "class"]).last()
        symbol = start_symbol[["finnhub_id"]].copy(deep=True)
        symbol["start"] = start_symbol["datetime"]
        symbol["end"] = end_symbol["datetime"]
        symbol.reset_index(inplace=True)
        try:
            ticker_id_meta_data.insert_many(symbol.to_dict("records"))
        except pymongo.errors.BulkWriteError as e:
            print(e.details["writeErrors"]) 
            #Use to catch duplications in data (problems in dataset)


# Create collection and symbol_to_id meta data all in one function
def create_database_id_and_ticker(db_name,data_directory):
    create_database_id(db_name,data_directory)
    create_ticker_id_map(db_name)

if __name__ == "__main__":

    """
    NOTE: THE LOADING OPERATION WILL TAKE A LONG TIME TO RUN
    Run this function once after you install mongoDB.
    Once the database is created you do not need to run this function again.
    (Even if you close mongoDB and open it again)
    """
    
    '''
    Create Database with ticker data:
    Change first entry to any name you want the database to be called
    Change second entry to the path your csv files are located in.
    '''
    # create_database_id_and_ticker("kaggle_US_Equity_daily", "../data/kaggle_us_eod")

    '''
    Database already created using create_database_id, and just need ticker data
    NOTE: if you ran create_ticker_id with old function, you might already have an "ticker_id_meta_data" collection.
    Please remove it before running the following command with client[db_name].ticker_id_meta_data.drop()
    '''
    # create_ticker_id_map("kaggle_US_Equity_daily")

    pass
