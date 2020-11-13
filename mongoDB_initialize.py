import os
import csv
import pymongo
from pymongo import MongoClient

from datetime import datetime

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
def create_database_id(db_name, data_directory):
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


if __name__ == "__main__":

    # Test Create Database
    create_database_id("Kaggle_US_Equity", "../data/kaggle_us_eod")

    """
    Change "kaggle_raw" to any name you want the database to be called
    Change "kaggle_data" to the path your csv files are located in.
    """

    """
    NOTE: THIS OPERATION WILL TAKE A LONG TIME TO RUN
    Run this function once after you install mongoDB.
    Once the database is created you do not need to run this function again.
    (Even if you close mongoDB and open it again)
    """
    pass
