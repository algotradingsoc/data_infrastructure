import os
import wrds
import csv
import typing
import random
import numpy as np
import pandas as pd
from datetime import datetime
from abc import ABC, abstractmethod


"""
NOTE:
You need to pip install wrds before using this data loader
You also need to have a valid wrds account to access the database
"""

# =============================================================================
# Data Loader Abstract Class
# =============================================================================

class wrds_loader(ABC):
    def __init__(self,library:str,meta_table_name:str):
        """
        :param library: name of library on WRDS
        :param meta_table_name: name of the metadata table name
        """
        self.db = wrds.Connection()
        self.library = library
        self.meta_table_name = meta_table_name
        self.ticker_id = self.__get_meta_data(meta_table_name)
        super().__init__()
    
    def __get_meta_data(self,meta_table_name) -> pd.DataFrame:
        """
        :param meta_table_name: name of library that contains the metadata
        
        :return: Dataframe of the metadata
        """
        return self.db.get_table(library = self.library, table=meta_table_name)
    
    def return_tables_in_library(self) -> typing.List[str]:
        """
        :return: List of tables that exist in this library
        """
        return self.db.list_tables(library=self.library)
    
    def return_dates_of_table(self,table_name:str) -> typing.Tuple[str]:
        """
        :param table_name: name of table of interest
        
        :return: (start_date,end_date) of that table
        """
        query = f"select max(data.date),min(data.date) from {self.library}.{table_name} as data"
        date = self.db.raw_sql(query)
        start = date["min"][0].strftime("%Y-%m-%d")
        end = date["max"][0].strftime("%Y-%m-%d")
        return(start,end)
    
    def load_table_all(self):
        pass
    
    def load_table_specific(self):
        pass
    
    def load_table_specific_multi(self):
        pass
    
    def close_connection(self):
        self.db.close()
    

# =============================================================================
# Option Metrics Data Loader
# =============================================================================

class wrds_loader_option_metrics(wrds_loader):
    
    def __init__(self,transform_tickers:bool = False, prefix:str ="stock", starting_int:int = 1, random_increment:bool = False):
        """
        :param transform_tickers: whether to transform the tickers or not
        :param prefix: new ticker name before number (i.e. stock => stock_01, s => s_01)
        :param starting_int: starting number for transformed ticker
        :param random_increment: whether the name is incremented by 1 or a random number between 2 and 100
        """
        super().__init__("optionm","securd1")
        self.transform_tickers = transform_tickers
        if transform_tickers == True:
            self.ticker_to_transformed = self.generate_transformed_tickers(prefix, starting_int, random_increment)
        else:
            self.ticker_to_transformed = None
    
    def load_table_all(self, start:datetime, end:datetime, other_table_name:str, columns:typing.List[str] = [], limit:int=10) -> typing.Dict[str,pd.DataFrame]:
        """
        :param start: starting date
        :param end: ending date
        :param other_table_name: name of data table in the library
        :param columns: columns in the data table that you want to extract  (if zero return all columns)
        :param limit: number of results you want to return (if zero return all results)
        
        
        :return: Dict of Dataframes (each df represents the time series for a particular ticker)
        """
        start_format = start.strftime("%Y-%m-%d")
        end_format = end.strftime("%Y-%m-%d")
        query = f"select id.ticker from {self.library}.{self.meta_table_name} as id join {self.library}.{other_table_name} as data on id.secid = data.secid where date(data.date) >= '{start_format}' and date(data.date) <= '{end_format}'"
        tickers = self.db.raw_sql(query)
        output = {}
        if self.transform_tickers  == True:
              for ticker in tickers["ticker"]:
                      if ticker != None:
                        df = self.__load_table_one(ticker, start, end, other_table_name,columns, limit)
                        secids = set(df["secid"])
                        if len(secids) > 1:
                            new_ticker = set()
                            for secid in secids:
                                new_ticker.add(self.ticker_to_transformed[int(secid)][1])
                            if len(new_ticker) > 1:
                                raise Exception("multiple new tickers for ticker, check!")
                            else:
                                df["ticker"] = new_ticker
                                output[new_ticker] = df
                        else:
                            new_ticker = self.ticker_to_transformed[int(secids.pop())][1]
                            df["ticker"] = new_ticker
                            output[new_ticker] = df   
        else:
            for ticker in tickers["ticker"]:
                if ticker != None:
                    df = self.__load_table_one(ticker, start, end, other_table_name,columns, limit)
                    output[ticker] = df

        return output
    
    
    def load_table_specific(self,ticker:str, start:datetime, end:datetime, other_table_name:str,columns:typing.List[str] = [], limit:int=10) -> typing.Dict[str,pd.DataFrame]:
        """
        :param ticker: name of ticker you desire
        :param start: starting date
        :param end: ending date
        :param other_table_name: name of data table in the library
        :param columns: columns in the data table that you want to extract  (if zero return all columns)
        :param limit: number of results you want to return (if zero return all results)
                
        :return: Dict of Dataframes (each df represents the time series for a particular ticker)
        """
        
        df = self.__load_table_one(ticker, start, end, other_table_name,columns, limit)
        
        if self.transform_tickers  == True:
            secids = set(df["secid"])
            if len(secids) > 1:
                new_ticker = set()
                for secid in secids:
                    new_ticker.add(self.ticker_to_transformed[int(secid)][1])
                if len(new_ticker) > 1:
                    raise Exception("multiple new tickers for ticker, check!")
                else:
                    df["ticker"] = new_ticker
                    ticker = new_ticker
            else:
                new_ticker = self.ticker_to_transformed[int(secids.pop())][1]
                df["ticker"] = new_ticker
                ticker = new_ticker
                
        return {ticker:df}
        
        
    def load_table_specific_multi(self,tickers:typing.List[str], start:datetime, end:datetime, other_table_name:str,columns:typing.List[str] = [], limit:int=10) -> typing.Dict[str,pd.DataFrame]:
        """
        :param tickerS: list of ticker you desire
        :param start: starting date
        :param end: ending date
        :param other_table_name: name of data table in the library
        :param columns: columns in the data table that you want to extract  (if zero return all columns)
        :param limit: number of results you want to return (if zero return all results)
                
        :return: Dict of Dataframes (each df represents the time series for a particular ticker)
        """
        output = {}
        if self.transform_tickers  == True:
            for ticker in tickers:
                df = self.__load_table_one(ticker, start, end, other_table_name,columns, limit)
                secids = set(df["secid"])
                if len(secids) > 1:
                    new_ticker = set()
                    for secid in secids:
                        new_ticker.add(self.ticker_to_transformed[int(secid)][1])
                    if len(new_ticker) > 1:
                        raise Exception("multiple new tickers for ticker, check!")
                    else:
                        df["ticker"] = new_ticker
                        output[new_ticker] = df
                else:
                    new_ticker = self.ticker_to_transformed[int(secids.pop())][1]
                    df["ticker"] = new_ticker
                    output[new_ticker] = df
        else: 
            for ticker in tickers:
                df = self.__load_table_one(ticker, start, end, other_table_name,columns, limit)
                output[ticker] = df
    
        return output
    
    def generate_transformed_tickers(self,prefix:str,start:int,random_increment:bool) -> typing.Dict[int,typing.Tuple[str,str]]:
        """
        :param prefix: new ticker name before number (i.e. stock => stock_01, s => s_01)
        :param start: starting number for increment 
        :param random_increment: whether the name is incremented by 1 or a random number between 2 and 100

        :return: Dict with secid as key and (ticker name, new transformed ticker name) as value 
        """
        if prefix != "":
            prefix += "_"
        
        if random_increment == True:
            increment = random.randint(2, 100)
        else:
            increment = 1
        
        ticker_to_random = {}
        for row in self.ticker_id.to_numpy():
            ticker_to_random[int(row[0])] = (row[2],prefix+str(start))
            start += increment
        
        return ticker_to_random
    
    def save_tickers_hashmap(self):
        pass
    
    def save_as_csv(self,results:typing.Dict[str,pd.DataFrame]):
        pass
    
    def __load_table_one(self,ticker:str, start:datetime, end:datetime, other_table_name:str,columns:typing.List[str] = [], limit:int=10) -> pd.DataFrame:
        """
        :param ticker: name of ticker you desire
        :param start: starting date
        :param end: ending date
        :param other_table_name: name of data table in the library
        :param columns: columns in the data table that you want to extract  (if zero return all columns)
        :param limit: number of results you want to return (if zero return all results)
        
        :raise ColumnNotInDataError if columns does not exist in data
        
        :return: dataframes with dataset required
        """
        start_format = start.strftime("%Y-%m-%d")
        end_format = end.strftime("%Y-%m-%d")
        if len(columns) != 0:
            other_data_column = set(self.db.get_table(library = self.library, table=other_table_name,obs=1).columns)
            columns_not_in_data = set(columns).difference(other_data_column)
            if len(columns_not_in_data) != 0:
                raise Exception(f"ColumnNotInDataError: '{columns_not_in_data}' is not in {other_table_name} \n The available columns are {other_data_column}")
            else:
                if "date" not in columns:
                    columns.append("date")
                if "secid" not in columns:
                    columns.append("secid")
                columns_string = ', '.join([f"data.{column}" for column in columns])
                if limit != 0:
                    query = f"select id.ticker, {columns_string} from {self.library}.{self.meta_table_name} as id join {self.library}.{other_table_name} as data on id.secid = data.secid where id.ticker = '{ticker}' and date(data.date) >= '{start_format}' and date(data.date) <= '{end_format}' limit {limit}"
                else:
                    query = f"select id.ticker, {columns_string} from {self.library}.{self.meta_table_name} as id join {self.library}.{other_table_name} as data on id.secid = data.secid where id.ticker = '{ticker}' and date(data.date) >= '{start_format}' and date(data.date) <= '{end_format}'"
        else:
            if limit != 0:
                query = f"select id.ticker, data.*  from {self.library}.{self.meta_table_name} as id join {self.library}.{other_table_name} as data on id.secid = data.secid where id.ticker = '{ticker}' and date(data.date) >= '{start_format}' and date(data.date) <= '{end_format}' limit {limit}"
            else:
                query = f"select id.ticker, data.*  from {self.library}.{self.meta_table_name} as id join {self.library}.{other_table_name} as data on id.secid = data.secid where id.ticker = '{ticker}' and date(data.date) >= '{start_format}' and date(data.date) <= '{end_format}'"
        
        return self.db.raw_sql(query,date_cols=["date"],index_col=["date"])
    
    
# =============================================================================
# Compustats Data Loader
# =============================================================================



# =============================================================================
# Simple Tutorial
# =============================================================================

if __name__ == "__main__":
    
    loader = wrds_loader_option_metrics()
    
    table_name = "secprd"
    # table_name = "hvold2015"
    # table_name = "opprcd2015"
    # table_name = "stdopd2015"
    # table_name = "vsurfd2015"
    
    start_date = datetime(2015,12,10)
    end_date = datetime(2015,12,21)
    limit = 10
    
    # data_all = loader.load_table_all(start_date, end_date, table_name,limit=limit)
    
    ticker = "AAPL"
    
    data_one = loader.load_table_specific(ticker, start_date, end_date, table_name)
        
    tickers = ["AAPL","GOOGL","FB"]
    
    data_multi = loader.load_table_specific_multi(tickers, start_date, end_date, table_name)
    
    loader.close_connection()
