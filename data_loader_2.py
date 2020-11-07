import os
import csv
import pymongo
import pandas as pd
import numpy as np

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
    
    def __init__(self,datasource: str, 
                  tickers: typing.List[str],
                  features: typing.List[str],
                  start: datetime,
                  end: datetime
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
    def load_data(self) -> typing.List[pd.DataFrame]:
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
     
    def __init__(self,datasource: str, 
                  tickers: typing.List[str],
                  features: typing.List[str],
                  start: datetime,
                  end: datetime
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
        self.__datetime_filename_HashMap = {datetime.strptime(filename[:-4], '%Y%m%d'):filename for filename in self._file_names}
    
    def load_data(self) -> typing.List[pd.DataFrame]:
        """
        :return: List of Dataframes (each df represents the time series for a particular stock)
        :raise FeaturesMismatchException if feature does not exist in dataset
        """
        
        filenames = self._extract_filesnames_from_date()
        
        if len(self.features) == 0:
            columns = self._features_list
        else:
            if len(set(self.features).intersection(set(self._features_list))) == len(self.features):
                features = set(self.features)
                features.add("symbol")
                columns = list(features)
            else:
                raise Exception("FeaturesMismatchException: Some input features not present in dataset")
        
        data_dict = {}
        for filename in filenames:
            date_time = datetime.strptime(filename[:-4], '%Y%m%d')
            df = pd.read_csv(self.datasource +"/"+filename,usecols= columns,index_col="symbol")
            
            for ticker in self.tickers:
                try:
                    row = df.loc[ticker]
                except:
                    pass #Pass for now, implement method for checking missing data later
                df_row = row.to_frame().transpose()
                df_row["datetime"] = date_time
                df_row = df_row.set_index("datetime")
                df_row.insert(loc=0, column='symbol', value=ticker)
                
                try:
                    data_dict[ticker] = data_dict[ticker].append(df_row, verify_integrity=True)
                except KeyError:
                    data_dict[ticker] = df_row
        
        return list(data_dict.values())
    
    def return_features(self) -> typing.List[str]:
        """
        :return: List of features found in dataset (column names)
        """
        file = open(self.datasource +"/"+self._file_names[0])
        return next(csv.reader(file))
    
    
    def _extract_filesnames_from_date(self) -> typing.List[str]:
        """
        :return: List of filenames from start date to end date
        """
        dt = sorted(list(self.__datetime_filename_HashMap))
        
        try:
            start_index = dt.index(self.start)
        except:
            raise Exception(f"DateNoInvalidException: {self.start.date()} is not in the dataset")
        
        try:
            end_index = dt.index(self.end)
        except:
            raise Exception(f"DateNoInvalidException: {self.end.date()} is not in the dataset")
        
        filenames = []
        for index in dt[start_index:end_index+1]:
            filenames.append(self.__datetime_filename_HashMap[index])
        
        return filenames
        

# =============================================================================
# MongoDB Data Loader
# =============================================================================

# class Data_Loader_mongo(Data_Loader):
#     pass

# =============================================================================
# Exceptions
# =============================================================================

class TimeInvalid(Exception):
    pass

# =============================================================================
# Test
# =============================================================================

if __name__ == "__main__":

    #Test csv loader
    data_loader_csv = Data_Loader_CSV("kaggle_data",["DLS","CTY","AB"],["open","close"],datetime(1992,6,15),datetime(1992,9,17))
    data = data_loader_csv.load_data()
    

    pass
    
    
    
    