import unittest

from datetime import datetime
from dataloader import Data_Loader_CSV


class Test_Data_Loader(unittest.TestCase):
    
    def test_csv_loader(self):
        """
        test script testing whether the csv loader works
        """
        
        data_directory = "kaggle_data" #"../data/kaggle_us_eod"
        tickers = ["DIS", "GE", "AAPL"]
        features = []
        start = datetime(2016, 10, 13)
        end = datetime(2016, 11, 7)
        
        
        data_loader_csv = Data_Loader_CSV(data_directory,tickers,features,start,end)
        data = data_loader_csv.load_data()
        
        if features == []:
            no_features = 12
        else:
            no_features = len(features)
            
            
        tickers_match = len(tickers) == len(data)
        
        #no_working_days = number of working days between start and end date (CALENDAR person please implement)
        
        features_match = True
        days_match = True
        for df in data.values():
            shape_temp = df.shape
            if shape_temp[1] != no_features:
                features_match = False
                
            # if shape_temp[0] != no_working_days:
            #     days_match = False
        
        self.assertTrue(tickers_match and features_match and days_match)


if __name__ == "__main__":
    unittest.main()
    
    # If you are wondering how to implement a test function: Google unittest. There are a lot of tutorials online about this.
        
    
    pass