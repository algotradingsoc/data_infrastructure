# data_infrastructure

## Overview 

Managing the data infrastrcture and computing resources of Imperial Algosoc  
Database: price and fundamentals data of US Equities and other asset classes  
Cloud computing: deploy machine learning models on cloud computing services to generate live predictions  

## Resources and skills invovled 

Python: Pandas, Dask  
Database: MongoDB  
Computing: Docker, Linux  

## Data Sources 

Kaggle US Equities Data (1992-2019)     
Kaggle US reported financial Data (2010-2020)   
Quandl Financial Industry Regulatory Authority (2013-2020)

https://www.kaggle.com/finnhub/reported-financials      
https://www.kaggle.com/finnhub/end-of-day-us-market-data-survivorship-bias-free     
https://www.quandl.com/data/FINRA-Financial-Industry-Regulatory-Authority  

Calendar for US Equities
Installation
```bash
pip install pandas_market_calendars
```
https://github.com/rsheftel/pandas_market_calendars

## Project stages 

Stage 1: Historical database 

Design database schema for storing daily US Equity and fundamentals data  

Build metadata for database
- Trading calendar for US Equities  
- Listing and delisting days of Equities data  
- Ranking of Equities by trade volume  
- Report date of financial statements   

Build functions for feature engineering 
- Dividend and split adjustments
- Log returns and realised volatility measures  
- Financial ratios  

Build API for database access
-  Point-in-time accesss of price data from database
-  Search tickers by name and fundamentals


Stage 2: Cloud services 

Process live data
- Set up Websocket connection with live datafeeds 
- Build database cache for processing live data from exchanges

Deploy database on cloud services 
- Manage access control of the database 
- Manage resources usage of database 

Deploy machine learning models
- Create docker images for Alpha models developed by the research team
- Deploy models for live predictions 
