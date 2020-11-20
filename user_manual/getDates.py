# -*- coding: utf-8 -*-
import os
import pandas as pd

path = 'c:\\Users\\xu_ho\\Downloads\\Kaggle US EOD 1994_2019\\data'

arr = os.listdir(path)

df = pd.DataFrame(arr, columns = ['raw_name'])

df['date'] = df['raw_name'].map(lambda x: x.split('.')[0])

df['date'] = pd.to_datetime(df['date'])

df.drop('raw_name', axis = 1).to_csv('trading_dates.csv')


