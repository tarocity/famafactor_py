# -*- coding: utf-8 -*-
"""
Created on Tue Apr 29 22:15:09 2025

@author: e1155_l2c4ye3
"""

import pandas as pd
import numpy as np
# adjprice_to20250227
adjprice_to20250227 = pd.read_parquet('adjprice_to20250227.parquet')
check_df = adjprice_to20250227.describe()
adjprice_to20250227.info
adjprice_to20250227.to_csv(
    "C:/Users/e1155_l2c4ye3/Desktop/finmind_data/adjprice_to20250227.csv",
                           index = False)
# adjprice_from20250102
adjprice_from20250102 = pd.read_parquet('adjprice_from20250102.parquet')
adjprice_from20250102.to_csv(
    "C:/Users/e1155_l2c4ye3/Desktop/finmind_data/adjprice_from20250102.csv",
                           index = False)

# monthret_to202503
monthret_to202503 = pd.read_parquet('monthret_to202503.parquet')
# MySQL does not recognise NaN values in CSV files.
# Replace all NaN values with NULL.
# the max ret is 56.667 so
# replace np.nan with 123, then replace 123 with NULL in MySQL
check_df = monthret_to202503.describe()
monthret_to202503 = monthret_to202503.replace(np.nan,123)
monthret_to202503.info
monthret_to202503.to_csv(
    "C:/Users/e1155_l2c4ye3/Desktop/finmind_data/monthret_to202503.csv",
                           index = False)

# dayret_to20250402, 
dayret_to20250402 = pd.read_parquet('dayret_to20250402.parquet')
check_df = dayret_to20250402.describe()
dayret_to20250402 = dayret_to20250402.replace(np.nan,123)
dayret_to20250402.info
dayret_to20250402.to_csv(
    "C:/Users/e1155_l2c4ye3/Desktop/finmind_data/dayret_to20250402.csv",
                           index = False)
# marketcap
marketcap = pd.read_parquet('marketcap.parquet')
marketcap = marketcap.assign(
    market_value = lambda x: np.select([x.market_value < 0],
                                       [0],
                                       default= x.market_value))
check_df = marketcap.describe()
marketcap.info
marketcap.to_csv(
    "C:/Users/e1155_l2c4ye3/Desktop/finmind_data/marketcap.csv",
                           index = False)

# balancesheet
balancesheet = pd.read_parquet('balancesheet.parquet')
balancesheet.info()
balancesheet.isna().sum()
balancesheet = balancesheet.describe()
balancesheet.to_csv(
    "C:/Users/e1155_l2c4ye3/Desktop/finmind_data/balancesheet.csv",
                           index = False)

# incomestatement
incomestatement = pd.read_parquet('incomestatement.parquet')
incomestatement.info()
incomestatement.isna().sum()
check_df = incomestatement.describe()
incomestatement.to_csv(
    "C:/Users/e1155_l2c4ye3/Desktop/finmind_data/incomestatement.csv",
                           index = False)

# taiex
taiex = pd.read_parquet('taiex.parquet')
taiex.info
taiex = taiex[['date','stock_id','price']]
taiex.to_csv(
    "C:/Users/e1155_l2c4ye3/Desktop/finmind_data/taiex.csv",
                           index = False)




























