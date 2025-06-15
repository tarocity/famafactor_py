# -*- coding: utf-8 -*-
"""
Created on Tue Mar 11 10:26:31 2025

@author: e1155_l2c4ye3
"""

#%% 計算日報酬
import pandas as pd
import numpy as np
# StockPrice_TW = pd.read_parquet('TWStockPrice_yfinance.parquet')
StockPrice_TW = pd.read_parquet('TWStockPriceAdj_Finmind.parquet')
StockPrice_TW.to_parquet('adjprice_to20250227.parquet')
# 取收盤價計算日報酬率
TWDailyRet = StockPrice_TW[['date','stock_id','close']].\
    rename(columns = {'close':'Cprice'})
#
# StockPrice_TW.stock_id.unique()
# 等等要平移資料 先排序
TWDailyRet = TWDailyRet.sort_values(by= ['stock_id','date'])

TWDailyRet = TWDailyRet.assign(# 計算真實報酬跟 對數報酬
    Dtrue_ret = TWDailyRet.groupby('stock_id')['Cprice'].\
        transform(lambda x: (x-x.shift(1))/x.shift(1) ),
    # Dlog_ret = TWDailyRet.groupby('stock_id')['Cprice'].\
    #     transform(lambda x: np.log(x).diff(periods = 1)),
    Dlog_ret = TWDailyRet.groupby('stock_id')['Cprice'].\
        transform(lambda x: np.log(x) - np.log(x.shift(1)) )
        )


# TWDailyRet = TWDailyRet.drop(labels = ['Cprice'], axis = 1)
TWDailyRet.to_parquet('TWDailyRet.parquet')
#%% 
import pandas as pd
# 先做出月的 str 等等分組用
TWDailyRet = pd.read_parquet('TWDailyRet.parquet')
TWDailyRet['month'] = TWDailyRet['date'].str.\
    slice_replace(start = -2,repl = '01')
# 月真實報酬跟對數報酬分開算
# 對數報酬直接同月日對數報酬相加,有na不計算，即每支股票的起始資料月的月報酬
month_ret_log = TWDailyRet.groupby(['stock_id','month'],as_index = False)['Dlog_ret'].\
    agg([('Mlog_ret',lambda x: x.sum(skipna=False) )])

# 真實報酬用每月收盤價去計算
# 先抓出每月收盤價
month_ret_true = TWDailyRet.groupby(['stock_id','month'],as_index = False)['Cprice'].\
    agg([('Mlast_price',lambda x: x.tail(n = 1))])
# 用本月收盤價-上月收盤價 / 上月收盤價 算月真實報酬
month_ret_true = month_ret_true.assign(
    Mtrue_ret = month_ret_true.groupby('stock_id')['Mlast_price'].\
        transform(lambda x: (x - x.shift(1))/x.shift(1) ))
# 最後兩月報酬dataframe合併即完成
TWMonthRet = pd.merge(month_ret_true, month_ret_log,
                     how= 'left', on= ['stock_id','month'])

TWMonthRet = TWMonthRet.drop(labels = ['Mlast_price'], axis = 1)
TWMonthRet.to_parquet('TWMonthRet.parquet')
#%%
import pandas as pd
# TWDailyRet.to_csv('TWDailyRet.csv',index = False)
# month_ret.to_csv('TWMonthRet.csv',index = False)

# TWDailyRet = pd.read_csv('TWDailyRet.csv')
# TWMonthRet = pd.read_csv('TWMonthRet.csv')

# TWDailyRet.to_parquet('TWDailyRet.parquet')
# TWMonthRet.to_parquet('TWMonthRet.parquet')
    
TWDailyRet = pd.read_parquet('TWDailyRet.parquet')
TWDailyRet_202504 = pd.read_parquet('TWDailyRet_202504.parquet')
TWMonthRet = pd.read_parquet('TWMonthRet.parquet')
TWMonthRet_202504 = pd.read_parquet('TWMonthRet_202504.parquet')

TAIEX_Mreturn = pd.read_parquet('TAIEX_Mreturn.parquet')
TAIEX_Dreturn = pd.read_parquet('TAIEX_Dreturn.parquet')
    
#%%
import pandas as pd
TAIEX_index = pd.read_parquet('TAIEX_index.parquet')
TAIEX_Dreturn = TAIEX_index.sort_values('date').assign(
    daily_ret = lambda x: (x.price-x.price.shift(1))/x.price.shift(1) )

TAIEX_Mreturn = TAIEX_Dreturn.assign(
    month = lambda x: x.date.str.slice_replace(start = -2,repl = '01')).\
    groupby(['month'],as_index = False).price.\
    agg([('price', lambda x: x.tail(1) )]).assign(
    month_ret = lambda x: (x.price-x.price.shift(1))/x.price.shift(1),
    stock_id = '9999')

TAIEX_Mreturn.to_parquet('TAIEX_Mreturn.parquet')
#
import pandas as pd
TAIEX_index = pd.read_parquet('TAIEX_index.parquet')
TAIEX_Dreturn = TAIEX_index.assign(
    ret = lambda x: (x.price-x.price.shift(1))/x.price.shift(1))

TAIEX_Dreturn.to_parquet('TAIEX_Dreturn.parquet')
#%% 合併 2025 年 3月完整 dret
import pandas as pd
Finmindprice_2025 = pd.read_parquet('TWStockPriceAdj_Finmind_2025.parquet')
TWDailyRet = pd.read_parquet('TWDailyRet.parquet')
#
dret_a2502 = Finmindprice_2025.sort_values(
    ['stock_id','date'])[['stock_id','date','close']]
dret_a2502 = dret_a2502.assign(
    ret = dret_a2502.groupby(['stock_id'],as_index = False).close.transform(
        lambda x: (x-x.shift(1))/x.shift(1) )).loc[
            lambda x: x.date >= '2025-02-01'].drop(columns = ['close'])
#
TWDailyRet_b2502 = TWDailyRet.rename(
    columns = {'Dtrue_ret':'ret'}).loc[
        lambda x: x.date < '2025-02-01'][['stock_id','date','ret']]
TWDailyRet_202504 = pd.concat([TWDailyRet_b2502,dret_a2502])

TWDailyRet_202504.to_parquet('TWDailyRet_202504.parquet')
#%% 合併 2025 年 3月完整 mret
import pandas as pd
Finmindprice_2025 = pd.read_parquet('TWStockPriceAdj_Finmind_2025.parquet')
TWMonthRet = pd.read_parquet('TWMonthRet.parquet')

#
mret_a2502 = Finmindprice_2025.sort_values(['stock_id','date']).assign(
    month = lambda x: x.date.str.slice_replace(start = -2, repl = '01')).\
    groupby(['stock_id','month'],as_index = False).close.agg(
        [('MLP',lambda x: x.tail(1))])

mret_a2502 = mret_a2502.assign(
    ret = lambda x: x.groupby('stock_id',as_index = False).MLP.transform(
        lambda x: (x-x.shift(1))/x.shift(1) ) ).loc[
            lambda x: x.month < '2025-04-01'].drop(
                columns = ['MLP']).dropna()
#
TWMonthRet_b2502 = TWMonthRet.rename(
    columns = {'Mtrue_ret':'ret'}).loc[
        lambda x: x.month < '2025-02-01'].drop(
            columns = ['Mlog_ret'])

TWMonthRet_202504 = pd.concat([TWMonthRet_b2502,mret_a2502])

TWMonthRet_202504.to_parquet('TWMonthRet_202504.parquet')

#
StockPrice_TW.info

