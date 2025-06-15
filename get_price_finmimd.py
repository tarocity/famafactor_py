# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 21:19:04 2025

@author: e1155_l2c4ye3
"""
#%% code_list
import pandas as pd
TSE_company = pd.read_csv('TSE_company.csv')
OTC_company = pd.read_csv('OTC_company.csv')
code_list = list(TSE_company['公司代號'].astype('str')) + \
    list(OTC_company['公司代號'].astype('str'))
del [TSE_company,OTC_company]

#%% TWStockPriceAdj_Finmind
import requests
import pandas as pd
import time
from datetime import datetime
TWStockPriceAdj_Finmind = pd.DataFrame()
for i in range(0,len(code_list) ):
    tmp_code = code_list[i]
    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {
        "dataset": "TaiwanStockPriceAdj",
        "data_id": tmp_code,
        # "start_date": "1994-10-01",
        "start_date": "2025-01-01",
        "end_date": "2025-04-04",
        "token": "", # 參考登入，獲取金鑰
    }
    resp = requests.get(url, params=parameter)
    tmp_data = resp.json()
    tmp_data = pd.DataFrame(tmp_data["data"])
    
    TWStockPriceAdj_Finmind = pd.concat([TWStockPriceAdj_Finmind,
                                         tmp_data]) #rbind
    print(tmp_code,'is done!!!','\n','(',i,'/',len(code_list)-1,')')
    
    url = "https://api.web.finmindtrade.com/v2/user_info"
    payload = {
        "token": "", # 參考登入，獲取金鑰
    }
    resp = requests.get(url, params=payload)
    API_counts = resp.json()["user_count"]
    while API_counts > 1560:
        print('使用次數: ',API_counts)
        print(datetime.now())
        time.sleep(300)
        resp = requests.get(url, params=payload)
        API_counts = resp.json()["user_count"]
        

# TWStockPriceAdj_Finmind.to_parquet('TWStockPriceAdj_Finmind.parquet')
TWStockPriceAdj_Finmind.to_parquet('TWStockPriceAdj_Finmind_2025.parquet')
#%%
import requests
import pandas as pd
url = "https://api.finmindtrade.com/api/v4/data"
parameter = {
    "dataset": "TaiwanStockInfo",
    "token": "", # 參考登入，獲取金鑰
}
resp = requests.get(url, params=parameter)
data = resp.json()
data = pd.DataFrame(data["data"])
