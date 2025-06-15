# -*- coding: utf-8 -*-
"""
Created on Tue Mar 11 23:07:14 2025

@author: e1155_l2c4ye3
"""

#%% code_list
import pandas as pd
TSE_company = pd.read_csv('TSE_company.csv')
OTC_company = pd.read_csv('OTC_company.csv')
code_list = list(TSE_company['公司代號'].astype('str')) + \
    list(OTC_company['公司代號'].astype('str'))
finmind_delist_code = pd.read_csv('finmind_delist_code.csv')
finmind_delist_code = finmind_delist_code.assign(
    stock_id = lambda x: x.stock_id.astype('str')).loc[
    lambda x: x.stock_id.str.len() == 4]
finmind_delist_code = finmind_delist_code.stock_id.to_list()
del [TSE_company,OTC_company,finmind_delist_code]

#%% BalanceSheet 下載
import requests
import pandas as pd
import time
from datetime import datetime
BalanceSheet = pd.DataFrame()
url = "https://api.finmindtrade.com/api/v4/data"
url_API = "https://api.web.finmindtrade.com/v2/user_info"
API_counts = 0
for i in range(0,len(code_list)):
    tmp_code = code_list[i]
    parameter = {
        "dataset": "TaiwanStockBalanceSheet",
        "data_id": tmp_code,
        # "start_date": "2010-01-01",
        "start_date": "2024-12-01",
        "token": "", # 參考登入，獲取金鑰
    }
    tmp_data = requests.get(url, params=parameter)
    tmp_data = tmp_data.json()
    tmp_data = pd.DataFrame(tmp_data['data'])
    
    BalanceSheet = pd.concat([BalanceSheet,tmp_data]) #rbind
    print(tmp_code,'is done!!!','\n','(',i,'/',len(code_list)-1,')')
    
    
    
    # url_API = "https://api.web.finmindtrade.com/v2/user_info"
    while (i % 10 == 0) | (API_counts >= 1500):
        payload = {
            "token": "", # 參考登入，獲取金鑰
        }
        resp = requests.get(url_API, params=payload)
        API_counts = resp.json()["user_count"]
        
        if (API_counts >= 1500):
            print('使用次數: ',API_counts)
            print(datetime.now())
            resp = requests.get(url_API, params=payload)
            API_counts = resp.json()["user_count"]
            time.sleep(300)
        else:
            break
            
        
    
    
    



# BalanceSheet.to_parquet('BalanceSheet.parquet')
# TWBalceSht_2024.to_parquet('TWBalceSht_2024.parquet')
# BalanceSheet = pd.read_parquet('BalanceSheet.parquet')
#%% FinancialStatements
import requests
import pandas as pd
import time
from datetime import datetime
FinStat = pd.DataFrame()
url = "https://api.finmindtrade.com/api/v4/data"
url_API = "https://api.web.finmindtrade.com/v2/user_info"
API_counts = 0
for i in range(0,len(code_list) ):
    tmp_code = code_list[i]
    parameter = {
        "dataset": "TaiwanStockFinancialStatements",
        "data_id": tmp_code,
        # "start_date": "1990-01-01",
        "start_date": "2024-12-01",
        "token": "", # 參考登入，獲取金鑰
    }
    tmp_data = requests.get(url, params=parameter)
    tmp_data = tmp_data.json()
    tmp_data = pd.DataFrame(tmp_data['data'])
    
    FinStat = pd.concat([FinStat,tmp_data]) #rbind
    print(tmp_code,'is done!!!','\n','(',i,'/',len(code_list)-1,')')
    

    while (i % 10 == 0) | (API_counts >= 1500):
        payload = {
            "token": "", # 參考登入，獲取金鑰
        }
        resp = requests.get(url_API, params=payload)
        API_counts = resp.json()["user_count"]
        
        if (API_counts >= 1500):
            print('使用次數: ',API_counts)
            print(datetime.now())
            resp = requests.get(url_API, params=payload)
            API_counts = resp.json()["user_count"]
            time.sleep(300)
        else:
            break
        
# FinStat.to_csv('TWFinStat.csv',index = False,encoding='big5') 
FinStat.to_parquet('TWFinStat_2024Q4.parquet')

#%% TWMarketCap
import requests
import pandas as pd
import time
from datetime import datetime
TWMarketCap = pd.DataFrame()
url = "https://api.finmindtrade.com/api/v4/data"
url_API = "https://api.web.finmindtrade.com/v2/user_info"
API_counts = 0
for i in range(0,len(code_list) ):
    tmp_code = code_list[i]
    parameter = {
        "dataset": "TaiwanStockMarketValue",
        "data_id": tmp_code,
        # "start_date": "2003-12-01",
        "start_date": "2025-01-01",
        "end_date": "2025-04-05",
        "token": "", # 參考登入，獲取金鑰
    }
    resp = requests.get(url, params=parameter)
    tmp_data = resp.json()
    tmp_data = pd.DataFrame(tmp_data["data"])
    
    TWMarketCap = pd.concat([TWMarketCap,tmp_data]) #rbind
    print(tmp_code,'is done!!!','\n','(',i,'/',len(code_list)-1,')')

    
    while (i % 10 == 0) | (API_counts >= 1500):
        payload = {
            "token": "", # 參考登入，獲取金鑰
        }
        resp = requests.get(url_API, params=payload)
        API_counts = resp.json()["user_count"]
        
        if (API_counts >= 1500):
            print('使用次數: ',API_counts)
            print(datetime.now())
            resp = requests.get(url_API, params=payload)
            API_counts = resp.json()["user_count"]
            time.sleep(300)
        else:
            break
        

# TWMarketCap.to_parquet('TWMarketCap.parquet')
# TWMarketCap.to_parquet('TWMarketCap_202504.parquet')
#%% 加權、櫃買報酬指數
import requests
import pandas as pd
url = "https://api.finmindtrade.com/api/v4/data"
parameter = {
    "dataset": "TaiwanStockTotalReturnIndex",
    "data_id": "TAIEX", # 發行量加權股價報酬指數
    # "data_id": "TPEx", # 櫃買指數與報酬指數
    "start_date": "2002-01-01",
    "end_date": "2025-04-08",
    "token": "", # 參考登入，獲取金鑰
}
resp = requests.get(url, params=parameter)
TAIEX_index = resp.json()
TAIEX_index = pd.DataFrame(TAIEX_index["data"])
TAIEX_index.to_parquet('TAIEX_index.parquet')
#%%
import pandas as pd
# TWFinStmt.to_parquet('TWFinStmt.parquet')
# TWBalceSht.to_parquet('TWBalceSht.parquet')

TWBalceSht = pd.read_parquet('TWBalceSht.parquet')
TWFinStmt = pd.read_parquet('TWFinStmt.parquet')
TWMarketCap = pd.read_parquet('TWMarketCap.parquet')
TAIEX_index = pd.read_parquet('TAIEX_index.parquet')
#
TWBalceSht_2024 = pd.read_parquet('TWBalceSht_2024.parquet')
TWFinStmt_2024 = pd.read_parquet('TWFinStmt_2024.parquet')
TWMarketCap_to2025 = pd.read_parquet('TWMarketCap_to2025.parquet')


#%% process 2024Q4 TWBalceSht TWFinStmt MKTCAP
import pandas as pd
TWBalceSht = pd.read_parquet('TWBalceSht.parquet')
BalanceSheet_2024Q4 = pd.read_parquet('BalanceSheet_2024Q4.parquet')
TWBalceSht.info()
BalanceSheet_2024Q4.info()

BalceSht_d2024Q4 = TWBalceSht.assign(
    stock_id = TWBalceSht.stock_id.astype('str')).loc[
        lambda x: x.date < '2024-12-01']

TWBalceSht_2024 = pd.concat([BalceSht_d2024Q4,BalanceSheet_2024Q4])

TWBalceSht_2024.info()

TWBalceSht_2024.to_parquet('TWBalceSht_2024.parquet')
############

TWFinStat_2024Q4 = pd.read_parquet('TWFinStat_2024Q4.parquet')
TWFinStmt = pd.read_parquet('TWFinStmt.parquet')
TWFinStat_2024Q4.info()
TWFinStmt.info()

TWFinStmt_d2024Q4 = TWFinStmt.loc[lambda x: x.date <= '2024-12-01']

TWFinStmt_2024 = pd.concat([TWFinStmt_d2024Q4,TWFinStat_2024Q4])

TWFinStmt_2024.to_parquet('TWFinStmt_2024.parquet')
###
TWMarketCap_202504 = pd.read_parquet('TWMarketCap_202504.parquet')
TWMarketCap = pd.read_parquet('TWMarketCap.parquet')

TWMarketCap_b2025 = TWMarketCap.loc[lambda x :x.date < '2025-01-01']
TWMarketCap_b2025.info()
TWMarketCap_202504.info()

TWMarketCap_to2025 = pd.concat([TWMarketCap_b2025,TWMarketCap_202504])

TWMarketCap_to2025.to_parquet('TWMarketCap_to2025.parquet')











