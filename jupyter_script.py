# -*- coding: utf-8 -*-
"""
Created on Mon May  5 22:31:03 2025

@author: e1155_l2c4ye3
"""

import pandas as pd
import numpy as np
import mysql.connector
myconn = mysql.connector.connect(
  host = "localhost",
  user = "root",
  password = "password",
  database = "finmind_tw",
  )
conn = myconn.cursor()

#%%
result = conn.fetchall()
print(result)
#%%
conn.execute('USE finmind_tw')
conn.execute('SHOW TABLES')
conn.execute('SELECT * FROM balancesheet LIMIT 5')
# %%
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine
engine = create_engine("mysql+mysqlconnector://root:password@localhost/finmind_tw")

# %% FF_MC
FF_MC = pd.read_sql_query(
    sql = 'SELECT * FROM marketcap',
    con = engine)
FF_MC = FF_MC.assign(date = lambda x: x.date.astype('str'))
FF_MC = FF_MC.sort_values(['stock_id','date']).\
    assign(year = 
           lambda x: x.date.str.slice_replace(start=-5,repl ='12-01')).\
    groupby(['stock_id','year'],as_index = False).market_value.\
    agg([('MC', lambda x: x.tail(n = 1) )]).loc[lambda x: x.MC != 0]
# Asset_Equity
FF_AstEqt = pd.read_sql_query(
    sql = "SELECT * FROM balancesheet WHERE type IN ('Equity','TotalAssets')",
    con = engine)
FF_AstEqt = FF_AstEqt.pivot(index = ['date','stock_id'],
                         columns = 'type',
                         values = 'value').reset_index()
FF_AstEqt = FF_AstEqt.assign(date = lambda x: x.date.astype('str'))
FF_AstEqt = FF_AstEqt.assign(
    date = lambda x: x.date.str.slice_replace(start = -2,repl = '01'),
    stock_id = lambda x: x.stock_id.astype('str')).rename(
        columns = {'date':'year'}).loc[
            lambda x: x.Equity > 0]
    
FF_AstEqt = FF_AstEqt[FF_AstEqt.year.str.contains('12-01')]
#  OP_df
FF_OP = pd.read_sql_query(
    sql = "SELECT * FROM incomestatement WHERE type IN \
    ('Revenue','CostOfGoodsSold','OperatingExpenses')",
    con = engine)

FF_OP = FF_OP.pivot(index = ['date','stock_id'],
                    columns = 'type',
                    values = 'value').\
        reset_index().dropna().assign(
            OP = lambda x: x.Revenue - x.CostOfGoodsSold - x.OperatingExpenses)\
        [['date','stock_id','OP']]
FF_OP = FF_OP.assign(date = lambda x: x.date.astype('str'))
FF_OP = FF_OP.assign(
    # date  = FF_OP.date.str.slice_replace(start = -2,repl = '01'),
    year = FF_OP.date.str.slice_replace(start = -5,repl = '12-01') )

FF_OP = FF_OP.groupby(['stock_id','year'],as_index = False).OP.agg(
    [('yOP', lambda x: x.sum())])
# 

FF_material = pd.merge(FF_AstEqt,FF_MC,
    on=['stock_id','year'],how='left').\
    merge(FF_OP,
    on=['stock_id','year'],how='left')

# remove financail companies
# =============================================================================
TSE_company = pd.read_csv('TSE_company.csv')
OTC_company = pd.read_csv('OTC_company.csv')
bank_code = pd.concat([TSE_company,OTC_company])
bank_code = bank_code[['公司代號','產業類別']].\
    rename(columns = {'公司代號':'stock_id','產業類別' : 'Industry'})
bank_code = bank_code[bank_code.Industry.str.contains('金融')].\
    drop(columns = ['Industry']).stock_id.astype('str').to_list()

FF_material = FF_material[~FF_material.stock_id.isin(bank_code)]

FF_material = FF_material.sort_values(['stock_id','year']).dropna()
                                         
FF_material = FF_material.assign(
    BMratio = FF_material.Equity / FF_material.MC,
    OPtoE = FF_material.yOP/FF_material.Equity,
    Inv = FF_material.groupby('stock_id').\
        TotalAssets.transform(
            lambda x: np.log(x) - np.log(x.shift(1)) ) ).dropna()

FF_material = FF_material.assign(
    g_size = FF_material.groupby('year').\
        MC.transform(
            lambda x: pd.qcut(x,q = [0,0.5,1],
                              labels=['s1','s2']) ),
    g_bm = FF_material.groupby('year').\
        BMratio.transform(
            lambda x: pd.qcut(x, q = [0,0.3,0.7,1],
                              labels=['bm1','bm2','bm3']) ),
    g_op = FF_material.groupby('year').\
        OPtoE.transform(
            lambda x: pd.qcut(x, q = [0,0.3,0.7,1],
                              labels=['op1','op2','op3']) ),
    g_inv = FF_material.groupby('year').\
        Inv.transform(lambda x: pd.qcut(x, q = [0,0.3,0.7,1],
                          labels=['inv1','inv2','inv3']) ))

FF_material = FF_material[['year','stock_id','MC',
                               'g_size','g_bm','g_op','g_inv']]


# monthly return section
# =============================================================================
# FF_ret = pd.read_parquet('monthret_to202503.parquet')
# FF_ret = FF_ret.rename(columns = {'Mtrue_ret':'ret','month':'ret_date'}) 
FF_ret = pd.read_sql_query(
    sql = 'SELECT stock_id,`month` as ret_date,ret FROM monthret_to202503',
    con = engine).assign(
        ret_date = lambda x: x.ret_date.astype('string'))

Corresponding = pd.DataFrame({
    'ret_date':pd.Series(pd.date_range(
        start = '2001-07-01',end='2026-06-01', freq = 'MS')).\
        dt.date.astype('str'),
    # 因為 quarter 有重複值, 他的serires.index 也是重複值:0,0,0,1,1,1....
    'year':pd.Series(pd.date_range(
        start = '2000-12-01',end='2024-12-01', freq = '12MS')).\
        repeat(12).dt.date.astype('str').reset_index(drop = True) 
    }) 
    
# =============================================================================

FF_ret = pd.merge(FF_ret,Corresponding,
                         on='ret_date',how='left',validate='m:1').dropna()


factor_cal = pd.merge(FF_ret,FF_material,
                      on=['stock_id','year'],how='left').dropna()

HML_data = factor_cal.assign(
    GMktCap = factor_cal.groupby(['ret_date','g_size','g_bm'],\
                                 as_index = False).MC.\
        transform(lambda x: x.sum() ),
    weight = lambda x: x.MC / x.GMktCap,
    weight_ret = lambda x: x.weight * x.ret).\
    groupby(['ret_date','g_size','g_bm'],as_index = False).weight_ret.\
        agg([('Portret', lambda x: x.sum() )]).\
    assign(Port = 
           lambda x: x.g_size.astype('str') +''+ x.g_bm.astype('str')).\
        drop(columns = ['g_size','g_bm'])

RMW_data = factor_cal.assign(
    GMktCap = factor_cal.groupby(['ret_date','g_size','g_op'],\
                                 as_index = False).MC.\
        transform(lambda x: x.sum() ),
    weight = lambda x: x.MC / x.GMktCap,
    weight_ret = lambda x: x.weight * x.ret).\
    groupby(['ret_date','g_size','g_op'],as_index = False).weight_ret.\
        agg([('Portret', lambda x: x.sum() )]).\
    assign(Port = 
           lambda x: x.g_size.astype('str') +''+ x.g_op.astype('str')).\
        drop(columns = ['g_size','g_op'])
#
CMA_data = factor_cal.assign(
    GMktCap = factor_cal.groupby(['ret_date','g_size','g_inv'],\
                                 as_index = False).MC.\
        transform(lambda x: x.sum() ),
    weight = lambda x: x.MC / x.GMktCap,
    weight_ret = lambda x: x.weight * x.ret).\
    groupby(['ret_date','g_size','g_inv'],as_index = False).weight_ret.\
        agg([('Portret', lambda x: x.sum() )]).\
    assign(Port = 
           lambda x: x.g_size.astype('str') +''+ x.g_inv.astype('str')).\
        drop(columns = ['g_size','g_inv'])
        
     
FF5_long = pd.concat([HML_data, RMW_data,CMA_data])


FF5_wide = FF5_long.pivot(index='ret_date',columns='Port',values='Portret').\
    reset_index().\
    assign(HML = lambda x:(x.s1bm3+x.s2bm3-x.s1bm1-x.s2bm1)/2,
           RMW = lambda x:(x.s1op3+x.s2op3-x.s1op1-x.s2op1)/2,
           CMA = lambda x:(x.s1inv1+x.s2inv1-x.s1inv3-x.s2inv3)/2,
           SMB3 = lambda x:((x.s1bm1+x.s1bm2+x.s1bm3)-
                             (x.s2bm1+x.s2bm2+x.s2bm3))/3,
           SMB5 = lambda x:((x.s1bm1+x.s1bm2+x.s1bm3+
                              x.s1op1+x.s1op2+x.s1op3+
                              x.s1inv1+x.s1inv2+x.s1inv3)-
                             (x.s2bm1+x.s2bm2+x.s2bm3+
                              x.s2op1+x.s2op2+x.s2op3+
                              x.s2inv1+x.s2inv2+x.s2inv3))/9)

# monthly return section
# =============================================================================
TAIEX_index = pd.read_parquet('taiex.parquet')
TAIEX_mret = TAIEX_index.sort_values(['date']).assign(
    ret_date = lambda x: x.date.str.slice_replace(
        start = -2, repl = '01')).groupby('ret_date').tail(1).assign(
    ret = lambda x:
        (x.price-x.price.shift(1))/x.price)[['ret_date','ret']]
TW10yearbond = pd.read_csv('臺灣十年期國債債券報酬率歷史數據.csv')
RFrate_month = TW10yearbond[['日期','收市']].rename(
    columns = {'日期':'date','收市':'RFrate'}).\
    assign(
    date = lambda x: 
        pd.to_datetime(x.date,format='%Y/%m/%d').dt.strftime('%Y-%m-%d'),
    ret_date = lambda x: x.date.str.slice_replace(start = -2,repl = '01')).\
    sort_values('date').groupby('ret_date',as_index = False).tail(1).\
    assign(RFrate = lambda x: x.RFrate/(12*100))[['ret_date','RFrate']]
    #年利率轉月
#
MKTPR_wide = pd.merge(TAIEX_mret,RFrate_month,
                       on='ret_date',how='left').dropna()
MKTPR_wide = MKTPR_wide.assign(
    MKTPR = lambda x: x.ret - x.RFrate)[['ret_date','MKTPR']]
FF5_wide = pd.merge(FF5_wide, MKTPR_wide,
                    on='ret_date',how='left')  

# =============================================================================

FF5_long = FF5_wide.melt(id_vars = 'ret_date',
                    var_name = 'Port',
                    value_name = 'Portret')

# %%    
ls = %who_ls
vtr = [x for x in ls if x not in ['FF5_long','FF5_wide','pd','engine'] ]
for x in vtr:
    del globals()[x]
del ls,x,vtr

# %%
# UMD
FF_mom = pd.read_sql_query(
    sql = 'SELECT stock_id,month AS mom_month, ret  FROM monthret_to202503',
    con = engine).assign(
        mom_month = lambda x: x.mom_month.astype('str'))

FF_mom = FF_mom.sort_values(['stock_id','mom_month']).dropna()
    
FF_mom = FF_mom.assign(
    mom = FF_mom.groupby('stock_id',as_index = False).ret.transform(
            lambda x: pd.Series(
                [1+x.shift(i) for i in range(2,13)]).prod(skipna=False)) )\
    .dropna()
FF_mom = FF_mom.assign(
    g_mom = FF_mom.groupby('mom_month').mom.\
        transform(lambda x: pd.qcut(x, q = [0,0.3,0.7,1],
                                    labels=['mom1','mom2','mom3']) ))

MOM_mc = pd.read_sql_query(
    sql = 'SELECT date, stock_id, market_value as MC FROM marketcap',
    con = engine).assign(date = lambda x: x.date.astype('str'))
MOM_mc = MOM_mc.sort_values(['stock_id','date']).assign(
    mc_month = lambda x: x.date.str.slice_replace(start = -2,repl = '01')).\
    groupby(['stock_id','mc_month']).tail(1)[['stock_id','mc_month','MC']]
    
MOM_mc = MOM_mc.assign(
    g_size = MOM_mc.groupby('mc_month').MC.transform(
        lambda x: pd.qcut(x, q = [0,0.5,1],labels=['s1','s2']) )) 

# 月報酬
Corresponding = pd.DataFrame({
    'ret_date':pd.Series(pd.date_range(start = '1980-02-01',
                                 end='2026-02-01', freq = 'MS')).\
                                 dt.date.astype('str'),
    'mom_month':pd.Series(pd.date_range(start = '1980-02-01',
                                 end='2026-02-01', freq = 'MS')).\
                                 dt.date.astype('str'),
    # 因為 quarter 有重複值, 他的serires.index 也是重複值:0,0,0,1,1,1....
    'mc_month':pd.Series(pd.date_range(start = '1980-01-01',
                                 end='2026-01-01', freq = 'MS')).\
                                 dt.date.astype('str'),
    })
#  

#
UMD_material = Corresponding.\
    merge(FF_mom,on = ['mom_month'],how = 'left').\
    merge(MOM_mc,on = ['stock_id','mc_month'],how = 'left').dropna()
#

UMD_long = UMD_material.assign(
    GMktCap = UMD_material.groupby(['ret_date','g_size','g_mom']).MC.\
        transform(lambda x: x.sum()),
    weight = lambda x: x.MC / x.GMktCap,
    weight_ret = lambda x: x.ret * x.weight).\
    groupby(['ret_date','g_size','g_mom'],as_index = False).weight_ret.\
        agg([('Portret',lambda x: x.sum())]).\
    assign(Port = 
           lambda x: x.g_size.astype('str') + x.g_mom.astype('str')).\
        drop(columns = ['g_size','g_mom'])

UMD_wide = UMD_long.pivot(index = ['ret_date'],
                          columns = 'Port',
                          values = 'Portret')

UMD_wide = UMD_wide.assign(
    UMD = lambda x:(x.s1_mom3+x.s2_mom3-x.s1_mom1-x.s2_mom1)/2)

# %%


FF6_wide = FF5_wide.merge(UMD_wide,on = 'ret_date')


FF6_long = FF6_wide.melt(id_vars = 'ret_date',
                    var_name = 'Port',
                    value_name = 'Portret')


# %%

ls = %who_ls
vtr = [x for x in ls if x not in ['FF6_long','FF6_wide','pd','engine'] ]
for x in vtr:
    del globals()[x]
del ls,x,vtr
# %%
import pandas as pd
Tableau_df = FF6_long.copy()
Tableau_df = Tableau_df.sort_values(['Port','ret_date']).\
    assign(cumret = Tableau_df.groupby(['Port'],as_index = False).Portret.\
           transform(lambda x: ((x+1).cumprod())*100 ),
           Portret = lambda x: x.Portret * 100)
        
rank_dict = Tableau_df.sort_values(['ret_date']).groupby(
    'Port',as_index = False).tail(1).assign(
    Portrank = lambda x: x.cumret.rank(ascending = False))[['Port','Portrank']]

rank_dict= dict(zip(rank_dict.Port, rank_dict.Portrank))

Tableau_df = Tableau_df.assign(Portrank = lambda x: x.Port.map(rank_dict))
#
MKT_dict = Tableau_df.loc[lambda x: x.Port == 'MKTPR'][['ret_date','cumret']]

MKT_dict = dict(zip(MKT_dict.ret_date,MKT_dict.cumret))

Tableau_df = Tableau_df.assign(
   MKT_cumret = lambda x: x.ret_date.map(MKT_dict))

Tableau_df.to_excel('Fama_Tableau.xlsx',index = False)





