# -*- coding: utf-8 -*-
"""
Created on Thu Mar 27 19:49:03 2025

@author: e1155_l2c4ye3
"""

#%% mom_df 2000-02-01 ~ 2025-02-01
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
engine = create_engine("mysql+mysqlconnector://root:password@localhost/finmind_tw")
# %%

FF_mom = pd.read_sql_query(
    sql = 'SELECT stock_id,month AS ret_date, ret  FROM monthret_to202503',
    con = engine).assign(
        ret_date = lambda x: x.ret_date.astype('str'))
        
FF_mom = FF_mom.sort_values(['stock_id','ret_date']).dropna()
    
FF_mom = FF_mom.assign(
    mom = FF_mom.groupby('stock_id',as_index = False).ret.transform(
            lambda x: pd.Series(
                [1+x.shift(i) for i in range(2,13)]).prod(skipna=False)) )\
    .dropna()
FF_mom = FF_mom.assign(
    g_mom = FF_mom.groupby('ret_date').mom.\
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
    # 'mom_month':pd.Series(pd.date_range(start = '1980-02-01',
    #                              end='2026-02-01', freq = 'MS')).\
    #                              dt.date.astype('str'),
    # 因為 quarter 有重複值, 他的serires.index 也是重複值:0,0,0,1,1,1....
    'mc_month':pd.Series(pd.date_range(start = '1980-01-01',
                                 end='2026-01-01', freq = 'MS')).\
                                 dt.date.astype('str'),
    })

#
UMD_material = Corresponding.\
    merge(FF_mom,on = ['ret_date'],how = 'left').\
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
           lambda x: x.g_size.astype('str') +'_'+ x.g_mom.astype('str')).\
        drop(columns = ['g_size','g_mom'])

UMD_wide = UMD_long.pivot(index = ['ret_date'],
                          columns = 'Port',
                          values = 'Portret')

UMD_wide = UMD_wide.assign(
    UMD = lambda x:(x.s1_mom3+x.s2_mom3-x.s1_mom1-x.s2_mom1)/2)

# %%
ls = %who_ls
vtr = [x for x in ls if x not in ['UMD_wide','UMD_long',
                                  'UMD_monthRet','UMD_dayRet'] ]
for x in vtr:
    del globals()[x]
del ls,x,vtr

#%%

def UMD_calculation(ret_freq):
    FF_mom = pd.read_parquet('monthret_to202503.parquet')

    FF_mom = FF_mom.rename(
        columns = {'month':'mom_month'}).\
        sort_values(['stock_id','mom_month']).dropna()
        
    FF_mom = FF_mom.assign(
        mom = FF_mom.groupby('stock_id',as_index = False).ret.transform(
                lambda x: pd.Series(
                    [1+x.shift(i) for i in range(2,13)]).prod(skipna=False)) )\
        .dropna().drop(columns = ['ret'])
    FF_mom = FF_mom.assign(
        g_mom = FF_mom.groupby('mom_month').mom.\
            transform(lambda x: pd.qcut(x, q = [0,0.3,0.7,1],
                                        labels=['mom1','mom2','mom3']) ))
    # 市值處理
    MOM_mc = pd.read_parquet('marketcap.parquet')
    MOM_mc = MOM_mc.sort_values(['stock_id','date']).assign(
        mc_month = lambda x: x.date.str.slice_replace(start = -2,repl = '01')).\
        groupby(['stock_id','mc_month']).tail(1).rename(
            columns = {'market_value':'MC'})[['stock_id','mc_month','MC']]
    MOM_mc = MOM_mc.assign(
        g_size = MOM_mc.groupby('mc_month').MC.transform(
            lambda x: pd.qcut(x, q = [0,0.5,1],labels=['s1','s2']) )) 
    # UMD的mom每月重新分組,以前12~前2月累積報酬為該公司mom分組,
    # UMD的size也每月重新分組,以前1個月月底市值為該公司mc分組
    # 以4月為例,mom為去年4月到今年2月累積月報酬,mc為3月底報酬
    if ret_freq == 'month':
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
        
        #月報酬
        FF_ret = pd.read_parquet('monthret_to202503.parquet')
        FF_ret = FF_ret.rename(columns = {'month':'ret_date'}).dropna()
        
    elif ret_freq == 'day':
        Corresponding = pd.DataFrame({
            'ret_date':pd.Series(pd.date_range(start = '1980-02-01',
                                         end='2026-02-01', freq = 'D')).\
                                         dt.date.astype('str')
            })
            
        Corresponding = Corresponding.assign(
            mom_month = lambda x: x.ret_date.str.slice_replace(start = -2,repl = '01'),
            mc_month = lambda x: (pd.to_datetime(x.mom_month)+\
                pd.DateOffset(months = -1)).dt.strftime('%Y-%m-%d') )

        #
        FF_ret = pd.read_parquet('dayret_to20250402.parquet').rename(
            columns = {'date':'ret_date'})
    else:
        raise NameError('Input error: ret_freq')


    # 以報酬為原始df再去合併其他df
    UMD_material = FF_ret.merge(Corresponding,on = 'ret_date',how = 'left').\
        merge(FF_mom,on = ['stock_id','mom_month'],how = 'left').\
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
               lambda x: x.g_size.astype('str') +'_'+ x.g_mom.astype('str')).\
            drop(columns = ['g_size','g_mom'])

    UMD_wide = UMD_long.pivot(index = ['ret_date'],
                              columns = 'Port',
                              values = 'Portret').reset_index()

    UMD_wide = UMD_wide.assign(
        UMD = lambda x:(x.s1_mom3+x.s2_mom3-x.s1_mom1-x.s2_mom1)/2)
    UMD_long = UMD_wide.melt(id_vars = 'ret_date',
                        var_name = 'Port',
                        value_name = 'Portret')
    return dict({'UMD_long':UMD_long,
                 'UMD_wide':UMD_wide})

#%%
import pandas as pd
import numpy as np

UMD_monthRet = UMD_calculation(ret_freq= 'month')
UMD_dayRet = UMD_calculation(ret_freq= 'day')

