# -*- coding: utf-8 -*-
"""
Created on Thu Mar 20 12:33:14 2025

@author: e1155_l2c4ye3
"""

import pandas as pd
import numpy as np
FF_MC = pd.read_parquet('marketcap.parquet')
# 直接刪除當年公司年底 MC 為 0 的公司
FF_MC = FF_MC.sort_values(['stock_id','date']).\
    assign(year = 
           lambda x: x.date.str.slice_replace(start=-5,repl ='12-01')).\
    groupby(['stock_id','year'],as_index = False).market_value.\
    agg([('MC', lambda x: x.tail(n = 1) )]).loc[lambda x: x.MC != 0]

#
# 資產負債表抓出總資產跟淨值
FF_AstEqt = pd.read_parquet('balancesheet.parquet')
FF_AstEqt = FF_AstEqt[FF_AstEqt['type'].isin(['Equity','TotalAssets'])]

FF_AstEqt = FF_AstEqt.pivot(index = ['date','stock_id'],
                         columns = 'type',
                         values = 'value').reset_index()

FF_AstEqt = FF_AstEqt.assign(
    date = lambda x: x.date.str.slice_replace(start = -2,repl = '01'),
    stock_id = lambda x: x.stock_id.astype('str')).rename(
        columns = {'date':'year'}).loc[
            lambda x: x.Equity > 0]
    
FF_AstEqt = FF_AstEqt[FF_AstEqt.year.str.contains('12-01')]

#

FF_OP = pd.read_parquet('incomestatement.parquet')
FF_OP = FF_OP[
    lambda x: x.type.isin(['Revenue','CostOfGoodsSold','OperatingExpenses'])]

FF_OP = FF_OP.pivot(index = ['date','stock_id'],
                    columns = 'type',
                    values = 'value').\
        reset_index().dropna().assign(
            OP = lambda x: x.Revenue - x.CostOfGoodsSold - x.OperatingExpenses)\
        [['date','stock_id','OP']]

FF_OP = FF_OP.assign(
    date  = FF_OP.date.str.slice_replace(start = -2,repl = '01'),
    year = FF_OP.date.str.slice_replace(start = -5,repl = '12-01') )

FF_OP = FF_OP.groupby(['stock_id','year'],as_index = False).OP.agg(
    [('yOP', lambda x: x.sum())])

  
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
del [TSE_company,OTC_company,bank_code]
# =============================================================================
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

# Corresponding = pd.DataFrame({
#     'ret_date':pd.Series(pd.date_range(
#         start = '2001-07-01',end='2026-06-01', freq = 'MS')).\
#         dt.date.astype('str'),
#     # 因為 quarter 有重複值, 他的serires.index 也是重複值:0,0,0,1,1,1....
#     'year':pd.Series(pd.date_range(
#         start = '2000-12-01',end='2024-12-01', freq = '12MS')).\
#         repeat(12).dt.date.astype('str').reset_index(drop = True) 
#     }) 
    
# =============================================================================


# daily return section
## =============================================================================
# FF_ret = pd.read_parquet('dayret_to20250402.parquet')
# FF_ret = FF_ret.rename(columns ={'date':'ret_date'}).dropna()
# Corresponding = pd.DataFrame()  
# for i in range(2001,2026):
#     tmp = pd.DataFrame({
#         'ret_date':pd.Series(pd.date_range(
#             start = str(i)+'-07-01',
#             end= str(i+1)+'-06-30',
#             freq = 'd')).dt.date.astype('str')
#         })
#     tmp = tmp.assign(year = str(i-1)+'-12-01')
#     Corresponding = pd.concat([Corresponding,tmp])
    
## =============================================================================
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
# TAIEX_index = pd.read_parquet('taiex.parquet')
# TAIEX_mret = TAIEX_index.sort_values(['date']).assign(
#     ret_date = lambda x: x.date.str.slice_replace(
#         start = -2, repl = '01')).groupby('ret_date').tail(1).assign(
#     ret = lambda x:
#         (x.price-x.price.shift(1))/x.price)[['ret_date','ret']]
# TW10yearbond = pd.read_csv('臺灣十年期國債債券報酬率歷史數據.csv')
# RFrate_month = TW10yearbond[['日期','收市']].rename(
#     columns = {'日期':'date','收市':'RFrate'}).\
#     assign(
#     date = lambda x: 
#         pd.to_datetime(x.date,format='%Y/%m/%d').dt.strftime('%Y-%m-%d'),
#     ret_date = lambda x: x.date.str.slice_replace(start = -2,repl = '01')).\
#     sort_values('date').groupby('ret_date',as_index = False).tail(1).\
#     assign(RFrate = lambda x: x.RFrate/(12*100))[['ret_date','RFrate']]
#     #年利率轉月
# #
# MKTPR_wide = pd.merge(TAIEX_mret,RFrate_month,
#                        on='ret_date',how='left').dropna()
# MKTPR_wide = MKTPR_wide.assign(
#     MKTPR = lambda x: x.ret - x.RFrate)[['ret_date','MKTPR']]
# FF5_wide = pd.merge(FF5_wide, MKTPR_wide,
#                     on='ret_date',how='left')  

# =============================================================================


  
# daily return section
## =============================================================================
# TAIEX_index = pd.read_parquet('taiex.parquet')
# TAIEX_dret = TAIEX_index.sort_values(['date']).assign(
#     ret = lambda x: (x.price-x.price.shift(1))/x.price).rename(
#         columns = {'date':'ret_date'})[['ret_date','ret']]
# TW10yearbond = pd.read_csv('臺灣十年期國債債券報酬率歷史數據.csv')
# RFrate_day = TW10yearbond[['日期','收市']].rename(
#     columns = {'日期':'ret_date','收市':'RFrate'}).\
#     assign(
#     ret_date = lambda x: 
#         pd.to_datetime(x.ret_date,format='%Y/%m/%d').dt.strftime('%Y-%m-%d'),
#     RFrate = lambda x: x.RFrate/(100*365)).sort_values('ret_date')
# MKTPR_wide = pd.merge(TAIEX_dret,RFrate_day,
#                        on='ret_date',how='left')#.dropna()
# MKTPR_wide = MKTPR_wide.assign(
#     RFrate = MKTPR_wide.RFrate.fillna(method = 'ffill'),
#     MKTPR = lambda x: x.ret - x.RFrate).dropna()\
#     [['ret_date','MKTPR']]

# FF5_wide = pd.merge(FF5_wide, MKTPR_wide,on='ret_date',how='left')

## =============================================================================
FF5_wide = FF5_wide.loc[lambda x: x.ret_date < '2025-04-01']
FF5_long = FF5_wide.melt(id_vars = 'ret_date',
                    var_name = 'Port',
                    value_name = 'Portret')
# %%    
ls = %who_ls
vtr = [x for x in ls if x not in ['FF5_long','FF5_wide','month_y','day_y'] ]
for x in vtr:
    del globals()[x]
del ls,x,vtr
    
#%%                           ,年四月      ,tej 7月
FF5_wide.HML.mean()  #0.002929, 0.001976, 0.348118
FF5_wide.RMW.mean()  #0.002565, 0.001738, 0.029225
FF5_wide.CMA.mean()  #0.000253,-0.000960,-0.133316
FF5_wide.SMB_3.mean()#0.001398, 0.000937, 0.122628
FF5_wide.SMB_5.mean()#0.002081, 0.001124, 0.1698539
FF5_wide.MKTPR.mean()  #0.010429, 0.010652, 0.0091457

#%%

def FF_YearGroup(ret_freq):
    # TWMarketCap = pd.read_parquet('TWMarketCap.parquet')
    FF_MC = pd.read_parquet('marketcap.parquet')
    # 直接刪除當年公司年底 MC 為 0 的公司
    FF_MC = FF_MC.sort_values(['stock_id','date']).\
        assign(year = 
               lambda x: x.date.str.slice_replace(start=-5,repl ='12-01')).\
        groupby(['stock_id','year'],as_index = False).market_value.\
        agg([('MC', lambda x: x.tail(n = 1) )]).loc[lambda x: x.MC != 0]
    # 資產負債表抓出總資產跟淨值
    
    # TWBalceSht = pd.read_parquet('TWBalceSht.parquet')
    FF_AstEqt = pd.read_parquet('balancesheet.parquet')
    FF_AstEqt = FF_AstEqt[FF_AstEqt['type'].isin(['Equity','TotalAssets'])]

    FF_AstEqt = FF_AstEqt.pivot(index = ['date','stock_id'],
                             columns = 'type',
                             values = 'value').reset_index()

    FF_AstEqt = FF_AstEqt.assign(
        date = lambda x: x.date.str.slice_replace(start = -2,repl = '01'),
        stock_id = lambda x: x.stock_id.astype('str')).rename(
            columns = {'date':'year'}).loc[
                lambda x: x.Equity > 0]
        
    FF_AstEqt = FF_AstEqt[FF_AstEqt.year.str.contains('12-01')]

   

    # TWFinStmt = pd.read_parquet('TWFinStmt.parquet')
    FF_OP = pd.read_parquet('incomestatement.parquet')
    FF_OP = FF_OP[
        lambda x: x.type.isin(['Revenue','CostOfGoodsSold','OperatingExpenses'])]

    FF_OP = FF_OP.pivot(index = ['date','stock_id'],
                            columns = 'type',
                            values = 'value').\
            reset_index().dropna().\
            assign(OP = lambda x: 
                   x.Revenue - x.CostOfGoodsSold - x.OperatingExpenses)\
                [['date','stock_id','OP']]

    FF_OP = FF_OP.assign(
        date  = FF_OP.date.str.slice_replace(start = -2,repl = '01'),
        year = FF_OP.date.str.slice_replace(start = -5,repl = '12-01') )

    FF_OP = FF_OP.groupby(['stock_id','year'],as_index = False).OP.agg(
        [('yOP', lambda x: x.sum())])

    FF_material = pd.merge(
        FF_AstEqt,FF_MC,
        on=['stock_id','year'],how='left').\
        merge(FF_OP,
        on=['stock_id','year'],how='left')
    # =============================================================================
    TSE_company = pd.read_csv('TSE_company.csv')
    OTC_company = pd.read_csv('OTC_company.csv')
    bank_code = pd.concat([TSE_company,OTC_company])
    bank_code = bank_code[['公司代號','產業類別']].\
        rename(columns = {'公司代號':'stock_id','產業類別' : 'Industry'})
    bank_code = bank_code[bank_code.Industry.str.contains('金融')].\
        drop(columns = ['Industry']).stock_id.astype('str').to_list()

    FF_material = FF_material[~FF_material.stock_id.isin(bank_code)]
    # =============================================================================
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

    # df_ret = pd.read_parquet('TWMonthRet.parquet')
    # df_ret = df_ret.rename(columns = {'Mtrue_ret':'ret',
    #                                   'month':'ret_date'}) 
    
    
    if ret_freq == 'month':
        FF_ret = pd.read_parquet('monthret_to202503.parquet')
        FF_ret = FF_ret.rename(columns = {'month':'ret_date'}) 
        
        Corresponding = pd.DataFrame({
            'ret_date':pd.Series(pd.date_range(
                start = '2001-07-01',end='2026-06-01', freq = '1MS')).\
                dt.date.astype('str'),
            # 因為 quarter 有重複值, 他的serires.index 也是重複值:0,0,0,1,1,1....
            'year':pd.Series(pd.date_range(
                start = '2000-12-01',end='2024-12-01', freq = '12MS')).\
                repeat(12).dt.date.astype('str').reset_index(drop = True) 
            }) 
            
        
    elif ret_freq == 'day':
       
        FF_ret = pd.read_parquet('dayret_to20250402.parquet')
        FF_ret = FF_ret.rename(columns ={'date':'ret_date'}).dropna()
        Corresponding = pd.DataFrame()  
        for i in range(2001,2026):
            tmp = pd.DataFrame({
                'ret_date':pd.Series(pd.date_range(
                    start = str(i)+'-07-01',
                    end= str(i+1)+'-06-30',
                    freq = 'd')).dt.date.astype('str')
                })
            tmp = tmp.assign(year = str(i-1)+'-12-01')
            Corresponding = pd.concat([Corresponding,tmp])
            
        
        
    else:
        raise NameError('Input error: ret_freq')
    #
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
    #.query('ret_date <= "2024-12-31"')
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
    if ret_freq == 'month':
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
        # MKTPR_long = MKTPR_wide.assign(
        #     Port = 'MKTPR').rename(
        #         columns = {'MKTPR':"Portret"})
        # FF5_long = pd.concat([FF5_long,MKTPR_long])
    elif ret_freq == 'day':
        TAIEX_index = pd.read_parquet('taiex.parquet')
        TAIEX_dret = TAIEX_index.sort_values(['date']).assign(
            ret = lambda x: (x.price-x.price.shift(1))/x.price).rename(
                columns = {'date':'ret_date'})[['ret_date','ret']]
        TW10yearbond = pd.read_csv('臺灣十年期國債債券報酬率歷史數據.csv')
        RFrate_day = TW10yearbond[['日期','收市']].rename(
            columns = {'日期':'ret_date','收市':'RFrate'}).\
            assign(
            ret_date = lambda x: 
                pd.to_datetime(x.ret_date,format='%Y/%m/%d').dt.strftime('%Y-%m-%d'),
            RFrate = lambda x: x.RFrate/(100*365)).sort_values('ret_date')
        MKTPR_wide = pd.merge(TAIEX_dret,RFrate_day,
                               on='ret_date',how='left')#.dropna()
        MKTPR_wide = MKTPR_wide.assign(
            RFrate = MKTPR_wide.RFrate.fillna(method = 'ffill'),
            MKTPR = lambda x: x.ret - x.RFrate).dropna()\
            [['ret_date','MKTPR']]
        
        # MKTPR_wide = MKTPR_wide.assign(
        #     MKTPR = lambda x: x.ret - x.RFrate)[['ret_date','MKTPR']]
        FF5_wide = pd.merge(FF5_wide, MKTPR_wide,on='ret_date',how='left')
        
        # MKTPR_long = MKTPR_wide.assign(
        #     Port = 'MKTPR').rename(columns = {'MKTPR':"Portret"})
        # FF5_long = pd.concat([FF5_long,MKTPR_long])
        
        
    else:
        raise NameError('Input error: ret_freq')
    FF5_wide = FF5_wide.loc[lambda x: x.ret_date < '2025-04-01']
    FF5_long = FF5_wide.melt(id_vars = 'ret_date',
                        var_name = 'Port',
                        value_name = 'Portret')
        
    return dict({'FF5_wide':FF5_wide,
                 'FF5_long':FF5_long})
    
#%%
   
import pandas as pd
import numpy as np
month_y['FF5_wide'].to_string()
month_y = FF_YearGroup( ret_freq= 'month')        
day_y = FF_YearGroup( ret_freq= 'day')        

from scipy import stats
ttest_ar = np.array( month_y[1].SMB_3 )
t_stat, p_value = stats.ttest_1samp(ttest_ar, 0)
print("T statistic:", t_stat)
print("P-value:", p_value)


AAA = month_y[0].compare(FF5_long)
BBB = month_y[1].compare(FF5_wide)

FF5_long = FF5_long.assign(
    Portret = lambda x: x.Portret*100)
AAA = day_y[0].compare(FF5_long)
BBB = day_y[1].compare(FF5_wide)



month_y[1].isna().sum()
month_y[1].HML.mean()  #0.0029293519976766706
month_y[1].RMW.mean()  #0.002565074814920454
month_y[1].CMA.mean()  #0.0002530267390224755
month_y[1].SMB_3.mean()#0.0013984889755610395
month_y[1].SMB_5.mean()#0.0020810683971280892
month_y[1].MKTPR.mean()#0.008482306883503835
    

day_y[1].isna().sum()
day_y[1].HML.mean()  #0.0001172687660481006
day_y[1].RMW.mean()  #0.00015961425798109903
day_y[1].CMA.mean()  #1.9451643273232913e-05
day_y[1].SMB_3.mean()#4.332826116483128e-05
day_y[1].SMB_5.mean()#7.689493334693046e-05
day_y[1].MKTPR.mean()#0.00045486935673261914



TW10yearbond = pd.read_csv('臺灣十年期國債債券報酬率歷史數據.csv')
