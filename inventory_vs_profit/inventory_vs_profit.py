import sys
#sys.path.append("c:\lyhproject\python")


sys.path.append(("../../"))
import function_file.data_load as dl
import function_file.lyhTrade.lyhTradingSys as lyh
from WindPy import w
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pylab
import time
import xlrd
import datetime
import tushare as ts

ts.set_token('a63e497e8212c66642311eb645ee785bb7aae3edc15fce7bf026470f')
pro = ts.pro_api()

import MySQLdb

entiretyList = []
medianList = []
oneEnList = []
oneMidList = []

trade_date_list = []
report_date_list = []

record_date = lyh.getTradeNReportDate(returnKind = 'quarter', beginYear = 2008)

for i in range(len(record_date[0])):
    if i%2 ==1:
        trade_date_list.append(record_date[0][i])
        report_date_list.append(record_date[1][i])

report_date_list_init = report_date_list
report_date_list = []
for i_date in report_date_list_init:
    if i_date[-4:] == '0930':
        report_date_list.append(i_date[:4]+'1231')
    else:
        report_date_list.append(i_date)

        



#连接天风wind数据库
connWind = MySQLdb.connect("192.168.41.56", "infoasadep01","tfyfInfo@1522", "wind", charset = 'utf8')

connWindtf = MySQLdb.connect("192.168.41.56", "ref_reader", "ref_reader", "tfrefdb", charset='utf8', port = 3307)

rdf_field_ashareeodderivativeindicator = "S_DQ_CLOSE_TODAY, TOT_SHR_TODAY, S_VAL_PE_TTM, s_val_pb_new, S_VAL_MV"
rdf_field_asharefinancialindicator = "s_fa_roe, S_FA_YOY_OR, S_FA_YOYNETPROFIT_DEDUCTED, S_FA_ROIC"
rdf_field_asharettmhis = "oper_rev_ttm, S_FA_ASSET_MRQ, S_FA_DEBT_MRQ, NET_PROFIT_PARENT_COMP_TTM"
rdf_field_asharebalancesheet = "INVENTORIES, CONSUMPTIVE_BIO_ASSETS"
#rdf_field_asharefinancialderivative =  "ROIC, INVTURN"




d_lag_1 = None
SW_LEVEL = 2 #申万几级行业

d_result_dict = {}

#i = 0

result_list = []

for i_date in range(len(report_date_list)):

    print(report_date_list[i_date])

    df1 = dl.get_ashareeodderivativeindicator(connWind, rdf_field_ashareeodderivativeindicator, report_date_list[i_date])
    df2 = dl.get_asharefinancialindicator(connWind, rdf_field_asharefinancialindicator, report_date_list[i_date])
    df3 = dl.get_asharettmhis(connWind, rdf_field_asharettmhis, report_date_list[i_date])
    df4 = dl.get_asharebalancesheet(connWind, rdf_field_asharebalancesheet, report_date_list[i_date])

    #合并财务数据
    d = pd.merge(df1, df2, how='outer', on=['s_info_windcode', 's_info_windcode'])
    d = pd.merge(d, df3, how='outer', on=['s_info_windcode', 's_info_windcode'])
    d = pd.merge(d, df4, how='outer', on=['s_info_windcode', 's_info_windcode'])

    #合并申万行业数据
    d_industry = dl.get_sw_industry_name(connWind,report_date_list[i_date], SW_LEVEL)


    d = pd.merge(d, d_industry, on = ['s_info_windcode', 's_info_windcode'], how = 'outer')

    d = d.dropna(subset=['Industriesname'])



    if d_lag_1 is None:
        d_lag_1 = d
        continue
    else:

        d_merge = pd.merge(d, d_lag_1, on = 's_info_windcode', how = 'inner')

        d_industry_sum = d_merge.groupby('Industriesname_x').sum()
        d_industry_mean = d_merge.groupby('Industriesname_x').mean()
        #d_industry_sum_dvd = d_industry_sum[['NET_PROFIT_PARENT_COMP_TTM_x', 'INVENTORIES_x', 'CONSUMPTIVE_BIO_ASSETS_x']]/d_industry_sum[['NET_PROFIT_PARENT_COMP_TTM_y', 'INVENTORIES_y', 'CONSUMPTIVE_BIO_ASSETS_y']]

        #d_industry_sum_dvd = d_industry_sum['INVENTORIES_x'] / d_industry_sum['INVENTORIES_x'] - 1

        #d_industry_mean_dvd = d_merge.groupby('Industriesname_x').mean()


        if len(d_result_dict) == 0:
            d_profit_dvd = d_industry_sum['NET_PROFIT_PARENT_COMP_TTM_x'] / d_industry_sum['NET_PROFIT_PARENT_COMP_TTM_y'] - 1
            d_profit_dvd_df = pd.DataFrame(d_profit_dvd.values, index = d_profit_dvd.index, columns= [report_date_list[i_date]])
            d_result_dict['净利润增速'] = d_profit_dvd_df


            d_roic_mean = d_merge.groupby('Industriesname_x').mean()['S_FA_ROIC_x']
            d_roic_mean_df = pd.DataFrame(d_roic_mean.values, index=d_roic_mean.index, columns=[report_date_list[i_date]])
            d_result_dict['平均ROIC'] = d_roic_mean_df


            d_inventories_dvd = d_industry_sum['INVENTORIES_x'] / d_industry_sum[
                'INVENTORIES_y'] - 1
            d_inventories_dvd_df = pd.DataFrame(d_inventories_dvd.values, index=d_inventories_dvd.index, columns=[report_date_list[i_date]])
            d_result_dict['存货增速'] = d_inventories_dvd_df


            # d_consumptive_dvd = d_industry_sum['CONSUMPTIVE_BIO_ASSETS_x'] / d_industry_sum[
            #     'CONSUMPTIVE_BIO_ASSETS_y'] - 1
            # d_consumptive_dvd_df = pd.DataFrame(d_consumptive_dvd.values, index=d_consumptive_dvd.index,
            #                         columns=[report_date_list[i_date]])
            # d_result_dict['消耗性生物资产增速'] = d_consumptive_dvd_df

        else:
            d_profit_dvd = d_industry_sum['NET_PROFIT_PARENT_COMP_TTM_x'] / d_industry_sum[
                'NET_PROFIT_PARENT_COMP_TTM_y'] - 1
            d_profit_dvd_df = pd.DataFrame(d_profit_dvd.values, index=d_profit_dvd.index, columns=[report_date_list[i_date]])
            d_result_dict['净利润增速'] = pd.merge(d_result_dict['净利润增速'], d_profit_dvd_df, left_index= True, right_index = True, how = 'outer')

            d_roic_mean = d_merge.groupby('Industriesname_x').mean()['S_FA_ROIC_x']
            d_roic_mean_df = pd.DataFrame(d_roic_mean.values, index=d_roic_mean.index, columns=[report_date_list[i_date]])
            d_result_dict['平均ROIC'] = pd.merge(d_result_dict['平均ROIC'], d_roic_mean_df, left_index= True, right_index = True, how = 'outer')

            d_inventories_dvd = d_industry_sum['INVENTORIES_x'] / d_industry_sum[
                'INVENTORIES_y'] - 1
            d_inventories_dvd_df = pd.DataFrame(d_inventories_dvd.values, index=d_inventories_dvd.index,
                                                columns=[report_date_list[i_date]])
            d_result_dict['存货增速'] = pd.merge(d_result_dict['存货增速'], d_inventories_dvd_df, left_index= True, right_index = True, how = 'outer')

        d_lag_1 = d

for i_key in d_result_dict.keys():
    d_result_dict[i_key]['指标'] = i_key


d_result = pd.concat(d_result_dict.values())
d_result['行业'] = d_result.index
d_result = d_result.set_index(["行业", "指标"])
d_result.sort_index().to_excel('d_result.xlsx')

d_temp = d_result.T

for i_industry in d_temp.columns.levels[0]:

    profit_mean = d_temp[(i_industry,'净利润增速')].mean()
    roic_mean = d_temp[(i_industry,'平均ROIC')].mean()
    inventories_mean = d_temp[(i_industry,'存货增速')].mean()

    d_temp[(i_industry,'产能周期')] = '未知'
    d_temp[(i_industry,'产能周期')] = np.where((d_temp[(i_industry,'存货增速')]>inventories_mean) & (d_temp[(i_industry,'平均ROIC')]>roic_mean) & (d_temp[(i_industry,'净利润增速')]>profit_mean) , '主动提库存', d_temp[(i_industry,'产能周期')])
    d_temp[(i_industry,'产能周期')] = np.where((d_temp[(i_industry,'存货增速')]>inventories_mean) & (d_temp[(i_industry,'平均ROIC')]<roic_mean) & (d_temp[(i_industry,'净利润增速')]<profit_mean) , '被动提库存', d_temp[(i_industry,'产能周期')])
    d_temp[(i_industry,'产能周期')] = np.where((d_temp[(i_industry,'存货增速')]<inventories_mean) & (d_temp[(i_industry,'平均ROIC')]>roic_mean) & (d_temp[(i_industry,'净利润增速')]>profit_mean) , '被动降库存', d_temp[(i_industry,'产能周期')])
    d_temp[(i_industry,'产能周期')] = np.where((d_temp[(i_industry,'存货增速')]<inventories_mean) & (d_temp[(i_industry,'平均ROIC')]<roic_mean) & (d_temp[(i_industry,'净利润增速')]<profit_mean) , '主动降库存', d_temp[(i_industry,'产能周期')])


#d_temp.T.sort_index().to_excel('d_result_final.xlsx')

d_result_final = d_temp.T.sort_index()
d_temp2 = d_temp

#sm.tsa.stattools.grangercausalitytests
gxsx_str = ""
xqfzd_str = ""
gjfzd_str = ""

from statsmodels.tsa.stattools  import   grangercausalitytests
#print(grangercausalitytests([dtest['存货增速'], dtest['净利润增速']], maxlag=15, addconst=True, verbose=True))
for i_industry in d_result_final.index.levels[0]:
    dtest = d_result_final.loc[i_industry]

    #供给方主导
    data1 = dtest.loc[['净利润增速','存货增速']].T
    data1 = data1.replace([np.inf, -np.inf], np.nan)
    data1 = data1.dropna()
    if len(data1) > 4:
        r1 = grangercausalitytests(data1, maxlag=1, verbose=True)
        profit_to_inventories = r1[1][0]['ssr_chi2test'][0] < 0.05
    else:
        profit_to_inventories = False

    #需求方主导
    data2 = dtest.loc[['存货增速','净利润增速']].T
    data2 = data2.replace([np.inf, -np.inf], np.nan)
    data2 = data2.dropna()
    if len(data2) > 4:
        r2 = grangercausalitytests(data2, maxlag=1, verbose=True)
        inventories_to_profit = r2[1][0]['ssr_chi2test'][0] < 0.05
    else:
        inventories_to_profit = False

    dtest = dtest.T

    if profit_to_inventories and inventories_to_profit:
        d_temp2[(i_industry,'因果检验结果')] = '供需双向传导'
        gxsx_str = gxsx_str + '、' + i_industry
    elif profit_to_inventories and not inventories_to_profit:
        d_temp2[(i_industry,'因果检验结果')] = '供给方主导'
        gjfzd_str = gjfzd_str + '、' + i_industry
    elif not profit_to_inventories and inventories_to_profit:
        d_temp2[(i_industry,'因果检验结果')] = '需求方主导'
        xqfzd_str = xqfzd_str + '、' + i_industry
    else:
        d_temp2[(i_industry,'因果检验结果')] = '无传导关系'

d_result_final2 = d_temp2.T.sort_index()
index_init = d_result_final2.index
d_result_final2 = d_result_final2.reindex([(d1, d2) for d1 in index_init.get_level_values("行业").unique() for d2 in ['因果检验结果','产能周期', '净利润增速', '存货增速', '平均ROIC']])
d_result_final2.to_excel('d_result_final.xlsx')

