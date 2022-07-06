import templates._05_regional_cm.regional_func as rf
import templates._05_regional_cm.widget as widget
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from jinja2 import Template

from utils.config import PathReader
from utils.conn import RedshiftConnector
from utils.func import Reader
from utils.func import DateSetter


#1. 직접 입력해야하는 데이터 (샛별, 분류비 등)

def main():

    y = datetime.now().strftime('%y')
    m = datetime.now().strftime('%m')
    d = datetime.now().strftime('%d')
    m_prev_strp = (datetime.now() - relativedelta(months=1)).month

    y_prev = (datetime.now() - relativedelta(months=1)).strftime('%y')
    m_prev = (datetime.now() - relativedelta(months=1)).strftime('%m')
    now_history_day = datetime.now().strftime('%y%m%d')
    now_history_time = datetime.now().strftime('%H00')

    path = PathReader()
    file_path = path.path["file"]
    fname = "(REGIONAL_CM)raw_{}.xlsx".format(
        datetime.now().strftime("%y") + (datetime.now() - relativedelta(months=1)).strftime("%m"))

    # widget.widget_start()
    #
    # if widget.method == "input_data":
    #     widget.widget_input_data()
    #
    # elif widget.method == "excel_data":
    #     df_kpi = pd.read_excel(os.path.join(file_path, fname))
    #     params = { x: y for x, y in zip(df_kpi["item"], df_kpi["value"])}

    df_kpi = pd.read_excel(os.path.join(file_path, fname))
    params = { x: y for x, y in zip(df_kpi["item"], df_kpi["value"])}

    #2. SQL로 Redshift Raw 데이터 읽어오기

    engine = RedshiftConnector()
    cursor = engine.connect()

    params_date = DateSetter('default').setter()
    # params_date = {
    #     "start_date": "'2022-03-01'",
    #     "end_date": "'2022-04-01'"
    # }

    sql_sd = Reader('./templates/_05_regional_cm/region_sd.sql').text
    # sql_sd = Reader('region_sd.sql').text
    sql_sd = Template(sql_sd).render(params=params_date)


    sql_sgg = Reader('./templates/_05_regional_cm/region_sgg.sql').text
    # sql_sgg = Reader('region_sgg.sql').text
    sql_sgg = Template(sql_sgg).render(params=params_date)

    df_sgg = pd.read_sql(sql_sgg, cursor)
    df_sd = pd.read_sql(sql_sd, cursor)

    df_sd_total = df_sd[df_sd["dlvy_type"] == "TOTAL"][["ord_ym","sd_nm", "ord_cnt", "cust_cnt", "gmv", "ord_pay",
                                                        "partial_em_excltax", "partial_em", "mngr_avg"]]
    df_sd_type = df_sd[df_sd["dlvy_type"] != "TOTAL"][["ord_ym", "sd_nm", "dlvy_type", "ord_cnt", "cust_cnt", "gmv", "ord_pay",
                                                       "partial_em_excltax", "partial_em", "mngr_avg"]]
    df_sgg_total = df_sgg[df_sgg["dlvy_type"] == "TOTAL"][["ord_ym", "sd_nm", "sgg_nm", "ord_cnt", "cust_cnt", "gmv",
                                                           "ord_pay", "partial_em_excltax", "partial_em", "mngr_avg"]]
    df_sgg_type = df_sgg[df_sgg["dlvy_type"] != "TOTAL"][["ord_ym", "sd_nm", "sgg_nm", "dlvy_type", "ord_cnt", "cust_cnt", "gmv",
                                                          "ord_pay", "partial_em_excltax", "partial_em", "mngr_avg"]]

    lst = [df_sd_total, df_sd_type, df_sgg_total, df_sgg_type]


    params = rf.base_params(params)

    for i in lst:
        if "sgg_nm" in i.columns:
            i.insert(1, "region_nm", i["sd_nm"] + "_" + i["sgg_nm"])
            i = rf.portion(i, params["gmv"], "gmv", sd=False)
            i = rf.portion(i, params["orders"], "ord_cnt", sd=False)
            # i = rf.portion(i, params["purchasers"], "cust_cnt", sd=False)
        else:
            i = rf.portion(i, params["gmv"], "gmv", sd=True)
            i = rf.portion(i, params["orders"], "ord_cnt", sd=True)
            # i = rf.portion(i, params["purchasers"], "cust_cnt", sd=True)

    for i in lst:
        i = rf.vat(i)
        i = rf.acct_rev(i, params)
        i = rf.basket_size(i)

    df_household = pd.read_excel(os.path.join(file_path, "household.xlsx"))

    df_sgg_type = rf.common_cost(df_sgg_type, params=params)
    df_sgg_type = rf.direct_delivery_cost(df_sgg_type, params=params)
    df_sgg_type = rf.cm(df_sgg_type, params=params)
    df_sgg_type = rf.penetration(df_household=df_household, df_sgg_type=df_sgg_type)

    df_sgg_type.to_excel("~/Desktop/Sample_CM.xlsx", index=False)

    return "Success!"







