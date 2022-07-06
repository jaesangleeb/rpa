import templates._11_regional_dashboard.regional_dashboard_func as rd
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
    path = PathReader()
    file_path = path.path["file"]

    #2. SQL로 Redshift Raw 데이터 읽어오기

    engine = RedshiftConnector()
    cursor = engine.connect()

    params_date = DateSetter('default').setter()
    # params_date = {
    #     "start_date": "'2022-03-01'",
    #     "end_date": "'2022-04-01'"
    # }

    sql_sd = Reader('./templates/_11_regional_dashboard/dashboard_region_sd.sql').text
    # sql_sd = Reader('region_sd.sql').text
    sql_sd = Template(sql_sd).render(params=params_date)


    sql_sgg = Reader('./templates/_11_regional_dashboard/dashboard_region_sgg.sql').text
    # sql_sgg = Reader('region_sgg.sql').text
    sql_sgg = Template(sql_sgg).render(params=params_date)

    df_sgg = pd.read_sql(sql_sgg, cursor)

    df_sgg_total = df_sgg[df_sgg["dlvy_type"] == "TOTAL"][["ord_ym", "sd_nm", "sgg_nm", "ord_cnt", "cust_cnt", "gmv",
                                                           "ord_pay", "partial_em_excltax", "partial_em", "mngr_avg"]]
    df_sgg_type = df_sgg[df_sgg["dlvy_type"] != "TOTAL"][["ord_ym", "sd_nm", "sgg_nm", "dlvy_type", "ord_cnt", "cust_cnt", "gmv",
                                                          "ord_pay", "partial_em_excltax", "partial_em", "mngr_avg"]]


    df_household = pd.read_excel(os.path.join(file_path, "household.xlsx"))

    df_sgg_type = rd.penetration(df_household=df_household, df_sgg_type=df_sgg_type)

    df_sgg_type.to_excel("~/Desktop/sample.xlsx", index=False)

    return "Success!"







