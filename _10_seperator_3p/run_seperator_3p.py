from utils.conn import RedshiftConnector
from utils.func import Reader
from utils.func import DateSetter
from utils.config import PathReader

import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import jinja2
import os
import pprint


def main():

    y = datetime.now().strftime('%y')
    m = datetime.now().strftime('%m')
    d = datetime.now().strftime('%d')
    m_prev_strp = (datetime.now() - relativedelta(months=1)).month

    y_prev = (datetime.now() - relativedelta(months=1)).strftime('%y')
    m_prev = (datetime.now() - relativedelta(months=1)).strftime('%m')
    now_history_day = datetime.now().strftime('%y%m%d')
    now_history_time = datetime.now().strftime('%H00')

    engine = RedshiftConnector()
    cursor = engine.connect()

    path = PathReader()
    drive_path = path.path["analysis"]
    endpoint = "1.1.7 3P Analysis/{0}.{1}월".format(y_prev, m_prev_strp)
    path = os.path.join(path.path["analysis"], endpoint)
    os.makedirs(path, exist_ok=True)
    params = DateSetter('default').setter()

    agg_func = {
        "ord_cd": pd.Series.nunique,
        "cnt": sum,
        "gmv_retail": sum,
        "dc_deal_coupon": sum,
        "free_shipping": sum,
        "dc_deal_tot": sum,
        "dc_deal_point": sum
    }

    # module 3) 3P 집계 데이터
    sql = Reader("./templates/_10_seperator_3p/seperator_3p.sql").text
    sql = jinja2.Template(sql).render(params=params)


    engine = RedshiftConnector()
    cursor = engine.connect()

    with cursor as cur:
        df = pd.read_sql(sql, cur)

    df_grouped = df.groupby(by="ord_cd").sum().reset_index()[["ord_cd", "yn_3p", "sku"]]
    df_grouped["only_yn"] = np.where(df_grouped["yn_3p"] == df_grouped["sku"], "only", "mixed")

    res = df.merge(df_grouped, left_on="ord_cd", right_on="ord_cd", how="left")
    result = res.groupby(by=["ord_ym", "only_yn", "yn_3p_x", "dlvy_yn_3p"]).agg(agg_func).reset_index()
    result["3p_yn"] = np.where(result["yn_3p_x"] == 1, "3p", "1p")
    result = result[
        ["ord_ym", "only_yn", "3p_yn", "dlvy_yn_3p", "ord_cd", "cnt", "gmv_retail", "dc_deal_coupon", "free_shipping",
         "dc_deal_tot", "dc_deal_point"]]
    result = result.rename({"dlvy_yn_3p": "3p_dlvy_yn",
                            "ord_cd": "ord_cnt"}, axis="columns")

    fname = "(AGG) 3P Separated {0}년 {1}월 {2} {3} LJS .xlsx".format(y_prev, m_prev, now_history_day, now_history_time)

    result.to_excel(os.path.join(path, fname), index=False)

    return "Success!"