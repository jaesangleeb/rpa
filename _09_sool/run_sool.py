from utils.conn import RedshiftConnector
from utils.func import Reader
from utils.func import DateSetter
from utils.config import PathReader

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import jinja2
import os
import pprint

def main():

    engine = RedshiftConnector()
    cursor = engine.connect()

    path = PathReader()
    endpoint = "{0}.{1}월결산 raw/CO MD CS/3P KPI".format(datetime.now().strftime('%y'), datetime.now().month-1)
    path = os.path.join(path.path["raw"], endpoint)
    os.makedirs(path, exist_ok=True)
    params = DateSetter('default').setter()

    y = datetime.now().strftime('%y')
    m = datetime.now().strftime('%m')
    d = datetime.now().strftime('%d')


    # module 3) 3P 집계 데이터
    total_3p = Reader("./templates/_09_sool/sool.sql").text
    sql_total_3p = jinja2.Template(total_3p).render(params=params)

    engine = RedshiftConnector()
    cursor = engine.connect()

    with cursor as cur:
        df_total_3p = pd.read_sql(sql_total_3p, cur)

    # 3P만 담겨있는 주문
    df_total_3p["row_cnt"] = df_total_3p["ptype"].apply(lambda x: 1 if x == '1p' else '0')
    df_total_3p["row_cnt"] = df_total_3p["row_cnt"].astype('int')
    res = df_total_3p.groupby(by="ord_cd")[["cnt", "prd_tot_price", "dc_prd_tot",
                                            "dc_prd_coupon", "using_point", "row_cnt"]].sum()
    only = res[res["row_cnt"] == 0]
    only["row_cnt"] = 1

    # 3p + 1p 혼합된 주문
    mixed_total = res[res["row_cnt"] != 0]
    mixed_total["row_cnt"] = 1

    # 3p + 1p 혼합 주문 중에서 "3p"만 걸러내기
    mixed_total_re = mixed_total.reset_index()
    mixed_total_ord_cd = list(mixed_total_re["ord_cd"])
    mixed_total_res = df_total_3p[df_total_3p["ord_cd"].isin(mixed_total_ord_cd)]
    mixed_3p = mixed_total_res[mixed_total_res["ptype"] == "3p"].groupby(by="ord_cd")[
        ["cnt", "prd_tot_price", "dc_prd_tot", "dc_prd_coupon", "using_point"]].sum()
    mixed_3p["row_cnt"] = 1

    # 3p + 1p 혼합 주문 중에서 "3p"만 걸러내기
    mixed_1p = mixed_total_res[mixed_total_res["ptype"] == "1p"].groupby(by="ord_cd")[
        ["cnt", "prd_tot_price", "dc_prd_tot", "dc_prd_coupon", "using_point"]].sum()
    mixed_1p["row_cnt"] = 1

    # 정리
    only = only.sum().to_frame().transpose()
    mixed_total = mixed_total.sum().to_frame().transpose()
    mixed_3p = mixed_3p.sum().to_frame().transpose()
    mixed_1p = mixed_1p.sum().to_frame().transpose()

    df = only
    df = df.append(mixed_1p)
    df = df.append(mixed_3p)
    df = df.append(mixed_total)
    indexes = [["only", "mixed", "mixed", "mixed"], ["only", "total", "1p", "3p"]]
    df.index = pd.MultiIndex.from_arrays(indexes)

    path = PathReader()
    drive_path = path.path["raw"]
    endpoint = "{0}.{1}월결산 raw/CO MD CS/3P KPI".format(datetime.now().strftime('%y'), datetime.now().month-1)
    path = os.path.join(drive_path, endpoint)
    os.makedirs(path, exist_ok=True)
    fname = "{0}월_전통주_{1}{2}{3} LJS .xlsx".format((datetime.now() - relativedelta(months=1)).month, y, m, d)
    df.columns = ["units", "gmv", "dc_prd", "dc_coupon", "using_point", "ord_cnt"]

    df.to_excel(os.path.join(path, fname))

    return "Success!"