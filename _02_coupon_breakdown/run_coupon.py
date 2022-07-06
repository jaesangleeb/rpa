import pandas as pd
from utils.conn import RedshiftConnector
from utils.func import Reader, DateSetter
from utils.config import PathReader
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import jinja2
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
    endpoint = "{0}.{1}월결산 raw/CD".format(y_prev, m_prev_strp)
    drive_path = os.path.join(path.path["raw"], endpoint)
    os.makedirs(drive_path, exist_ok=True)

    f = Reader("./templates/_02_coupon_breakdown/coupon.sql")
    sql = f.text

    params = DateSetter('default').setter()
    sql = jinja2.Template(sql).render(params=params)

    with cursor as cur:
        df = pd.read_sql(sql, cur)

    fname = "(RAW) Coupon Breakdown {0}년 {1}월 {2} {3} LJS.xlsx".format(y_prev, m_prev, now_history_day, now_history_time)

    df.to_excel(os.path.join(drive_path, fname), index=False)

    return "Success!"
