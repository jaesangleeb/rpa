from utils.conn import RedshiftConnector
from utils.func import Reader
from utils.func import DateSetter
from utils.config import PathReader

import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from jinja2 import Template


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
    endpoint = "1.1.6. 카테고리 KPI/{0}년 {1}월".format(y_prev, m_prev)
    drive_path = os.path.join(path.path["analysis"], endpoint)
    os.makedirs(drive_path, exist_ok=True)

    engine = RedshiftConnector()
    cursor = engine.connect()

    sql = Reader('./templates/_04_catg_kpi/catg_kpi.sql').text

    params = DateSetter('default').setter()

    sql = Template(sql).render(params=params)

    df = pd.read_sql(sql, cursor)

    fname = "(RAW) KPI by Category {0}년 {1}월 {2} {3} LJS.xlsx".format(y_prev,
                                                                      m_prev,
                                                                      now_history_day,
                                                                      now_history_time)
    drive_path = os.environ["PROC"]

    df.to_excel(os.path.join(drive_path, fname), index=False)

    return "Success!"