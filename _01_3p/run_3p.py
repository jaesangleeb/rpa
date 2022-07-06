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
    params = DateSetter('default').setter()

    # module 1) 3P Raw로 추출
    f = Reader("./templates/_01_3p/raw_3p_extract.sql")
    # f = Reader("raw_3p_extract.sql")
    sql = f.text

    sql = jinja2.Template(sql).render(params=params)

    with cursor as cur:
        df = pd.read_sql(sql, cur)

    fname = "(RAW) 3P By Product {0}년 {1}월 {2} {3} LJS.xlsx".format(y, m_prev, now_history_day, now_history_time)

    pprint.pprint(sql)

    df.to_excel(os.path.join(drive_path, fname), index=False)

    df_grouped = df.groupby(['center_cd', 'gubn', 'gubn_detail']).sum()

    # module 2) 3P CC breakdown에 입력되는 집계 데이터 추출
    # df_grouped.reset_index().to_excel(os.path.join(drive_path, '3P_{0}년_{1}월.xlsx'.format(y, m_prev)))
    fname = '(AGG) 3P for CC Breakdown {0}년 {1}월 {2} {3} LJS.xlsx'.format(y_prev, m_prev, now_history_day, now_history_time)
    df_grouped.reset_index().to_excel(os.path.join(drive_path, fname))

    return "Success!"