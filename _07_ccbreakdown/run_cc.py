import pandas as pd
import numpy as np
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from jinja2 import Template
import pprint

from utils.conn import RedshiftConnector
from utils.func import Reader
from utils.func import DateSetter
from utils.config import PathReader

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
    endpoint = "{0}.{1}월결산 raw/CD".format(y_prev, m_prev_strp)
    drive_path = os.path.join(path.path["raw"], endpoint)
    os.makedirs(drive_path, exist_ok=True)

    engine = RedshiftConnector()
    cursor = engine.connect()

    params = DateSetter('default').setter()
    # params = {"start_date": "'2022-07-01'",
    #           "end_date": "'2022-08-01'"}

    sql = Reader('./templates/_07_ccbreakdown/ccbreakdown_gmv.sql').text

    sql = Template(sql).render(params=params)


    gmv = pd.read_sql(sql, cursor)

    gmv = gmv.pivot_table(values = 'gmv', columns= 'feature', index=['ord_ym', 'center_cd'])
    gmv["Others"] = gmv["1p"] - gmv["Beauty"]
    gmv = gmv[['3p', '1p', 'Beauty', 'Others', 'TOTAL']]

    fname = os.path.join(drive_path, '(AGG) GMV for CC Breakdown {0}년 {1}월 {2} {3} LJS.xlsx'.format(y_prev,
                                                                                                    m_prev,
                                                                                                    now_history_day,
                                                                                                    now_history_time))

    with pd.ExcelWriter(fname) as writer:
        gmv.to_excel(writer, sheet_name='gmv')

    return 'Success!'