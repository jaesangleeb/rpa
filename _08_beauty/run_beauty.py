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

    sql = Reader('./templates/_08_beauty/beauty.sql').text

    sql = Template(sql).render(params=params)


    beauty = pd.read_sql(sql, cursor)

    fname = "(AGG) Beauty for Breakdown {0}년 {1}월 {2} {3} LJS.xlsx".format(y_prev,
                                                                              m_prev,
                                                                              now_history_day,
                                                                              now_history_time)
    beauty.to_excel(os.path.join(drive_path, fname))

    return 'Success!'