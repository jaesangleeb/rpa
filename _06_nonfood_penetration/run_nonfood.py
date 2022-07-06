import pandas as pd
import numpy as np
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from jinja2 import Template

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
    endpoint = "{0}.{1}월".format(y_prev, m_prev_strp)
    path = os.path.join(path.path["nonfood"], endpoint)
    os.makedirs(path, exist_ok=True)

    params = {}
    params = DateSetter('default').setter()

    catg_non_food_cd = Reader('./templates/_06_nonfood_penetration/catg_non_food_cd.sql').text
    # catg_non_food_cd = Reader('catg_non_food_cd.sql').text
    catg_non_food_cd = Template(catg_non_food_cd).render(params=params)

    catg_non_food_co = Reader('./templates/_06_nonfood_penetration/catg_non_food_co.sql').text
    # catg_non_food_co = Reader('catg_non_food_co.sql').text
    catg_non_food_co = Template(catg_non_food_co).render(params=params)

    non_food_cd = Reader('./templates/_06_nonfood_penetration/non_food_cd.sql').text
    # non_food_cd = Reader('non_food_cd.sql').text
    non_food_cd = Template(non_food_cd).render(params=params)

    non_food_co = Reader('./templates/_06_nonfood_penetration/non_food_co.sql').text
    # non_food_co = Reader('non_food_co.sql').text
    non_food_co = Template(non_food_co).render(params=params)

    total_cust = Reader('./templates/_06_nonfood_penetration/total_cust.sql').text
    # total_cust = Reader('total_cust.sql').text
    total_cust = Template(total_cust).render(params=params)

    engine = RedshiftConnector()
    cursor = engine.connect()

    catg_non_food_df_cd = pd.read_sql(catg_non_food_cd, cursor)
    catg_non_food_df_cd = catg_non_food_df_cd.fillna(0)
    catg_non_food_df_cd = pd.pivot_table(catg_non_food_df_cd,
                                      values=['cust_n', 'gmv', 'ord_cnt'],
                                      columns='ord_ym',
                                      index=['catg_1_nm'],
                                      margins=True,
                                      margins_name='합계',
                                      aggfunc='sum',
                                      fill_value=0)
    catg_non_food_df_cd = catg_non_food_df_cd.reindex(['가전제품', '반려동물', '뷰티', '생활용품', '생활잡화',
                                                       '여행/문화/서비스', '유아동', '주방용품', '주류', '가구/인테리어',
                                                       '패션/잡화', '스포츠/레저', '합계'])
    catg_non_food_df_cd = catg_non_food_df_cd.drop('합계', axis=0)

    non_food_df_cd = pd.read_sql(non_food_cd, cursor)
    non_food_df_cd = non_food_df_cd.fillna(0)
    non_food_df_cd = pd.pivot_table(non_food_df_cd, values=['cust_n', 'gmv', 'ord_cnt'], columns='ord_ym', fill_value=0)

    catg_non_food_df_co = pd.read_sql(catg_non_food_co, cursor)
    catg_non_food_df_co = catg_non_food_df_co.fillna(0)
    catg_non_food_df_co = pd.pivot_table(catg_non_food_df_co,
                                      values=['cust_n', 'gmv', 'ord_cnt'],
                                      columns='ord_ym',
                                      index=['catg_1_nm'],
                                      margins=True,
                                      margins_name='합계',
                                      aggfunc='sum',
                                      fill_value=0)
    catg_non_food_df_co = catg_non_food_df_co.reindex(['가전제품', '반려동물', '뷰티', '생활용품', '생활잡화',
                                                       '여행/문화/서비스', '유아동', '주방용품', '가구/인테리어',
                                                       '패션/잡화', '스포츠/레저', '헬스', '기타', '합계'])
    catg_non_food_df_co = catg_non_food_df_co.drop('합계', axis=0)
    catg_non_food_df_co = catg_non_food_df_co.fillna(0)

    non_food_df_co = pd.read_sql(non_food_co, cursor)
    non_food_df_co = non_food_df_co.fillna(0)
    non_food_df_co = pd.pivot_table(non_food_df_co, values=['cust_n', 'gmv', 'ord_cnt'], columns='ord_ym', fill_value=0)
    non_food_df_co = non_food_df_co.fillna(0)

    total_cust = pd.read_sql(total_cust, cursor)
    total_cust = total_cust.fillna(0)
    total_cust = pd.pivot_table(total_cust, values=['cust_n', 'gmv', 'ord_cnt'], columns='ord_ym', fill_value=0)


    fname_cd = os.path.join(path, '(RAW) (재무기획 ver.)NonFood Penetration {0}년 {1}월 {2} {3} LJS.xlsx'.format(y_prev,
                                                                                                           m_prev,
                                                                                                           now_history_day,
                                                                                                           now_history_time))

    with pd.ExcelWriter(fname_cd) as writer:
        total_cust.to_excel(writer, sheet_name='CD_total_cust')
        non_food_df_cd.to_excel(writer, sheet_name='CD_non_food')
        catg_non_food_df_cd.to_excel(writer, sheet_name='CD_catg_non_food')

    fname_co = os.path.join(path, '(RAW) (상품운영 ver.)NonFood Penetration {0}년 {1}월 {2} {3} LJS.xlsx'.format(y_prev,
                                                                                                           m_prev,
                                                                                                           now_history_day,
                                                                                                           now_history_time))

    with pd.ExcelWriter(fname_co) as writer:
        total_cust.to_excel(writer, sheet_name='CO_total_cust')
        non_food_df_co.to_excel(writer, sheet_name='CO_non_food')
        catg_non_food_df_co.to_excel(writer, sheet_name='CO_catg_non_food')

    return "Success!"
