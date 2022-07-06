import pandas as pd
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

    path = PathReader()
    endpoint = "{0}.{1}월결산 raw/CD".format(y_prev, m_prev_strp)
    drive_path = os.path.join(path.path["raw"], endpoint)
    os.makedirs(drive_path, exist_ok=True)

    file_path = path.path["file"]

    y = datetime.now().strftime('%y')
    m = datetime.now().strftime('%m')
    d = datetime.now().strftime('%d')

    y_prev = (datetime.now() - relativedelta(months=1)).strftime('%y')
    m_prev = (datetime.now() - relativedelta(months=1)).strftime('%m')
    now_history_day = datetime.now().strftime('%y%m%d')
    now_history_time = datetime.now().strftime('%H00')

    print('불러올 Raw 파일명은 "(CS)raw_yymm.xlsx" 형태로 해주세요')
    try:
        fname = '(CS)raw_{}.xlsx'.format(y_prev+m_prev)
        df = pd.read_excel(os.path.join(file_path, fname))
    except:
        fname = input("파일이름명을 확장자 포함해서 입력해주세요: \n")
        df = pd.read_excel(os.path.join(file_path, fname))

    df = df.fillna(0)

    # 프로모션
    # 기획전, 특가전, 카테고리, 상품, 일일특가, 은퇴, 전액부담

    # MD요청
    # MD추천, 경쟁사가격대응

    # 선물세트
    # 명절(설/추석), 기타

    # CO Markdown
    # D-5, D+1, 데일리

    # Others
    # 미확인

    cols_lv_one = ["Supplier Discounts"]+["Fixed Discounts"]+["프로모션"]*7+["MD요청"]*2+["선물세트"]*2+["CO Markdown"]*3+["미확인"]
    cols_lv_zero =["공급사 분담 할인액",
                   "상시할인",
                   "기획전", "특가전", "카테고리", "상품", "일일특가", "은퇴", "전액부담",
                   "MD추천", "경쟁사가격대응",
                   "명절(설/추석)", "기타",
                   "D-5", "D+1", "데일리",
                   "미확인"]
    cols_default = ['ord_ymd', 'center_cd', 'prd_cd', 'prd_nm', 'catg_1_nm', 'catg_2_nm']
    df = df[cols_default + cols_lv_zero]
    df_grouped = df.groupby(by=['center_cd']).sum()[cols_lv_zero]
    df_grouped.columns = [cols_lv_one, cols_lv_zero]

    file_name = "(AGG) (CS)N% 할인 Tracking {0}년 {1}월 {2} {3} LJS.xlsx".format(y_prev, m_prev, now_history_day, now_history_time)

    df_grouped.to_excel(os.path.join(drive_path, file_name))

    return 'Success!'

