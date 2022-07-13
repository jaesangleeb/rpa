from utils.conn import RedshiftConnector
from utils.func import Reader
from utils.func import DateSetter
from utils.config import PathReader

from datetime import datetime
from dateutil.relativedelta import relativedelta
import jinja2
import os
import msoffcrypto
import io
import pandas as pd
import numpy as np
import re

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

    file_path = os.path.join(drive_path, "LF_index_check")

    file_lst = sorted(os.listdir(file_path))

    temp = io.BytesIO()

    def make_df(fname):
        with open(os.path.join(file_path, fname), 'rb') as f:
            excel = msoffcrypto.OfficeFile(f)
            excel.load_key('Corpdev1!')
            excel.decrypt(temp)

            df = pd.read_excel(temp)

        return df

    def sort_buul(target):
        pattern = re.compile('.*(BS).*|.*(UL).*')
        try:
            string = list(re.findall(pattern, target)[0])
            string.remove('')
            string = string[0]
            if string == 'BS':
                string = '부산'
            elif string == 'UL':
                string = '울산'
        except:
            string = '수도권'

        return string

    def sort_saetbyulcrew(target):
        pattern = re.compile('.*(SC).*|.*(FC).*')
        try:
            string = list(re.findall(pattern, target)[0])
            string.remove('')
            string = string[0]
            if string == 'SC':
                string = '컬리샛별'
            elif string == 'FC':
                string = '프솔샛별'
        except:
            string = '그 외'

        return string

    print("파일을 읽어옵니다. 총 파일 수 {}개".format(len(file_lst)))
    print("========================")
    print("1번째 파일을 읽습니다...")
    print("현재시간 : ", datetime.now())
    # df = make_df(os.path.join(file_path, "운송장 정보 다운로드_DOS_220601.xls"))
    df = make_df(os.path.join(file_path, file_lst[0]))

    for idx in range(1, len(file_lst)):
        print("========================")
        print(f"{idx+1}번째 파일을 읽습니다...")
        print("현재시간 : ", datetime.now())
        df_temp = make_df(file_lst[idx])
        df = df.append(df_temp)

    print("모든 파일을 읽었습니다. 데이터 전처리를 시작합니다...")

    # df = pd.read_excel(os.path.join(file_path, 'test.xls'), engine='xlrd')

    fname = os.path.join("/Users/mk-mac-310/Desktop/for_drive",
                         '(AGG) LF Index {0}년 {1}월 {2} {3} LJS.xlsx'.format(y_prev,
                                                                            m_prev,
                                                                            now_history_day,
                                                                            now_history_time))

    df_parsed = df[["주문번호", "배송예정일", "거래처명", "배송유형", "상태", "매니저명", "고객입력주소", "배송완료 일시", "소분류권역"]]
    df_parsed["배송완료 일시"] = df_parsed["배송완료 일시"].apply(lambda x: str(x)[:13])
    df_parsed["소분류권역"] = df_parsed["소분류권역"].apply(lambda x: sort_buul(x))
    df_parsed["크루"] = df_parsed["매니저명"].apply(lambda x: sort_saetbyulcrew(x))
    df_parsed["거래처명"] = df_parsed["거래처명"].str.upper()
    df_parsed["매니저명"] = df_parsed["매니저명"].str.upper()
    df_parsed["고객입력주소"] = df_parsed["고객입력주소"].str.upper()

    df_parsed = df_parsed.drop_duplicates(subset=['배송예정일', '고객입력주소', '배송완료 일시', '매니저명'])
    df_parsed = df_parsed.drop_duplicates(subset=['매니저명', '주문번호'])
    df_parsed["cnt"] = 1

    df_parsed["전통주"] = np.where(df_parsed["배송유형"] == "샛별3PL", np.where(df_parsed["거래처명"] == "마켓컬리","전통주", None), None)
    # df_sool = df_parsed[df_parsed["전통주"] == "전통주"]

    # df_result = df_parsed[df_parsed["배송유형"]=="샛별배송"]
    # df_parsed = df_parsed.append(df_sool)
    df_parsed["전통주"] = np.where(df_parsed["전통주"] == "전통주", "전통주", "그 외")

    df_grouped = df_parsed.groupby(by=["소분류권역", "배송유형", "전통주", "크루"])["cnt"].sum()

    df_parsed.to_excel(os.path.join(drive_path, fname), index=False)


    fgname = os.path.join("/Users/mk-mac-310/Desktop/for_drive",
                         '(AGG) LF Index {0}년 {1}월 {2} {3} LJS_grouped.xlsx'.format(y_prev,
                                                                            m_prev,
                                                                            now_history_day,
                                                                            now_history_time))
    df_grouped.to_excel((os.path.join(drive_path, fgname)))
    print("완료되었습니다!")
    print("현재시간 : ", datetime.now())

    return "Success!"