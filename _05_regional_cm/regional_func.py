import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np

def vat(df):

    df["vat"] = df["partial_em"] - df["partial_em_excltax"]

    return df

def acct_rev(df, params):

    df["acct_rev"] = df["vat"] + df["ord_pay"]
    ratio = df["acct_rev"] / df["acct_rev"].sum()
    df["acct_rev"] = params["accounting_revenue"] * ratio

    return df

def portion(df, model_value, factor, sd=True):

    if sd == True:
        pop = df[f"{factor}"].sum()
        adjust_ratio = model_value / pop
        df[f"sum_{factor}"] = pop * adjust_ratio
        df[f"ratio_{factor}"] = df[f"{factor}"] * adjust_ratio / pop
        if factor == "gmv":
            pass
        else:
            df[f"result_{factor}"] = df[f"sum_{factor}"] * df[f"ratio_{factor}"]
    else:
        rs_pop = df[f"{factor}"].sum()
        adjust_ratio = model_value / rs_pop
        pop = df.groupby("sd_nm")[f"{factor}"].transform(sum) * adjust_ratio
        df[f"sum_{factor}"] = pop
        df[f"ratio_{factor}"] = df[f"{factor}"] * adjust_ratio / pop
        if factor == "gmv":
            pass
        else:
            df[f"result_{factor}"] = df[f"sum_{factor}"] * df[f"ratio_{factor}"]

    return df

def base_params(params):

    #1) cogs_ratio = cogs / gmv
    params["cogs_ratio"] = params["cogs_cost"] / params["gmv"]

    #2) order_processing_ratio = order_processing_cost / gmv
    params["order_processing_ratio"] = params["order_processing_cost"] / params["gmv"]
    params["packaging_ratio"] = params["packaging_cost"] / params["gmv"]
    params["PG_ratio"] = params["PG_cost"] / params["gmv"]

    #3) direct_cost_ratio(배분되는 공통 직접비율) = 주문처리비율(%of 1p GMV) + 패키징비율
    params["direct_common_cost_ratio"] = params["order_processing_ratio"] + params["packaging_ratio"] + params["PG_ratio"]

    #4) common_cost_ratio(배분되는 공통비율, %of 1p GMV) = 매출원가비율 + 배분되는 공통 직접비율
    params["common_cost_ratio"] = params["cogs_ratio"] + params["direct_common_cost_ratio"]

    #5) common_cost(배분되는 공통비) = GMV * 2)비율 -> %of 1p GMV이므로 GMV 곱해주면 absolute value가 나옴
    params["common_cost"] = params["gmv"] * params["common_cost_ratio"]

    #6) delivery_morning_cost(수도권 새벽/샛별비용) = GMV * 수도권 새벽/샛별비율
    # params["delivery_morning_cost"] = params["delivery_morning_ratio"] * params["gmv"]

    #7) delivery_3pl_cost(택배비용) = GMV * 택배비율
    # params["delivery_3pl_cost"] = params["delivery_3pl_ratio"] * params["gmv"]

    #8) delivery_morning_cj_cost(CJ 새벽배송 비용) = GMV * CJ새벽배송비율
    # params["delivery_morning_cj_cost"] = params["delivery_morning_cj_ratio"] * params["gmv"]

    #9) 대구 CJ 새벽배송 비용 = 전체 CJ 새벽배송 비용 - 충청 CJ 새벽배송 비용
    params["dawn_daegu_cost"] = params["delivery_morning_cj_cost"] - params["dawn_choongchung_cost"]

    return params

def basket_size(df):

    df["basket_size"] = df["gmv"] / df["ord_cnt"]

    return df

def common_cost(df, params):

    df["common_cost"] = df["gmv"] / df["gmv"].sum() * params["common_cost"]

    return df


def direct_delivery_cost(df, params):

    sudogwon = ["서울특별시", "경기도", "인천광역시"]
    daegu = ["대구광역시"]
    choongchung = ["충청북도", "충청남도", "대전광역시", "세종특별자치시"]
    buul = ["부산광역시", "울산광역시"]

    dawn = ["FRESH_DAWN", "CJ_DAWN"]
    parcel = ["CJ_PARCEL"]

    sudogwon_dawn = df["sd_nm"].isin(sudogwon) & df["dlvy_type"].isin(dawn)
    daegu_dawn = df["sd_nm"].isin(daegu) & df["dlvy_type"].isin(dawn)
    choongchung_dawn = df["sd_nm"].isin(choongchung) & df["dlvy_type"].isin(dawn)
    buul_dawn = df["sd_nm"].isin(buul) & df["dlvy_type"].isin(dawn)
    parcel_total = df["dlvy_type"].isin(parcel)

    df["direct_delivery_cost_ratio"] = np.where(sudogwon_dawn,
                                       df["mngr_avg"]/df[sudogwon_dawn]["mngr_avg"].sum(),
                                       0)
    df["direct_delivery_cost_ratio"] = np.where(daegu_dawn,
                                       df["ord_cnt"] / df[daegu_dawn]["ord_cnt"].sum(),
                                       df["direct_delivery_cost_ratio"])
    df["direct_delivery_cost_ratio"] = np.where(choongchung_dawn,
                                       df["ord_cnt"] / df[choongchung_dawn]["ord_cnt"].sum(),
                                       df["direct_delivery_cost_ratio"])
    df["direct_delivery_cost_ratio"] = np.where(buul_dawn,
                                       df["ord_cnt"] / df[buul_dawn]["ord_cnt"].sum(),
                                       df["direct_delivery_cost_ratio"])
    df["direct_delivery_cost_ratio"] = np.where(parcel_total,
                                       df["gmv"] / df[parcel_total]["gmv"].sum(),
                                       df["direct_delivery_cost_ratio"])

    #수도권 샛별
    df["direct_delivery_cost"] = np.where(sudogwon_dawn, params["delivery_morning_cost"] * df["direct_delivery_cost_ratio"], 0)
    #대구 새벽
    df["direct_delivery_cost"] = np.where(daegu_dawn, params["dawn_daegu_cost"] * df["direct_delivery_cost_ratio"], df["direct_delivery_cost"])
    #충청 새벽
    df["direct_delivery_cost"] = np.where(choongchung_dawn, params["dawn_choongchung_cost"] * df["direct_delivery_cost_ratio"], df["direct_delivery_cost"])
    #부울 새벽
    df["direct_delivery_cost"] = np.where(buul_dawn, params["dawn_buul_cost"] * df["direct_delivery_cost_ratio"], df["direct_delivery_cost"])
    #택배 배송
    df["direct_delivery_cost"] = np.where(parcel_total, params["delivery_3pl_cost"] * df["direct_delivery_cost_ratio"], df["direct_delivery_cost"])

    return df


def cm(df, params):

    df["cm"] = df["acct_rev"] - df["common_cost"] - df["direct_delivery_cost"]

    return df

def penetration(df_household, df_sgg_type):
    col_list = list(df_household.columns)
    col_list[4:] = list(col[:4] + '-' + col[5:] for col in col_list[4:])
    df_household.columns = col_list

    household = pd.melt(df_household, id_vars=df_household.columns[0:4],
                        value_vars=df_household.columns[4:],
                        var_name='ord_ym',
                        value_name='household')

    df_sgg_type = df_sgg_type.merge(household,
                                    left_on=['ord_ym', 'sd_nm', 'sgg_nm'],
                                    right_on=['ord_ym', 'sd_nm', 'sgg_nm'],
                                    how='left')

    # df_sgg_type = df_sgg_type.dropna()
    try:
        df_sgg_type['penetration'] = df_sgg_type['cust_cnt'] / df_sgg_type['household']
        df_sgg_type['penetration'] = df_sgg_type['penetration'].apply(lambda x: '{:.2%}'.format(x))
    except:
        df_sgg_type['penetration'] = 0
    columns = list(df_sgg_type.columns)
    columns.remove("gvn_sd_nm")
    columns.remove("gvn_sgg_nm")
    df_sgg_type = df_sgg_type[columns]

    return df_sgg_type


