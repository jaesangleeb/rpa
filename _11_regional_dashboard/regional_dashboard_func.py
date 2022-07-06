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


def basket_size(df):

    df["basket_size"] = df["gmv"] / df["ord_cnt"]

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


