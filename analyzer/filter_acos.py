import numpy as np
import pandas as pd
import spacy
import pymysql
import warnings
warnings.filterwarnings("ignore")

from nltk.util import ngrams, everygrams
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from PyDictionary import PyDictionary
from datetime import datetime
from datetime import timedelta
from loguru import logger

import helper_filter as hfilter

import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)


def filter_asin_acos(target_report, attributed_sales, attributed_units):
    database_ppcwiz  = os.getenv('DATABASE_URL')
    if ((target_report == "sp_target_report") or (target_report == "sp_keyword_report")):
        filter_asin    = "sp_filter_asin"
        sp_vs_sb_vs_sd = "sp"
    elif (target_report == "sb_target_report"):
        filter_asin    = "sb_filter_asin"
        sp_vs_sb_vs_sd = "sb"
    elif (target_report == "sd_target_report"):
        filter_asin    = "sd_filter_asin"
        sp_vs_sb_vs_sd = "sd"
    else:
        filter_asin    = ""
        sp_vs_sb_vs_sd = ""

    df = hfilter.sql_to_pandas(target_report,
                               database_ppcwiz,
                               3306,
                               "admin",
                               "ppcwiz",
                               "ppcwiz",
                               "utf8")

    if (sp_vs_sb_vs_sd == "sp"):
        df_begin = hfilter.df_rename(df)
    else:
        df_begin = hfilter.df_rename_target(df)
    df_begin.rename(columns={
        attributed_sales: "sales",
        attributed_units: "orders"
    }, inplace=True)
    for i in range(df_begin.shape[0]):
        df_begin["asin"][i] = df_begin["asin"][i].upper()
    #df_begin = df_begin[df_begin["cost"] >= 0.3 * df_begin["sales"]]
    #df_begin.reset_index(drop=True, inplace=True)

    df_currency = hfilter.sql_to_pandas_basic("dtb_profile",
                                     database_ppcwiz,
                                     3306,
                                     "admin",
                                     "ppcwiz",
                                     "ppcwiz",
                                     "utf8")
    df_begin = hfilter.currency_code(df_begin, df_currency)
    df_begin = hfilter.exchange_rate(df_begin, "cost", "sales")
    df_begin.reset_index(drop=True, inplace=True)

    set_adgroup = set(df_begin["ad_group_id"])
    for i in set_adgroup:
        print("---------------------------------------")
        df = df_begin.copy()

        # specific adgroup id
        adgroup_id = i
        df = df[df["ad_group_id"] == adgroup_id]
        df.reset_index(inplace=True)

        # from start date to end date
        date_pd = pd.to_datetime(df["date"])
        days = (np.max(date_pd) - np.min(date_pd) + timedelta(days=1)).days
        print(days, "days")
        if (days <= 0):
            print("---------------------------------------")
            continue

        # profile id and campaign id related to adgroup id
        prof_id, camp_id        = df["profile_id"][0], df["campaign_id"][0]
        camp_name, adgroup_name = df["campaign_name"][0], df["ad_group_name"][0]
        temp_id                 = df["user_id"][0]

        print("Campaign ID:", camp_id)
        print("Ad Group ID:", adgroup_id)

        # dtb_user table
        temp_user    = hfilter.sql_to_pandas_compare("dtb_user",
                                                     "id, keyword_active, asin_active, acos_active, default_profile, neg_list",
                                                     "id = " + str(temp_id),
                                                     database_ppcwiz,
                                                     3306,
                                                     "admin",
                                                     "ppcwiz",
                                                     "ppcwiz",
                                                     "utf8")
        temp_acos = hfilter.sql_to_pandas_compare(filter_asin,
                                                     "ad_group_id, expression",
                                                     "ad_group_id = " + str(adgroup_id),
                                                     database_ppcwiz,
                                                     3306,
                                                     "admin",
                                                     "ppcwiz",
                                                     "ppcwiz",
                                                     "utf8")

        df = df[["date", "asin", "impressions", "clicks", "cost", "sales", "orders"]]

        # sales price before the beginning
        sales_price = np.sum(df["sales"]) / np.sum(df["orders"])

        print("1. Keyword Validation Filter (Not Exist)")
        print("2. ACoS Filter")

        # delete duplicates
        df_acos_explode = df.groupby("asin").sum()
        df_acos_explode.reset_index(inplace=True)

        #df_acos_explode = df_acos_explode[df_acos_explode["cost"] >= 0.3 * df_acos_explode["sales"]]
        #df_acos_explode.reset_index(drop=True, inplace=True)

        # median cost
        median_cost = np.median(df_acos_explode["cost"])

        # cost > median
        df_acos_explode = df_acos_explode[df_acos_explode["cost"] > median_cost]
        df_acos_explode.reset_index(drop=True, inplace=True)

        # ACoS
        df_acos_explode["ACoS"] = df_acos_explode["cost"] / df_acos_explode["sales"]
        df_acos_explode["ACoS"][df_acos_explode["sales"] == 0] = -1

        # sales = 0 and cost >= sales_price / 2
        temp = df_acos_explode[df_acos_explode["ACoS"] == -1]
        temp = temp[temp["cost"] >= (sales_price / 2)]

        # sales > 0
        df_acos_temp = df_acos_explode[df_acos_explode["ACoS"] != -1]
        df_acos_temp.sort_values("ACoS", ascending=False, inplace=True)

        # total ACoS
        total_acos = np.sum(df_acos_explode["cost"]) / np.sum(df_acos_temp["sales"]) #<----------

        # compare with total ACoS
        df_acos_temp = df_acos_temp[df_acos_temp["ACoS"] >= 2 * total_acos]
        df_acos_temp = df_acos_temp[df_acos_temp["clicks"] > 5]
        df_acos_temp = df_acos_temp[df_acos_temp["cost"] > 5]
        df_acos_temp.sort_values("ACoS", ascending=False, inplace=True)
        df_acos_temp.reset_index(drop=True, inplace=True)

        # combine [sales = 0 and cost >= sales_price / 2] and [sales > 0]
        df_acos_comb = pd.concat([temp, df_acos_temp])
        df_acos_comb.reset_index(drop=True, inplace=True)

        df_acos_comb["profile_id"]       = prof_id
        df_acos_comb["campaign_id"]      = camp_id
        df_acos_comb["campaign_name"]    = camp_name
        df_acos_comb["ad_group_id"]      = adgroup_id
        df_acos_comb["ad_group_name"]    = adgroup_name
        df_acos_comb["state"]            = "ENABLED"
        df_acos_comb["expression_type"]  = "MANUAL"
        df_acos_comb["active"]           = hfilter.user_payment(temp_user, "acos_active")
        df_acos_comb["created_datetime"] = datetime.now()
        df_acos_comb["updated_datetime"] = datetime.now()
        df_acos_comb["saved"]            = round(np.sum(df_acos_comb["cost"]) * 30 / days, 2)
        #round(np.sum(df_acos_comb["cost"]) * 30 / days, 2)
        df_acos_comb.rename(columns={
            "asin": "expression",
            "cost": "associated_cost",
            "sales": "associated_sales",
            "clicks": "associated_clicks",
            "orders": "associated_orders"
        }, inplace=True)
        df_acos_comb = df_acos_comb[["profile_id",
                                     "campaign_id",
                                     "campaign_name",
                                     "ad_group_id",
                                     "ad_group_name",
                                     "state",
                                     "expression",
                                     "expression_type",
                                     "associated_cost",
                                     "associated_sales",
                                     "associated_clicks",
                                     "associated_orders",
                                     "active",
                                     "created_datetime",
                                     "updated_datetime",
                                     "saved"]]
        logger.info(df_acos_comb)

        #drop previous data which are the same
        result = []
        for j in range(len(temp_acos["expression"])):
            for i in range(len(df_acos_comb["expression"])):
                if (df_acos_comb["expression"][i] == temp_acos["expression"][j]):
                    result.append(i)
        df_acos_comb.drop(result, axis=0, inplace=True)
        df_acos_comb.reset_index(drop=True, inplace=True)
        print(df_acos_comb)

        # database from pandas to sql
        hfilter.pandas_to_sql(df_acos_comb,
                              database_ppcwiz,
                              3306,
                              "admin",
                              "ppcwiz",
                              "ppcwiz",
                              "utf8",
                              sp_vs_sb_vs_sd,
                              "asin")
        
        print("---------------------------------------")
        print("")
    '''
    # database from pandas to sql (update)
    hfilter.pandas_to_sql_update(df_begin,
                                 database_ppcwiz,
                                 3306,
                                 "admin",
                                 "ppcwiz",
                                 "ppcwiz",
                                 "utf8",
                                 target_report)
    '''
