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


def filter_key_acos(keyword_report, attributed_sales, attributed_units):
    database_ppcwiz  = os.getenv('DATABASE_URL')
    if ((keyword_report == "sp_keyword_report") or (keyword_report == "sp_target_report")):
        filter_neg_keyword = "sp_filter_neg_keyword"
        filter_acos        = "sp_filter_acos"
        sp_vs_sb           = "sp"
    elif (keyword_report == "sb_keyword_report"):
        filter_neg_keyword = "sb_filter_neg_keyword"
        filter_acos        = "sb_filter_acos"
        sp_vs_sb           = "sb"
    else:
        filter_neg_keyword = ""
        filter_acos        = ""
        sp_vs_sb           = ""

    df_begin = hfilter.sql_to_pandas(keyword_report,
                                     database_ppcwiz,
                                     3306,
                                     "admin",
                                     "ppcwiz",
                                     "ppcwiz",
                                     "utf8")
    #df_begin = df_begin[df_begin["cost"] >= 0.3 * df_begin[attributed_sales]]
    #df_begin.reset_index(drop=True, inplace=True)

    df_currency = hfilter.sql_to_pandas_basic("dtb_profile",
                                     database_ppcwiz,
                                     3306,
                                     "admin",
                                     "ppcwiz",
                                     "ppcwiz",
                                     "utf8")
    df_begin = hfilter.currency_code(df_begin, df_currency)
    df_begin = hfilter.exchange_rate(df_begin, "cost", attributed_sales)
    df_begin.reset_index(drop=True, inplace=True)

    for i in set(df_begin["ad_group_id"]):
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
        temp_keyword = hfilter.sql_to_pandas_compare(filter_neg_keyword,
                                                     "ad_group_id, keyword_text",
                                                     "ad_group_id = " + str(adgroup_id),
                                                     database_ppcwiz,
                                                     3306,
                                                     "admin",
                                                     "ppcwiz",
                                                     "ppcwiz",
                                                     "utf8")
        temp_acos = hfilter.sql_to_pandas_compare(filter_acos,
                                                     "ad_group_id, keyword_text",
                                                     "ad_group_id = " + str(adgroup_id),
                                                     database_ppcwiz,
                                                     3306,
                                                     "admin",
                                                     "ppcwiz",
                                                     "ppcwiz",
                                                     "utf8")

        df["query"] = df["query"].str.replace(r"[/\\]", "")

        if ((keyword_report == "sp_keyword_report") or (keyword_report == "sb_keyword_report")):
            df.rename(columns={
                "keyword_text": "keyword_text_temp",
                "query": "keyword_text",
                attributed_sales: "sales",
                attributed_units: "orders"
            }, inplace=True)
        elif (keyword_report == "sp_target_report"):
            df.rename(columns={
                #"keyword_text": "keyword_text_temp",
                "query": "keyword_text",
                attributed_sales: "sales",
                attributed_units: "orders"
            }, inplace=True)

        df = df[["date", "keyword_text", "impressions", "clicks", "cost", "sales", "orders"]]
        df = df[~df["keyword_text"].str.startswith("b0")]
        df = df.groupby("keyword_text").sum()
        df.sort_values("impressions", ascending=False, inplace=True)
        df.reset_index(inplace=True)

        # sales price before the beginning
        sales_price = np.sum(df["sales"]) / np.sum(df["orders"])

        print("1. Keyword Validation Filter")
        # valid keyword characters and split terms
        df = hfilter.val_key_char(df)

        # remove unnecessary terms through NLTK
        df = hfilter.remove_unnecessary(df)

        # text format
        df = hfilter.text_format(df)

        # from plural to singular if there are any
        df = hfilter.singularize_spacy(df)

        # split names
        df = hfilter.split_name(df)
        df = hfilter.singularize_nltk(df)

        # remove unnecessary terms through NLTK
        df = hfilter.remove_unnecessary_split(df)

        # copy for new filter
        df_acos = df.copy()
        #df["acos"] = df["cost"] / df["sales"]
        df_temp = df.copy()

        # everygrams
        df = hfilter.n_grams(df, 1)

        # explode lists
        df_explode = hfilter.explode_list(df)

        # text format (split)
        df_explode = hfilter.text_format_split(df_explode)

        # top in appearance
        df_count = df_explode[["match_txt_split", "impressions"]].groupby("match_txt_split").sum()
        df_count.sort_values("impressions", ascending=False, inplace=True)
        df_count.reset_index(inplace=True)
        df_count.rename(columns={
            "impressions": "count"
        }, inplace=True)

        # the most important keywords
        dom_key = hfilter.domain_keyword(df_count)
        dom_key_temp = hfilter.domain_keyword_split(dom_key)

        # their synonyms
        dom_keyword = hfilter.get_synonym(dom_key_temp)

        # list to set, and set to list
        dom_keyword = set(dom_key + list(dom_keyword))
        dom_keyword = list(dom_keyword)

        # drop irrelevant
        _, df_drop = hfilter.drop_irrelevant(df_temp, dom_keyword)

        df_drop["profile_id"]       = prof_id
        df_drop["campaign_id"]      = camp_id
        df_drop["campaign_name"]    = camp_name
        df_drop["ad_group_id"]      = adgroup_id
        df_drop["ad_group_name"]    = adgroup_name
        if (sp_vs_sb == "sp"):
            df_drop["state"]        = "ENABLED"
        df_drop["match_type"]       = "NEGATIVE_EXACT"
        df_drop["active"]           = hfilter.user_payment(temp_user, "keyword_active")
        df_drop["created_datetime"] = datetime.now()
        df_drop["updated_datetime"] = datetime.now()
        df_drop["saved"]            = round(np.sum(df_drop["associated_cost"]) * 30 / days, 2)
        if (sp_vs_sb == "sp"):
            df_drop = df_drop[["profile_id",
                               "campaign_id",
                               "campaign_name",
                               "ad_group_id",
                               "ad_group_name",
                               "state",
                               "keyword_text",
                               "match_type",
                               "associated_cost",
                               "associated_sales",
                               "associated_clicks",
                               "associated_orders",
                               "active",
                               "created_datetime",
                               "updated_datetime",
                               "saved"]]
        elif (sp_vs_sb == "sb"):
            df_drop = df_drop[["profile_id",
                               "campaign_id",
                               "campaign_name",
                               "ad_group_id",
                               "ad_group_name",
                               "keyword_text",
                               "match_type",
                               "associated_cost",
                               "associated_sales",
                               "associated_clicks",
                               "associated_orders",
                               "active",
                               "created_datetime",
                               "updated_datetime",
                               "saved"]]
        logger.info(df_drop)

        #drop previous data which are the same
        result = []
        for j in range(len(temp_keyword["keyword_text"])):
            for i in range(len(df_drop["keyword_text"])):
                if (df_drop["keyword_text"][i] == temp_keyword["keyword_text"][j]):
                    result.append(i)
        df_drop.drop(result, axis=0, inplace=True)
        df_drop.reset_index(drop=True, inplace=True)

        #keep 30 percents above (or equal)
        df_drop = df_drop[df_drop["associated_cost"] > 0.3 * df_drop["associated_sales"]]
        df_drop.reset_index(drop=True, inplace=True)
        print(df_drop)

        # database from pandas to sql
        hfilter.pandas_to_sql(df_drop,
                              database_ppcwiz,
                              3306,
                              "admin",
                              "ppcwiz",
                              "ppcwiz",
                              "utf8",
                              sp_vs_sb,
                              "neg_keyword")

        print("2. ACoS Filter")
        # 1-gram
        df_acos_1garm   = hfilter.n_grams(df_acos, 1)
        df_acos_explode = hfilter.explode_list(df_acos_1garm)
        df_acos_explode = hfilter.text_format_split(df_acos_explode)

        # delete duplicates
        df_acos_explode = df_acos_explode.groupby("match_txt_split").sum()
        df_acos_explode.reset_index(inplace=True)

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
        if (sp_vs_sb == "sp"):
            df_acos_comb["state"]        = "ENABLED"
        df_acos_comb["match_type"]       = "NEGATIVE_PHRASE"
        df_acos_comb["active"]           = hfilter.user_payment(temp_user, "acos_active")
        df_acos_comb["created_datetime"] = datetime.now()
        df_acos_comb["updated_datetime"] = datetime.now()
        df_acos_comb["saved"]            = round(np.sum(df_acos_comb["cost"]) * 30 / days, 2)
        #round(np.sum(df_acos_comb["cost"]) * 30 / days, 2)
        df_acos_comb.rename(columns={
            "match_txt_split": "keyword_text",
            "cost": "associated_cost",
            "sales": "associated_sales",
            "clicks": "associated_clicks",
            "orders": "associated_orders"
        }, inplace=True)
        if (sp_vs_sb == "sp"):
            df_acos_comb = df_acos_comb[["profile_id",
                                         "campaign_id",
                                         "campaign_name",
                                         "ad_group_id",
                                         "ad_group_name",
                                         "state",
                                         "keyword_text",
                                         "match_type",
                                         "associated_cost",
                                         "associated_sales",
                                         "associated_clicks",
                                         "associated_orders",
                                         "active",
                                         "created_datetime",
                                         "updated_datetime",
                                         "saved"]]
        elif (sp_vs_sb == "sb"):
            df_acos_comb = df_acos_comb[["profile_id",
                                         "campaign_id",
                                         "campaign_name",
                                         "ad_group_id",
                                         "ad_group_name",
                                         "keyword_text",
                                         "match_type",
                                         "associated_cost",
                                         "associated_sales",
                                         "associated_clicks",
                                         "associated_orders",
                                         "active",
                                         "created_datetime",
                                         "updated_datetime",
                                         "saved"]]
        logger.info(df_acos_comb)
        '''
        #drop previous data which are the same
        result = []
        for j in range(len(temp_acos["keyword_text"])):
            for i in range(len(df_acos_comb["keyword_text"])):
                if (df_acos_comb["keyword_text"][i] == temp_acos["keyword_text"][j]):
                    result.append(i)
        df_acos_comb.drop(result, axis=0, inplace=True)
        df_acos_comb.reset_index(drop=True, inplace=True)
        '''
        #drop previous data which are the same and domain keywords
        result = []
        result_key_txt = list(temp_acos["keyword_text"]) + dom_keyword
        for j in range(len(result_key_txt)):
            for i in range(len(df_acos_comb["keyword_text"])):
                if (df_acos_comb["keyword_text"][i] == result_key_txt[j]):
                    result.append(i)
                    print("!!!!!!!!!!!!!!!!!!!!!!!!")
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
                              sp_vs_sb,
                              "acos")
        
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
                                 keyword_report)
    '''
