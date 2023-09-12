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

from sp_api.api import Catalog
import time

import helper_filter as hfilter

import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)


def filter_asin_asin(product_ad_report, target_report, attributed_sales, attributed_units):
    database_ppcwiz   = os.getenv('DATABASE_URL')
    if ((target_report == "sp_target_report") or (target_report == "sp_keyword_report")):
        filter_asin = "sp_filter_asin"
        sp_vs_sd    = "sp"
    elif (target_report == "sd_target_report"):
        filter_asin = "sd_filter_asin"
        sp_vs_sd    = "sd"
    else:
        filter_asin = ""
        sp_vs_sd    = ""

    df = hfilter.sql_to_pandas(target_report,
                               database_ppcwiz,
                               3306,
                               "admin",
                               "ppcwiz",
                               "ppcwiz",
                               "utf8")

    if (sp_vs_sd == "sp"):
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

    # developer id info
    refresh_token     = os.getenv('REFRESH_TOKEN')
    lwa_client_id     = os.getenv('AMAZON_LWA_CLIENT_ID')
    lwa_client_secret = os.getenv('AMAZON_LWA_CLIENT_SECRET')

    aws_access_key    = os.getenv('AWS_ACCESS_KEY')
    aws_secret_key    = os.getenv('AWS_SECRET_KEY')
    role_arn          = os.getenv('ROLE_ARN')

    credentials = dict(
        refresh_token     = refresh_token,
        lwa_app_id        = lwa_client_id,
        lwa_client_secret = lwa_client_secret,
        aws_secret_key    = aws_secret_key,
        aws_access_key    = aws_access_key,
        role_arn          = role_arn,
    )

    df_ours = hfilter.sql_to_pandas_four(product_ad_report,
                                database_ppcwiz,
                                3306,
                                "admin",
                                "ppcwiz",
                                "ppcwiz",
                                "utf8")

    logger.info("start s*_sin")
    set_adgroup = set(df_begin["ad_group_id"])
    for i in set_adgroup:
        print("---------------------------------------")
        df = df_begin.copy()
        df_o = df_ours.copy()

        # specific adgroup id
        adgroup_id = i
        df = df[df["ad_group_id"] == adgroup_id]
        df_o = df_o[df_o["ad_group_id"] == adgroup_id]
        df.reset_index(drop=True, inplace=True)
        df_o.reset_index(drop=True, inplace=True)

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
        temp_asin = hfilter.sql_to_pandas_compare(filter_asin,
                                                     "ad_group_id, expression",
                                                     "ad_group_id = " + str(adgroup_id),
                                                     database_ppcwiz,
                                                     3306,
                                                     "admin",
                                                     "ppcwiz",
                                                     "ppcwiz",
                                                     "utf8")

        try:
            # our product info
            ASIN_CUSTOMERS = df_o["asin"][0].upper()
            res_ours = Catalog(credentials=credentials).get_item(asin=ASIN_CUSTOMERS)
            #res_ours.payload["SalesRankings"]
            time.sleep(2)
            cate_ours, rank_ours = hfilter.category_id_and_rank(res_ours)

            df = df.groupby("asin").sum()
            df.reset_index(inplace=True)

            # expense saved
            clicks_above = 0

            df_drop = pd.DataFrame({"expression": [], "associated_cost": [], "associated_sales": [], "associated_clicks": [], "associated_orders": []})
            for j in range(df["asin"].shape[0]):#asin_target:
                ASIN_ADVERTISE = df["asin"][j].upper()
                if (ASIN_ADVERTISE == ASIN_CUSTOMERS):
                    continue
                print("---------------------------------------")
                print("Our Customer:", ASIN_CUSTOMERS)
                print("Targeting Ad:", ASIN_ADVERTISE)

                # target product info
                res_trgt = Catalog(credentials=credentials).get_item(asin=ASIN_ADVERTISE)
                #res_trgt.payload["SalesRankings"]

                cate_trgt, rank_trgt = hfilter.category_id_and_rank(res_trgt)

                if (len(res_ours.payload) > 2) and (len(res_trgt.payload) > 2):
                    cate = cate_ours.intersection(cate_trgt)
                    if (len(cate) == 0):
                        df_drop = df_drop.append({
                            "expression": ASIN_ADVERTISE,
                            "associated_cost": df["cost"][j],
                            "associated_sales": df["sales"][j],
                            "associated_clicks": df["clicks"][j],
                            "associated_orders": df["orders"][j]
                        },
                        ignore_index=True)

                        # expense saved
                        if (df["clicks"][j] == 0):
                            clicks_above += 0.5

            # expense saved
            expense_saved = round(np.sum(df_drop["associated_cost"]) * 30 / days + clicks_above, 2)# + clicks_above

            df_drop["profile_id"]       = prof_id
            df_drop["campaign_id"]      = camp_id
            df_drop["campaign_name"]    = camp_name
            df_drop["ad_group_id"]      = adgroup_id
            df_drop["ad_group_name"]    = adgroup_name
            df_drop["state"]            = "ENABLED"
            df_drop["expression_type"]  = "MANUAL"
            df_drop["active"]           = hfilter.user_payment(temp_user, "asin_active")
            df_drop["created_datetime"] = datetime.now()
            df_drop["updated_datetime"] = datetime.now()
            df_drop["saved"]            = expense_saved
            df_drop = df_drop[["profile_id",
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
            logger.info(df_drop)

            #drop previous data which are the same
            result = []
            for j in range(len(temp_asin["expression"])):
                for i in range(len(df_drop["expression"])):
                    if (df_drop["expression"][i] == temp_asin["expression"][j]):
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
                                  sp_vs_sd,
                                  "asin")

        except:
            continue
        print("---------------------------------------")
        print("")
    '''
    # database from pandas to sql (update)
    hfilter.pandas_to_sql_update(df_trgt,
                                 database_ppcwiz,
                                 3306,
                                 "admin",
                                 "ppcwiz",
                                 "ppcwiz",
                                 "utf8",
                                 target_report)

    # database from pandas to sql (update)
    hfilter.pandas_to_sql_update(df_prod_ad,
                                 database_ppcwiz,
                                 3306,
                                 "admin",
                                 "ppcwiz",
                                 "ppcwiz",
                                 "utf8",
                                 product_ad_report)
    '''
    '''
    # database from pandas to sql (2 updates at the same time)
    hfilter.pandas_to_sql_update2(df_trgt,
                                  df_prod_ad,
                                  database_ppcwiz,
                                  3306,
                                  "admin",
                                  "ppcwiz",
                                  "ppcwiz",
                                  "utf8",
                                  target_report,
                                  product_ad_report)
    '''
