import numpy as np
import pandas as pd
import stripe
import uuid
import pymysql
import warnings
warnings.filterwarnings("ignore")

from datetime import datetime
from datetime import timedelta
from loguru import logger

import helper_filter as hfilter
import helper_billing as hbilling

import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)


def sp_sb_sd_payment():
    database_ppcwiz  = os.getenv('DATABASE_URL')
    stripe.api_key   = os.getenv('STRIPE_API')
    mail_server      = os.getenv('MAIL_SERVER')

    df_profile = hbilling.sql_to_pandas_compare("dtb_profile",
                                                "*",
                                                "start_billing_date IS NOT NULL",
                                                database_ppcwiz,
                                                3306,
                                                "admin",
                                                "ppcwiz",
                                                "ppcwiz",
                                                "utf8")
    '''
    df_currency = hfilter.sql_to_pandas_basic("dtb_profile",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")
    '''
    df_currency = df_profile.copy()

    df_temp1 = hbilling.sql_to_pandas_compare("sp_keyword_report",
                                             "date, profile_id, attributed_sales_7d, active",
                                             "active = 1",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")
    df_temp1 = hfilter.currency_code(df_temp1, df_currency)
    df_temp1 = hfilter.exchange_rate_sale(df_temp1, "attributed_sales_7d")

    df_temp2 = hbilling.sql_to_pandas_compare("sb_keyword_report",
                                             "date, profile_id, attributed_sales_14d, active",
                                             "active = 1",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")
    df_temp2 = hfilter.currency_code(df_temp2, df_currency)
    df_temp2 = hfilter.exchange_rate_sale(df_temp2, "attributed_sales_14d")

    df_temp3 = hbilling.sql_to_pandas_compare("sd_target_report",
                                             "date, profile_id, attributed_sales_30d, active",
                                             "active = 1",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")
    df_temp3 = hfilter.currency_code(df_temp3, df_currency)
    df_temp3 = hfilter.exchange_rate_sale(df_temp3, "attributed_sales_30d")

    df_temp4 = hbilling.sql_to_pandas_compare("sp_target_report",
                                             "date, profile_id, attributed_sales_7d, active",
                                             "active = 1",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")
    df_temp4 = hfilter.currency_code(df_temp4, df_currency)
    df_temp4 = hfilter.exchange_rate_sale(df_temp4, "attributed_sales_7d")

    df_temp5 = hbilling.sql_to_pandas_compare("sb_target_report",
                                             "date, profile_id, attributed_sales_14d, active",
                                             "active = 1",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")
    df_temp5 = hfilter.currency_code(df_temp5, df_currency)
    df_temp5 = hfilter.exchange_rate_sale(df_temp5, "attributed_sales_14d")

    df_filter_sp_acos = hbilling.sql_to_pandas_compare("sp_filter_acos",
                                             "profile_id, associated_sales, billing_active",
                                             "billing_active = 1 AND active != 3",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")

    df_filter_sb_acos = hbilling.sql_to_pandas_compare("sb_filter_acos",
                                             "profile_id, associated_sales, billing_active",
                                             "billing_active = 1 AND active != 3",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")

    df_filter_sd_acos = hbilling.sql_to_pandas_compare("sd_filter_acos",
                                             "profile_id, associated_sales, billing_active",
                                             "billing_active = 1 AND active != 3",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")

    df_filter_sp_asin = hbilling.sql_to_pandas_compare("sp_filter_asin",
                                             "profile_id, associated_sales, billing_active",
                                             "billing_active = 1 AND active != 3",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")

    df_payment = hbilling.sql_to_pandas_compare("payment",
                                             "*",
                                             "billing = 1 OR billing = 2",
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")

    val = []
    for i in range(df_profile["user_id"].shape[0]):
        # profile id and start billing date (dtb_profile)
        profile_id = df_profile["id"][i]
        start_date = df_profile["start_billing_date"][i]
        #is_stripe  = df_profile["stripe_id"][i]

        # skip the first day (60 days)
        if (datetime.today().date() <= start_date + timedelta(days=2)):
            continue

        # profile id (keyword_report and target_report)
        df_sp_kwrd = df_temp1[df_temp1["profile_id"] == profile_id]  #1
        df_sb_kwrd = df_temp2[df_temp2["profile_id"] == profile_id]  #2
        df_sd_trgt = df_temp3[df_temp3["profile_id"] == profile_id]  #3
        df_sp_trgt = df_temp4[df_temp4["profile_id"] == profile_id]  #4
        df_sb_trgt = df_temp5[df_temp5["profile_id"] == profile_id]  #5

        df_sp_kwrd.reset_index(drop=True, inplace=True)
        df_sb_kwrd.reset_index(drop=True, inplace=True)
        df_sd_trgt.reset_index(drop=True, inplace=True)
        df_sp_trgt.reset_index(drop=True, inplace=True)
        df_sb_trgt.reset_index(drop=True, inplace=True)

        '''
        # acos (sales > 0)
        acos_temp1 = hbilling.total_sales(df_sp_kwrd, "attributed_sales_7d")   #1
        acos_temp2 = hbilling.total_sales(df_sb_kwrd, "attributed_sales_14d")  #2
        acos_temp3 = hbilling.total_sales(df_sd_trgt, "attributed_sales_30d")  #3

        # asin (sales > 0)
        asin_temp1 = hbilling.total_sales(df_sp_trgt, "attributed_sales_7d")   #4
        '''

        # sp_filter_key_acos (1, 2)  #1
        # sb_filter_key_acos (3, 4)  #2
        # sp_filter_asin     (5)     #4
        # sd_filter_acos     (6)     #3

        # total acos (sales > 0 and sales = 0)
        total_acos = 0.0
        #if (df_filter_sp_acos.shape[0] > 0):
        total_acos += hbilling.total_sales(df_sp_kwrd, "attributed_sales_7d")   #1
        #total_acos += hbilling.zeros_sales(df_filter_sp_acos, "associated_sales")
        #if (df_filter_sb_acos.shape[0] > 0):
        total_acos += hbilling.total_sales(df_sb_kwrd, "attributed_sales_14d")  #2
        #total_acos += hbilling.zeros_sales(df_filter_sb_acos, "associated_sales")
        #if (df_filter_sd_acos.shape[0] > 0):
        total_acos += hbilling.total_sales(df_sd_trgt, "attributed_sales_30d")  #3
        #total_acos += hbilling.zeros_sales(df_filter_sd_acos, "associated_sales")

        total_acos += hbilling.total_sales(df_sb_trgt, "attributed_sales_14d")  #5

        # total asin (sales > 0 and sales = 0)
        #total_asin = 0.0
        #if (df_filter_sp_asin.shape[0] > 0):
        total_acos += hbilling.total_sales(df_sp_trgt, "attributed_sales_7d")   #4
        #total_acos += hbilling.zeros_sales(df_filter_sp_asin, "associated_sales")

        #
        df_user = hbilling.sql_to_pandas_compare("dtb_user",
                                             "name, email, asin_active, acos_active",
                                             "id = " + str(df_profile["user_id"][i]),
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")
        # to database
        '''
        if (is_stripe == None):
            billing   = 1
        else:
            billing   = hbilling.payment_date(hbilling.date_error(start_date))
        '''
        billing       = hbilling.payment_date(hbilling.date_error(start_date.day))
        keyword_sales = 0
        acos_sales    = 0.0
        if (df_user["acos_active"][0] == 1):
            print("!!!!!!!!!!")
            acos_sales = round(total_acos, 2)
        asin_sales    = 0.0
        if (df_user["asin_active"][0] == 1):
            print("!!!!!!!!!!")
            asin_sales = round(total_acos, 2)
        billing_date  = datetime.now()
        user_id       = df_profile["user_id"][i]

        temp = [billing, keyword_sales, acos_sales, asin_sales, billing_date, user_id, profile_id]
        val.append(temp)
        #print(val)
        logger.info(val)

    # database from pandas to sql (payment)
    hbilling.pandas_to_sql_payment(val,
                                   database_ppcwiz,
                                   3306,
                                   "admin",
                                   "ppcwiz",
                                   "ppcwiz",
                                   "utf8")


    # database from pandas to sql (update)
    hfilter.pandas_to_sql_update5(df_temp1,
                                  df_temp2,
                                  df_temp3,
                                  df_temp4,
                                  df_temp5,
                                  database_ppcwiz,
                                  3306,
                                  "admin",
                                  "ppcwiz",
                                  "ppcwiz",
                                  "utf8",
                                  "sp_keyword_report",
                                  "sb_keyword_report",
                                  "sd_target_report",
                                  "sp_target_report",
                                  "sb_target_report")

    # database from pandas to sql (update)
    hbilling.pandas_to_sql_update4(df_filter_sp_acos,
                                   df_filter_sb_acos,
                                   df_filter_sd_acos,
                                   df_filter_sp_asin,
                                   database_ppcwiz,
                                   3306,
                                   "admin",
                                   "ppcwiz",
                                   "ppcwiz",
                                   "utf8",
                                   "sp_filter_acos",
                                   "sb_filter_acos",
                                   "sd_filter_acos",
                                   "sp_filter_asin")

    '''
    set_id     = set(df_profile["user_id"])
    set_stripe = set(df_profile["stripe_id"])
    set_last4  = set(df_profile["stripe_last4"])
    set_expiry = set(df_profile["stripe_expiry"])
    ext_type   = set(df_profile["stripe_type"])
    for j, k, l, m, n in zip(set_id, set_stripe, set_last4, set_expiry, ext_type):
    '''
    df_profile_drop = df_profile.drop_duplicates(subset = "user_id")
    df_profile_drop.reset_index(drop=True, inplace=True)
    for z in range(len(df_profile_drop)):
        j = df_profile_drop["user_id"][z]
        k = df_profile_drop["stripe_id"][z]
        l = df_profile_drop["stripe_last4"][z]
        m = df_profile_drop["stripe_expiry"][z]
        n = df_profile_drop["stripe_type"][z]

        # send email, check whether asin_active = 0 and acos_active = 0 or not
        df_user = hbilling.sql_to_pandas_compare("dtb_user",
                                             "name, email, asin_active, acos_active",
                                             "id = " + str(j),
                                             database_ppcwiz,
                                             3306,
                                             "admin",
                                             "ppcwiz",
                                             "ppcwiz",
                                             "utf8")

        # not with stripe (payment) yet
        if (k == None):
            hbilling.send_fail(df_user["email"][0], "", "", mail_server + '/mail/payments/failed')
            continue

        # user_id
        df = df_payment[df_payment["user_id"] == j]
        df.reset_index(drop=True, inplace=True)

        # is it a month?
        is_month = df[df["billing"] == 2]
        if (is_month.shape[0] == 0):
            continue

        # payment from 1 to 2, not 0
        temp = pd.DataFrame({"billing": [], "acos_sales": [], "asin_sales": [], "user_id": []})
        for i in range(df.shape[0]):
            if (i == 0):
                temp = temp.append({"billing": df["billing"][i],
                                    "acos_sales": df["acos_sales"][i],
                                    "asin_sales": df["asin_sales"][i],
                                    "user_id": df["user_id"][i]},
                                  ignore_index=True)
                continue

            # from 1 to 2, not from 2 to 1
            if (df["billing"][i - 1] > df["billing"][i]):
                break
            else:
                temp = temp.append({"billing": df["billing"][i],
                                    "acos_sales": df["acos_sales"][i],
                                    "asin_sales": df["asin_sales"][i],
                                    "user_id": df["user_id"][i]},
                                  ignore_index=True)
        #print(temp)
        logger.info(temp)

        # total payment (per month)
        amount_acos = np.sum(temp["acos_sales"])
        amount_asin = np.sum(temp["asin_sales"])
        db_payment  = round(amount_acos + amount_asin, 2)
        print("User ID", j, end="")
        print(":", db_payment)

        # stripe (payment)
        try:
            stripe_pay = int(db_payment * 100)
            stripe.Charge.create(
                amount=stripe_pay,
                currency="usd",
                description="User ID " + str(j),
                idempotency_key=uuid.uuid4().hex,
                customer=k
            )

            # coming expiry
            hbilling.expiry_email(m,
                                  df_profile,
                                  df_user,
                                  j,
                                  df_user["email"][0],
                                  df_user["name"][0],
                                  l,
                                  mail_server + '/mail/cards/expiry',
                                  database_ppcwiz,
                                  3306,
                                  "admin",
                                  "ppcwiz",
                                  "ppcwiz",
                                  "utf8",
                                  "dtb_profile",
                                  "dtb_user")

            # database from pandas to sql (billing update)
            hbilling.pandas_to_sql_billing_update(j,
                                                  temp,
                                                  database_ppcwiz,
                                                  3306,
                                                  "admin",
                                                  "ppcwiz",
                                                  "ppcwiz",
                                                  "utf8",
                                                  "payment")

            # completed notice
            hbilling.send_complete(df_user["email"][0],
                                   n,
                                   amount_asin,
                                   amount_acos,
                                   db_payment,
                                   l,
                                   mail_server + '/mail/payments/completed')
        except:
            hbilling.send_fail(df_user["email"][0], l, m, mail_server + '/mail/payments/failed')
        '''
        except stripe.error.CardError as e:
            print("************************************************")
            # Since it's a decline, stripe.error.CardError will be caught
            print("Status is:", e.http_status)
            print("Code is:", e.code)
            # param is '' in this case
            print("Param is:", e.param)
            print("Message is:", e.user_message)
            print("************************************************")
            hbilling.send_fail(df_user["email"][0], l, m, mail_server + '/mail/payments/failed')
        except stripe.error.RateLimitError as e:
            print("************************************************")
            print("*** Too many requests made to the API        ***")
            print("************************************************")
        except stripe.error.InvalidRequestError as e:
            print("************************************************")
            print("*** Invalid parameters were supplied to API  ***")
            print("************************************************")
            hbilling.send_fail(df_user["email"][0], l, m, mail_server + '/mail/payments/failed')
        except stripe.error.AuthenticationError as e:
            print("************************************************")
            print("*** Authentication with Stripe's API failed  ***")
            print("*** (maybe you changed API keys recently)    ***")
            print("************************************************")
        except stripe.error.APIConnectionError as e:
            print("************************************************")
            print("*** Network communication with Stripe failed ***")
            print("************************************************")
        except stripe.error.StripeError as e:
            print("************************************************")
            print("*** Display a very generic error to the user ***")
            print("************************************************")
        except Exception as e:
            print("************************************************")
            print("*** Something (unrelated to Stripe) happened ***")
            print("************************************************")
            hbilling.send_fail(df_user["email"][0], l, m, mail_server + '/mail/payments/failed')
        '''
