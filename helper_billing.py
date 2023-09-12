#!python -m spacy download en_core_web_sm
import numpy as np
import pandas as pd
import spacy
import pymysql
import requests
from nltk.util import ngrams, everygrams
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from PyDictionary import PyDictionary
from datetime import datetime
from datetime import timedelta


# database from sql to pandas (basic)
def sql_to_pandas_basic(searchterm, host, port, user, pw, db, char):
    conn  = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    query = "SELECT * FROM " + searchterm
    df    = pd.read_sql_query(query, conn)
    conn.close()
    return df

# database from sql to pandas (compare)
def sql_to_pandas_compare(term, term_sub, term_sub_sub, host, port, user, pw, db, char):
    conn  = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    query = "SELECT " + term_sub + " FROM " + term + " WHERE " + term_sub_sub
    df    = pd.read_sql_query(query, conn)
    conn.close()
    return df

# database from pandas to sql (expiry: profile)
def pandas_to_sql_expiry_profile(user_id, df, host, port, user, pw, db, char, searchterm):
    conn = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    curs = conn.cursor()
    sql  = "UPDATE " + searchterm + " SET start_billing_date = %s, stripe_id = %s, stripe_type = %s, stripe_last4 = %s, stripe_expiry = %s WHERE user_id = %s"

    val  = []
    for _ in range(df.shape[0]):
        val.append([None, None, None, None, None, user_id])

    curs.executemany(sql, val)
    conn.commit()
    conn.close()
    return

# database from pandas to sql (expiry: user)
def pandas_to_sql_expiry_user(user_id, df, host, port, user, pw, db, char, searchterm):
    conn = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    curs = conn.cursor()
    sql  = "UPDATE " + searchterm + " SET asin_active = %s, acos_active = %s WHERE id = %s"

    val  = []
    for _ in range(df.shape[0]):
        val.append([0, 0, user_id])

    curs.executemany(sql, val)
    conn.commit()
    conn.close()
    return

# database from pandas to sql (billing update)
def pandas_to_sql_billing_update(user_id, df, host, port, user, pw, db, char, searchterm):
    conn = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    curs = conn.cursor()
    sql  = "SET @min_id = (SELECT MIN(id) FROM payment WHERE billing = 1 AND user_id = %s)"
    curs.execute(sql, [user_id])
    sql  = "SET @max_id = (SELECT MAX(id) FROM payment WHERE billing = 2 AND user_id = %s)"
    curs.execute(sql, [user_id])
    sql  = "UPDATE " + searchterm + " SET billing = %s WHERE id >= @min_id AND id <= @max_id AND user_id = %s"

    val  = []
    for _ in range(df.shape[0]):
        val.append([0, user_id])

    curs.executemany(sql, val)
    conn.commit()
    conn.close()
    return

# database from pandas to sql (update)
def pandas_to_sql_update(df, host, port, user, pw, db, char, searchterm):
    conn = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    curs = conn.cursor()
    sql  = "UPDATE " + searchterm + " SET billing_active = %s WHERE billing_active = %s"

    val  = []
    for _ in range(df.shape[0]):
        val.append([0, 1])

    curs.executemany(sql, val)
    conn.commit()
    conn.close()
    return

# database from pandas to sql (four updates)
def pandas_to_sql_update4(df1, df2, df3, df4, host, port, user, pw, db, char, term1, term2, term3, term4):
    pandas_to_sql_update(df1, host, port, user, pw, db, char, term1)
    pandas_to_sql_update(df2, host, port, user, pw, db, char, term2)
    pandas_to_sql_update(df3, host, port, user, pw, db, char, term3)
    pandas_to_sql_update(df4, host, port, user, pw, db, char, term4)
    return

# database from pandas to sql (payment)
def pandas_to_sql_payment(val, host, port, user, pw, db, char):
    conn = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    curs = conn.cursor()
    sql = """INSERT INTO payment(billing,
                                 keyword_sales,
                                 acos_sales,
                                 asin_sales,
                                 billing_date,
                                 user_id,
                                 profile_id)
             VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    curs.executemany(sql, val)
    conn.commit()
    conn.close()
    return

# sales > 0
def total_sales(df, sale):
    total_sales = 0.0
    for i in range(df.shape[0]):
        if (df[sale][i] > 0):
            total_sales += df[sale][i]
    return 0.015 * total_sales

# sales = 0
def zeros_sales(df, sale):
    count_zeros = 0.0
    for i in range(df.shape[0]):
        if (df[sale][i] == 0):
            count_zeros += 1
    return 0.1 * count_zeros

# date error with 2/29, 2/30, 2/31, 4/31, 6/31, 9/31, and 11/31
def date_error(date):
    month_this = datetime.now().month
    month_next = (datetime.now() + timedelta(days=1)).month
    if (date == 29):
        if (month_this == 2 and month_next == 3):
            day_this = datetime.now().day
            if (day_this == 28):
                return day_this
    elif (date == 30):
        if (month_this == 2 and month_next == 3):
            day_this = datetime.now().day
            if (day_this == 28 or day_this == 29):
                return day_this
    elif (date == 31):
        if (month_this == 2 and month_next == 3):
            day_this = datetime.now().day
            if (day_this == 28 or day_this == 29):
                return day_this
        if (month_this == 4 and month_next == 5):
            return 30
        elif (month_this == 6 and month_next == 7):
            return 30
        elif (month_this == 9 and month_next == 10):
            return 30
        elif (month_this == 11 and month_next == 12):
            return 30
    return date

# payment date
def payment_date(date):
    if (datetime.now().day == date):
        return 2
    return 1

# send completed email
def send_complete(email, type, asin, acos, total, last4, temp_url):
    r = requests.post(url=temp_url, headers={
            'Content-Type': 'application/json'
        }, json={
            "email": email,
            "stripe_type": type,
            "paid": {
                "asin": asin,
                "acos": acos,
                "total": total
            },
            "card_last4": last4
        })

# send failed email
def send_fail(email, last4, expiry, temp_url):
    r = requests.post(url=temp_url, headers={
            'Content-Type': 'application/json'
        }, json={
            "email": email,
            "card_last4": last4,
            "card_expiry": expiry
        })

# send expiry email
def send_expiry(email, name, last4, temp_url):
    r = requests.post(url=temp_url, headers={
            'Content-Type': 'application/json'
        }, json={
            "email": email,
            "username": name,
            "card_last4": last4
        })

# coming expiry
def expiry_email(m, df_profile, df_user, user_id, email, name, last4, temp_url, host, port, user, pw, db, char, term1, term2):
    index_slash  = m.index('/')
    expiry_year  = int(m[index_slash + 2:])  # 2024 from 8 / 2024
    today_year   = datetime.now().year
    if (expiry_year == today_year):
        expiry_month = int(m[:index_slash - 1])  # 8 from 8 / 2024
        today_day    = datetime.now().day
        today_month  = datetime.now().month
        if (expiry_month == today_month):
            if (today_day == 1):
                send_expiry(email, name, last4, temp_url)
            elif (today_day == 25):
                pandas_to_sql_expiry_profile(user_id, df_profile, host, port, user, pw, db, char, term1)
                pandas_to_sql_expiry_user(user_id, df_user, host, port, user, pw, db, char, term2)
