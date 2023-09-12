#!python -m spacy download en_core_web_sm
import numpy as np
import pandas as pd
import spacy
import pymysql

import nltk
nltk.download('stopwords')
nltk.download('wordnet')
from nltk.util import ngrams, everygrams
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from PyDictionary import PyDictionary
from currency_converter import CurrencyConverter

#####################
### Exchange Rate ###
#####################

# currency
def currency_code(df_begin, df_currency):
    df_begin["currency_code"] = "USD"
    df_begin["user_id"] = 0
    for i in range(df_begin.shape[0]):
        for j in range(df_currency.shape[0]):
            if (df_begin["profile_id"][i] == df_currency["id"][j]):
                df_begin["user_id"][i] = df_currency["user_id"][j]
                df_begin["currency_code"][i] = df_currency["currency_code"][j]
    return df_begin

# exchange rate
def exchange_rate(df_begin, cost, sale):
    c = CurrencyConverter(fallback_on_wrong_date=True, fallback_on_missing_rate=True)
    for i in range(df_begin.shape[0]):
        date = df_begin["date"][i]
        code = df_begin["currency_code"][i]
        if (code != 'USD'):
            df_begin[cost][i] = c.convert(df_begin[cost][i], code, 'USD', date=date)
            df_begin[sale][i] = c.convert(df_begin[sale][i], code, 'USD', date=date)
    return df_begin

# exchange rate
def exchange_rate_sale(df_begin, sale):
    c = CurrencyConverter(fallback_on_wrong_date=True, fallback_on_missing_rate=True)
    for i in range(df_begin.shape[0]):
        date = df_begin["date"][i]
        code = df_begin["currency_code"][i]
        if (code != 'USD'):
            df_begin[sale][i] = c.convert(df_begin[sale][i], code, 'USD', date=date)
    return df_begin



#################################
### Keyword Validation Filter ###
#################################
'''
class KeywordValidationFilter(pd.DataFrame):
    pass

    def __init__(self, searchterm, host, port, user, pw, db, char, ad_grp_id):
        self.searchterm = searchterm
        self.host       = host
        self.port       = port
        self.user       = user
        self.pw         = pw
        self.db         = db
        self.char       = char
        self.ad_grp_id  = ad_grp_id
'''

# database from sql to pandas (basic)
def sql_to_pandas_basic(searchterm, host, port, user, pw, db, char):
    conn  = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    query = "SELECT * FROM " + searchterm
    df    = pd.read_sql_query(query, conn)
    conn.close()
    return df

# database from sql to pandas (58 days)
def sql_to_pandas(searchterm, host, port, user, pw, db, char):
    conn  = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    query = "SELECT * FROM " + searchterm + " WHERE date BETWEEN ADDDATE(DATE(NOW()), -60) AND ADDDATE(DATE(NOW()), -3)"
    df    = pd.read_sql_query(query, conn)
    conn.close()
    return df

# database from sql to pandas (4 days)
def sql_to_pandas_four(searchterm, host, port, user, pw, db, char):
    conn  = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    query = "SELECT * FROM " + searchterm + " WHERE date BETWEEN ADDDATE(DATE(NOW()), -4) AND ADDDATE(DATE(NOW()), -3)"
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

# database from pandas to sql (update)
def pandas_to_sql_update(df, host, port, user, pw, db, char, searchterm):
    conn = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    curs = conn.cursor()
    sql  = "UPDATE " + searchterm + " SET active = %s WHERE active = %s"

    val  = []
    for _ in range(df.shape[0]):
        val.append([0, 1])

    curs.executemany(sql, val)
    conn.commit()
    conn.close()
    return

# database from pandas to sql (two updates)
def pandas_to_sql_update2(df1, df2, host, port, user, pw, db, char, term1, term2):
    pandas_to_sql_update(df1, host, port, user, pw, db, char, term1)
    pandas_to_sql_update(df2, host, port, user, pw, db, char, term2)
    return

# database from pandas to sql (four updates)
def pandas_to_sql_update4(df1, df2, df3, df4, host, port, user, pw, db, char, term1, term2, term3, term4):
    pandas_to_sql_update(df1, host, port, user, pw, db, char, term1)
    pandas_to_sql_update(df2, host, port, user, pw, db, char, term2)
    pandas_to_sql_update(df3, host, port, user, pw, db, char, term3)
    pandas_to_sql_update(df4, host, port, user, pw, db, char, term4)
    return

# database from pandas to sql (five updates)
def pandas_to_sql_update5(df1, df2, df3, df4, df5, host, port, user, pw, db, char, term1, term2, term3, term4, term5):
    pandas_to_sql_update(df1, host, port, user, pw, db, char, term1)
    pandas_to_sql_update(df2, host, port, user, pw, db, char, term2)
    pandas_to_sql_update(df3, host, port, user, pw, db, char, term3)
    pandas_to_sql_update(df4, host, port, user, pw, db, char, term4)
    pandas_to_sql_update(df5, host, port, user, pw, db, char, term5)
    return

# database from pandas to sql
def pandas_to_sql(df, host, port, user, pw, db, char, term, term_sub):
    conn = pymysql.connect(host=host, port=port, user=user, password=pw, db=db, charset=char)
    curs = conn.cursor()
    if (term == "sp"):
        if (term_sub == "asin"):
            sql = """INSERT INTO sp_filter_""" + term_sub + """(profile_id,
                                                                campaign_id,
                                                                campaign_name,
                                                                ad_group_id,
                                                                ad_group_name,
                                                                state,
                                                                expression,
                                                                expression_type,
                                                                associated_cost,
                                                                associated_sales,
                                                                associated_clicks,
                                                                associated_orders,
                                                                active,
                                                                created_datetime,
                                                                updated_datetime,
                                                                saved)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            val = []
            for i in range(df.shape[0]):
                temp = [df["profile_id"][i],
                        df["campaign_id"][i],
                        df["campaign_name"][i],
                        df["ad_group_id"][i],
                        df["ad_group_name"][i],
                        df["state"][i],
                        df["expression"][i],
                        df["expression_type"][i],
                        df["associated_cost"][i],
                        df["associated_sales"][i],
                        df["associated_clicks"][i],
                        df["associated_orders"][i],
                        df["active"][i],
                        df["created_datetime"][i],
                        df["updated_datetime"][i],
                        df["saved"][i]]
                val.append(temp)
        else:
            sql = """INSERT INTO sp_filter_""" + term_sub + """(profile_id,
                                                                campaign_id,
                                                                campaign_name,
                                                                ad_group_id,
                                                                ad_group_name,
                                                                state,
                                                                keyword_text,
                                                                match_type,
                                                                associated_cost,
                                                                associated_sales,
                                                                associated_clicks,
                                                                associated_orders,
                                                                active,
                                                                created_datetime,
                                                                updated_datetime,
                                                                saved)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            val = []
            for i in range(df.shape[0]):
                temp = [df["profile_id"][i],
                        df["campaign_id"][i],
                        df["campaign_name"][i],
                        df["ad_group_id"][i],
                        df["ad_group_name"][i],
                        df["state"][i],
                        df["keyword_text"][i],
                        df["match_type"][i],
                        df["associated_cost"][i],
                        df["associated_sales"][i],
                        df["associated_clicks"][i],
                        df["associated_orders"][i],
                        df["active"][i],
                        df["created_datetime"][i],
                        df["updated_datetime"][i],
                        df["saved"][i]]
                val.append(temp)
    elif (term == "sb"):
        if (term_sub == "asin"):
            sql = """INSERT INTO sb_filter_""" + term_sub + """(profile_id,
                                                                campaign_id,
                                                                campaign_name,
                                                                ad_group_id,
                                                                ad_group_name,
                                                                expression,
                                                                expression_type,
                                                                associated_cost,
                                                                associated_sales,
                                                                associated_clicks,
                                                                associated_orders,
                                                                active,
                                                                created_datetime,
                                                                updated_datetime,
                                                                saved)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            val = []
            for i in range(df.shape[0]):
                temp = [df["profile_id"][i],
                        df["campaign_id"][i],
                        df["campaign_name"][i],
                        df["ad_group_id"][i],
                        df["ad_group_name"][i],
                        df["expression"][i],
                        df["expression_type"][i],
                        df["associated_cost"][i],
                        df["associated_sales"][i],
                        df["associated_clicks"][i],
                        df["associated_orders"][i],
                        df["active"][i],
                        df["created_datetime"][i],
                        df["updated_datetime"][i],
                        df["saved"][i]]
                val.append(temp)
        else:
            sql = """INSERT INTO sb_filter_""" + term_sub + """(profile_id,
                                                                campaign_id,
                                                                campaign_name,
                                                                ad_group_id,
                                                                ad_group_name,
                                                                keyword_text,
                                                                match_type,
                                                                associated_cost,
                                                                associated_sales,
                                                                associated_clicks,
                                                                associated_orders,
                                                                active,
                                                                created_datetime,
                                                                updated_datetime,
                                                                saved)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            val = []
            for i in range(df.shape[0]):
                temp = [df["profile_id"][i],
                        df["campaign_id"][i],
                        df["campaign_name"][i],
                        df["ad_group_id"][i],
                        df["ad_group_name"][i],
                        df["keyword_text"][i],
                        df["match_type"][i],
                        df["associated_cost"][i],
                        df["associated_sales"][i],
                        df["associated_clicks"][i],
                        df["associated_orders"][i],
                        df["active"][i],
                        df["created_datetime"][i],
                        df["updated_datetime"][i],
                        df["saved"][i]]
                val.append(temp)
    elif (term == "sd"):
        if (term_sub == "acos" or term_sub == "asin"):
            sql = """INSERT INTO sd_filter_""" + term_sub + """(profile_id,
                                                                campaign_id,
                                                                campaign_name,
                                                                ad_group_id,
                                                                ad_group_name,
                                                                state,
                                                                expression,
                                                                expression_type,
                                                                associated_cost,
                                                                associated_sales,
                                                                associated_clicks,
                                                                associated_orders,
                                                                active,
                                                                created_datetime,
                                                                updated_datetime,
                                                                saved)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            val = []
            for i in range(df.shape[0]):
                temp = [df["profile_id"][i],
                        df["campaign_id"][i],
                        df["campaign_name"][i],
                        df["ad_group_id"][i],
                        df["ad_group_name"][i],
                        df["state"][i],
                        df["expression"][i],
                        df["expression_type"][i],
                        df["associated_cost"][i],
                        df["associated_sales"][i],
                        df["associated_clicks"][i],
                        df["associated_orders"][i],
                        df["active"][i],
                        df["created_datetime"][i],
                        df["updated_datetime"][i],
                        df["saved"][i]]
                val.append(temp)
        else:
            sql = """INSERT INTO sd_filter_""" + term_sub + """(profile_id,
                                                                campaign_id,
                                                                campaign_name,
                                                                ad_group_id,
                                                                ad_group_name,
                                                                state,
                                                                expression,
                                                                expression_type,
                                                                associated_cost,
                                                                associated_sales,
                                                                associated_clicks,
                                                                associated_orders,
                                                                active,
                                                                created_datetime,
                                                                updated_datetime,
                                                                saved)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            val = []
            for i in range(df.shape[0]):
                temp = [df["profile_id"][i],
                        df["campaign_id"][i],
                        df["campaign_name"][i],
                        df["ad_group_id"][i],
                        df["ad_group_name"][i],
                        df["state"][i],
                        df["expression"][i],
                        df["expression_type"][i],
                        df["associated_cost"][i],
                        df["associated_sales"][i],
                        df["associated_clicks"][i],
                        df["associated_orders"][i],
                        df["active"][i],
                        df["created_datetime"][i],
                        df["updated_datetime"][i],
                        df["saved"][i]]
                val.append(temp)

    curs.executemany(sql, val)
    conn.commit()
    conn.close()
    return

# dtb_user and payment
def user_payment(user, fname):
    user.reset_index(drop=True, inplace=True)
    if (user.shape[0] > 0 and user[fname][0] == 1):
        if (user["neg_list"][0] == 1):
            print("active = 1")
            return 1
        else:
            print("active = 2")
            return 2
    print("active = 3")
    return 3



#################################
### Keyword Validation Filter ###
#################################

# valid keyword characters and split terms
def val_key_char(x):
    # "\u00C2": "Â", "\u00C3": "Ã", "\u00C5": "Å"
    # remove "\&", "\‘", "\“", "\’", "\„", "\‚", \”", "\'"
    x["match_txt_temp"] = x["keyword_text"].str.replace(r"[^0-9a-zA-Z\u00C2\u00C3\u00C5\®\\‰\\š\œ\¡\©\­\±\³\º\¼\–\Ÿ\¤\¶\€\†\‡\ˆ\Š\‹\Ž\\™\›\¸\¢\¦\§\¨\©\ª\«\¯\´\¹\»\¿\u3000-\u309F\u30A0-\u30FF\u4E00-\u9FFF\uFF00-\uFFEF\ \-\b\+\t\n\r\&]+", "")\
                                           .str.split()
    return x

# remove unnecessary terms through NLTK
def remove_unnecessary(x):
    stop_words = set(stopwords.words("english"))
    for i in range(x["match_txt_temp"].shape[0]):
        result = []
        for j in x["match_txt_temp"][i]:
            if (j not in stop_words) or (j in "no"):
                result.append(j)
        x["match_txt_temp"][i] = result
    return x

def remove_unnecessary_split(x):
    stop_words = {"woman", "man", "boy", "girl"}
    for i in range(x["match_txt_split"].shape[0]):
        result = []
        for j in x["match_txt_split"][i]:
            if (j not in stop_words):
                result.append(j)
        x["match_txt_split"][i] = result
    return x

# text format
def text_format(x):
    for i in range(x["match_txt_temp"].shape[0]):
        x["match_txt_temp"][i] = " ".join(x["match_txt_temp"][i])
    return x

# from plural to singular if there are any
def singularize_spacy(x):
    en_core = spacy.load('en_core_web_sm')
    for i in range(3):
        x["match_txt_temp"] = x['match_txt_temp'].apply(lambda z: " ".join([y.lemma_ for y in en_core(z)]))
    return x

# split names
def split_name(x):
    x["match_txt_split"] = x["match_txt_temp"].str.split()
    return x

def singularize_nltk(x):
    n = WordNetLemmatizer()
    for i in range(x["match_txt_split"].shape[0]):
        x["match_txt_split"][i] = [n.lemmatize(w) for w in x["match_txt_split"][i]]
    return x

# everygrams
def n_grams(x, n):
    for i in range(x["match_txt_split"].shape[0]):
        if (n == 1):
            x["match_txt_split"][i] = list(ngrams(x["match_txt_split"][i], 1))
        elif (n == 0):
            x["match_txt_split"][i] = list(everygrams(x["match_txt_split"][i]))
        else:
            # last grams of a row
            x_argmax = np.argmax(x["match_txt_split"][i]) + 1
            x["match_txt_split"][i] = list(ngrams(x["match_txt_split"][i], x_argmax))
    return x

# explode lists
def explode_list(x):
    x_explode = x.explode("match_txt_split")
    x_explode.reset_index(drop=True, inplace=True)
    return x_explode

# text format (split)
def text_format_split(x):
    for i in range(x["match_txt_split"].shape[0]):
        try:
            x["match_txt_split"][i] = " ".join(x["match_txt_split"][i])
        except:
            print("!!!!!:", x["match_txt_split"][i])
            continue
    return x

# the most important keywords
def domain_keyword(x):
    domain_key = list(x["match_txt_split"][:5])
    return domain_key

def domain_keyword_split(x):
    dom_key_temp = [j for i in x for j in i.split()]
    return dom_key_temp

# their synonyms
def get_synonym(x):
    dom_keyword = set(x)
    dom_keyword_temp = dom_keyword.copy()

    dictionary = PyDictionary()
    for i in dom_keyword_temp:
        try:
            dom_keyword.update(dictionary.synonym(i))
        except:
            continue
    return dom_keyword

# drop irrelevant
def drop_irrelevant(x, y):
    x_drop = pd.DataFrame({"keyword_text": [], "associated_cost": [], "associated_sales": [], "associated_clicks": [], "associated_orders": []})
    for i in range(x["match_txt_temp"].shape[0]):
        if not any(j in x["match_txt_temp"][i] for j in y):
            x_drop = x_drop.append({
                    "keyword_text": x["keyword_text"][i],
                    "associated_cost": x["cost"][i],
                    "associated_sales": x["sales"][i],
                    "associated_clicks": x["clicks"][i],
                    "associated_orders": x["orders"][i]
                },
                ignore_index=True)
            x.drop(i, axis=0, inplace=True)
    return x, x_drop



###################
### ASIN Filter ###
###################

# category id and rank
def category_id_and_rank(x):
    cid, cr = [], []
    if len(x.payload) > 2:
        for i in x.payload["SalesRankings"]:
            if (i["ProductCategoryId"].isdigit()):
                cid.append(int(i["ProductCategoryId"]))
                cr.append(i)
    return set(cid), cr

# category list
def category_list(x):
    clist = []
    if len(x.payload) > 0:
        for i in x.payload:
            if (i["ProductCategoryId"].isdigit()):
                clist.append(int(i["ProductCategoryId"]))
            try:
                while len(i["parent"]) > 0:
                    i = i["parent"]
                    if (i["ProductCategoryId"].isdigit()):
                        clist.append(int(i["ProductCategoryId"]))
            except:
                continue
    return set(clist)

# two categories
def category_two(x):
    clist = []
    if len(x.payload) > 0:
        for i in x.payload:
            if (i["ProductCategoryId"].isdigit()):
                clist.append(int(i["ProductCategoryId"]))
            try:
                if (i["parent"]["ProductCategoryId"].isdigit()):
                    clist.append(int(i["parent"]["ProductCategoryId"]))
            except:
                continue
    return set(clist)



###################
### ACOS Filter ###
###################

# rename
def df_rename(df):
    df.rename(columns={"query": "asin"}, inplace=True)
    df.dropna(subset=["asin"], inplace=True)
    df = df.loc[df["asin"].str.startswith(("b0", "B0"))]

    #df["asin"] = df["asin"].str.replace('asin=', '')
    #df["asin"] = df["asin"].str.replace('"', '')

    df.reset_index(drop=True, inplace=True)
    return df

# rename
def df_rename_target(df):
    df.rename(columns={"targeting_expression": "asin"}, inplace=True)
    df.dropna(subset=["asin"], inplace=True)
    df = df.loc[df["asin"].str.startswith("asin=")]

    df["asin"] = df["asin"].str.replace('asin=', '')
    df["asin"] = df["asin"].str.replace('"', '')

    df.reset_index(drop=True, inplace=True)
    return df

# combine two tables
def df_merge(df1, df2, sale, unit):
    df = pd.merge(df1, df2, on=["profile_id", "campaign_id", "campaign_name", "ad_group_id", "ad_group_name"])
    df.rename(columns={
        "date_x": "date",
        "asin_x": "asin_ours",
        "asin_y": "asin_trgt",
        sale: "sales",
        unit: "orders"
    }, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df[["date", "profile_id", "campaign_id", "campaign_name", "ad_group_id", "ad_group_name", "asin_ours", "asin_trgt", "impressions", "clicks", "cost", "sales", "orders"]]
