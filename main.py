import time
import uvicorn
import uvloop
import asyncio
from pytz import timezone
from fastapi import FastAPI
#from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
'''
from analyzer.filter_sp_keyword_acos import sp_filter_key_acos  #1, 2
from analyzer.filter_sb_keyword_acos import sb_filter_key_acos  #3, 4
from analyzer.filter_sp_keyword_acos_target import sp_filter_key_acos_target  #9, 10

from analyzer.filter_sp_asin import sp_filter_asin  #5
from analyzer.filter_sd_asin import sd_filter_asin  #not yet
from analyzer.filter_sp_asin_keyword import sp_filter_asin_keyword  #12

from analyzer.filter_sd_acos import sd_filter_acos  #6
from analyzer.filter_sp_asin_acos import sp_filter_asin_acos  #7
from analyzer.filter_sb_asin_acos import sb_filter_asin_acos  #8
from analyzer.filter_sp_asin_acos_keyword import sp_filter_asin_acos_keyword  #11
'''
from analyzer.filter_keyword_acos import filter_key_acos  #1, 2, 3, 4, 9, 10
from analyzer.filter_asin import filter_asin_asin  #5, not yet, 12
from analyzer.filter_acos import filter_asin_acos  #6, 7, 8, 11

from billing.filter_billing import sp_sb_sd_payment


app = FastAPI()

#1, 2, 3, 4, 9, 10
async def filter_keyword_acos_start():
    logger.info("start keyword acos filter #1, 2")
    filter_key_acos("sp_keyword_report", "attributed_sales_7d", "attributed_units_ordered_7d")  #1, 2

    logger.info("start keyword acos filter #3, 4")
    filter_key_acos("sb_keyword_report", "attributed_sales_14d", "attributed_conversions_14d")  #3, 4

    logger.info("start keyword acos filter #9, 10")
    filter_key_acos("sp_target_report", "attributed_sales_7d", "attributed_units_ordered_7d")   #9, 10

#5, not yet, 12
async def filter_asin_start():
    logger.info("start asin filter #5")
    filter_asin_asin("sp_product_ad_report", "sp_target_report", "attributed_sales_7d", "attributed_units_ordered_7d")    #5

    logger.info("start asin filter #not yet")
    filter_asin_asin("sd_product_ad_report", "sd_target_report", "attributed_sales_30d", "attributed_units_ordered_30d")  #not yet

    logger.info("start asin filter #12")
    filter_asin_asin("sp_product_ad_report", "sp_keyword_report", "attributed_sales_7d", "attributed_units_ordered_7d")   #12

#6, 7, 8, 11
async def filter_acos_start():
    logger.info("start acos filter #6")
    filter_asin_acos("sd_target_report", "attributed_sales_30d", "attributed_units_ordered_30d")  #6

    logger.info("start acos filter #7")
    filter_asin_acos("sp_target_report", "attributed_sales_7d", "attributed_units_ordered_7d")    #7

    logger.info("start acos filter #8")
    filter_asin_acos("sb_target_report", "attributed_sales_14d", "attributed_conversions_14d")    #8

    logger.info("start acos filter #11")
    filter_asin_acos("sp_keyword_report", "attributed_sales_7d", "attributed_units_ordered_7d")   #11



@app.on_event('startup')
async def main():
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    # await start_up_event()
    scheduler = AsyncIOScheduler()
    '''
    #1, 2
    scheduler.add_job(
        sp_filter_key_acos,
        'interval',
        days=1,
        start_date='2021-01-01 01:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )
    #3, 4
    scheduler.add_job(
        sb_filter_key_acos,
        'interval',
        days=1,
        start_date='2021-01-01 01:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )
    #5
    scheduler.add_job(
        sp_filter_asin,
        'interval',
        days=1,
        start_date='2021-01-01 01:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )
    #6
    scheduler.add_job(
        sd_filter_acos,
        'interval',
        days=1,
        start_date='2021-01-01 01:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )
    # not yet
    scheduler.add_job(
        sd_filter_asin,
        'interval',
        days=1,
        start_date='2021-01-01 01:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )
    #7
    scheduler.add_job(
        sp_filter_asin_acos,
        'interval',
        days=1,
        start_date='2021-01-01 02:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )
    #8
    scheduler.add_job(
        sb_filter_asin_acos,
        'interval',
        days=1,
        start_date='2021-01-01 02:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )

    #new ones
    #9, 10
    scheduler.add_job(
        sp_filter_key_acos_target,
        'interval',
        days=1,
        start_date='2021-01-01 02:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )
    #11
    scheduler.add_job(
        sp_filter_asin_acos_keyword,
        'interval',
        days=1,
        start_date='2021-01-01 02:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )
    #12
    scheduler.add_job(
        sp_filter_asin_keyword,
        'interval',
        days=1,
        start_date='2021-01-01 02:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )
    '''

    #1, 2, 3, 4, 9, 10
    scheduler.add_job(
        filter_keyword_acos_start,
        'interval',
        days=1,
        start_date='2021-01-01 01:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )

    #5, not yet, 12
    scheduler.add_job(
        filter_asin_start,
        'interval',
        days=1,
        start_date='2021-01-01 03:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )

    #6, 7, 8, 11
    scheduler.add_job(
        filter_acos_start,
        'interval',
        days=1,
        start_date='2021-01-01 05:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )

    # payment
    scheduler.add_job(
        sp_sb_sd_payment,
        'interval',
        days=1,
        start_date='2021-01-01 07:00:00',
        timezone=timezone('Asia/Seoul'),
        jitter=120
    )

    scheduler.start()


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
