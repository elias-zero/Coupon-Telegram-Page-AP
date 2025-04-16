import os
import json
import logging
import pandas as pd
from flask import Flask
from threading import Thread
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import asyncio
from telegram.ext import ApplicationBuilder
import pytz

# ━━━━━━━━━━━━━━━━━━━━━ إعدادات البوت الأساسية ━━━━━━━━━━━━━━━━━━━━━
CHANNEL_USERNAME = "@discountcoupononline"
COUPONS_FILE = "coupons.xlsx"
STATUS_FILE = "status.json"  # لحفظ حالة النشر (last_index و cycle_date)

# ━━━━━━━━━━━━━━━━━━━━━ Flask للـ Health Check ━━━━━━━━━━━━━━━━━━━━━
app = Flask(__name__)

@app.route('/health')
def health_check():
    return "OK", 200

def run_flask():
    app.run(host='0.0.0.0', port=8080)

# ━━━━━━━━━━━━━━━━━━━━━ إدارة حالة النشر (status) ━━━━━━━━━━━━━━━━━━━━━
def get_local_date():
    tz = pytz.timezone("Africa/Algiers")
    return datetime.now(tz).strftime("%Y-%m-%d")

def load_status():
    try:
        with open(STATUS_FILE, 'r', encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        # في حال عدم وجود الملف، نهيئ الحالة باستخدام تاريخ الجزائر الحالي ورقم بدء 0
        current_day = get_local_date()
        status = {"last_index": 0, "cycle_date": current_day}
        save_status(status)
        return status

def save_status(status):
    with open(STATUS_FILE, 'w', encoding="utf-8") as f:
        json.dump(status, f)

# ━━━━━━━━━━━━━━━━━━━━━ وظائف الكوبونات ━━━━━━━━━━━━━━━━━━━━━
def load_coupons():
    try:
        df = pd.read_excel(COUPONS_FILE)
        df = df.dropna(how='all')
        return df
    except Exception as e:
        logger.error(f'خطأ في قراءة الملف: {e}')
        return pd.DataFrame()

def get_next_coupon(df):
    status = load_status()
    total_coupons = len(df)
    if total_coupons == 0:
        return None, status
    current_day = get_local_date()
    # إذا تغير اليوم عن الدورة السابقة
    if status["cycle_date"] != current_day:
        # إذا كانت الدورة السابقة انتهت (تم استخدام كل الكوبونات)
        if status["last_index"] >= total_coupons:
            status["last_index"] = 0  # إعادة التعيين
        status["cycle_date"] = current_day
        save_status(status)
    current_index = status["last_index"]
    # إذا ما زال هناك كوبونات غير منشورة في الدورة الحالية
    if current_index < total_coupons:
        coupon = df.iloc[current_index]
        new_index = current_index + 1
        return coupon, new_index, status
    else:
        # لم يتبقَ كوبونات للنشر في هذه الدورة (اليوم)
        return None, current_index, status

# ━━━━━━━━━━━━━━━━━━━━━ النشر التلقائي ━━━━━━━━━━━━━━━━━━━━━
async def post_scheduled_coupon():
    df = load_coupons()
    if df.empty:
        logger.error("لا توجد كوبونات متاحة للنشر")
        return

    result = get_next_coupon(df)
    # نتيجة الدالة تختلف حسب توفر الكوبونات
    if result is None:
        logger.info("لا يوجد كوبون متبقي للنشر اليوم")
        return
    coupon, new_index, status = result
    if coupon is None:
        logger.info("لا يوجد كوبون متبقي للنشر اليوم")
        return

    try:
        message = (
            f"🎉 كوبون {coupon['title']}\n"
            f"{coupon['description']}\n\n"
            f"✅ الكوبون : {coupon['code']}\n"
            f"🌍 صالح لـ : {coupon['countries']}\n"
            f"📌 ملاحظة : {coupon['note']}\n"
            f"🛒 رابط الشراء : {coupon['link']}"
        )

        if pd.notna(coupon['image']) and str(coupon['image']).startswith('http'):
            await application.bot.send_photo(
                chat_id=CHANNEL_USERNAME,
                photo=coupon['image'],
                caption=message
            )
        else:
            await application.bot.send_message(
                chat_id=CHANNEL_USERNAME,
                text=message
            )

        # تحديث الحالة بعد النشر
        status["last_index"] = new_index
        save_status(status)
        logger.info(f"تم نشر الكوبون رقم {new_index - 1} بنجاح")
    except Exception as e:
        logger.error(f"فشل في النشر: {e}")

# دالة وسيطة لتشغيل الدوال غير المتزامنة باستخدام asyncio.run()
def run_async_task(coro):
    asyncio.run(coro())

# ━━━━━━━━━━━━━━━━━━━━━ جدولة المهام ━━━━━━━━━━━━━━━━━━━━━
def schedule_jobs():
    # نستخدم منطقة زمنية الجزائر "Africa/Algiers"
    scheduler = BackgroundScheduler(timezone="Africa/Algiers")
    # جدولة النشر من الساعة 3 صباحًا إلى الساعة 22:00 (أي 20 توقيتاً)
    for hour in range(3, 23):  # 3,4,...,22
        scheduler.add_job(
            run_async_task,
            'cron',
            hour=hour,
            minute=0,
            args=[post_scheduled_coupon]
        )
    scheduler.start()

# ━━━━━━━━━━━━━━━━━━━━━ الدالة الرئيسية ━━━━━━━━━━━━━━━━━━━━━
def main():
    # بدء حلقة الأحداث (في حال عدم وجود واحدة)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # بدء Flask في Thread منفصل لفحص الـ Health Check
    Thread(target=run_flask).start()

    global application
    token = os.getenv("TOKEN")
    application = ApplicationBuilder().token(token).build()

    schedule_jobs()

    logger.info("✅ البوت يعمل...")
    application.run_polling()

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    main()
