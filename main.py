import os
import logging
import pandas as pd
from flask import Flask
from threading import Thread
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone, timedelta
import asyncio
from telegram.ext import ApplicationBuilder

# ━━━━━━━━━━━━━━━━━━━━━ إعدادات البوت الأساسية ━━━━━━━━━━━━━━━━━━━━━
CHANNEL_USERNAME = "@discountcoupononline"
COUPONS_FILE = "coupons.xlsx"
LAST_INDEX_FILE = "last_index.txt"

# ━━━━━━━━━━━━━━━━━━━━━ Flask للـ Health Check ━━━━━━━━━━━━━━━━━━━━━
app = Flask(__name__)

@app.route('/health')
def health_check():
    return "OK", 200

def run_flask():
    app.run(host='0.0.0.0', port=8080)

# ━━━━━━━━━━━━━━━━━━━━━ إدارة حالة النشر ━━━━━━━━━━━━━━━━━━━━━
def save_last_index(index: int):
    with open(LAST_INDEX_FILE, 'w') as f:
        f.write(str(index))

def load_last_index():
    try:
        with open(LAST_INDEX_FILE, 'r') as f:
            return int(f.read().strip())
    except Exception as e:
        logger.error(f"خطأ في قراءة last_index: {e}")
        return 0

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
    last_index = load_last_index()
    total_coupons = len(df)
    if total_coupons == 0:
        return None, 0
    current_index = last_index % total_coupons
    next_index = (current_index + 1) % total_coupons
    return df.iloc[current_index], next_index

# ━━━━━━━━━━━━━━━━━━━━━ النشر التلقائي ━━━━━━━━━━━━━━━━━━━━━
async def post_scheduled_coupon():
    df = load_coupons()
    if df.empty:
        logger.error("لا توجد كوبونات متاحة للنشر")
        return

    coupon, new_index = get_next_coupon(df)
    if coupon is None:
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

        save_last_index(new_index)
        logger.info(f"تم نشر الكوبون رقم {new_index} بنجاح")

    except Exception as e:
        logger.error(f"فشل في النشر: {e}")

# دالة وسيطة لتشغيل الدوال غير المتزامنة باستخدام asyncio.run()
def run_async_task(coro):
    asyncio.run(coro())

# ━━━━━━━━━━━━━━━━━━━━━ جدولة المهام ━━━━━━━━━━━━━━━━━━━━━
def schedule_jobs():
    scheduler = BackgroundScheduler(timezone="UTC")
    # جدولة النشر من 8 صباحًا إلى 2 ليلاً (18 ساعة)
    for hour in range(8, 26):
        scheduled_time = datetime.now(timezone.utc).replace(
            hour=hour % 24,
            minute=0,
            second=0,
            microsecond=0
        ) + timedelta(days=hour // 24)
        scheduler.add_job(
            run_async_task,
            'interval',
            hours=1,
            start_date=scheduled_time,
            args=[post_scheduled_coupon],
            max_instances=1
        )
    scheduler.start()

# ━━━━━━━━━━━━━━━━━━━━━ الدالة الرئيسية ━━━━━━━━━━━━━━━━━━━━━
def main():
    # التأكد من وجود حلقة أحداث قبل بدء الجدولة
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
