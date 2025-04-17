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
    except Exception:
        # في حال عدم وجود الملف، نهيئ الحالة بتاريخ اليوم ورقم بدء 0
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
    # إذا تجاوزنا يوم الدورة السابقة
    if status["cycle_date"] != current_day:
        # لو انتهت الدورة (وصلنا لنهاية القائمة)
        if status["last_index"] >= total_coupons:
            status["last_index"] = 0
        status["cycle_date"] = current_day
        save_status(status)
    current_index = status["last_index"]
    # إذا تبقى كوبونات
    if current_index < total_coupons:
        coupon = df.iloc[current_index]
        new_index = current_index + 1
        return coupon, new_index, status
    else:
        return None, current_index, status

# ━━━━━━━━━━━━━━━━━━━━━ النشر التلقائي ━━━━━━━━━━━━━━━━━━━━━
async def post_scheduled_coupon():
    df = load_coupons()
    if df.empty:
        logger.error("لا توجد كوبونات متاحة للنشر")
        return

    result = get_next_coupon(df)
    if result is None:
        logger.info("لا يوجد كوبون متبقي للنشر اليوم")
        return
    coupon, new_index, status = result
    if coupon is None:
        logger.info("لا يوجد كوبون متبقي للنشر اليوم")
        return

    try:
        message = (
            f"🎉 كوبون {coupon['title']}\n\n"
            f"🔥 {coupon['description']}\n\n"
            f"✅ الكوبون : {coupon['code']}\n\n"
            f"🌍 صالح لـ : {coupon['countries']}\n\n"
            f"📌 ملاحظة : {coupon['note']}\n\n"
            f"🛒 رابط الشراء : {coupon['link']}\n\n"
            "💎 لمزيد من الكوبونات والخصومات قم بزيارة موقعنا:\n"
            "https://www.discountcoupon.online"
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
    scheduler = BackgroundScheduler(timezone="Africa/Algiers")
    # CronTrigger واحدة تنفّذ كل ساعة من 03:00 حتى 22:00
    scheduler.add_job(
        run_async_task,
        'cron',
        hour='3-22',
        minute=0,
        args=[post_scheduled_coupon],
        id='daily_coupon_job'
    )
    scheduler.start()

# ━━━━━━━━━━━━━━━━━━━━━ الدالة الرئيسية ━━━━━━━━━━━━━━━━━━━━━
def main():
    # ضمان وجود حلقة أحداث
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # إنشاء status.json عند الإقلاع
    load_status()

    # تشغيل Flask في Thread منفصل لفحص الـ Health Check
    Thread(target=run_flask).start()

    global application
    token = os.getenv("TOKEN")
    application = ApplicationBuilder().token(token).build()

    schedule_jobs()

    # إزالة أي Webhook سابق وتفريغ التحديثات العالقة
    asyncio.run(application.bot.delete_webhook())
    logger.info("🔄 تمت إزالة أي Webhook سابق وتفريغ التحديثات العالقة")

    logger.info("✅ البوت يعمل...")
    # تشغيل polling مع حذف التحديثات القديمة
    application.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    main()
