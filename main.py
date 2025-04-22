import os
import json
import logging
import pandas as pd
from flask import Flask
from threading import Thread
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import asyncio
from telegram import Bot
from telegram.ext import ApplicationBuilder
import pytz
import time
import signal
import sys
import fcntl

# ━━━━━━━━━━━━━━━━━━━━━ إعدادات البوت الأساسية ━━━━━━━━━━━━━━━━━━━━━
CHANNEL_USERNAME = "@discountcoupononline"
COUPONS_FILE = "coupons.xlsx"
STATUS_FILE = "status.json"
LOCK_FILE = "/tmp/telegrambot.lock"

# ━━━━━━━━━━━━━━━━━━━━━ Flask للـ Health Check ━━━━━━━━━━━━━━━━━━━━━
app = Flask(__name__)

@app.route('/health')
def health_check():
    return "OK", 200

def run_flask():
    app.run(host='0.0.0.0', port=8080, use_reloader=False)

# ━━━━━━━━━━━━━━━━━━━━━ إدارة القفل المحسنة ━━━━━━━━━━━━━━━━━━━━━
def create_lock():
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except (IOError, OSError):
        logger.error("هناك نسخة أخرى تعمل بالفعل!")
        sys.exit(1)

# ━━━━━━━━━━━━━━━━━━━━━ إدارة حالة النشر ━━━━━━━━━━━━━━━━━━━━━
def get_local_date():
    tz = pytz.timezone("Africa/Algiers")
    return datetime.now(tz).strftime("%Y-%m-%d")

def load_status():
    try:
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        status = {"last_index": 0, "cycle_date": get_local_date()}
        save_status(status)
        return status

def save_status(status):
    with open(STATUS_FILE, 'w') as f:
        json.dump(status, f)

# ━━━━━━━━━━━━━━━━━━━━━ وظائف الكوبونات ━━━━━━━━━━━━━━━━━━━━━
def load_coupons():
    try:
        df = pd.read_excel(COUPONS_FILE)
        return df.dropna(how='all')
    except Exception as e:
        logger.error(f'خطأ في قراءة الملف: {e}')
        return pd.DataFrame()

def get_next_coupon(df):
    status = load_status()
    current_day = get_local_date()
    
    if status["cycle_date"] != current_day:
        status["last_index"] = 0
        status["cycle_date"] = current_day
    
    if status["last_index"] >= len(df):
        status["last_index"] = 0
    
    coupon = df.iloc[status["last_index"]]
    status["last_index"] += 1
    save_status(status)
    return coupon

# ━━━━━━━━━━━━━━━━━━━━━ النشر التلقائي المحسّن ━━━━━━━━━━━━━━━━━━━━━
async def post_coupon():
    try:
        logger.info("بدء عملية نشر كوبون جديد")
        
        df = load_coupons()
        if df.empty:
            logger.error("لا توجد كوبونات متاحة للنشر")
            return

        coupon = get_next_coupon(df)
        
        message = (
            f"🎉 كوبون {coupon['title']}\n\n"
            f"🔥 {coupon['description']}\n\n"
            f"✅ الكوبون : {coupon['code']}\n\n"
            f"🌍 صالح لـ : {coupon['countries']}\n\n"
            f"📌 ملاحظة : {coupon['note']}\n\n"
            f"🛒 رابط الشراء : {coupon['link']}\n\n"
            "💎 لمزيد من الكوبونات والخصومات:\n"
            "https://www.discountcoupon.online"
        )

        try:
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
            logger.info("تم النشر بنجاح")
        except Exception as send_error:
            logger.error(f"فشل في إرسال الرسالة: {send_error}")

    except Exception as e:
        logger.error(f"خطأ عام في النشر: {e}")

# ━━━━━━━━━━━━━━━━━━━━━ جدولة المهام المحسنة ━━━━━━━━━━━━━━━━━━━━━
def trigger_post():
    try:
        loop = asyncio.get_event_loop()
        asyncio.run_coroutine_threadsafe(post_coupon(), loop)
    except Exception as e:
        logger.error(f"فشل في تشغيل المهمة: {e}")

def schedule_jobs():
    scheduler = BackgroundScheduler(timezone="Africa/Algiers")
    scheduler.add_job(
        trigger_post,
        'cron',
        hour='3-22',
        minute=0,
        misfire_grace_time=600
    )
    scheduler.start()

# ━━━━━━━━━━━━━━━━━━━━━ الدالة الرئيسية المعدلة ━━━━━━━━━━━━━━━━━━━━━
def main():
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    
    lock_fd = create_lock()
    
    global application
    token = os.getenv("TOKEN")
    
    if not token:
        logger.error("لم يتم تعيين TOKEN في المتغيرات البيئية!")
        sys.exit(1)

    try:
        application = (
            ApplicationBuilder()
            .token(token)
            .post_init(lambda app: logger.info("تم تهيئة البوت بنجاح"))
            .build()
        )

        Thread(target=run_flask, daemon=True).start()
        time.sleep(2)  # إعطاء وقت لبدء Flask
        
        schedule_jobs()
        
        logger.info("✅ البوت يعمل...")
        application.run_polling(
            drop_pending_updates=True,
            close_loop=False
        )
    finally:
        os.close(lock_fd)
        os.unlink(LOCK_FILE)

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler("bot.log"),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    # التحقق من الوقت عند البدء
    logger.info(f"الوقت الحالي على السيرفر: {datetime.now()}")
    main()
