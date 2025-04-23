import os
import logging
import pandas as pd
from flask import Flask
from threading import Thread
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
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
INDEX_FILE = "last_index.txt"
LOCK_FILE = "/tmp/telegrambot.lock"

# ━━━━━━━━━━━━━━━━━━━━━ Flask للـ Health Check ━━━━━━━━━━━━━━━━━━━━━
app = Flask(__name__)

@app.route('/health')
def health_check():
    return "OK", 200

def run_flask():
    from waitress import serve
    serve(app, host="0.0.0.0", port=8080)

# ━━━━━━━━━━━━━━━━━━━━━ إدارة القفل المحسنة ━━━━━━━━━━━━━━━━━━━━━
def create_lock():
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except (IOError, OSError):
        logger.error("هناك نسخة أخرى تعمل بالفعل!")
        sys.exit(1)

# ━━━━━━━━━━━━━━━━━━━━━ إدارة الفهرس ━━━━━━━━━━━━━━━━━━━━━
def load_index():
    try:
        with open(INDEX_FILE, 'r') as f:
            return int(f.read().strip())
    except:
        return 0

def save_index(index):
    with open(INDEX_FILE, 'w') as f:
        f.write(str(index))

# ━━━━━━━━━━━━━━━━━━━━━ وظائف الكوبونات ━━━━━━━━━━━━━━━━━━━━━
def load_coupons():
    try:
        df = pd.read_excel(COUPONS_FILE, engine='openpyxl')
        return df.dropna(how='all')
    except Exception as e:
        logger.error(f'خطأ في قراءة الملف: {e}')
        return pd.DataFrame()

def get_next_coupon():
    df = load_coupons()
    if df.empty:
        return None
    
    current_index = load_index()
    total = len(df)
    
    if current_index >= total:
        current_index = 0
    
    coupon = df.iloc[current_index]
    save_index(current_index + 1)
    
    return coupon

# ━━━━━━━━━━━━━━━━━━━━━ النشر التلقائي المحسّن ━━━━━━━━━━━━━━━━━━━━━
async def post_coupon():
    try:
        logger.info("--- بدء محاولة نشر جديدة ---")
        
        coupon = get_next_coupon()
        if coupon is None:
            logger.error("لا توجد كوبونات متاحة!")
            return
            
        message = (
            f"🎉 كوبون {coupon['title']}\n\n"
            f"🔥 {coupon['description']}\n\n"
            f"✅ الكوبون: {coupon['code']}\n\n"
            f"🌍 صالح لـ: {coupon['countries']}\n\n"
            f"📌 ملاحظة: {coupon['note']}\n\n"
            f"🛒 رابط الشراء: {coupon['link']}\n\n"
            "💎 لمزيد من الكوبونات:\nhttps://www.discountcoupon.online"
        )

        if pd.notna(coupon.get('image')) and str(coupon['image']).startswith('http'):
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
            
        logger.info("تم النشر بنجاح ✅")
        
    except Exception as e:
        logger.error(f"فشل في النشر: {str(e)}")

# ━━━━━━━━━━━━━━━━━━━━━ جدولة المهام المحسنة ━━━━━━━━━━━━━━━━━━━━━
def trigger_post():
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(post_coupon())
    except Exception as e:
        logger.error(f"خطأ في الجدولة: {str(e)}")

def schedule_jobs():
    scheduler = BackgroundScheduler(timezone="Africa/Algiers")
    
    # جدولة كل ساعة من 3-22
    scheduler.add_job(
        trigger_post,
        'cron',
        hour='3-22',
        minute=0,
        misfire_grace_time=3600
    )
    
    scheduler.start()

# ━━━━━━━━━━━━━━━━━━━━━ الدالة الرئيسية المعدلة ━━━━━━━━━━━━━━━━━━━━━
async def main_async():
    global application
    token = os.getenv("TOKEN")
    
    if not token:
        logger.error("المتغير البيئي TOKEN غير موجود!")
        sys.exit(1)

    application = ApplicationBuilder().token(token).build()
    
    # تشغيل الخدمات
    Thread(target=run_flask, daemon=True).start()
    schedule_jobs()
    
    await application.initialize()
    await application.start()
    await application.updater.start_polling()

def main():
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    lock_fd = create_lock()
    
    try:
        asyncio.run(main_async())
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
    main()
