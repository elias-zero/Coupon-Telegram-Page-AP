import os
import json
import logging
import pandas as pd
from flask import Flask
from threading import Thread, Lock
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import asyncio
from telegram.ext import ApplicationBuilder
import pytz
import time
import signal
import sys
import socket
import fcntl
import struct

# ━━━━━━━━━━━━━━━━━━━━━ إعدادات البوت الأساسية ━━━━━━━━━━━━━━━━━━━━━
CHANNEL_USERNAME = "@discountcoupononline"
COUPONS_FILE = "coupons.xlsx"
STATUS_FILE = "status.json"  # لحفظ حالة النشر (last_index و cycle_date)
LOCK_FILE = "/tmp/telegrambot.lock"  # ملف لقفل البوت لضمان تشغيل نسخة واحدة فقط
JOB_LOCK = Lock()  # قفل لمنع تشغيل وظائف متعددة في نفس الوقت

# ━━━━━━━━━━━━━━━━━━━━━ قفل لضمان تشغيل نسخة واحدة فقط ━━━━━━━━━━━━━━━━━━━━━
def create_singleton_lock():
    """تأكد من أن هناك نسخة واحدة فقط من البوت تعمل"""
    try:
        # إنشاء سوكت للاستماع على منفذ محدد
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', 29876))  # منفذ مخصص للبوت فقط
        s.listen(1)
        return s
    except socket.error:
        logger.error("هناك نسخة أخرى من البوت تعمل بالفعل! إيقاف هذه النسخة...")
        sys.exit(1)

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
        return None, status["last_index"], status
    
    current_day = get_local_date()
    
    # إعادة الترتيب عند بداية يوم جديد فقط إذا انتهت جميع الكوبونات
    if status["cycle_date"] != current_day:
        if status["last_index"] >= total_coupons:
            status["last_index"] = 0
        status["cycle_date"] = current_day
        save_status(status)
    
    current_index = status["last_index"]
    if current_index < total_coupons:
        coupon = df.iloc[current_index]
        new_index = current_index + 1
        return coupon, new_index, status
    else:
        # إعادة دورة جديدة إذا تم استنفاد جميع الكوبونات
        status["last_index"] = 0
        save_status(status)
        # نحاول مرة أخرى الآن بعد إعادة التعيين
        if total_coupons > 0:
            coupon = df.iloc[0]
            return coupon, 1, status
        return None, 0, status

# ━━━━━━━━━━━━━━━━━━━━━ النشر التلقائي ━━━━━━━━━━━━━━━━━━━━━
async def post_scheduled_coupon():
    # استخدام قفل لمنع تشغيل وظائف متعددة في نفس الوقت
    if not JOB_LOCK.acquire(blocking=False):
        logger.warning("هناك عملية نشر قيد التنفيذ بالفعل، تخطي هذه المهمة")
        return
    
    try:
        logger.info("بدء عملية نشر كوبون جديد")
        current_hour = datetime.now(pytz.timezone("Africa/Algiers")).hour
        logger.info(f"الساعة الحالية: {current_hour}")
        
        df = load_coupons()
        if df.empty:
            logger.error("لا توجد كوبونات متاحة للنشر")
            return

        result = get_next_coupon(df)
        coupon, new_index, status = result
        
        if coupon is None:
            logger.info("لا يوجد كوبون متبقي للنشر")
            return

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

        status["last_index"] = new_index
        save_status(status)
        
        # إضافة تأخير قصير لضمان إكمال الطلب
        await asyncio.sleep(2)
        
        logger.info(f"تم نشر الكوبون رقم {new_index - 1} بنجاح")
    except Exception as e:
        logger.error(f"فشل في النشر: {e}")
    finally:
        JOB_LOCK.release()

# ━━━━━━━━━━━━━━━━━━━━━ تشغيل دوال async في حلقة جديدة ━━━━━━━━━━━━━━━━━━━━━
def run_async_task(coro):
    try:
        # استخدام حلقة موجودة إذا كانت متاحة
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("الحلقة مغلقة")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        return loop.run_until_complete(coro())
    except Exception as e:
        logger.error(f"خطأ أثناء تنفيذ المهمة غير المتزامنة: {e}")

# ━━━━━━━━━━━━━━━━━━━━━ جدولة المهام ━━━━━━━━━━━━━━━━━━━━━
def schedule_jobs():
    scheduler = BackgroundScheduler(timezone="Africa/Algiers", misfire_grace_time=120)
    
    # إضافة مهمة للنشر كل ساعة من 3 صباحًا حتى 22 مساءً
    for hour in range(3, 23):
        scheduler.add_job(
            run_async_task,
            'cron',
            hour=hour,
            minute=0,
            args=[post_scheduled_coupon],
            id=f'daily_coupon_job_{hour}',
            max_instances=1,  # تأكد من عدم وجود أكثر من مثيل لنفس الوظيفة
            coalesce=True,    # دمج المهام المتأخرة
            replace_existing=True  # استبدال المهام الموجودة عند إعادة التشغيل
        )
        logger.info(f"تمت جدولة النشر للساعة {hour}:00")
    
    scheduler.start()
    logger.info("تم بدء المجدول بنجاح")

# ━━━━━━━━━━━━━━━━━━━━━ معالج الخروج ━━━━━━━━━━━━━━━━━━━━━
def signal_handler(sig, frame):
    logger.info("تم استلام إشارة الإيقاف، إغلاق البوت بأمان...")
    # تنفيذ عمليات التنظيف هنا
    sys.exit(0)

# ━━━━━━━━━━━━━━━━━━━━━ الدالة الرئيسية ━━━━━━━━━━━━━━━━━━━━━
def main():
    # تسجيل معالج الإشارات
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # تأكد من أن هناك نسخة واحدة فقط من البوت تعمل
    lock_socket = create_singleton_lock()
    
    # انتظار قبل بدء البوت لضمان عدم وجود مثيلات أخرى قيد التشغيل
    logger.info("انتظار 10 ثوانٍ قبل البدء للتأكد من عدم وجود مثيلات أخرى قيد التشغيل...")
    time.sleep(10)
    
    # إنشاء أو استرجاع حلقة أحداث رئيسية في MainThread
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # إنشاء status.json عند الإقلاع
    load_status()

    # تشغيل Flask في Thread منفصل لفحص الـ Health Check
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()

    global application
    token = os.getenv("TOKEN")
    if not token:
        logger.error("لم يتم تعيين TOKEN في متغيرات البيئة!")
        return
    
    # إنشاء تطبيق البوت مع إعدادات إضافية
    application = (
        ApplicationBuilder()
        .token(token)
        .concurrent_updates(False)  # تعطيل التحديثات المتزامنة
        .build()
    )

    # حذف أي webhook وتفريغ أي تحديثات معلقة
    try:
        loop.run_until_complete(application.bot.delete_webhook(drop_pending_updates=True))
        logger.info("🔄 تمت إزالة أي Webhook سابق وتفريغ التحديثات العالقة")
    except Exception as e:
        logger.error(f"خطأ في حذف الـ webhook: {e}")
        # انتظار وإعادة المحاولة
        time.sleep(5)
        loop.run_until_complete(application.bot.delete_webhook(drop_pending_updates=True))

    # جدولة الوظائف
    schedule_jobs()

    # تشغيل البوت
    logger.info("✅ البوت يعمل...")
    application.run_polling(
        drop_pending_updates=True,
        close_loop=False,
        timeout=30,
        read_timeout=30,
        write_timeout=30,
        connect_timeout=30
    )

if __name__ == '__main__':
    # إعداد التسجيل
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    
    # تشغيل الدالة الرئيسية
    try:
        main()
    except Exception as e:
        logger.critical(f"خطأ حرج أدى إلى توقف البوت: {e}")
        sys.exit(1)
