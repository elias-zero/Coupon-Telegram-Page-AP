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
    logger.info("بدء عملية نشر كوبون جديد")
    df = load_coupons()
    if df.empty:
        logger.error("لا توجد كوبونات متاحة للنشر")
        return

    result = get_next_coupon(df)
    coupon, new_index, status = result
    
    if coupon is None:
        logger.info("لا يوجد كوبون متبقي للنشر")
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

        status["last_index"] = new_index
        save_status(status)
        logger.info(f"تم نشر الكوبون رقم {new_index - 1} بنجاح")
    except Exception as e:
        logger.error(f"فشل في النشر: {e}")

# ━━━━━━━━━━━━━━━━━━━━━ تشغيل دوال async في حلقة جديدة ━━━━━━━━━━━━━━━━━━━━━
def run_async_task(coro):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(coro())
    except Exception as e:
        logger.error(f"خطأ أثناء تنفيذ المهمة غير المتزامنة: {e}")
    finally:
        loop.close()

# ━━━━━━━━━━━━━━━━━━━━━ جدولة المهام ━━━━━━━━━━━━━━━━━━━━━
def schedule_jobs():
    scheduler = BackgroundScheduler(timezone="Africa/Algiers")
    
    # إضافة مهمة للنشر كل ساعة من 3 صباحًا حتى 22 مساءً
    for hour in range(3, 23):
        scheduler.add_job(
            run_async_task,
            'cron',
            hour=hour,
            minute=0,
            args=[post_scheduled_coupon],
            id=f'daily_coupon_job_{hour}'
        )
        logger.info(f"تمت جدولة النشر للساعة {hour}:00")
    
    scheduler.start()
    logger.info("تم بدء المجدول بنجاح")

# ━━━━━━━━━━━━━━━━━━━━━ الدالة الرئيسية ━━━━━━━━━━━━━━━━━━━━━
def main():
    # إنشاء أو استرجاع حلقة أحداث رئيسية في MainThread
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # إنشاء status.json عند الإقلاع
    load_status()

    # تشغيل Flask في Thread منفصل لفحص الـ Health Check
    Thread(target=run_flask, daemon=True).start()

    global application
    token = os.getenv("TOKEN")
    if not token:
        logger.error("لم يتم تعيين TOKEN في متغيرات البيئة!")
        return
        
    application = ApplicationBuilder().token(token).build()

    # جدولة الوظائف
    schedule_jobs()

    # باستخدام نفس حلقة الأحداث الرئيسية نحذف الـ webhook القديم
    loop.run_until_complete(application.bot.delete_webhook())
    logger.info("🔄 تمت إزالة أي Webhook سابق وتفريغ التحديثات العالقة")

    logger.info("✅ البوت يعمل...")
    application.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    main()
