import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
from fastapi import FastAPI, Request, HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, inspect, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import uvicorn
import logging
import traceback

# ==================== تنظیمات لاگ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="PIPPIN Netflow Bot - PostgreSQL")

# ==================== Database Setup ====================
# تبدیل postgres:// به postgresql:// برای SQLAlchemy
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# اضافه کردن تنظیمات اضافی برای پایداری کانکشن در Railway
engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={"connect_timeout": 10}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ==================== Models ====================
class CEXWallet(Base):
    __tablename__ = "cex_wallets"
    
    id = Column(Integer, primary_key=True, index=True)
    wallet = Column(String, unique=True, index=True, nullable=False)
    label = Column(String, nullable=False)
    chain = Column(String, default="solana")
    added_at = Column(DateTime, default=datetime.utcnow)

class Transaction(Base):
    __tablename__ = "transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    signature = Column(String, unique=True, index=True, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    slot = Column(Integer)
    wallet_from = Column(String, index=True)
    wallet_to = Column(String, index=True)
    amount = Column(Float)
    token = Column(String, index=True)
    tx_type = Column(String, index=True)  # inflow, outflow, internal
    cex_from = Column(String, index=True)
    cex_to = Column(String, index=True)
    netflow_bucket = Column(String, index=True)  # 4h bucket
    created_at = Column(DateTime, default=datetime.utcnow)

class NetflowHourly(Base):
    __tablename__ = "netflow_hourly"
    
    id = Column(Integer, primary_key=True, index=True)
    bucket_time = Column(DateTime, index=True, nullable=False)
    inflow_amount = Column(Float, default=0.0)
    outflow_amount = Column(Float, default=0.0)
    net_flow = Column(Float, default=0.0)
    tx_count = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# ==================== Config ====================
PORT = int(os.getenv("PORT", "8000"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "default-secret")
CSV_FILE = "combined_cex_wallets.csv"
TOKEN_ADDRESS = "Dfh5DzRgSvvCFDoYc2ciTkMrbDfRKybA4SoFbPmApump"  # PIPPIN

api_key_header = APIKeyHeader(name="Authorization", auto_error=False)

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==================== Helper Functions ====================
def get_netflow_bucket(ts: datetime) -> str:
    """گروه‌بندی ۴ ساعته"""
    bucket_hour = (ts.hour // 4) * 4
    bucket_time = ts.replace(hour=bucket_hour, minute=0, second=0, microsecond=0)
    return bucket_time.strftime("%Y-%m-%d %H:00")

def load_cex_wallets_to_db(db: Session):
    """لود کردن والت‌های CEX از CSV به دیتابیس"""
    try:
        if not os.path.exists(CSV_FILE):
            logger.warning(f"⚠️ CSV file {CSV_FILE} not found. Ensure it is uploaded to Railway.")
            return 0
            
        df = pd.read_csv(CSV_FILE)
        
        # --- اصلاح مهم: استفاده از نام ستون‌های صحیح (address به جای wallet) ---
        # بررسی اینکه آیا فایل CSV ستون‌های درست را دارد یا خیر
        if 'address' not in df.columns or 'exchange_name' not in df.columns:
            logger.error(f"❌ CSV format error! Columns found: {df.columns}. Expected: 'address', 'exchange_name'")
            return 0

        # فیلتر فقط Solana (آدرس‌های Solana معمولاً 32-44 کاراکتر و base58 هستند)
        # اصلاح: استفاده از df['address']
        df_sol = df[df['address'].str.len() >= 32]
        
        count = 0
        for _, row in df_sol.iterrows():
            # اصلاح: استفاده از نام ستون‌های فایل CSV
            wallet_str = str(row['address']).strip()        # تغییر از row['wallet']
            label_str = str(row['exchange_name']).strip()   # تغییر از row['label']
            
            # چک کردن اینکه قبلاً وجود داره یا نه
            exists = db.query(CEXWallet).filter(CEXWallet.wallet == wallet_str).first()
            if not exists:
                new_wallet = CEXWallet(wallet=wallet_str, label=label_str)
                db.add(new_wallet)
                count += 1
        
        db.commit()
        logger.info(f"✅ Loaded {count} new CEX wallets from CSV (Total: {db.query(CEXWallet).count()})")
        return count
    except Exception as e:
        logger.error(f"❌ Error loading CEX wallets: {e}")
        # نمایش جزئیات بیشتر برای دیباگ
        logger.error(traceback.format_exc())
        db.rollback()
        return 0

def get_cex_wallets_dict(db: Session) -> Dict[str, str]:
    """گرفتن همه CEX wallets به صورت دیکشنری"""
    wallets = db.query(CEXWallet).all()
    return {w.wallet: w.label for w in wallets}

# ==================== Webhook Processing ====================
def process_helius_webhook(payload: Union[dict, list], db: Session):
    """پردازش داده‌های Helius با آپدیت Real-time Netflow"""
    
    # هندل کردن هر دو فرمت: لیست مستقیم یا دیکشنری با کلید data
    if isinstance(payload, list):
        transactions = payload
        logger.info(f"📦 Received list with {len(transactions)} transactions")
    elif isinstance(payload, dict):
        transactions = payload.get('data', [])
        if not isinstance(transactions, list):
            logger.error(f"❌ Expected list in 'data', got {type(transactions)}")
            return
        logger.info(f"📦 Received dict with {len(transactions)} transactions in 'data'")
    else:
        logger.error(f"❌ Unexpected payload type: {type(payload)}")
        return
    
    cex_wallets = get_cex_wallets_dict(db)
    processed_count = 0
    pippin_count = 0
    
    for tx in transactions:
        try:
            if not isinstance(tx, dict):
                logger.warning(f"⚠️ Skipping non-dict transaction: {type(tx)}")
                continue
                
            sig = tx.get('signature', 'unknown')
            ts_raw = tx.get('timestamp', 0)
            
            # هندل کردن timestamp
            if isinstance(ts_raw, (int, float)):
                ts = datetime.fromtimestamp(ts_raw)
            else:
                ts = datetime.now()
            
            bucket = get_netflow_bucket(ts)
            slot = tx.get('slot', 0)
            
            # پردازش Token Transfers (PIPPIN)
            token_transfers = tx.get('tokenTransfers', [])
            if not isinstance(token_transfers, list):
                token_transfers = []
            
            for transfer in token_transfers:
                if not isinstance(transfer, dict):
                    continue
                    
                mint = transfer.get('mint', '')
                
                # فیلتر فقط برای توکن PIPPIN
                if mint != TOKEN_ADDRESS:
                    continue
                
                pippin_count += 1
                from_addr = transfer.get('fromUserAccount', '')
                to_addr = transfer.get('toUserAccount', '')
                
                # پارس کردن مقدار
                amount_str = transfer.get('tokenAmount', '0')
                try:
                    amount = float(amount_str)
                except (ValueError, TypeError):
                    amount = 0.0
                
                # تشخیص CEX
                cex_from = cex_wallets.get(from_addr)
                cex_to = cex_wallets.get(to_addr)
                
                # تعیین نوع تراکنش
                tx_type = None
                if cex_from and not cex_to:
                    tx_type = "outflow"  # خروج از CEX
                elif cex_to and not cex_from:
                    tx_type = "inflow"   # ورود به CEX
                elif cex_from and cex_to:
                    tx_type = "internal"  # جابجایی داخلی بین CEXها
                
                if tx_type:
                    # چک کردن duplicate بودن تراکنش
                    exists = db.query(Transaction).filter(Transaction.signature == sig).first()
                    if not exists:
                        # 1. ذخیره تراکنش در جدول transactions
                        new_tx = Transaction(
                            signature=sig,
                            timestamp=ts,
                            slot=slot,
                            wallet_from=from_addr,
                            wallet_to=to_addr,
                            amount=amount,
                            token=TOKEN_ADDRESS,
                            tx_type=tx_type,
                            cex_from=cex_from,
                            cex_to=cex_to,
                            netflow_bucket=bucket
                        )
                        db.add(new_tx)
                        
                        # 2. آپدیت Real-time Aggregation در netflow_hourly
                        bucket_dt = datetime.strptime(bucket, "%Y-%m-%d %H:00")
                        
                        netflow_record = db.query(NetflowHourly).filter(
                            NetflowHourly.bucket_time == bucket_dt
                        ).first()
                        
                        if not netflow_record:
                            netflow_record = NetflowHourly(
                                bucket_time=bucket_dt,
                                inflow_amount=0.0,
                                outflow_amount=0.0,
                                net_flow=0.0,
                                tx_count=0
                            )
                            db.add(netflow_record)
                        
                        # آپدیت مقادیر بر اساس نوع تراکنش
                        if tx_type == "inflow":
                            netflow_record.inflow_amount += amount
                            netflow_record.net_flow += amount
                        elif tx_type == "outflow":
                            netflow_record.outflow_amount += amount
                            netflow_record.net_flow -= amount
                        
                        netflow_record.tx_count += 1
                        
                        processed_count += 1
                        logger.info(f"💾 SAVED: {tx_type.upper()} | {amount:,.2f} PIPPIN | Sig: {sig[:8]}...")
            
        except Exception as e:
            sig_str = tx.get('signature', 'unknown') if isinstance(tx, dict) else 'unknown'
            logger.error(f"❌ Error processing tx {sig_str}: {e}")
            logger.error(traceback.format_exc()) 
            continue
    
    # کامیت نهایی به دیتابیس
    if processed_count > 0:
        try:
            db.commit()
            logger.info(f"✅ SUCCESSFULLY committed {processed_count} transactions to PostgreSQL")
        except Exception as e:
            logger.error(f"❌ Database commit error: {e}")
            db.rollback()
    else:
        logger.info(f"ℹ️ No new CEX transactions to save. (Total PIPPIN transfers seen in payload: {pippin_count})")

# ==================== Endpoints ====================
@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Starting up...")
    
    # ساخت جداول
    Base.metadata.create_all(bind=engine)
    logger.info("✅ Database tables created/verified")
    
    # لود کردن CEX wallets
    db = SessionLocal()
    try:
        load_cex_wallets_to_db(db)
    finally:
        db.close()

@app.post("/webhook")
async def receive_webhook(
    request: Request, 
    api_key: str = Security(api_key_header),
    db: Session = Depends(get_db)
):
    """دریافت Webhook از Helius"""
    # بررسی Auth
    if api_key and f"Bearer {WEBHOOK_SECRET}" != api_key:
        logger.warning(f"⚠️ Unauthorized webhook attempt. Provided key: {api_key}")
        raise HTTPException(status_code=403, detail="Invalid auth")
    
    try:
        payload = await request.json()
        
        logger.info(f"📩 Webhook received | Type: {type(payload).__name__}")
        
        # پردازش
        process_helius_webhook(payload, db)
        
        # محاسبه تعداد برای ریسپانس
        if isinstance(payload, list):
            count = len(payload)
        elif isinstance(payload, dict):
            count = len(payload.get('data', []))
        else:
            count = 0
            
        return {
            "status": "ok", 
            "processed": count,
            "type": type(payload).__name__
        }
        
    except Exception as e:
        logger.error(f"❌ Webhook endpoint error: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/netflow/current")
async def get_current_netflow(hours: int = 24, db: Session = Depends(get_db)):
    """مشاهده Netflow فعلی (از جدول تجمیعی netflow_hourly)"""
    try:
        since = datetime.utcnow() - timedelta(hours=hours)
        
        query = text("""
            SELECT 
                bucket_time,
                inflow_amount,
                outflow_amount,
                net_flow,
                tx_count
            FROM netflow_hourly 
            WHERE bucket_time > :since
            ORDER BY bucket_time DESC
        """)
        
        result = db.execute(query, {"since": since})
        rows = result.fetchall()
        
        data = []
        for row in rows:
            data.append({
                "bucket_time": row.bucket_time.strftime("%Y-%m-%d %H:00"),
                "inflow_amount": float(row.inflow_amount) if row.inflow_amount else 0,
                "outflow_amount": float(row.outflow_amount) if row.outflow_amount else 0,
                "net_flow": float(row.net_flow) if row.net_flow else 0,
                "tx_count": row.tx_count
            })
        
        return {
            "hours": hours,
            "data": data,
            "total_records": len(data)
        }
    except Exception as e:
        logger.error(f"Error in netflow query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    """آمار کلی"""
    try:
        total_tx = db.query(Transaction).count()
        total_cex = db.query(CEXWallet).count()
        recent_tx = db.query(Transaction).filter(
            Transaction.timestamp > datetime.utcnow() - timedelta(hours=24)
        ).count()
        
        total_buckets = db.query(NetflowHourly).count()
        latest_bucket = db.query(NetflowHourly).order_by(NetflowHourly.bucket_time.desc()).first()
        
        last_tx = db.query(Transaction).order_by(Transaction.timestamp.desc()).limit(5).all()
        last_tx_data = [{
            "signature": t.signature[:20] + "...",
            "type": t.tx_type,
            "amount": t.amount,
            "from": t.cex_from or "Unknown",
            "to": t.cex_to or "Unknown",
            "time": t.timestamp.strftime("%Y-%m-%d %H:%M")
        } for t in last_tx]
        
        return {
            "total_transactions": total_tx,
            "total_cex_wallets": total_cex,
            "last_24h_transactions": recent_tx,
            "netflow_buckets": total_buckets,
            "latest_bucket": {
                "time": latest_bucket.bucket_time.strftime("%Y-%m-%d %H:00"),
                "net_flow": latest_bucket.net_flow,
                "tx_count": latest_bucket.tx_count
            } if latest_bucket else None,
            "last_transactions": last_tx_data
        }
    except Exception as e:
        logger.error(f"Error in stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health(db: Session = Depends(get_db)):
    """چک کردن سلامت سرور و دیتابیس"""
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
        
        tx_count = db.query(Transaction).count()
        netflow_count = db.query(NetflowHourly).count()
        cex_count = db.query(CEXWallet).count()
        
    except Exception as e:
        db_status = f"error: {str(e)}"
        tx_count = 0
        netflow_count = 0
        cex_count = 0
    
    return {
        "status": "ok", 
        "database": db_status,
        "transactions_count": tx_count,
        "netflow_buckets_count": netflow_count,
        "cex_wallets_loaded": cex_count,
        "token": TOKEN_ADDRESS[:10] + "...",
        "mode": "PostgreSQL"
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
