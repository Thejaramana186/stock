from fastapi import FastAPI, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from pathlib import Path
import csv
import time
import asyncio

import pandas as pd
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from prometheus_fastapi_instrumentator import Instrumentator

from app.db import engine, get_db, Base
from app.models import Company, DailyOHLC
from app import crud, fetcher



Base.metadata.create_all(bind=engine)

app = FastAPI(title="Nifty 50 Stock Data Fetcher")
Instrumentator().instrument(app).expose(app)

BASE_DIR = Path(__file__).resolve().parent

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

scheduler = BackgroundScheduler(timezone="Asia/Kolkata")




def load_companies_from_csv(db: Session):
    csv_path = BASE_DIR.parent / "tickers.csv"
    if not csv_path.exists():
        print("❌ tickers.csv not found")
        return

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            crud.get_or_create_company(
                db,
                symbol=row["symbol"],
                name=row["name"],
                yahoo_ticker=row["yahoo_ticker"],
            )

    print("✅ Companies loaded from CSV")




def scheduled_auto_fetch():
    print("🔄 Running scheduled auto-fetch")
    db = next(get_db())

    try:
        companies = crud.get_all_companies(db)

        if not companies:
            load_companies_from_csv(db)
            companies = crud.get_all_companies(db)

        for i, company in enumerate(companies):
            if i > 0:
                time.sleep(0.5)  

            last_date = crud.get_latest_date(db, company.id)
            start_date = (
                (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                if last_date else "2000-01-01"
            )

            data = fetcher.fetch_historical_data(
                company.yahoo_ticker,
                start_date=start_date,
            )

            if data:
                inserted = crud.bulk_insert_ohlc(db, company.id, data)
                print(f"✔ {company.symbol}: {inserted} rows")
            else:
                print(f"⏩ {company.symbol}: no new data")

        print("✅ Auto-fetch complete")

    except Exception as e:
        print("❌ Auto-fetch failed:", e)

    finally:
        db.close()




@app.on_event("startup")
async def start_scheduler():
    await asyncio.sleep(1)

    if not scheduler.running:
        scheduler.add_job(
            scheduled_auto_fetch,
            trigger="interval",
            hours=24,
            id="daily_fetch",
            replace_existing=True,
        )

        scheduler.add_job(
            scheduled_auto_fetch,
            trigger="date",
            run_date=datetime.now() + timedelta(seconds=5),
            id="startup_fetch",
            replace_existing=True,
        )

        scheduler.start()
        print("🟢 Scheduler started")




@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: Session = Depends(get_db)):
    companies = crud.get_all_companies(db)

    stats = [
        {
            "company": c,
            "count": crud.get_data_count(db, c.id),
            "latest_date": crud.get_latest_date(db, c.id),
        }
        for c in companies
    ]

    return templates.TemplateResponse(
        "index.html",
        {"request": request, "companies": stats},
    )






@app.get("/api/nifty")
def get_nifty(db: Session = Depends(get_db)):
    rows = (
        db.query(DailyOHLC)
        .join(Company)
        .filter(Company.yahoo_ticker == "^NSEI")
        .order_by(DailyOHLC.date)
        .all()
    )

    data = []

    for r in rows:
        
        dt = datetime.combine(r.date, datetime.min.time())

        data.append({
            "time": int(dt.timestamp()),   
            "open": float(r.open),
            "high": float(r.high),
            "low": float(r.low),
            "close": float(r.close),
        })

    return data




@app.get("/api/nifty", response_class=JSONResponse)
async def nifty_live():
    try:
        df = yf.download(
            "^NSEI",
            period="5d",
            interval="5m",
            progress=False,
            auto_adjust=True,
        )

        if df.empty:
            df = yf.download(
                "^NSEI",
                period="1mo",
                interval="1d",
                progress=False,
                auto_adjust=True,
            )

        if df.empty:
            return []

        df.reset_index(inplace=True)
        
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        data = []
        
        
        time_col = "Datetime" if "Datetime" in df.columns else "Date"

        for _, row in df.iterrows():
            try:
                dt = pd.to_datetime(row[time_col], errors="coerce")
                if pd.isna(dt):
                    continue

                timestamp = int(dt.timestamp())
                
                open_val = float(row["Open"])
                high_val = float(row["High"])
                low_val = float(row["Low"])
                close_val = float(row["Close"])

                if any(pd.isna(v) for v in [open_val, high_val, low_val, close_val]):
                    continue

                data.append({
                    "time": timestamp,
                    "open": open_val,
                    "high": high_val,
                    "low": low_val,
                    "close": close_val,
                })
            except Exception as e:
                continue

        return data

    except Exception as e:
        print(f"Error fetching NIFTY data: {e}")
        return []




@app.get("/company/{symbol}", response_class=HTMLResponse)
async def company_detail(symbol: str, request: Request, db: Session = Depends(get_db)):
    company = crud.get_company_by_symbol(db, symbol)
    if not company:
        return HTMLResponse("Company not found", status_code=404)

    data = crud.get_company_data(db, company.id)[:100]

    return templates.TemplateResponse(
        "company.html",
        {
            "request": request,
            "company": company,
            "data": data,
            "total_records": crud.get_data_count(db, company.id),
        },
    )


@app.get("/api/company/{symbol}/history", response_class=JSONResponse)
def company_history(symbol: str, db: Session = Depends(get_db)):
    company = crud.get_company_by_symbol(db, symbol)
    if not company:
        return []

    rows = (
        db.query(DailyOHLC)
        .filter(DailyOHLC.company_id == company.id)
        .order_by(DailyOHLC.date)
        .all()
    )

    return [
        {
            "date": r.date.strftime("%Y-%m-%d"),  # IMPORTANT
            "open": float(r.open),
            "high": float(r.high),
            "low": float(r.low),
            "close": float(r.close),
        }
        for r in rows
    ]


@app.post("/api/fetch-company/{symbol}")
async def api_fetch_company(symbol: str, db: Session = Depends(get_db)):
    company = crud.get_company_by_symbol(db, symbol)
    if not company:
        return {"error": "Company not found"}

    last_date = crud.get_latest_date(db, company.id)
    start_date = (
        (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        if last_date else "2000-01-01"
    )

    data = fetcher.fetch_historical_data(
        company.yahoo_ticker,
        start_date=start_date,
    )

    if not data:
        return {"message": "No new data"}

    inserted = crud.bulk_insert_ohlc(db, company.id, data)
    return {"symbol": symbol, "inserted": inserted}




if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
