"""
CS2 Market Intelligence — FastAPI Backend
===========================================
Serves API endpoints. No automatic scheduling — scrape manually via POST /api/scrape.

    uvicorn main:app --host 0.0.0.0 --port 8000
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from scraper import (
    cmd_scrape, cmd_backfill, get_status, get_export_data,
    init_db, get_db, TRACKED_SKINS,
)
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cs2intel")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing database...")
    init_db()
    status = get_status()
    if status["updates"] == 0:
        logger.warning("Database is empty. Run backfill: POST /api/backfill")
    else:
        logger.info(f"DB ready: {status['updates']} updates, {status['price_snapshots']} price snapshots")
    yield
    logger.info("Shutting down")


app = FastAPI(
    title="CS2 Market Intelligence",
    description="CS2 update tracking, price correlation, and market prediction API",
    version="1.0.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory="static"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── ROUTES ─────────────────────────────────────────────────────────────────

@app.get("/ui")
async def serve_ui():
    return FileResponse("static/index.html")


@app.get("/")
def root():
    return {
        "service": "CS2 Market Intelligence",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": ["/api/data", "/api/status", "/api/updates", "/api/prices/{skin}", "/api/correlations", "/api/spreads", "/api/reddit"],
    }


@app.get("/api/data")
def api_data():
    try:
        return get_export_data()
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get("/api/status")
def api_status():
    try:
        return get_status()
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get("/api/updates")
def api_updates(category: str = None, limit: int = 50):
    db = get_db()
    rows = db.execute("SELECT * FROM updates ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
    db.close()
    updates = []
    for row in rows:
        cats = json.loads(row["categories"])
        if category and category not in cats:
            continue
        updates.append({
            "id": row["id"], "title": row["title"], "date": row["date"],
            "timestamp": row["timestamp"], "categories": cats,
            "weapons": json.loads(row["weapons"]), "content": row["content"],
        })
    return {"updates": updates, "count": len(updates)}


@app.get("/api/prices/{skin}")
def api_price_history(skin: str, days: int = 90):
    skin = skin.replace("_", " ").replace("%7C", "|")
    db = get_db()
    rows = db.execute("""
        SELECT substr(date,1,10) as day, AVG(lowest_price) as avg_low,
               AVG(median_price) as avg_med, MAX(volume) as max_vol
        FROM price_snapshots WHERE skin_name=?
        GROUP BY day ORDER BY day DESC LIMIT ?
    """, (skin, days)).fetchall()
    db.close()
    if not rows:
        raise HTTPException(404, detail=f"No price data for '{skin}'")
    return {
        "skin": skin,
        "history": [{"date": r["day"],
                     "low": round(r["avg_low"], 2) if r["avg_low"] else None,
                     "med": round(r["avg_med"], 2) if r["avg_med"] else None,
                     "vol": r["max_vol"]} for r in reversed(rows)],
        "points": len(rows),
    }


@app.get("/api/correlations")
def api_correlations():
    db = get_db()
    updates = [
        {"id": r["id"], "title": r["title"], "date": r["date"],
         "timestamp": r["timestamp"],
         "categories": json.loads(r["categories"]),
         "weapons": json.loads(r["weapons"])}
        for r in db.execute("SELECT * FROM updates ORDER BY timestamp")
    ]
    results = []
    for update in updates:
        ts = update["timestamp"]
        before = db.execute("""
            SELECT AVG(median_price) as avg_price FROM price_snapshots
            WHERE timestamp BETWEEN ? AND ?
        """, (ts - 172800, ts - 3600)).fetchone()
        after = db.execute("""
            SELECT AVG(median_price) as avg_price FROM price_snapshots
            WHERE timestamp BETWEEN ? AND ?
        """, (ts + 43200, ts + 345600)).fetchone()
        if before["avg_price"] and after["avg_price"] and before["avg_price"] > 0:
            pct = round(((after["avg_price"] - before["avg_price"]) / before["avg_price"]) * 100, 2)
            results.append({**update, "avg_impact": pct})
    db.close()

    cat_agg = {}
    for r in results:
        for cat in r["categories"]:
            if cat not in cat_agg:
                cat_agg[cat] = {"impacts": [], "count": 0}
            cat_agg[cat]["impacts"].append(r["avg_impact"])
            cat_agg[cat]["count"] += 1
    for cat, data in cat_agg.items():
        arr = data["impacts"]
        data["mean"] = round(sum(arr) / len(arr), 2)
        data["min"] = round(min(arr), 2)
        data["max"] = round(max(arr), 2)
        data["std"] = round((sum((x - data["mean"]) ** 2 for x in arr) / len(arr)) ** 0.5, 2) if len(arr) > 1 else 0
        del data["impacts"]

    return {"correlations": results, "by_category": cat_agg, "total": len(results)}


@app.get("/api/spreads")
def api_spreads():
    db = get_db()
    latest_ts = db.execute("SELECT MAX(timestamp) FROM spreads").fetchone()[0]
    if not latest_ts:
        db.close()
        return {"spreads": [], "as_of": None}
    rows = db.execute("SELECT * FROM spreads WHERE timestamp=? ORDER BY best_profit DESC", (latest_ts,)).fetchall()
    db.close()
    return {
        "spreads": [{
            "skin": r["skin_name"], "steam_buy": r["steam_buy"], "steam_sell": r["steam_sell"],
            "sp_buy": r["sp_buy"], "sp_sell": r["sp_sell"],
            "play_a_profit": r["play_a_profit"], "play_b_profit": r["play_b_profit"],
            "best_profit": r["best_profit"], "signal": r["signal"],
        } for r in rows],
        "as_of": rows[0]["date"] if rows else None,
    }


@app.get("/api/reddit")
def api_reddit(limit: int = 20):
    db = get_db()
    rows = db.execute("SELECT * FROM reddit_posts ORDER BY date DESC LIMIT ?", (limit,)).fetchall()
    db.close()
    return {"posts": [{"id": r["id"], "title": r["title"], "date": r["date"], "content": r["content"]} for r in rows]}


# ── ACTIONS ────────────────────────────────────────────────────────────────

@app.post("/api/scrape")
def api_scrape(background_tasks: BackgroundTasks):
    """Trigger a full scrape cycle (runs in background)."""
    background_tasks.add_task(cmd_scrape)
    return {"status": "started", "message": "Scrape running in background"}


@app.post("/api/backfill")
def api_backfill(background_tasks: BackgroundTasks):
    """Trigger news + price history backfill (runs in background)."""
    background_tasks.add_task(cmd_backfill)
    return {"status": "started", "message": "Backfilling ~6 months of news in background"}