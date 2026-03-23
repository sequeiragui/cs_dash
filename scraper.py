"""
CS2 Market Intelligence — Scraper Module
==========================================
Importable scraper functions for use by FastAPI + APScheduler.
Also runnable standalone: python scraper.py [--backfill|--status|--export]
"""
import os
import re
import sys
import json
import time
import sqlite3
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── CONFIG ─────────────────────────────────────────────────────────────────

DB_PATH = Path(os.environ.get("DB_PATH", Path(__file__).parent / "cs2_intel.db"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

TRACKED_SKINS = [
    "AK-47 | Redline (Field-Tested)",
    "AK-47 | Asiimov (Field-Tested)",
    "AK-47 | Vulcan (Field-Tested)",
    "AK-47 | Fire Serpent (Field-Tested)",
    "M4A1-S | Hyper Beast (Field-Tested)",
    "M4A4 | Asiimov (Field-Tested)",
    "M4A4 | The Emperor (Field-Tested)",
    "AWP | Asiimov (Field-Tested)",
    "AWP | Redline (Field-Tested)",
    "AWP | Chromatic Aberration (Field-Tested)",
    "Desert Eagle | Blaze (Factory New)",
    "Glock-18 | Fade (Factory New)",
    "Glock-18 | Water Elemental (Field-Tested)",
    "USP-S | Kill Confirmed (Field-Tested)",
    "USP-S | Printstream (Field-Tested)",
    "Revolution Case",
    "Kilowatt Case",
    "Gallery Case",
    "Dreams & Nightmares Case",
    "Recoil Case",
]

STEAM_FEE = 0.15
SKINPORT_FEE = 0.13

# Steam login cookie — needed for price history backfill
# Get it from your browser: open Steam Community Market while logged in,
# DevTools → Application → Cookies → steamLoginSecure
# Set via env var or paste here
from urllib.parse import unquote
STEAM_COOKIE = unquote(os.environ.get("STEAM_COOKIE", ""))

CATEGORY_RULES = [
    ("new_case",         ["new case", "case release", "introducing the", "case is now", "case has been"]),
    ("operation",        ["operation", "new operation", "operation pass"]),
    ("balance_change",   ["damage", "recoil", "accuracy", "fire rate", "reload", "movement speed",
                          "armor penetration", "nerf", "buff", "weapon balance", "spray pattern",
                          "falloff", "magazine", "ammo"]),
    ("major_tournament", ["major", "championship", "sticker", "rmr", "tournament", "qualifier",
                          "legends stage", "champions stage", "pick'em"]),
    ("map_update",       ["map", "dust2", "mirage", "inferno", "nuke", "overpass", "ancient",
                          "anubis", "vertigo", "active duty", "map pool"]),
    ("anticheat_vac",    ["vac", "anti-cheat", "trust factor", "overwatch", "ban wave", "cheating"]),
    ("trade_policy",     ["trade hold", "trade cooldown", "community market", "steam guard",
                          "trade restriction", "market restriction"]),
    ("ui_misc",          ["ui", "hud", "scoreboard", "menu", "sound", "audio", "visual",
                          "bug fix", "crash fix", "stability"]),
]

WEAPON_KEYWORDS = [
    "ak-47", "m4a1-s", "m4a4", "awp", "desert eagle", "deagle", "glock",
    "usp-s", "p250", "five-seven", "tec-9", "cz75", "famas", "galil",
    "aug", "sg 553", "ssg 08", "scout", "mac-10", "mp9", "mp7", "mp5",
    "ump-45", "p90", "pp-bizon", "nova", "xm1014", "sawed-off", "mag-7",
    "m249", "negev", "knife", "karambit", "butterfly",
]


# ── DATABASE ───────────────────────────────────────────────────────────────

def get_db():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS updates (
            id           TEXT PRIMARY KEY,
            title        TEXT NOT NULL,
            date         TEXT NOT NULL,
            timestamp    INTEGER NOT NULL,
            categories   TEXT NOT NULL DEFAULT '[]',
            weapons      TEXT NOT NULL DEFAULT '[]',
            content      TEXT,
            url          TEXT,
            author       TEXT,
            created_at   TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS price_snapshots (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            skin_name    TEXT NOT NULL,
            source       TEXT NOT NULL DEFAULT 'steam',
            lowest_price REAL,
            median_price REAL,
            volume       TEXT,
            timestamp    INTEGER NOT NULL,
            date         TEXT NOT NULL,
            created_at   TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS skinport_prices (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            skin_name       TEXT NOT NULL,
            suggested_price REAL,
            min_price       REAL,
            max_price       REAL,
            median_price    REAL,
            quantity        INTEGER,
            timestamp       INTEGER NOT NULL,
            date            TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS reddit_posts (
            id         TEXT PRIMARY KEY,
            title      TEXT NOT NULL,
            date       TEXT NOT NULL,
            content    TEXT,
            url        TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS spreads (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            skin_name     TEXT NOT NULL,
            steam_buy     REAL,
            steam_sell    REAL,
            sp_buy        REAL,
            sp_sell       REAL,
            play_a_profit REAL,
            play_b_profit REAL,
            best_profit   REAL,
            signal        TEXT,
            timestamp     INTEGER NOT NULL,
            date          TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_prices_skin_ts ON price_snapshots(skin_name, timestamp);
        CREATE INDEX IF NOT EXISTS idx_updates_ts ON updates(timestamp);
        CREATE INDEX IF NOT EXISTS idx_spreads_ts ON spreads(timestamp);
    """)
    db.commit()
    db.close()


# ── HELPERS ────────────────────────────────────────────────────────────────

def update_id(title, date):
    return hashlib.md5(f"{date}:{title}".encode()).hexdigest()[:12]

def categorize(title, content):
    text = f"{title} {content}".lower()
    cats = []
    for cat, keywords in CATEGORY_RULES:
        if any(kw in text for kw in keywords):
            cats.append(cat)
    if len(cats) > 1 and "ui_misc" in cats:
        cats.remove("ui_misc")
    return cats or ["ui_misc"]

def extract_weapons(title, content):
    text = f"{title} {content}".lower()
    return [w for w in WEAPON_KEYWORDS if w in text]

def clean_price(raw):
    if not raw: return None
    return float(re.sub(r"[^\d.]", "", raw.replace(",", "")))

def log(msg, indent=0):
    prefix = "  " * indent
    print(f"  {prefix}{msg}", flush=True)

def _parse_news_items(items):
    results = []
    for item in items:
        raw = item.get("contents", "")
        clean = re.sub(r"\{[^}]*\}", "", raw)
        clean = re.sub(r"<[^>]+>", " ", clean)
        clean = re.sub(r"https?://\S+", "", clean)
        clean = re.sub(r"\s+", " ", clean).strip()
        results.append({
            "title":     item.get("title", "").strip(),
            "date":      datetime.fromtimestamp(item.get("date", 0), tz=timezone.utc).strftime("%Y-%m-%d"),
            "timestamp": item.get("date", 0),
            "content":   clean[:2000],
            "url":       item.get("url", ""),
            "author":    item.get("author", ""),
        })
    return results


# ── SCRAPERS ───────────────────────────────────────────────────────────────

def scrape_steam_news(count=30):
    url = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
    params = {"appid": 730, "count": count, "maxlength": 3000, "format": "json"}
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return _parse_news_items(r.json().get("appnews", {}).get("newsitems", []))
    except Exception as e:
        log(f"!! Steam news: {e}")
        return []


def scrape_steam_news_backfill(months=6):
    all_news = []
    end_date = None
    for batch in range(months + 2):
        log(f"Batch {batch + 1}...", 1)
        params = {"appid": 730, "count": 100, "maxlength": 3000, "format": "json"}
        if end_date:
            params["enddate"] = end_date
        try:
            r = requests.get("https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/",
                             params=params, headers=HEADERS, timeout=15)
            r.raise_for_status()
            items = r.json().get("appnews", {}).get("newsitems", [])
            if not items: break
            all_news.extend(_parse_news_items(items))
            end_date = min(item.get("date", 0) for item in items)
            oldest = datetime.fromtimestamp(end_date, tz=timezone.utc).strftime("%Y-%m-%d")
            log(f"→ {len(items)} items, oldest: {oldest}", 2)
            time.sleep(1)
        except Exception as e:
            log(f"!! Batch error: {e}", 2)
            break

    seen = set()
    unique = []
    for item in all_news:
        uid = update_id(item["title"], item["date"])
        if uid not in seen:
            seen.add(uid)
            unique.append(item)
    return unique


def scrape_steam_prices():
    results = {}
    for skin in TRACKED_SKINS:
        try:
            r = requests.get("https://steamcommunity.com/market/priceoverview/",
                             params={"appid": 730, "currency": 1, "market_hash_name": skin},
                             headers=HEADERS, timeout=8)
            if r.status_code == 429:
                log("Rate limited — waiting 60s", 2)
                time.sleep(60)
                r = requests.get("https://steamcommunity.com/market/priceoverview/",
                                 params={"appid": 730, "currency": 1, "market_hash_name": skin},
                                 headers=HEADERS, timeout=8)
            r.raise_for_status()
            data = r.json()
            if data.get("success"):
                lowest = clean_price(data.get("lowest_price"))
                median = clean_price(data.get("median_price"))
                if median and lowest and median < lowest * 0.5:
                    median = None
                results[skin] = {"lowest_price": lowest, "median_price": median, "volume": data.get("volume", "0")}
        except Exception as e:
            log(f"!! {skin}: {e}", 2)
        time.sleep(2)
    return results


def scrape_price_history(days_back=180):
    """
    Backfill historical prices using Steam's /market/pricehistory/ endpoint.
    Requires a steamLoginSecure cookie. Returns daily median prices + volume
    going back months/years.

    How to get your cookie:
      1. Log into steamcommunity.com in your browser
      2. Open DevTools → Application → Cookies → steamcommunity.com
      3. Copy the value of 'steamLoginSecure'
      4. Set it: export STEAM_COOKIE="your_cookie_value"
    """
    if not STEAM_COOKIE:
        log("!! No STEAM_COOKIE set — can't backfill price history")
        log("   Get it from browser DevTools → Cookies → steamLoginSecure", 1)
        return {}

    cookies = {"steamLoginSecure": STEAM_COOKIE}
    all_history = {}

    for skin in TRACKED_SKINS:
        try:
            url = "https://steamcommunity.com/market/pricehistory/"
            params = {"appid": 730, "market_hash_name": skin}
            r = requests.get(url, params=params, headers=HEADERS, cookies=cookies, timeout=15)

            if r.status_code == 400:
                log("!! Steam cookie expired — re-login and update STEAM_COOKIE")
                return all_history

            if r.status_code == 429:
                log("Rate limited — waiting 60s", 2)
                time.sleep(60)
                r = requests.get(url, params=params, headers=HEADERS, cookies=cookies, timeout=15)

            r.raise_for_status()
            data = r.json()

            if not data.get("success"):
                log(f"!! {skin}: API returned success=false", 2)
                continue

            raw_prices = data.get("prices", [])
            if not raw_prices:
                log(f"!! {skin}: No price data returned (cookie may be invalid)", 2)
                continue

            log(f"   {skin[:30]}: {len(raw_prices)} raw entries from Steam", 2)

            # prices format: ["MMM DD YYYY HH: +0", median_price, "volume"]
            # Aggregate to daily
            daily = {}
            for entry in raw_prices:
                try:
                    date_str = entry[0]
                    price = float(entry[1])
                    volume = int(str(entry[2]).replace(",", "")) if len(entry) > 2 else 0

                    # Robust date parse — extract "Mon DD YYYY" from various formats
                    # Examples: "Mar 18 2026 01: +0", "Mar  8 2026 01: +0"
                    match = re.match(r"(\w{3})\s+(\d{1,2})\s+(\d{4})", date_str)
                    if not match:
                        continue
                    month, day_num, year = match.groups()
                    dt = datetime.strptime(f"{month} {day_num} {year}", "%b %d %Y")
                    day = dt.strftime("%Y-%m-%d")

                    if day not in daily:
                        daily[day] = {"prices": [], "volumes": []}
                    daily[day]["prices"].append(price)
                    daily[day]["volumes"].append(volume)
                except (ValueError, IndexError, TypeError):
                    continue

            # Compute daily median
            if not daily:
                continue

            history = []
            cutoff = datetime.now(timezone.utc).timestamp() - (days_back * 86400)
            for day, d in sorted(daily.items()):
                day_ts = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
                if day_ts < cutoff:
                    continue
                prices = sorted(d["prices"])
                median = prices[len(prices) // 2]
                total_vol = sum(d["volumes"])
                history.append({
                    "date": day,
                    "median_price": round(median, 2),
                    "volume": total_vol,
                })

            all_history[skin] = history
            short = skin.split("|")[1].split("(")[0].strip() if "|" in skin else skin
            log(f"→ {short}: {len(history)} days of history", 2)

        except Exception as e:
            log(f"!! {skin}: {e}", 2)

        time.sleep(3)  # be gentle — ~20 req/min limit

    return all_history


def store_price_history(db, history):
    """Store backfilled price history into price_snapshots table."""
    inserted = 0
    for skin, days in history.items():
        for day in days:
            ts = int(datetime.strptime(day["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

            # Skip if we already have data for this skin+day
            existing = db.execute(
                "SELECT 1 FROM price_snapshots WHERE skin_name=? AND substr(date,1,10)=? LIMIT 1",
                (skin, day["date"])
            ).fetchone()
            if existing:
                continue

            db.execute(
                "INSERT INTO price_snapshots (skin_name, source, lowest_price, median_price, volume, timestamp, date) VALUES (?, 'steam_history', ?, ?, ?, ?, ?)",
                (skin, day["median_price"], day["median_price"], str(day["volume"]), ts, day["date"])
            )
            inserted += 1

    db.commit()
    return inserted


def scrape_skinport():
    try:
        r = requests.get("https://api.skinport.com/v1/items",
                         params={"app_id": 730, "currency": "USD", "tradable": 0},
                         headers={**HEADERS, "Accept-Encoding": "br"}, timeout=15)
        r.raise_for_status()
        tracked = set(TRACKED_SKINS)
        return {
            item["market_hash_name"]: {
                "suggested_price": item.get("suggested_price"),
                "min_price": item.get("min_price"),
                "max_price": item.get("max_price"),
                "median_price": item.get("median_price"),
                "quantity": item.get("quantity", 0),
            }
            for item in r.json()
            if item.get("market_hash_name") in tracked
        }
    except Exception as e:
        log(f"!! Skinport: {e}")
        return {}


def scrape_reddit(limit=15):
    try:
        r = requests.get("https://old.reddit.com/r/csgomarketforum/hot/.rss",
                         headers={"User-Agent": "CS2MarketIntel/1.0", "Accept": "application/rss+xml"}, timeout=10)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        posts = []
        for entry in root.findall("atom:entry", ns)[:limit]:
            title = entry.findtext("atom:title", "", ns)
            updated = entry.findtext("atom:updated", "", ns)
            content = entry.findtext("atom:content", "", ns)
            entry_id = entry.findtext("atom:id", "", ns)
            clean = re.sub(r"<[^>]+>", " ", content or "")
            clean = re.sub(r"&[a-z]+;", " ", clean)
            clean = re.sub(r"\s+", " ", clean).strip()
            posts.append({"id": hashlib.md5((entry_id or title).encode()).hexdigest()[:12],
                          "title": title, "date": updated[:10] if updated else "",
                          "content": clean[:500], "url": entry_id or ""})
        return posts
    except Exception as e:
        log(f"!! Reddit: {e}")
        return []


# ── STORE ──────────────────────────────────────────────────────────────────

def store_updates(db, items):
    new = 0
    for item in items:
        uid = update_id(item["title"], item["date"])
        cats = categorize(item["title"], item["content"])
        weapons = extract_weapons(item["title"], item["content"])
        try:
            db.execute("INSERT OR IGNORE INTO updates (id,title,date,timestamp,categories,weapons,content,url,author) VALUES (?,?,?,?,?,?,?,?,?)",
                       (uid, item["title"], item["date"], item["timestamp"], json.dumps(cats), json.dumps(weapons), item["content"][:2000], item["url"], item.get("author", "")))
            if db.total_changes: new += 1
        except sqlite3.IntegrityError: pass
    db.commit()
    return new

def store_steam_prices(db, prices, ts=None):
    ts = ts or int(datetime.now(timezone.utc).timestamp())
    date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    for skin, p in prices.items():
        db.execute("INSERT INTO price_snapshots (skin_name,source,lowest_price,median_price,volume,timestamp,date) VALUES (?,'steam',?,?,?,?,?)",
                   (skin, p.get("lowest_price"), p.get("median_price"), p.get("volume", "0"), ts, date))
    db.commit()

def store_skinport_prices(db, prices, ts=None):
    ts = ts or int(datetime.now(timezone.utc).timestamp())
    date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    for skin, p in prices.items():
        db.execute("INSERT INTO skinport_prices (skin_name,suggested_price,min_price,max_price,median_price,quantity,timestamp,date) VALUES (?,?,?,?,?,?,?,?)",
                   (skin, p.get("suggested_price"), p.get("min_price"), p.get("max_price"), p.get("median_price"), p.get("quantity", 0), ts, date))
    db.commit()

def store_reddit(db, posts):
    new = 0
    for p in posts:
        try:
            db.execute("INSERT OR IGNORE INTO reddit_posts (id,title,date,content,url) VALUES (?,?,?,?,?)",
                       (p["id"], p["title"], p["date"], p["content"][:500], p.get("url", "")))
            new += 1
        except sqlite3.IntegrityError: pass
    db.commit()
    return new

def store_spreads(db, steam_prices, skinport_prices, ts=None):
    ts = ts or int(datetime.now(timezone.utc).timestamp())
    date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    for skin in TRACKED_SKINS:
        steam = steam_prices.get(skin)
        sport = skinport_prices.get(skin)
        if not steam or not sport: continue
        sb, ss = steam.get("lowest_price"), steam.get("median_price")
        spb, sps = sport.get("min_price"), sport.get("suggested_price")
        pa = round(sps * (1 - SKINPORT_FEE) - sb, 2) if sb and sps else None
        pb = round(ss * (1 - STEAM_FEE) - spb, 2) if spb and ss else None
        profits = [p for p in [pa, pb] if p is not None]
        best = max(profits) if profits else None
        sig = "NO DATA"
        if best is not None:
            if best > 2: sig = "STRONG BUY"
            elif best > 0: sig = "MARGINAL"
            elif best > -3: sig = "NEUTRAL"
            else: sig = "AVOID"
        db.execute("INSERT INTO spreads (skin_name,steam_buy,steam_sell,sp_buy,sp_sell,play_a_profit,play_b_profit,best_profit,signal,timestamp,date) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                   (skin, sb, ss, spb, sps, pa, pb, best, sig, ts, date))
    db.commit()


# ── HIGH-LEVEL COMMANDS ────────────────────────────────────────────────────

def cmd_scrape():
    """Full scrape cycle — called by scheduler and CLI."""
    init_db()
    db = get_db()
    ts = int(datetime.now(timezone.utc).timestamp())

    log(f"Scrape started at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    news = scrape_steam_news(count=30)
    new_updates = store_updates(db, news)
    log(f"News: {len(news)} fetched, {new_updates} new")

    steam = scrape_steam_prices()
    store_steam_prices(db, steam, ts)
    log(f"Steam prices: {len(steam)}/{len(TRACKED_SKINS)}")

    skinport = scrape_skinport()
    if skinport:
        store_skinport_prices(db, skinport, ts)
        log(f"Skinport: {len(skinport)} skins")
    else:
        log("Skinport: failed")

    posts = scrape_reddit()
    store_reddit(db, posts)
    log(f"Reddit: {len(posts)} posts")

    if steam and skinport:
        store_spreads(db, steam, skinport, ts)
        log("Spreads: calculated")

    db.close()
    log("Scrape complete")


def cmd_backfill():
    """Backfill ~6 months of news + price history."""
    init_db()
    db = get_db()

    # 1. News backfill
    log("Backfilling ~6 months of news...")
    news = scrape_steam_news_backfill(months=6)
    new = store_updates(db, news)
    total = db.execute("SELECT COUNT(*) FROM updates").fetchone()[0]
    log(f"News: {len(news)} unique, {new} new, {total} total")

    # 2. Price history backfill (needs STEAM_COOKIE)
    log("\nBackfilling price history...")
    history = scrape_price_history(days_back=180)
    if history:
        inserted = store_price_history(db, history)
        total_prices = db.execute("SELECT COUNT(*) FROM price_snapshots").fetchone()[0]
        log(f"Prices: {inserted} new data points, {total_prices} total")
    else:
        log("No price history (set STEAM_COOKIE env var to enable)")

    db.close()


def get_status():
    """Return database status as dict."""
    init_db()
    db = get_db()
    status = {
        "updates": db.execute("SELECT COUNT(*) FROM updates").fetchone()[0],
        "price_snapshots": db.execute("SELECT COUNT(*) FROM price_snapshots").fetchone()[0],
        "skinport_prices": db.execute("SELECT COUNT(*) FROM skinport_prices").fetchone()[0],
        "spreads": db.execute("SELECT COUNT(*) FROM spreads").fetchone()[0],
        "reddit_posts": db.execute("SELECT COUNT(*) FROM reddit_posts").fetchone()[0],
        "unique_snapshot_times": db.execute("SELECT COUNT(DISTINCT timestamp) FROM price_snapshots").fetchone()[0],
    }
    oldest = db.execute("SELECT date FROM updates ORDER BY timestamp ASC LIMIT 1").fetchone()
    newest = db.execute("SELECT date FROM updates ORDER BY timestamp DESC LIMIT 1").fetchone()
    status["oldest_update"] = oldest["date"] if oldest else None
    status["newest_update"] = newest["date"] if newest else None
    status["tracked_skins"] = TRACKED_SKINS
    status["db_size_kb"] = round(DB_PATH.stat().st_size / 1024) if DB_PATH.exists() else 0
    db.close()
    return status


def get_export_data():
    """Build the full export dict for the frontend API."""
    init_db()
    db = get_db()

    updates = []
    for row in db.execute("SELECT * FROM updates ORDER BY timestamp DESC"):
        updates.append({
            "id": row["id"], "title": row["title"], "date": row["date"],
            "timestamp": row["timestamp"],
            "categories": json.loads(row["categories"]),
            "weapons": json.loads(row["weapons"]),
            "content": row["content"],
        })

    latest_prices = {}
    for skin in TRACKED_SKINS:
        row = db.execute("SELECT * FROM price_snapshots WHERE skin_name=? ORDER BY timestamp DESC LIMIT 1", (skin,)).fetchone()
        if row:
            latest_prices[skin] = {"lowest_price": row["lowest_price"], "median_price": row["median_price"],
                                    "volume": row["volume"], "date": row["date"]}

    price_history = {}
    for skin in TRACKED_SKINS:
        rows = db.execute("""
            SELECT substr(date,1,10) as day, AVG(lowest_price) as avg_low,
                   AVG(median_price) as avg_med, MAX(volume) as max_vol
            FROM price_snapshots WHERE skin_name=? GROUP BY day ORDER BY day
        """, (skin,)).fetchall()
        if rows:
            price_history[skin] = [{"date": r["day"],
                                     "low": round(r["avg_low"], 2) if r["avg_low"] else None,
                                     "med": round(r["avg_med"], 2) if r["avg_med"] else None,
                                     "vol": r["max_vol"]} for r in rows]

    cat_stats = {}
    for row in db.execute("SELECT categories FROM updates"):
        for cat in json.loads(row["categories"]):
            cat_stats[cat] = cat_stats.get(cat, 0) + 1

    spreads = []
    latest_ts = db.execute("SELECT MAX(timestamp) FROM spreads").fetchone()[0]
    if latest_ts:
        for row in db.execute("SELECT * FROM spreads WHERE timestamp=? ORDER BY best_profit DESC", (latest_ts,)):
            spreads.append({"skin": row["skin_name"], "steam_buy": row["steam_buy"],
                            "sp_buy": row["sp_buy"], "best_profit": row["best_profit"], "signal": row["signal"]})

    reddit = []
    for row in db.execute("SELECT * FROM reddit_posts ORDER BY date DESC LIMIT 20"):
        reddit.append({"id": row["id"], "title": row["title"], "date": row["date"], "content": row["content"]})

    db.close()

    return {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "updates": updates,
        "latest_prices": latest_prices,
        "price_history": price_history,
        "category_stats": cat_stats,
        "spreads": spreads,
        "reddit": reddit,
        "meta": {
            "total_updates": len(updates),
            "total_snapshots": sum(len(h) for h in price_history.values()),
            "tracked_skins": TRACKED_SKINS,
        },
    }


# ── CLI ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--backfill" in sys.argv:
        cmd_backfill()
    elif "--status" in sys.argv:
        import pprint; pprint.pprint(get_status())
    elif "--export" in sys.argv:
        data = get_export_data()
        out = Path(__file__).parent / "export.json"
        with open(out, "w") as f: json.dump(data, f, indent=2, default=str)
        print(f"Exported to {out}")
    else:
        cmd_scrape()