# ============================================================
# GLOBAL DATA ENGINE v1.4.7
# MicroCap Catalyst Radar API
# Yahoo Finance (screener + chart) + SEC EDGAR
# Gunicorn Safe + AgenticTrade Auth
# ============================================================

import os
import re
import json
import time
import math
import sqlite3
import hashlib
import threading
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request


# ============================================================
# CONFIG
# ============================================================

APP_NAME = "GlobalDataEngine"
VERSION = "1.4.7"

DB_FILE = "data_engine.db"
JSON_FILE = "api_database.json"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "1800"))

MAX_STOCK_PRICE = float(os.getenv("MAX_STOCK_PRICE", "5.0"))
MIN_STOCK_PRICE = float(os.getenv("MIN_STOCK_PRICE", "0.01"))
STOCK_LIMIT = int(os.getenv("STOCK_LIMIT", "200"))

MASTER_API_KEY = os.getenv("MASTER_API_KEY", "")
DEV_API_KEY = os.getenv("DEV_API_KEY", "dev-master-key-change-me")
RAPIDAPI_PROXY_SECRET = os.getenv("RAPIDAPI_PROXY_SECRET", "")
AGENTICTRADE_AUTH = os.getenv("AGENTICTRADE_AUTH", "")

RATE_LIMIT_RPM = int(os.getenv("RATE_LIMIT_RPM", "120"))

SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT",
    "Hasan Kurt MicroCapRadar hasankurt683@gmail.com"
)

STOCK_WATCHLIST = [
    x.strip().upper()
    for x in os.getenv("STOCK_WATCHLIST", "").split(",")
    if x.strip()
]


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/153.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*"
})


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL STATE
# ============================================================

SEC_TICKER_CACHE = {}
SEC_TICKER_CACHE_TIME = 0

API_USAGE = {}

SCAN_LOCK = threading.Lock()

LAST_SCAN = {
    "started_at": None,
    "finished_at": None,
    "stocks": 0,
    "ecommerce": 0,
    "b2b": 0,
    "status": "never"
}


# ============================================================
# DATABASE
# ============================================================

def get_db():
    conn = sqlite3.connect(
        DB_FILE,
        timeout=30,
        check_same_thread=False
    )

    conn.row_factory = sqlite3.Row

    return conn


def init_db():

    conn = get_db()

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS records (
            record_id TEXT PRIMARY KEY,
            category TEXT NOT NULL,
            symbol TEXT,
            data_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT,
            symbol TEXT,
            old_data TEXT,
            new_data TEXT,
            changed_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_type TEXT,
            started_at TEXT,
            finished_at TEXT,
            record_count INTEGER,
            status TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS api_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key TEXT,
            endpoint TEXT,
            used_at TEXT
        )
    """)

    conn.commit()
    conn.close()

    print("[DB] Initialized")


# ============================================================
# HELPERS
# ============================================================

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def stable_id(category, symbol=None):

    if symbol:
        raw = f"{category}|{symbol}"
    else:
        raw = f"{category}|generic"

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:32]


def safe_float(value, default=0.0):

    try:
        if value is None:
            return default

        value = float(value)

        if math.isnan(value) or math.isinf(value):
            return default

        return value

    except Exception:
        return default


def safe_int(value, default=0):

    try:
        return int(value)
    except Exception:
        return default


def save_record(category, data, symbol=None):

    try:

        conn = get_db()
        cur = conn.cursor()

        record_id = stable_id(category, symbol)

        timestamp = now_iso()

        new_json = json.dumps(
            data,
            ensure_ascii=False,
            sort_keys=True
        )

        cur.execute(
            """
            SELECT data_json
            FROM records
            WHERE record_id=?
            """,
            (record_id,)
        )

        old = cur.fetchone()

        if old:
            old_json = old["data_json"]

            if old_json != new_json:

                cur.execute(
                    """
                    INSERT INTO changes
                    (
                        category,
                        symbol,
                        old_data,
                        new_data,
                        changed_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        category,
                        symbol,
                        old_json,
                        new_json,
                        timestamp
                    )
                )

            cur.execute(
                """
                UPDATE records
                SET
                    data_json=?,
                    updated_at=?
                WHERE record_id=?
                """,
                (
                    new_json,
                    timestamp,
                    record_id
                )
            )

        else:

            cur.execute(
                """
                INSERT INTO records
                (
                    record_id,
                    category,
                    symbol,
                    data_json,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record_id,
                    category,
                    symbol,
                    new_json,
                    timestamp,
                    timestamp
                )
            )

        conn.commit()
        conn.close()

    except Exception as e:

        print(f"[DB WRITE ERROR] {category}/{symbol}: {e}")


# ============================================================
# YAHOO FINANCE
# ============================================================

def yahoo_chart(symbol):

    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{symbol}"
    )

    params = {
        "range": "1mo",
        "interval": "1d",
        "includePrePost": "true"
    }

    try:

        r = session.get(
            url,
            params=params,
            timeout=15
        )

        r.raise_for_status()

        return r.json()

    except Exception as e:

        print(f"[YAHOO ERROR] {symbol}: {e}")

        return None


def get_yahoo_screeners():

    names = [
        "most_actives",
        "day_gainers",
        "small_cap_gainers",
        "aggressive_small_caps",
        "most_shorted_stocks"
    ]

    result = {}

    for name in names:

        url = (
            "https://query1.finance.yahoo.com/"
            "v1/finance/screener/predefined/saved"
        )

        params = {
            "scrIds": name,
            "count": 250
        }

        try:

            r = session.get(
                url,
                params=params,
                timeout=20
            )

            r.raise_for_status()

            data = r.json()

            quotes = (
                data
                .get("finance", {})
                .get("result", [{}])[0]
                .get("quotes", [])
            )

            result[name] = quotes

            print(
                f"[SCREENER] {name}: "
                f"{len(quotes)} symbols"
            )

        except Exception as e:

            print(f"[SCREENER ERROR] {name}: {e}")

            result[name] = []

    return result


# ============================================================
# SYMBOL FILTER
# ============================================================

def is_valid_stock_symbol(symbol):

    if not symbol:
        return False

    symbol = symbol.upper().strip()

    if not re.fullmatch(
        r"[A-Z][A-Z0-9.\-]{0,9}",
        symbol
    ):
        return False

    banned_exact = {
        "BRK.A",
        "BRK.B"
    }

    if symbol in banned_exact:
        return False

    if re.search(
        r"\.(WS|WT|WW|U|UN|RT)$",
        symbol
    ):
        return False

    if (
        len(symbol) == 5
        and symbol.endswith("W")
        and "." not in symbol
    ):
        return False

    return True


# ============================================================
# SEC
# ============================================================

def load_sec_ticker_map():

    global SEC_TICKER_CACHE
    global SEC_TICKER_CACHE_TIME

    if (
        SEC_TICKER_CACHE
        and time.time() - SEC_TICKER_CACHE_TIME < 86400
    ):
        return SEC_TICKER_CACHE

    url = "https://www.sec.gov/files/company_tickers.json"

    headers = {
        "User-Agent": SEC_USER_AGENT
    }

    try:

        r = requests.get(
            url,
            headers=headers,
            timeout=30
        )

        r.raise_for_status()

        raw = r.json()

        mapping = {}

        for item in raw.values():

            ticker = str(
                item.get("ticker", "")
            ).upper()

            cik = str(
                item.get("cik_str", "")
            ).zfill(10)

            title = item.get(
                "title",
                ""
            )

            if ticker:
                mapping[ticker] = {
                    "cik": cik,
                    "title": title
                }

        SEC_TICKER_CACHE = mapping
        SEC_TICKER_CACHE_TIME = time.time()

        print(
            f"[SEC] Ticker map loaded: "
            f"{len(mapping)} symbols"
        )

        return mapping

    except Exception as e:

        print(f"[SEC ERROR] ticker map: {e}")

        return {}


def get_sec_filings(symbol):

    mapping = load_sec_ticker_map()

    info = mapping.get(symbol.upper())

    if not info:
        return {
            "company_name": "",
            "filings": []
        }

    cik = info["cik"]

    url = (
        "https://data.sec.gov/submissions/"
        f"CIK{cik}.json"
    )

    headers = {
        "User-Agent": SEC_USER_AGENT
    }

    try:

        r = requests.get(
            url,
            headers=headers,
            timeout=30
        )

        r.raise_for_status()

        data = r.json()

        recent = (
            data
            .get("filings", {})
            .get("recent", {})
        )

        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        cutoff = (
            datetime.now(timezone.utc)
            - timedelta(days=14)
        ).date()

        allowed_forms = {
            "8-K",
            "10-Q",
            "10-K",
            "6-K",
            "20-F",
            "S-1",
            "S-3",
            "S-8",
            "424B2",
            "424B3",
            "424B4",
            "424B5"
        }

        filings = []

        for i, form in enumerate(forms):

            if form not in allowed_forms:
                continue

            if i >= len(dates):
                continue

            try:
                filing_date = datetime.strptime(
                    dates[i],
                    "%Y-%m-%d"
                ).date()
            except Exception:
                continue

            if filing_date < cutoff:
                continue

            accession = (
                accessions[i]
                if i < len(accessions)
                else ""
            )

            accession_clean = accession.replace("-", "")

            document = (
                primary_docs[i]
                if i < len(primary_docs)
                else ""
            )

            filing_url = (
                "https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/"
                f"{accession_clean}/"
                f"{document}"
            )

            filings.append({
                "accession_number": accession,
                "filing_date": dates[i],
                "form": form,
                "url": filing_url
            })

            if len(filings) >= 10:
                break

        return {
            "company_name": info.get("title", ""),
            "filings": filings
        }

    except Exception as e:

        print(f"[SEC ERROR] {symbol}: {e}")

        return {
            "company_name": info.get("title", ""),
            "filings": []
        }


# ============================================================
# SEC CATALYST / DILUTION ANALYSIS
# ============================================================

def analyze_sec_catalyst(filings):

    score = 0
    catalysts = []
    dilution_risk = "UNKNOWN"

    if not filings:
        return {
            "score": 0,
            "catalysts": [],
            "dilution": "UNKNOWN",
            "quality": 0
        }

    recent_8k = 0
    recent_10q = 0
    recent_10k = 0
    registration_forms = 0
    offering_forms = 0
    insider_forms = 0

    for filing in filings:

        form = filing.get("form", "")

        if form == "8-K":
            recent_8k += 1

        elif form == "10-Q":
            recent_10q += 1

        elif form == "10-K":
            recent_10k += 1

        if form in {"S-1", "S-3", "S-8"}:
            registration_forms += 1

        if form in {"424B2", "424B3", "424B4", "424B5"}:
            offering_forms += 1

        if form == "S-8":
            insider_forms += 1

    if recent_8k >= 3:
        score += 12
    elif recent_8k == 2:
        score += 9
    elif recent_8k == 1:
        score += 6

    if recent_10q:
        score += 5

    if recent_10k:
        score += 4

    if registration_forms:
        score += 2

    if offering_forms:
        score += 1

    score = min(score, 20)

    if len(filings) > 0:
        catalysts.append(
            f"{len(filings)} recent SEC filing(s)"
        )

    if offering_forms > 0:

        dilution_risk = "HIGH"
        catalysts.append(
            "Recent securities offering filing(s)"
        )

    elif registration_forms > 0:

        dilution_risk = "MEDIUM"
        catalysts.append(
            "Recent securities registration filing(s)"
        )

    else:
        dilution_risk = "UNKNOWN"

    quality = 0

    if recent_8k:
        quality += min(40, recent_8k * 12)

    if recent_10q:
        quality += 20

    if recent_10k:
        quality += 15

    if registration_forms:
        quality += 5

    quality = min(100, quality)

    return {
        "score": score,
        "catalysts": catalysts,
        "dilution": dilution_risk,
        "quality": quality
    }


# ============================================================
# TECHNICAL ANALYSIS
# ============================================================

def calculate_volatility(closes):

    if len(closes) < 3:
        return 0.0

    returns = []

    for i in range(1, len(closes)):

        previous = closes[i - 1]
        current = closes[i]

        if previous <= 0:
            continue

        returns.append(
            ((current / previous) - 1) * 100
        )

    if len(returns) < 2:
        return 0.0

    mean = sum(returns) / len(returns)

    variance = sum(
        (x - mean) ** 2
        for x in returns
    ) / len(returns)

    return math.sqrt(variance)


def calculate_technical_scores(
    price,
    daily_change,
    relative_volume,
    volatility
):

    if daily_change >= 30:
        momentum = 25
    elif daily_change >= 20:
        momentum = 25
    elif daily_change >= 15:
        momentum = 20
    elif daily_change >= 10:
        momentum = 18
    elif daily_change >= 7:
        momentum = 15
    elif daily_change >= 5:
        momentum = 12
    elif daily_change >= 3:
        momentum = 8
    elif daily_change >= 1:
        momentum = 4
    elif daily_change >= 0:
        momentum = 2
    else:
        momentum = 0

    if relative_volume >= 10:
        volume = 25
    elif relative_volume >= 7:
        volume = 25
    elif relative_volume >= 5:
        volume = 23
    elif relative_volume >= 3:
        volume = 20
    elif relative_volume >= 2:
        volume = 15
    elif relative_volume >= 1.5:
        volume = 10
    elif relative_volume >= 1.2:
        volume = 6
    elif relative_volume >= 1:
        volume = 3
    else:
        volume = 0

    if volatility >= 15:
        volatility_label = "EXTREME"
    elif volatility >= 8:
        volatility_label = "HIGH"
    elif volatility >= 4:
        volatility_label = "MEDIUM"
    else:
        volatility_label = "LOW"

    flags = []

    if volatility >= 8:
        flags.append(
            f"High volatility ({volatility:.2f}%)"
        )

    if price < 0.50:
        flags.append("Sub-$0.50 penny stock")

    if price < 1:
        flags.append("Sub-$1 penny stock")

    return {
        "momentum": momentum,
        "volume": volume,
        "volatility": volatility_label,
        "flags": flags
    }


# ============================================================
# CATALYST QUALITY SCORE
# ============================================================

def calculate_catalyst_quality(
    daily_change,
    relative_volume,
    sec_quality,
    dilution
):

    score = 0

    if daily_change >= 10 and relative_volume >= 3:
        score += 35
    elif daily_change >= 7 and relative_volume >= 2:
        score += 28
    elif daily_change >= 5 and relative_volume >= 1.5:
        score += 20
    elif daily_change >= 3:
        score += 12

    if sec_quality >= 80:
        score += 40
    elif sec_quality >= 60:
        score += 32
    elif sec_quality >= 40:
        score += 25
    elif sec_quality >= 20:
        score += 15

    if dilution == "HIGH":
        score -= 20
    elif dilution == "MEDIUM":
        score -= 8

    return max(0, min(100, score))


# ============================================================
# SCREENER SCORE
# ============================================================

def calculate_screener_score(screener_names):

    names = set(screener_names or [])

    score = 0

    if "day_gainers" in names:
        score += 5

    if "small_cap_gainers" in names:
        score += 5

    if "aggressive_small_caps" in names:
        score += 4

    if "most_actives" in names:
        score += 3

    if "most_shorted_stocks" in names:
        score += 4

    if len(names) >= 3:
        score += 2

    return min(15, score)


# ============================================================
# FINAL RADAR SCORE - TRUE 100
# ============================================================

def calculate_radar_score(
    technical,
    sec_score,
    screener_score,
    catalyst_quality
):

    momentum = technical["momentum"]
    volume = technical["volume"]
    sec = min(20, sec_score)
    screener = min(15, screener_score)
    catalyst = round(
        min(100, catalyst_quality) * 15 / 100
    )

    total = momentum + volume + sec + screener + catalyst

    return {
        "momentum": momentum,
        "volume": volume,
        "sec": sec,
        "screener": screener,
        "catalyst": catalyst,
        "total": min(100, total)
    }


def radar_label(score):

    if score >= 75:
        return "HOT"

    if score >= 55:
        return "STRONG_WATCH"

    if score >= 40:
        return "WATCH"

    if score >= 20:
        return "HIGH_RISK"

    return "LOW_PRIORITY"


# ============================================================
# STOCK ANALYSIS
# ============================================================

def analyze_stock(
    symbol,
    screener_names=None,
    pre_radar_score=0
):

    screener_names = screener_names or []

    chart = yahoo_chart(symbol)

    if not chart:
        return None

    try:

        result = chart["chart"]["result"][0]

        meta = result.get("meta", {})

        indicators = result.get("indicators", {})

        quote = indicators.get("quote", [{}])[0]

        closes_raw = quote.get("close", [])
        volumes_raw = quote.get("volume", [])

        closes = []
        volumes = []

        for i, c in enumerate(closes_raw):

            if c is None:
                continue

            closes.append(safe_float(c))

            if i < len(volumes_raw) and volumes_raw[i] is not None:
                volumes.append(safe_int(volumes_raw[i]))
            else:
                volumes.append(0)

        if not closes:
            return None

        price = safe_float(
            meta.get("regularMarketPrice"),
            closes[-1]
        )

        if (
            price < MIN_STOCK_PRICE
            or price > MAX_STOCK_PRICE
        ):
            return None

        previous_close = safe_float(
            meta.get("previousClose")
        )

        if previous_close <= 0:

            if len(closes) >= 2:
                previous_close = closes[-2]

        daily_change = 0.0

        if previous_close > 0:

            daily_change = (
                (price / previous_close) - 1
            ) * 100

        current_volume = (
            volumes[-1] if volumes else 0
        )

        volume_window = (
            volumes[-21:-1] if len(volumes) > 1 else []
        )

        if volume_window:

            average_volume = (
                sum(volume_window) / len(volume_window)
            )

        else:

            average_volume = 0

        if average_volume > 0:

            relative_volume = (
                current_volume / average_volume
            )

        else:

            relative_volume = 0

        volatility = calculate_volatility(closes)

        sec_data = get_sec_filings(symbol)

        company_name = (
            meta.get("longName")
            or meta.get("shortName")
            or sec_data.get("company_name", "")
        )

        filings = sec_data.get("filings", [])

        sec_analysis = analyze_sec_catalyst(filings)

        technical = calculate_technical_scores(
            price,
            daily_change,
            relative_volume,
            volatility
        )

        screener_score = calculate_screener_score(
            screener_names
        )

        catalyst_quality = calculate_catalyst_quality(
            daily_change,
            relative_volume,
            sec_analysis["quality"],
            sec_analysis["dilution"]
        )

        score = calculate_radar_score(
            technical,
            sec_analysis["score"],
            screener_score,
            catalyst_quality
        )

        label = radar_label(score["total"])

        catalysts = []

        if daily_change >= 5:

            catalysts.append(
                f"Strong daily momentum "
                f"({daily_change:.2f}%)"
            )

        if relative_volume >= 2:

            catalysts.append(
                f"Very high relative volume "
                f"({relative_volume:.2f}x)"
            )

        elif relative_volume >= 1.5:

            catalysts.append(
                f"Elevated relative volume "
                f"({relative_volume:.2f}x)"
            )

        for filing in filings[:5]:

            catalysts.append(
                f"SEC {filing['form']} "
                f"filed {filing['filing_date']}"
            )

        for c in sec_analysis["catalysts"]:

            if c not in catalysts:
                catalysts.append(c)

        if sec_analysis["dilution"] == "HIGH":

            catalysts.append("Potential dilution risk")

        risk_flags = list(technical["flags"])

        if sec_analysis["dilution"] == "HIGH":

            risk_flags.append(
                "Recent securities offering "
                "or registration detected"
            )

        risk = {
            "dilution": sec_analysis["dilution"],
            "volatility": technical["volatility"],
            "flags": risk_flags
        }

        market_cap = safe_float(
            meta.get("marketCap")
        )

        record = {
            "symbol": symbol,
            "company_name": company_name,
            "price": round(price, 4),
            "daily_change_pct": round(daily_change, 2),
            "volume": current_volume,
            "average_volume": round(average_volume),
            "relative_volume": round(relative_volume, 2),
            "volatility_pct": round(volatility, 2),
            "market_cap": market_cap,
            "pre_radar_score": pre_radar_score,
            "radar_score": score["total"],
            "label": label,
            "score_breakdown": {
                "momentum": score["momentum"],
                "volume": score["volume"],
                "sec": score["sec"],
                "screener": score["screener"],
                "catalyst": score["catalyst"],
                "total": score["total"]
            },
            "catalyst_quality": catalyst_quality,
            "catalyst": catalysts[:15],
            "risk": risk,
            "screener_names": screener_names,
            "sec_filings": filings,
            "source": "Yahoo Finance + SEC EDGAR"
        }

        return record

    except Exception as e:

        print(f"[STOCK ERROR] {symbol}: {e}")

        return None


# ============================================================
# STOCK SCANNER
# ============================================================

def scan_stocks():

    screeners = get_yahoo_screeners()

    symbol_map = {}

    for screener_name, quotes in screeners.items():

        for quote in quotes:

            symbol = str(
                quote.get("symbol", "")
            ).upper()

            if not is_valid_stock_symbol(symbol):
                continue

            price = safe_float(
                quote.get("regularMarketPrice")
            )

            if (
                price < MIN_STOCK_PRICE
                or price > MAX_STOCK_PRICE
            ):
                continue

            if symbol not in symbol_map:

                symbol_map[symbol] = {
                    "screeners": [],
                    "quote": quote
                }

            symbol_map[symbol]["screeners"].append(
                screener_name
            )

    for symbol in STOCK_WATCHLIST:

        if not is_valid_stock_symbol(symbol):
            continue

        if symbol in symbol_map:
            continue

        symbol_map[symbol] = {
            "screeners": ["watchlist"],
            "quote": {}
        }

    print(
        f"[STOCKS] Unique candidates "
        f"(screener + watchlist): {len(symbol_map)}"
    )

    candidates = []

    for symbol, info in symbol_map.items():

        quote = info["quote"]

        screeners_for_symbol = list(
            dict.fromkeys(info["screeners"])
        )

        change = safe_float(
            quote.get("regularMarketChangePercent")
        )

        volume = safe_int(
            quote.get("regularMarketVolume")
        )

        screener_score = calculate_screener_score(
            screeners_for_symbol
        )

        momentum_score = 0

        if change >= 20:
            momentum_score = 20
        elif change >= 10:
            momentum_score = 15
        elif change >= 5:
            momentum_score = 10
        elif change >= 2:
            momentum_score = 5

        volume_score = 0

        if volume >= 50_000_000:
            volume_score = 20
        elif volume >= 20_000_000:
            volume_score = 15
        elif volume >= 10_000_000:
            volume_score = 10
        elif volume >= 5_000_000:
            volume_score = 5

        pre_score = (
            screener_score
            + momentum_score
            + volume_score
        )

        candidates.append(
            (pre_score, symbol, screeners_for_symbol)
        )

    candidates.sort(
        reverse=True,
        key=lambda x: x[0]
    )

    selected = candidates[:STOCK_LIMIT]

    print(
        f"[STOCKS] Selected for detailed analysis: "
        f"{len(selected)}"
    )

    records = []

    for pre_score, symbol, screener_names in selected:

        try:

            data = analyze_stock(
                symbol,
                screener_names,
                pre_score
            )

            if data:

                records.append(data)

                save_record("stock", data, symbol)

                print(
                    f"[STOCK] {symbol} "
                    f"${data['price']:.4f} "
                    f"pre={pre_score} "
                    f"score={data['radar_score']} "
                    f"{data['label']} "
                    f"vol={data['volume']:,} "
                    f"chg={data['daily_change_pct']:.2f}%"
                )

        except Exception as e:

            print(f"[STOCK ERROR] {symbol}: {e}")

    print(f"[STOCKS] {len(records)} records")

    return records


# ============================================================
# E-COMMERCE
# ============================================================

def scan_ecommerce():

    print("[BARGAIN] scanning...")

    url = "https://books.toscrape.com/"

    records = []

    try:

        r = session.get(url, timeout=20)

        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")

        products = soup.select("article.product_pod")

        for product in products[:20]:

            title_tag = product.select_one("h3 a")
            price_tag = product.select_one(".price_color")

            if not title_tag:
                continue

            title = (
                title_tag.get("title")
                or title_tag.get_text(strip=True)
            )

            price = (
                price_tag.get_text(strip=True)
                if price_tag
                else ""
            )

            data = {
                "title": title,
                "price": price,
                "source": "BooksToScrape"
            }

            records.append(data)

            save_record(
                "ecommerce",
                data,
                hashlib.md5(title.encode()).hexdigest()[:16]
            )

        print(f"[BARGAIN] {len(records)} records")

    except Exception as e:
        print(f"[BARGAIN ERROR] {e}")

    return records


# ============================================================
# B2B
# ============================================================

def scan_b2b():
    return []


# ============================================================
# COMPLETE SCAN
# ============================================================

def complete_scan():

    global LAST_SCAN

    if not SCAN_LOCK.acquire(blocking=False):
        print("[SCAN] Another scan already running.")
        return

    started = now_iso()

    LAST_SCAN = {
        "started_at": started,
        "finished_at": None,
        "stocks": 0,
        "ecommerce": 0,
        "b2b": 0,
        "status": "running"
    }

    scan_id = None

    try:

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO scans (scan_type, started_at, status)
            VALUES (?, ?, ?)
            """,
            ("complete", started, "running")
        )

        scan_id = cur.lastrowid

        conn.commit()
        conn.close()

    except Exception as e:

        print(f"[SCAN DB ERROR] {e}")

    try:

        stocks = scan_stocks()
        ecommerce = scan_ecommerce()
        b2b = scan_b2b()

        finished = now_iso()

        if scan_id is not None:

            try:

                conn = get_db()
                cur = conn.cursor()

                cur.execute(
                    """
                    UPDATE scans
                    SET finished_at=?, record_count=?, status=?
                    WHERE id=?
                    """,
                    (
                        finished,
                        len(stocks) + len(ecommerce) + len(b2b),
                        "success",
                        scan_id
                    )
                )

                conn.commit()
                conn.close()

            except Exception as e:

                print(f"[SCAN UPDATE ERROR] {e}")

        LAST_SCAN = {
            "started_at": started,
            "finished_at": finished,
            "stocks": len(stocks),
            "ecommerce": len(ecommerce),
            "b2b": len(b2b),
            "status": "success"
        }

        print(f"SCAN COMPLETE {finished}")

    except Exception as e:

        print(f"[SCAN LOOP ERROR] {e}")

        finished = now_iso()

        if scan_id is not None:

            try:

                conn = get_db()
                cur = conn.cursor()

                cur.execute(
                    """
                    UPDATE scans
                    SET finished_at=?, status=?
                    WHERE id=?
                    """,
                    (finished, "error", scan_id)
                )

                conn.commit()
                conn.close()

            except Exception:
                pass

        LAST_SCAN = {
            "started_at": started,
            "finished_at": finished,
            "stocks": 0,
            "ecommerce": 0,
            "b2b": 0,
            "status": "error"
        }

    finally:

        SCAN_LOCK.release()


# ============================================================
# BACKGROUND LOOP
# ============================================================

def background_loop():

    print(
        f"[ENGINE] Background scanner started. "
        f"Interval={SCAN_INTERVAL}s"
    )

    time.sleep(15)

    while True:

        try:

            complete_scan()

            try:
                export_json()
            except Exception as e:
                print(f"[EXPORT ERROR] {e}")

        except Exception as e:

            print(f"[SCAN LOOP ERROR] {e}")

        time.sleep(SCAN_INTERVAL)


# ============================================================
# API AUTH
# ============================================================

def check_auth():

    # AgenticTrade: Bearer token (any)
    auth_header = request.headers.get("Authorization", "")

    if auth_header.startswith("Bearer "):

        token = auth_header.replace("Bearer ", "").strip()

        if AGENTICTRADE_AUTH:
            if token == AGENTICTRADE_AUTH:
                return True
        elif token:
            return True

    # RapidAPI proxy secret
    rapidapi_key = request.headers.get(
        "X-RapidAPI-Proxy-Secret"
    )

    if (
        RAPIDAPI_PROXY_SECRET
        and rapidapi_key == RAPIDAPI_PROXY_SECRET
    ):
        return True

    # Direct API key
    supplied = (
        request.headers.get("X-API-Key")
        or request.args.get("api_key")
    )

    if not MASTER_API_KEY:
        return supplied in {None, "", DEV_API_KEY}

    return supplied in {MASTER_API_KEY, DEV_API_KEY}


def rate_limit():

    key = (
        request.headers.get("X-API-Key")
        or request.headers.get("Authorization")
        or request.remote_addr
        or "unknown"
    )

    now = time.time()

    timestamps = API_USAGE.get(key, [])

    timestamps = [
        x for x in timestamps
        if now - x < 60
    ]

    if len(timestamps) >= RATE_LIMIT_RPM:
        return False

    timestamps.append(now)

    API_USAGE[key] = timestamps

    return True


@app.before_request
def before_request():

    if request.path in {"/", "/health"}:
        return None

    if not check_auth():

        return jsonify({
            "error": "Unauthorized",
            "message": "Provide a valid X-API-Key."
        }), 401

    if not rate_limit():

        return jsonify({
            "error": "Rate limit exceeded",
            "limit_per_minute": RATE_LIMIT_RPM
        }), 429

    return None


# ============================================================
# API HELPERS
# ============================================================

def get_records(category=None, limit=100):

    conn = get_db()
    cur = conn.cursor()

    rows = []

    try:

        if category:

            cur.execute(
                """
                SELECT data_json
                FROM records
                WHERE category=?
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (category, limit)
            )

        else:

            cur.execute(
                """
                SELECT data_json
                FROM records
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (limit,)
            )

        rows = cur.fetchall()

    except Exception as e:

        print(f"[DB READ ERROR] {e}")

        rows = []

    finally:

        conn.close()

    output = []

    for row in rows:

        try:
            output.append(json.loads(row["data_json"]))
        except Exception:
            pass

    return output


def get_stock_records():

    return get_records("stock", 2000)


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def index():

    return jsonify({

        "name": APP_NAME,
        "version": VERSION,
        "product": "MicroCap Catalyst Radar API",

        "description": (
            "US micro-cap and penny-stock radar "
            "using Yahoo Finance screeners + market data "
            "and SEC EDGAR filings."
        ),

        "endpoints": [
            "/health",
            "/api/stats",
            "/api/stocks",
            "/api/stocks/hot",
            "/api/stocks/<symbol>",
            "/api/changes",
            "/api/ecommerce",
            "/api/b2b",
            "/api/all"
        ],

        "disclaimer": (
            "Radar signals are informational "
            "and are not investment advice."
        )
    })


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "service": APP_NAME,
        "version": VERSION,
        "time": now_iso(),
        "last_scan": LAST_SCAN
    })


@app.route("/api/stats")
def stats():

    conn = get_db()
    cur = conn.cursor()

    rows = []
    changes = 0
    usage = 0

    try:

        cur.execute(
            """
            SELECT category, COUNT(*) AS count
            FROM records
            GROUP BY category
            """
        )

        rows = cur.fetchall()

        cur.execute("SELECT COUNT(*) AS count FROM changes")
        changes = cur.fetchone()["count"]

        cur.execute("SELECT COUNT(*) AS count FROM api_usage")
        usage = cur.fetchone()["count"]

    except Exception as e:

        print(f"[STATS ERROR] {e}")

        rows = []
        changes = 0
        usage = 0

    finally:

        conn.close()

    return jsonify({

        "version": VERSION,

        "records": {
            row["category"]: row["count"]
            for row in rows
        },

        "changes": changes,
        "api_usage": usage,
        "last_scan": LAST_SCAN,

        "disclaimer": (
            "Radar signals are informational "
            "and are not investment advice."
        )
    })


@app.route("/api/stocks")
def stocks():

    limit = request.args.get("limit", default=100, type=int)
    limit = max(1, min(limit, 500))

    records = get_records("stock", 2000)

    records.sort(
        key=lambda x: x.get("radar_score", 0),
        reverse=True
    )

    records = records[:limit]

    return jsonify({

        "count": len(records),
        "stocks": records,

        "disclaimer": (
            "Radar signals are informational "
            "and are not investment advice."
        )
    })


@app.route("/api/stocks/hot")
def hot_stocks():

    records = get_stock_records()

    hot = [
        x for x in records
        if x.get("label") in {"HOT", "STRONG_WATCH"}
        and x.get("price", 0) <= MAX_STOCK_PRICE
        and x.get("price", 0) >= MIN_STOCK_PRICE
    ]

    hot.sort(
        key=lambda x: x.get("radar_score", 0),
        reverse=True
    )

    limit = request.args.get("limit", default=20, type=int)
    hot = hot[:max(1, min(limit, 100))]

    return jsonify({

        "count": len(hot),
        "stocks": hot,

        "disclaimer": (
            "Radar signals are informational "
            "and are not investment advice."
        )
    })


@app.route("/api/stocks/<symbol>")
def stock_detail(symbol):

    symbol = symbol.upper().strip()

    records = get_stock_records()

    for stock in records:

        if stock.get("symbol") == symbol:

            return jsonify({
                "stock": stock,
                "disclaimer": (
                    "Radar signals are informational "
                    "and are not investment advice."
                )
            })

    data = analyze_stock(symbol, [], 0)

    if data:

        save_record("stock", data, symbol)

        return jsonify({
            "stock": data,
            "disclaimer": (
                "Radar signals are informational "
                "and are not investment advice."
            )
        })

    return jsonify({
        "error": "Stock not found",
        "symbol": symbol
    }), 404


@app.route("/api/changes")
def changes():

    limit = request.args.get("limit", default=100, type=int)
    limit = max(1, min(limit, 500))

    conn = get_db()
    cur = conn.cursor()

    rows = []

    try:

        cur.execute(
            """
            SELECT id, category, symbol, old_data,
                   new_data, changed_at
            FROM changes
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,)
        )

        rows = cur.fetchall()

    except Exception as e:

        print(f"[CHANGES ERROR] {e}")

        rows = []

    finally:

        conn.close()

    output = []

    for row in rows:

        try:
            old_data = json.loads(row["old_data"])
        except Exception:
            old_data = {}

        try:
            new_data = json.loads(row["new_data"])
        except Exception:
            new_data = {}

        output.append({
            "id": row["id"],
            "category": row["category"],
            "symbol": row["symbol"],
            "old": old_data,
            "new": new_data,
            "changed_at": row["changed_at"]
        })

    return jsonify({
        "count": len(output),
        "changes": output
    })


@app.route("/api/ecommerce")
def ecommerce():

    limit = request.args.get("limit", default=100, type=int)
    limit = max(1, min(limit, 500))

    records = get_records("ecommerce", limit)

    return jsonify({
        "count": len(records),
        "records": records
    })


@app.route("/api/b2b")
def b2b():

    return jsonify({
        "count": 0,
        "records": [],
        "status": "disabled",
        "reason": (
            "External B2B source is disabled. "
            "No bypass is attempted."
        )
    })


@app.route("/api/all")
def all_data():

    limit = request.args.get("limit", default=100, type=int)
    limit = max(1, min(limit, 500))

    return jsonify({

        "stocks": get_records("stock", limit),
        "ecommerce": get_records("ecommerce", limit),
        "b2b": get_records("b2b", limit),
        "last_scan": LAST_SCAN,
        "version": VERSION
    })


# ============================================================
# EXPORT DATABASE JSON
# ============================================================

def export_json():

    data = {
        "generated_at": now_iso(),
        "version": VERSION,
        "stocks": get_records("stock", 2000),
        "ecommerce": get_records("ecommerce", 1000),
        "b2b": get_records("b2b", 1000)
    }

    try:

        with open(JSON_FILE, "w", encoding="utf-8") as f:

            json.dump(
                data,
                f,
                ensure_ascii=False,
                indent=2
            )

    except Exception as e:
        print(f"[JSON EXPORT ERROR] {e}")


# ============================================================
# STARTUP — runs on import (gunicorn safe)
# ============================================================

print("=" * 65)
print(f"{APP_NAME} v{VERSION}")
print("MicroCap Catalyst Radar API")
print("Data source: Yahoo Finance screeners + SEC EDGAR")
print("=" * 65)

init_db()


_worker = threading.Thread(
    target=background_loop,
    daemon=True
)
_worker.start()


# ============================================================
# MAIN — only for local run
# ============================================================

if __name__ == "__main__":

    port = int(os.getenv("PORT", "5000"))

    print(f"[API] Starting Flask server on port {port}...")

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True
    )