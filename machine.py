"""
╔══════════════════════════════════════════════════════════════════╗
║         MW TRADER — ML Pattern Signal Engine (machine.py)        ║
║         Binance WebSocket + Price Action + Orderflow + Supabase  ║
╚══════════════════════════════════════════════════════════════════╝

DISCLAIMER: Ye sirf signal engine hai. Koi trade execute nahi hota.
Profit ki koi guarantee nahi hai. Trading mein risk hota hai.
"""

import asyncio
import json
import time
import os
import logging
import statistics
import uuid
import requests
from datetime import datetime, timezone, timedelta
from collections import deque
from typing import Optional
from dotenv import load_dotenv
import websockets
from supabase import create_client, Client

# ─────────────────────────────────────────────
#  LOAD ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────
load_dotenv()

# ─────────────────────────────────────────────
#  ★ CENTRAL CONFIG — CHANGE ONLY THESE ★
# ─────────────────────────────────────────────
ENGINE_TIMEFRAME = "1m"   # Change to "5m", "15m" etc — whole engine shifts
SYMBOL           = "BTCUSDT"

# Historical candles used by the engine for PA structure, S/R, ATR, pattern matching.
# Change this once and the whole engine updates automatically.
HISTORICAL_CANDLE_LIMIT = 5000

# ─────────────────────────────────────────────
#  DERIVED TIMEFRAME SETTINGS (auto from ENGINE_TIMEFRAME)
# ─────────────────────────────────────────────
_TF_MAP = {
    "1m":  {"seconds": 60,    "expiry_min": 20,  "candles_needed": 50,  "swing_lookback": 10, "atr_period": 14},
    "3m":  {"seconds": 180,   "expiry_min": 45,  "candles_needed": 50,  "swing_lookback": 10, "atr_period": 14},
    "5m":  {"seconds": 300,   "expiry_min": 90,  "candles_needed": 50,  "swing_lookback": 10, "atr_period": 14},
    "15m": {"seconds": 900,   "expiry_min": 240, "candles_needed": 50,  "swing_lookback": 12, "atr_period": 14},
    "30m": {"seconds": 1800,  "expiry_min": 480, "candles_needed": 50,  "swing_lookback": 14, "atr_period": 14},
    "1h":  {"seconds": 3600,  "expiry_min": 720, "candles_needed": 50,  "swing_lookback": 14, "atr_period": 14},
}
TF_CFG = _TF_MAP.get(ENGINE_TIMEFRAME, _TF_MAP["1m"])

CANDLE_SECONDS    = TF_CFG["seconds"]
EXPIRY_MINUTES    = TF_CFG["expiry_min"]
CANDLES_NEEDED    = TF_CFG["candles_needed"]
SWING_LOOKBACK    = TF_CFG["swing_lookback"]
ATR_PERIOD        = TF_CFG["atr_period"]

# Binance WebSocket URLs
WS_AGG_TRADE = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@aggTrade"
WS_KLINE     = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@kline_{ENGINE_TIMEFRAME}"

# Supabase
SUPABASE_URL              = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

# ─────────────────────────────────────────────
#  LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("MW_ENGINE")

# ─────────────────────────────────────────────
#  SUPABASE CLIENT WITH RETRY
# ─────────────────────────────────────────────
def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

async def supabase_upsert_with_retry(table: str, data: dict, retries: int = 3, backoff: float = 2.0):
    """Supabase write failure par retry/backoff logic."""
    supabase = get_supabase()
    for attempt in range(retries):
        try:
            supabase.table(table).upsert(data).execute()
            return True
        except Exception as e:
            logger.warning(f"Supabase upsert attempt {attempt+1} failed for {table}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(backoff * (attempt + 1))
    logger.error(f"All {retries} attempts failed for {table}")
    return False

async def supabase_insert_with_retry(table: str, data: dict, retries: int = 3, backoff: float = 2.0):
    """Insert with retry."""
    supabase = get_supabase()
    for attempt in range(retries):
        try:
            supabase.table(table).insert(data).execute()
            return True
        except Exception as e:
            logger.warning(f"Supabase insert attempt {attempt+1} failed for {table}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(backoff * (attempt + 1))
    logger.error(f"All {retries} attempts failed for {table}")
    return False

# ─────────────────────────────────────────────
#  CANDLE DATA STRUCTURE
# ─────────────────────────────────────────────

def fetch_initial_klines(limit: int = HISTORICAL_CANDLE_LIMIT) -> list:
    """
    Fetch historical klines from Binance.
    Supports large limits like 1000/5000 by paging requests.
    """
    interval = ENGINE_TIMEFRAME
    url = "https://fapi.binance.com/fapi/v1/klines"

    all_rows = []
    end_time = None
    remaining = int(limit)

    try:
        while remaining > 0:
            batch_limit = min(1000, remaining)
            params = {
                "symbol": SYMBOL,
                "interval": interval,
                "limit": batch_limit,
            }
            if end_time is not None:
                params["endTime"] = end_time

            res = requests.get(url, params=params, timeout=10)
            res.raise_for_status()
            rows = res.json()

            if not rows:
                break

            all_rows = rows + all_rows
            remaining -= len(rows)

            # Next request older candles se pehle ka data le.
            end_time = int(rows[0][0]) - 1

            if len(rows) < batch_limit:
                break

            time.sleep(0.2)

        candles = []
        for k in all_rows[-limit:]:
            candles.append(Candle(
                open_time=float(k[0]),
                open=float(k[1]),
                high=float(k[2]),
                low=float(k[3]),
                close=float(k[4]),
                volume=float(k[5]),
                closed=True,
            ))

        return candles

    except Exception as e:
        logger.warning(f"Initial kline preload failed: {e}")
        return []

class Candle:
    def __init__(self, open_time: float, open: float, high: float, low: float,
                 close: float, volume: float, closed: bool = False):
        self.open_time = open_time
        self.open      = open
        self.high      = high
        self.low       = low
        self.close     = close
        self.volume    = volume
        self.closed    = closed

    def to_dict(self):
        return {
            "open_time": self.open_time,
            "open":  self.open,
            "high":  self.high,
            "low":   self.low,
            "close": self.close,
            "volume": self.volume,
        }

# ─────────────────────────────────────────────
#  ORDERFLOW WINDOW
# ─────────────────────────────────────────────
class OrderflowWindow:
    """Rolling orderflow calculator for a given window in seconds."""
    def __init__(self, window_seconds: int):
        self.window_seconds = window_seconds
        self.trades: deque = deque()  # (timestamp, side, qty, price)

    def add_trade(self, ts: float, is_buyer_maker: bool, qty: float, price: float):
        self.trades.append((ts, is_buyer_maker, qty, price))
        # Remove old trades outside window
        cutoff = time.time() - self.window_seconds
        while self.trades and self.trades[0][0] < cutoff:
            self.trades.popleft()

    def compute(self) -> dict:
        if not self.trades:
            return {
                "buy_vol": 0, "sell_vol": 0, "total_vol": 0,
                "delta": 0, "buy_count": 0, "sell_count": 0,
                "total_count": 0, "label": "NO_DATA",
                "volume_spike": False, "delta_pct": 0
            }
        buy_vol = sell_vol = 0.0
        buy_count = sell_count = 0
        for _, is_buyer_maker, qty, _ in self.trades:
            # is_buyer_maker=True means SELL aggressor (maker is buyer = taker is seller)
            if is_buyer_maker:
                sell_vol   += qty
                sell_count += 1
            else:
                buy_vol   += qty
                buy_count += 1

        total_vol   = buy_vol + sell_vol
        delta       = buy_vol - sell_vol
        total_count = buy_count + sell_count
        delta_pct   = (delta / total_vol * 100) if total_vol > 0 else 0

        # Volume spike: if recent window vol >> average
        volume_spike = total_vol > 0 and len(self.trades) > 50

        # Label
        if delta_pct > 20:
            label = "BUY_STRONG"
        elif delta_pct > 5:
            label = "BUY_WEAK"
        elif delta_pct < -20:
            label = "SELL_STRONG"
        elif delta_pct < -5:
            label = "SELL_WEAK"
        else:
            label = "NEUTRAL"

        return {
            "buy_vol":      round(buy_vol, 4),
            "sell_vol":     round(sell_vol, 4),
            "total_vol":    round(total_vol, 4),
            "delta":        round(delta, 4),
            "delta_pct":    round(delta_pct, 2),
            "buy_count":    buy_count,
            "sell_count":   sell_count,
            "total_count":  total_count,
            "label":        label,
            "volume_spike": volume_spike
        }

# ─────────────────────────────────────────────
#  PRICE ACTION ENGINE
# ─────────────────────────────────────────────
def compute_candle_features(candle: Candle) -> dict:
    """Calculate all price action features for a single candle."""
    o, h, l, c = candle.open, candle.high, candle.low, candle.close
    candle_range = h - l if (h - l) > 0 else 0.0001
    body         = abs(c - o)
    body_pct     = (body / candle_range) * 100
    upper_wick   = h - max(o, c)
    lower_wick   = min(o, c) - l
    upper_wick_pct = (upper_wick / candle_range) * 100
    lower_wick_pct = (lower_wick / candle_range) * 100
    close_pos    = ((c - l) / candle_range) * 100
    is_bullish   = c > o
    is_bearish   = c < o

    # Rejection candles
    bullish_rejection = (
        lower_wick_pct > 40 and
        body_pct < 50 and
        close_pos > 50
    )
    bearish_rejection = (
        upper_wick_pct > 40 and
        body_pct < 50 and
        close_pos < 50
    )

    # Liquidity sweep: long wick engulf + close back inside
    liq_sweep_bull = lower_wick_pct > 50 and close_pos > 60
    liq_sweep_bear = upper_wick_pct > 50 and close_pos < 40

    return {
        "range":              round(candle_range, 4),
        "body":               round(body, 4),
        "body_pct":           round(body_pct, 2),
        "upper_wick":         round(upper_wick, 4),
        "lower_wick":         round(lower_wick, 4),
        "upper_wick_pct":     round(upper_wick_pct, 2),
        "lower_wick_pct":     round(lower_wick_pct, 2),
        "close_pos":          round(close_pos, 2),
        "is_bullish":         is_bullish,
        "is_bearish":         is_bearish,
        "bullish_rejection":  bullish_rejection,
        "bearish_rejection":  bearish_rejection,
        "liq_sweep_bull":     liq_sweep_bull,
        "liq_sweep_bear":     liq_sweep_bear,
    }

def compute_atr(candles: list, period: int = 14) -> float:
    """Average True Range calculation."""
    if len(candles) < period + 1:
        ranges = [c.high - c.low for c in candles]
        return statistics.mean(ranges) if ranges else 0.0
    trs = []
    for i in range(1, len(candles)):
        tr = max(
            candles[i].high - candles[i].low,
            abs(candles[i].high - candles[i-1].close),
            abs(candles[i].low  - candles[i-1].close)
        )
        trs.append(tr)
    return statistics.mean(trs[-period:])

def compute_momentum(candles: list, period: int = 5) -> float:
    """Returns positive for bullish momentum, negative for bearish."""
    if len(candles) < period:
        return 0.0
    closes = [c.close for c in candles[-period:]]
    up   = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i-1])
    down = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i-1])
    return (up - down) / (period - 1)

def find_swing_highs_lows(candles: list, lookback: int = None) -> dict:
    """Identify swing highs and swing lows."""
    if lookback is None:
        lookback = SWING_LOOKBACK
    if len(candles) < lookback * 2 + 1:
        highs = [c.high for c in candles]
        lows  = [c.low  for c in candles]
        return {
            "swing_highs": [max(highs)] if highs else [],
            "swing_lows":  [min(lows)]  if lows  else [],
            "recent_swing_high": max(highs) if highs else 0,
            "recent_swing_low":  min(lows)  if lows  else 0,
        }
    swing_highs = []
    swing_lows  = []
    for i in range(lookback, len(candles) - lookback):
        window_h = [candles[j].high for j in range(i - lookback, i + lookback + 1)]
        window_l = [candles[j].low  for j in range(i - lookback, i + lookback + 1)]
        if candles[i].high == max(window_h):
            swing_highs.append(candles[i].high)
        if candles[i].low == min(window_l):
            swing_lows.append(candles[i].low)
    recent_sh = swing_highs[-1] if swing_highs else max(c.high for c in candles[-lookback:])
    recent_sl = swing_lows[-1]  if swing_lows  else min(c.low  for c in candles[-lookback:])
    return {
        "swing_highs":        swing_highs[-5:],
        "swing_lows":         swing_lows[-5:],
        "recent_swing_high":  recent_sh,
        "recent_swing_low":   recent_sl,
    }

def compute_support_resistance(candles: list, zone_pct: float = 0.002) -> dict:
    """Cluster price levels to find S/R zones."""
    if len(candles) < 10:
        return {"support_zones": [], "resistance_zones": []}
    highs = sorted([c.high for c in candles], reverse=True)
    lows  = sorted([c.low  for c in candles])

    def cluster(prices, n=3):
        zones = []
        used  = set()
        for i, p in enumerate(prices):
            if i in used:
                continue
            cluster_pts = [p]
            for j, q in enumerate(prices):
                if j != i and j not in used and abs(p - q) / p < zone_pct:
                    cluster_pts.append(q)
                    used.add(j)
            if len(cluster_pts) >= 2:
                zones.append(round(statistics.mean(cluster_pts), 2))
        return zones[:n]

    return {
        "support_zones":    cluster(lows),
        "resistance_zones": cluster(highs),
    }

def detect_market_regime(candles: list) -> str:
    """Trend / Range / Choppy detection."""
    if len(candles) < 20:
        return "INSUFFICIENT_DATA"
    closes     = [c.close for c in candles[-20:]]
    highs_20   = [c.high  for c in candles[-20:]]
    lows_20    = [c.low   for c in candles[-20:]]
    price_range = max(highs_20) - min(lows_20)
    atr        = compute_atr(candles[-20:])
    # Higher highs / higher lows = uptrend
    hh = closes[-1] > closes[-5] > closes[-10]
    ll = closes[-1] < closes[-5] < closes[-10]
    range_atr_ratio = price_range / (atr * 20) if atr > 0 else 1
    if hh and range_atr_ratio > 1.2:
        return "UPTREND"
    elif ll and range_atr_ratio > 1.2:
        return "DOWNTREND"
    elif range_atr_ratio < 0.8:
        return "CHOPPY"
    else:
        return "RANGE"

# ─────────────────────────────────────────────
#  ML-STYLE PROBABILITY SCORING
# ─────────────────────────────────────────────
def compute_probability(
    direction: str,
    features:  dict,
    historical_signals: list
) -> dict:
    """
    Pattern-matching probability scoring.
    historical_signals: list of dicts from signal_history table.
    Fallback to rule-based if not enough history.
    """
    MIN_HISTORY = 15

    # Feature vector for similarity matching
    def feature_similarity(hist_rec: dict, curr: dict) -> float:
        score = 0.0
        checks = 0

        def close_enough(a, b, tol):
            if a is None or b is None:
                return False
            return abs(float(a) - float(b)) <= tol

        # Market regime match
        if hist_rec.get("market_regime") == curr.get("market_regime"):
            score += 2.0
        checks += 2

        # Orderflow label match
        if hist_rec.get("orderflow_label") == curr.get("orderflow_label"):
            score += 2.0
        checks += 2

        # Signal quality match
        sq_map = {"A+": 4, "A": 3, "B": 2, "C": 1, "NO_TRADE": 0}
        curr_q = sq_map.get(curr.get("signal_quality", "C"), 1)
        hist_q = sq_map.get(hist_rec.get("signal_quality", "C"), 1)
        if abs(curr_q - hist_q) <= 1:
            score += 1.5
        checks += 1.5

        # Direction match
        if hist_rec.get("direction") == direction:
            score += 1.0
        checks += 1.0

        # Compare numerical features via features_json safely
        hist_f = hist_rec.get("features_json", {})
        if hist_f and isinstance(hist_f, dict):
            # body_pct
            if close_enough(hist_f.get("body_pct"), curr.get("body_pct"), 15):
                score += 1.0
                checks += 1.0
                
            # upper/lower wick
            if close_enough(hist_f.get("upper_wick_pct"), curr.get("upper_wick_pct"), 20):
                score += 0.5
                checks += 0.5
            if close_enough(hist_f.get("lower_wick_pct"), curr.get("lower_wick_pct"), 20):
                score += 0.5
                checks += 0.5
                
            # mom5
            if close_enough(hist_f.get("mom5"), curr.get("mom5"), 0.5):
                score += 1.0
                checks += 1.0
            
            # dist_sup / dist_res
            if close_enough(hist_f.get("dist_sup"), curr.get("dist_sup"), 1.0):
                score += 0.5
                checks += 0.5
            if close_enough(hist_f.get("dist_res"), curr.get("dist_res"), 1.0):
                score += 0.5
                checks += 0.5

        return score / checks if checks > 0 else 0.0

    # Filter similar historical signals (same direction, same timeframe)
    relevant = [
        h for h in historical_signals
        if h.get("direction") == direction
        and h.get("timeframe") == ENGINE_TIMEFRAME
        and h.get("result") in ("WIN", "LOSS", "EXPIRED")
    ]

    if len(relevant) < MIN_HISTORY:
        # ── FALLBACK RULE-BASED PROBABILITY ──
        logger.info("Rule-based probability used (not enough ML history)")
        regime = features.get("market_regime", "RANGE")
        of_label = features.get("orderflow_label", "NEUTRAL")
        quality  = features.get("signal_quality", "C")

        base_tp1 = 55.0
        base_tp2 = 40.0
        base_tp3 = 25.0
        base_sl  = 30.0

        # Regime boost
        if direction == "BUY" and regime == "UPTREND":
            base_tp1 += 10; base_tp2 += 8; base_sl -= 5
        elif direction == "SELL" and regime == "DOWNTREND":
            base_tp1 += 10; base_tp2 += 8; base_sl -= 5
        elif regime == "CHOPPY":
            base_tp1 -= 15; base_tp2 -= 15; base_sl += 10

        # Orderflow boost
        if (direction == "BUY"  and of_label in ("BUY_STRONG",  "BUY_WEAK"))  or \
           (direction == "SELL" and of_label in ("SELL_STRONG", "SELL_WEAK")):
            base_tp1 += 8; base_tp2 += 6

        # Quality boost
        q_boost = {"A+": 12, "A": 8, "B": 4, "C": 0}.get(quality, 0)
        base_tp1 += q_boost; base_tp2 += q_boost * 0.8

        # Clamp
        def clamp(v): return max(5.0, min(95.0, v))
        confidence = clamp((base_tp1 + base_tp2) / 2)

        return {
            "tp1_probability": clamp(base_tp1),
            "tp2_probability": clamp(base_tp2),
            "tp3_probability": clamp(base_tp3),
            "sl_risk":         clamp(base_sl),
            "confidence":      clamp(confidence),
            "similar_count":   0,
            "ml_based":        False,
            "note":            "Rule-based probability, not enough ML history."
        }

    # ── ML PATTERN-MATCHING PROBABILITY ──
    similarity_scores = []
    for h in relevant:
        sim = feature_similarity(h, features)
        similarity_scores.append((sim, h))

    # Top-N similar signals
    similarity_scores.sort(key=lambda x: x[0], reverse=True)
    top_n = similarity_scores[:max(10, len(similarity_scores) // 3)]

    wins   = sum(1 for _, h in top_n if h.get("result") == "WIN")
    losses = sum(1 for _, h in top_n if h.get("result") == "LOSS")
    total  = len(top_n)

    tp1_hits = sum(1 for _, h in top_n if h.get("hit_level") in ("TP1", "TP2", "TP3"))
    tp2_hits = sum(1 for _, h in top_n if h.get("hit_level") in ("TP2", "TP3"))
    tp3_hits = sum(1 for _, h in top_n if h.get("hit_level") == "TP3")

    def pct(n): return round((n / total) * 100, 1) if total > 0 else 50.0

    win_prob   = pct(wins)
    tp1_prob   = pct(tp1_hits)
    tp2_prob   = pct(tp2_hits)
    tp3_prob   = pct(tp3_hits)
    sl_risk    = pct(losses)
    confidence = round((win_prob + tp1_prob) / 2, 1)

    return {
        "tp1_probability": tp1_prob,
        "tp2_probability": tp2_prob,
        "tp3_probability": tp3_prob,
        "sl_risk":         sl_risk,
        "confidence":      confidence,
        "similar_count":   total,
        "ml_based":        True,
        "note":            f"ML pattern matching: {total} similar setups analyzed."
    }

# ─────────────────────────────────────────────
#  SIGNAL QUALITY GRADER
# ─────────────────────────────────────────────
def grade_signal(confidence: float, features: dict) -> str:
    of_ok     = features.get("orderflow_label", "NEUTRAL") not in ("NO_DATA",)
    regime_ok = features.get("market_regime", "CHOPPY") not in ("CHOPPY", "INSUFFICIENT_DATA")
    if confidence >= 72 and of_ok and regime_ok:
        return "A+"
    elif confidence >= 60 and of_ok:
        return "A"
    elif confidence >= 48:
        return "B"
    elif confidence >= 35:
        return "C"
    else:
        return "NO_TRADE"

# ─────────────────────────────────────────────
#  ENTRY / SL / TP CALCULATOR
# ─────────────────────────────────────────────
def calculate_levels(direction: str, candles: list, current_price: float,
                     swing_data: dict, atr: float) -> dict:
    """Generate entry zone, SL, TP1/2/3."""
    latest = candles[-1]
    recent_sl = swing_data["recent_swing_low"]
    recent_sh = swing_data["recent_swing_high"]

    atr_buf = atr * 0.5

    if direction == "BUY":
        entry_low       = current_price - atr * 0.3
        entry_high      = current_price + atr * 0.2
        suggested_entry = current_price
        stop_loss       = round(min(recent_sl, latest.low) - atr_buf, 2)
        risk            = suggested_entry - stop_loss
        tp1             = round(suggested_entry + risk * 1.0,  2)
        tp2             = round(suggested_entry + risk * 1.8,  2)
        tp3             = round(suggested_entry + risk * 3.0,  2)

    else:  # SELL
        entry_low       = current_price - atr * 0.2
        entry_high      = current_price + atr * 0.3
        suggested_entry = current_price
        stop_loss       = round(max(recent_sh, latest.high) + atr_buf, 2)
        risk            = stop_loss - suggested_entry
        tp1             = round(suggested_entry - risk * 1.0, 2)
        tp2             = round(suggested_entry - risk * 1.8, 2)
        tp3             = round(suggested_entry - risk * 3.0, 2)

    risk = abs(suggested_entry - stop_loss)
    rr1  = round(abs(tp1 - suggested_entry) / risk, 2) if risk > 0 else 0
    rr2  = round(abs(tp2 - suggested_entry) / risk, 2) if risk > 0 else 0
    rr3  = round(abs(tp3 - suggested_entry) / risk, 2) if risk > 0 else 0

    return {
        "entry_low":         round(entry_low, 2),
        "entry_high":        round(entry_high, 2),
        "suggested_entry":   round(suggested_entry, 2),
        "stop_loss":         stop_loss,
        "tp1":               tp1,
        "tp2":               tp2,
        "tp3":               tp3,
        "risk_reward_tp1":   rr1,
        "risk_reward_tp2":   rr2,
        "risk_reward_tp3":   rr3,
    }

# ─────────────────────────────────────────────
#  SIGNAL GENERATOR
# ─────────────────────────────────────────────
def generate_signal(
    candles:      list,
    current_price: float,
    orderflow_data: dict,
    historical_signals: list
) -> dict:
    """
    Main signal generation logic.
    Returns complete signal dict or NO_TRADE.
    """
    if len(candles) < CANDLES_NEEDED:
        return {
            "direction": "NO_TRADE",
            "status": "NO_TRADE",
            "full_reason": f"Not enough candles: {len(candles)}/{CANDLES_NEEDED}",
            "confidence": 0,
            "signal_quality": "NO_TRADE"
        }

    latest   = candles[-1]
    features = compute_candle_features(latest)
    atr      = compute_atr(candles)
    swing    = find_swing_highs_lows(candles)
    sr_zones = compute_support_resistance(candles)
    regime   = detect_market_regime(candles)
    mom5     = compute_momentum(candles, 5)
    mom10    = compute_momentum(candles, 10)
    mom20    = compute_momentum(candles, 20)
    
    primary_of_key = ENGINE_TIMEFRAME if ENGINE_TIMEFRAME in orderflow_data else "1m"
    of_label = orderflow_data.get(primary_of_key, {}).get("label", "NO_DATA")

    # Distance to nearest support / resistance
    supports    = sr_zones.get("support_zones", [swing["recent_swing_low"]])
    resistances = sr_zones.get("resistance_zones", [swing["recent_swing_high"]])
    nearest_sup = min(supports,    key=lambda x: abs(current_price - x)) if supports else swing["recent_swing_low"]
    nearest_res = min(resistances, key=lambda x: abs(current_price - x)) if resistances else swing["recent_swing_high"]
    dist_sup    = abs(current_price - nearest_sup) / atr if atr > 0 else 99
    dist_res    = abs(current_price - nearest_res) / atr if atr > 0 else 99

    # Volatility check
    vol_ok = 0 < atr < current_price * 0.05  # ATR not more than 5% of price

    # ── BUY CONDITIONS ──
    buy_score = 0
    buy_reasons = []

    if features["bullish_rejection"] or features["liq_sweep_bull"]:
        buy_score += 3
        buy_reasons.append("Bullish rejection / liq sweep")

    if dist_sup < 2.0 and current_price > nearest_sup:
        buy_score += 2
        buy_reasons.append(f"Price near support {nearest_sup:.2f}")

    if features["close_pos"] > 60:
        buy_score += 1
        buy_reasons.append("Strong close position")

    if mom5 > 0.3:
        buy_score += 1
        buy_reasons.append("Bullish short-term momentum")

    if of_label in ("BUY_STRONG", "BUY_WEAK"):
        buy_score += 2
        buy_reasons.append(f"Orderflow: {of_label}")

    if regime in ("UPTREND", "RANGE"):
        buy_score += 1
        buy_reasons.append(f"Regime: {regime}")

    if vol_ok:
        buy_score += 1
        buy_reasons.append("Volatility within safe range")

    # ── SELL CONDITIONS ──
    sell_score = 0
    sell_reasons = []

    if features["bearish_rejection"] or features["liq_sweep_bear"]:
        sell_score += 3
        sell_reasons.append("Bearish rejection / liq sweep")

    if dist_res < 2.0 and current_price < nearest_res:
        sell_score += 2
        sell_reasons.append(f"Price near resistance {nearest_res:.2f}")

    if features["close_pos"] < 40:
        sell_score += 1
        sell_reasons.append("Weak close position")

    if mom5 < -0.3:
        sell_score += 1
        sell_reasons.append("Bearish short-term momentum")

    if of_label in ("SELL_STRONG", "SELL_WEAK"):
        sell_score += 2
        sell_reasons.append(f"Orderflow: {of_label}")

    if regime in ("DOWNTREND", "RANGE"):
        sell_score += 1
        sell_reasons.append(f"Regime: {regime}")

    if vol_ok:
        sell_score += 1
        sell_reasons.append("Volatility within safe range")

    # ── DECIDE DIRECTION ──
    MIN_SCORE = 5
    direction = "NO_TRADE"
    reasons   = []

    if buy_score >= MIN_SCORE and buy_score > sell_score:
        direction = "BUY"
        reasons   = buy_reasons
    elif sell_score >= MIN_SCORE and sell_score > buy_score:
        direction = "SELL"
        reasons   = sell_reasons

    if direction == "NO_TRADE" or regime == "CHOPPY":
        return {
            "direction":      "NO_TRADE",
            "status":         "NO_TRADE",
            "market_regime":  regime,
            "orderflow_label": of_label,
            "full_reason":    f"No valid setup. Buy score: {buy_score}, Sell score: {sell_score}, Regime: {regime}",
            "confidence":     0,
            "signal_quality": "NO_TRADE",
            "current_price":  current_price,
        }

    # Feature dict for probability
    prob_features = {
        "market_regime":    regime,
        "orderflow_label":  of_label,
        "signal_quality":   "B",   # placeholder, graded after
        "direction":        direction,
        "body_pct":         features.get("body_pct"),
        "upper_wick_pct":   features.get("upper_wick_pct"),
        "lower_wick_pct":   features.get("lower_wick_pct"),
        "close_pos":        features.get("close_pos"),
        "atr":              atr,
        "mom5":             mom5,
        "mom10":            mom10,
        "dist_sup":         dist_sup,
        "dist_res":         dist_res,
    }
    prob = compute_probability(direction, prob_features, historical_signals)
    quality = grade_signal(prob["confidence"], {**prob_features, "signal_quality": "B"})
    prob_features["signal_quality"] = quality

    if quality == "NO_TRADE":
        return {
            "direction":      "NO_TRADE",
            "status":         "NO_TRADE",
            "market_regime":  regime,
            "orderflow_label": of_label,
            "full_reason":    f"Low confidence signal filtered. Quality: {quality}",
            "confidence":     prob["confidence"],
            "signal_quality": "NO_TRADE",
            "current_price":  current_price,
        }

    levels = calculate_levels(direction, candles, current_price, swing, atr)
    now    = datetime.now(timezone.utc)
    expiry = now + timedelta(minutes=EXPIRY_MINUTES)
    signal_id = f"{SYMBOL}_{ENGINE_TIMEFRAME}_{int(now.timestamp())}_{uuid.uuid4().hex[:8]}"

    structure_reason = f"{direction} setup: {'; '.join(reasons[:3])}"
    full_reason      = (
        f"{structure_reason} | "
        f"Regime: {regime} | OF({primary_of_key}): {of_label} | "
        f"Mom5: {mom5:.2f} | ATR: {atr:.2f} | "
        f"Score: {buy_score if direction=='BUY' else sell_score} | "
        f"{prob.get('note','')}"
    )

    signal = {
        "signal_id":        signal_id,
        "symbol":           SYMBOL,
        "timeframe":        ENGINE_TIMEFRAME,
        "direction":        direction,
        "status":           "WAITING_ENTRY",
        "current_price":    round(current_price, 2),
        "market_regime":    regime,
        "orderflow_label":  of_label,
        "structure_reason": structure_reason,
        "full_reason":      full_reason,
        "features_json":    prob_features,
        "signal_quality":   quality,
        "confidence":       prob["confidence"],
        "tp1_probability":  prob["tp1_probability"],
        "tp2_probability":  prob["tp2_probability"],
        "tp3_probability":  prob["tp3_probability"],
        "sl_risk":          prob["sl_risk"],
        "created_at":       now.isoformat(),
        "updated_at":       now.isoformat(),
        "expires_at":       expiry.isoformat(),
        **levels,
    }
    return signal

# ─────────────────────────────────────────────
#  ACTIVE SIGNAL TRACKER
# ─────────────────────────────────────────────
def track_active_signal(signal: dict, current_price: float) -> dict:
    """
    Monitor active signal. Update status based on price.
    Returns updated signal dict.
    """
    if signal.get("direction") == "NO_TRADE":
        return signal

    now = datetime.now(timezone.utc)

    # Check expiry
    expires_at = signal.get("expires_at")
    if expires_at:
        exp_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if now > exp_dt:
            signal["status"]     = "EXPIRED"
            signal["updated_at"] = now.isoformat()
            return signal

    direction = signal["direction"]
    status    = signal.get("status", "WAITING_ENTRY")
    entry_low  = signal.get("entry_low", 0)
    entry_high = signal.get("entry_high", 0)
    sl         = signal.get("stop_loss", 0)
    tp1        = signal.get("tp1", 0)
    tp2        = signal.get("tp2", 0)
    tp3        = signal.get("tp3", 0)

    # Entry zone check
    if status == "WAITING_ENTRY":
        if entry_low <= current_price <= entry_high:
            status = "ACTIVE"

    if status == "ACTIVE":
        if direction == "BUY":
            if current_price <= sl:
                status = "SL_HIT"
            elif current_price >= tp3:
                status = "TP3_HIT"
            elif current_price >= tp2:
                status = "TP2_HIT"
            elif current_price >= tp1:
                status = "TP1_HIT"
        else:  # SELL
            if current_price >= sl:
                status = "SL_HIT"
            elif current_price <= tp3:
                status = "TP3_HIT"
            elif current_price <= tp2:
                status = "TP2_HIT"
            elif current_price <= tp1:
                status = "TP1_HIT"

    signal["status"]        = status
    signal["current_price"] = round(current_price, 2)
    signal["updated_at"]    = now.isoformat()
    return signal

def get_result_from_status(status: str) -> tuple:
    """Returns (result, hit_level) from signal status."""
    mapping = {
        "TP1_HIT": ("WIN",  "TP1"),
        "TP2_HIT": ("WIN",  "TP2"),
        "TP3_HIT": ("WIN",  "TP3"),
        "SL_HIT":  ("LOSS", "SL"),
        "EXPIRED": ("EXPIRED", "NONE"),
        "CANCELLED":("CANCELLED","NONE"),
    }
    return mapping.get(status, ("OPEN", "NONE"))

# ─────────────────────────────────────────────
#  SUPABASE DB OPERATIONS
# ─────────────────────────────────────────────
async def upsert_latest_signal(signal: dict):
    """Write/update latest signal in Supabase."""
    data = {**signal, "id": 1}  # single-row upsert by id=1
    await supabase_upsert_with_retry("latest_signal", data)

async def save_signal_to_history(signal: dict, result: str = "OPEN", hit_level: str = "NONE"):
    """Insert signal into signal_history."""
    now = datetime.now(timezone.utc).isoformat()
    data = {
        "signal_id":       signal.get("signal_id"),
        "symbol":          signal.get("symbol", SYMBOL),
        "timeframe":       signal.get("timeframe", ENGINE_TIMEFRAME),
        "direction":       signal.get("direction"),
        "entry_low":       signal.get("entry_low"),
        "entry_high":      signal.get("entry_high"),
        "stop_loss":       signal.get("stop_loss"),
        "tp1":             signal.get("tp1"),
        "tp2":             signal.get("tp2"),
        "tp3":             signal.get("tp3"),
        "confidence":      signal.get("confidence"),
        "tp1_probability": signal.get("tp1_probability"),
        "tp2_probability": signal.get("tp2_probability"),
        "tp3_probability": signal.get("tp3_probability"),
        "sl_risk":         signal.get("sl_risk"),
        "signal_quality":  signal.get("signal_quality"),
        "market_regime":   signal.get("market_regime"),
        "orderflow_label": signal.get("orderflow_label"),
        "result":          result,
        "hit_level":       hit_level,
        "created_at":      signal.get("created_at", now),
        "closed_at":       now if result != "OPEN" else None,
        "full_reason":     signal.get("full_reason"),
        "features_json":   signal.get("features_json"),
    }
    await supabase_insert_with_retry("signal_history", data)

async def close_history_signal(signal: dict, result: str, hit_level: str):
    signal_id = signal.get("signal_id")
    if not signal_id:
        return False

    data = {
        "result": result,
        "hit_level": hit_level,
        "closed_at": datetime.now(timezone.utc).isoformat(),
    }

    supabase = get_supabase()
    try:
        supabase.table("signal_history").update(data).eq("signal_id", signal_id).execute()
        return True
    except Exception as e:
        logger.warning(f"Failed to update closed signal history: {e}")
        return False

async def update_engine_status(
    current_price: float,
    ws_status:     str,
    last_candle_time,
    last_signal_time,
    total_signals:  int,
    wins:           int,
    losses:         int
):
    """Update engine_status table."""
    win_rate = round((wins / total_signals * 100), 1) if total_signals > 0 else 0.0
    data = {
        "id":               1,
        "symbol":           SYMBOL,
        "timeframe":        ENGINE_TIMEFRAME,
        "status":           "RUNNING",
        "websocket_status": ws_status,
        "last_price":       round(current_price, 2),
        "total_signals":    total_signals,
        "wins":             wins,
        "losses":           losses,
        "win_rate":         win_rate,
        "updated_at":       datetime.now(timezone.utc).isoformat(),
    }
    if last_candle_time:
        data["last_candle_time"] = last_candle_time
    if last_signal_time:
        data["last_signal_time"] = last_signal_time
        
    await supabase_upsert_with_retry("engine_status", data)

async def update_model_stats(total_signals: int, wins: int, losses: int):
    win_rate = round((wins / total_signals * 100), 1) if total_signals > 0 else 0.0
    data = {
        "id": 1,
        "timeframe": ENGINE_TIMEFRAME,
        "total_patterns": total_signals,
        "total_signals": total_signals,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_confidence": 0.0,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    await supabase_upsert_with_retry("model_stats", data)

async def fetch_signal_history() -> list:
    """Fetch recent signal history for ML probability matching."""
    try:
        supabase = get_supabase()
        res = (
            supabase.table("signal_history")
            .select("*")
            .order("created_at", desc=True)
            .limit(200)
            .execute()
        )
        return res.data or []
    except Exception as e:
        logger.warning(f"Failed to fetch signal history: {e}")
        return []

# ─────────────────────────────────────────────
#  MAIN ENGINE STATE
# ─────────────────────────────────────────────
class EngineState:
    def __init__(self):
        self.candles:          list   = []         # closed candles
        self.current_candle:   Optional[Candle] = None
        self.current_price:    float  = 0.0
        self.active_signal:    dict   = {}
        self.historical_sigs:  list   = []
        self.total_signals:    int    = 0
        self.wins:             int    = 0
        self.losses:           int    = 0
        self.last_signal_time: Optional[str] = None
        self.last_candle_time: Optional[str] = None
        self.ws_status:        str    = "DISCONNECTED"

        # Active Signal Live Update Throttle
        self.last_live_signal_update = 0.0

        # Orderflow windows
        self.of_windows = {
            "30s": OrderflowWindow(30),
            "1m":  OrderflowWindow(60),
            "3m":  OrderflowWindow(180),
            "5m":  OrderflowWindow(300),
            "15m": OrderflowWindow(900),
        }
        # Signal cooldown (avoid duplicate signals)
        self.last_signal_ts: float = 0
        self.signal_cooldown: float = CANDLE_SECONDS * 2

    def get_orderflow_snapshot(self) -> dict:
        return {k: v.compute() for k, v in self.of_windows.items()}

    def add_trade(self, ts: float, is_buyer_maker: bool, qty: float, price: float):
        for w in self.of_windows.values():
            w.add_trade(ts, is_buyer_maker, qty, price)
        self.current_price = price

    def add_closed_candle(self, candle: Candle):
        self.candles.append(candle)
        self.last_candle_time = datetime.fromtimestamp(
            candle.open_time / 1000, tz=timezone.utc
        ).isoformat()
        # Keep rolling window to save memory
        if len(self.candles) > HISTORICAL_CANDLE_LIMIT:
            self.candles = self.candles[-HISTORICAL_CANDLE_LIMIT:]

# ─────────────────────────────────────────────
#  WEBSOCKET HANDLERS
# ─────────────────────────────────────────────
async def handle_kline(state: EngineState, data: dict):
    """Process kline/candle data from Binance."""
    k = data.get("k", {})
    candle = Candle(
        open_time = k["t"],
        open      = float(k["o"]),
        high      = float(k["h"]),
        low       = float(k["l"]),
        close     = float(k["c"]),
        volume    = float(k["v"]),
        closed    = k.get("x", False),
    )
    state.current_price = candle.close

    if candle.closed:
        state.add_closed_candle(candle)
        logger.info(
            f"[CANDLE] {ENGINE_TIMEFRAME} closed | "
            f"O:{candle.open:.2f} H:{candle.high:.2f} "
            f"L:{candle.low:.2f} C:{candle.close:.2f}"
        )
        await on_candle_close(state)

async def handle_agg_trade(state: EngineState, data: dict):
    """Process aggTrade stream for orderflow."""
    ts             = data.get("T", time.time() * 1000) / 1000
    is_buyer_maker = data.get("m", False)
    qty            = float(data.get("q", 0))
    price          = float(data.get("p", 0))
    state.add_trade(ts, is_buyer_maker, qty, price)

    now = time.time()
    if (
        state.active_signal
        and state.active_signal.get("direction") != "NO_TRADE"
        and now - getattr(state, "last_live_signal_update", 0.0) >= 5.0
    ):
        updated = track_active_signal(state.active_signal, state.current_price)
        state.active_signal = updated
        state.last_live_signal_update = now
        await upsert_latest_signal(updated)

        status = updated.get("status", "")
        if status in ("TP1_HIT", "TP2_HIT", "TP3_HIT", "SL_HIT", "EXPIRED", "CANCELLED"):
            result, hit_level = get_result_from_status(status)

            if result == "WIN":
                state.wins += 1
            elif result == "LOSS":
                state.losses += 1

            await close_history_signal(updated, result, hit_level)
            await upsert_latest_signal(updated)

            logger.info(
                f"[SIGNAL CLOSED LIVE] Status: {status} | "
                f"Result: {result} | Level: {hit_level}"
            )

            state.active_signal = {}
            state.historical_sigs = await fetch_signal_history()
            await update_model_stats(state.total_signals, state.wins, state.losses)

        # Update engine status live as well
        await update_engine_status(
            state.current_price,
            state.ws_status,
            state.last_candle_time,
            state.last_signal_time,
            state.total_signals,
            state.wins,
            state.losses,
        )

async def on_candle_close(state: EngineState):
    """Called each time a candle closes. Core signal logic runs here."""
    # Active signal tracking
    if state.active_signal and state.active_signal.get("direction") != "NO_TRADE":
        updated = track_active_signal(state.active_signal, state.current_price)
        state.active_signal = updated
        status = updated.get("status", "")

        # Check if signal is closed
        if status in ("TP1_HIT", "TP2_HIT", "TP3_HIT", "SL_HIT", "EXPIRED", "CANCELLED"):
            result, hit_level = get_result_from_status(status)
            if result == "WIN":
                state.wins += 1
            elif result == "LOSS":
                state.losses += 1
            await close_history_signal(state.active_signal, result, hit_level)
            await upsert_latest_signal(state.active_signal)
            logger.info(f"[SIGNAL CLOSED] Status: {status} | Result: {result} | Level: {hit_level}")
            state.active_signal = {}  # Reset active signal

            # Reload history for ML
            state.historical_sigs = await fetch_signal_history()
            await update_model_stats(state.total_signals, state.wins, state.losses)

        else:
            # Just update
            await upsert_latest_signal(updated)

        # Update engine status
        await update_engine_status(
            state.current_price,
            state.ws_status,
            state.last_candle_time,
            state.last_signal_time,
            state.total_signals,
            state.wins,
            state.losses,
        )
        return

    # ── Generate new signal if cooldown passed ──
    now_ts = time.time()
    if (now_ts - state.last_signal_ts) < state.signal_cooldown:
        return

    of_snapshot = state.get_orderflow_snapshot()
    signal = generate_signal(
        state.candles,
        state.current_price,
        of_snapshot,
        state.historical_sigs
    )

    if signal.get("direction") != "NO_TRADE":
        state.total_signals   += 1
        state.active_signal    = signal
        state.last_signal_ts   = now_ts
        state.last_signal_time = datetime.now(timezone.utc).isoformat()
        logger.info(
            f"[NEW SIGNAL] {signal['direction']} | "
            f"Quality: {signal['signal_quality']} | "
            f"Confidence: {signal['confidence']}% | "
            f"Entry: {signal['suggested_entry']}"
        )
        await upsert_latest_signal(signal)
        await save_signal_to_history(signal, "OPEN", "NONE")
    else:
        # Save NO_TRADE status
        signal["current_price"] = state.current_price
        signal["updated_at"]    = datetime.now(timezone.utc).isoformat()
        await upsert_latest_signal(signal)

    # Update engine status
    await update_engine_status(
        state.current_price,
        state.ws_status,
        state.last_candle_time,
        state.last_signal_time,
        state.total_signals,
        state.wins,
        state.losses,
    )

# ─────────────────────────────────────────────
#  WEBSOCKET RUNNERS WITH RECONNECT
# ─────────────────────────────────────────────
async def run_kline_ws(state: EngineState):
    """Kline WebSocket with reconnect logic."""
    backoff = 1
    while True:
        try:
            logger.info(f"[WS] Connecting to kline stream: {WS_KLINE}")
            async with websockets.connect(WS_KLINE, ping_interval=20, ping_timeout=10) as ws:
                state.ws_status = "CONNECTED"
                backoff = 1
                logger.info("[WS] Kline WebSocket connected.")
                async for msg in ws:
                    data = json.loads(msg)
                    await handle_kline(state, data)
        except websockets.exceptions.ConnectionClosed as e:
            state.ws_status = "RECONNECTING"
            logger.warning(f"[WS] Kline connection closed: {e}. Reconnecting in {backoff}s...")
        except Exception as e:
            state.ws_status = "ERROR"
            logger.error(f"[WS] Kline error: {e}. Reconnecting in {backoff}s...")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)

async def run_agg_trade_ws(state: EngineState):
    """AggTrade WebSocket with reconnect logic."""
    backoff = 1
    while True:
        try:
            logger.info(f"[WS] Connecting to aggTrade stream: {WS_AGG_TRADE}")
            async with websockets.connect(WS_AGG_TRADE, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1
                logger.info("[WS] AggTrade WebSocket connected.")
                async for msg in ws:
                    data = json.loads(msg)
                    await handle_agg_trade(state, data)
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"[WS] AggTrade connection closed: {e}. Reconnecting in {backoff}s...")
        except Exception as e:
            logger.error(f"[WS] AggTrade error: {e}. Reconnecting in {backoff}s...")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)

# ─────────────────────────────────────────────
#  PERIODIC TASKS
# ─────────────────────────────────────────────
async def periodic_status_update(state: EngineState):
    """Update engine status every 30s even without candle close."""
    while True:
        await asyncio.sleep(30)
        if state.current_price > 0:
            await update_engine_status(
                state.current_price,
                state.ws_status,
                state.last_candle_time,
                state.last_signal_time,
                state.total_signals,
                state.wins,
                state.losses,
            )
            await update_model_stats(state.total_signals, state.wins, state.losses)

async def periodic_history_refresh(state: EngineState):
    """Refresh ML history from Supabase every 5 minutes."""
    while True:
        await asyncio.sleep(300)
        state.historical_sigs = await fetch_signal_history()
        logger.info(f"[ML] History refreshed: {len(state.historical_sigs)} records")

# ─────────────────────────────────────────────
#  MAIN ENTRY POINT
# ─────────────────────────────────────────────
async def main():
    logger.info("="*60)
    logger.info(" MW TRADER — ML Signal Engine Starting")
    logger.info(f" Symbol:    {SYMBOL}")
    logger.info(f" Timeframe: {ENGINE_TIMEFRAME}")
    logger.info(f" Expiry:    {EXPIRY_MINUTES} minutes")
    logger.info("="*60)

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY not set. Exiting.")
        return

    state = EngineState()

    logger.info("[ML] Preloading historical candles from Binance...")
    preloaded = fetch_initial_klines(limit=HISTORICAL_CANDLE_LIMIT)
    if preloaded:
        state.candles = preloaded[-HISTORICAL_CANDLE_LIMIT:]
        state.current_price = preloaded[-1].close
        state.last_candle_time = datetime.fromtimestamp(
            preloaded[-1].open_time / 1000, tz=timezone.utc
        ).isoformat()
        logger.info(f"[PRELOAD] Loaded {len(preloaded)} historical candles / requested {HISTORICAL_CANDLE_LIMIT}")
    else:
        logger.warning("[PRELOAD] No historical candles loaded; engine will warm up from WebSocket")

    # Initial history load
    logger.info("[ML] Loading historical signal data...")
    state.historical_sigs = await fetch_signal_history()
    logger.info(f"[ML] Loaded {len(state.historical_sigs)} historical records")

    # Run all tasks concurrently
    await asyncio.gather(
        run_kline_ws(state),
        run_agg_trade_ws(state),
        periodic_status_update(state),
        periodic_history_refresh(state),
    )

if __name__ == "__main__":
    asyncio.run(main())