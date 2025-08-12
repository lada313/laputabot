import os
import asyncio
from datetime import datetime, timedelta
import numpy as np
from typing import List, Optional
from tinkoff.invest import AsyncClient, CandleInterval

TINKOFF_TOKEN = os.getenv("TINKOFF_TOKEN")

# --- Кэш FIGI, чтобы не гонять каталог каждый раз
_figi_cache: dict[str, str] = {}

async def get_figi_by_ticker(ticker: str) -> Optional[str]:
    t = (ticker or "").upper()
    if not t:
        return None
    if t in _figi_cache:
        return _figi_cache[t]
    async with AsyncClient(TINKOFF_TOKEN) as client:
        shares = await client.instruments.shares()
        for share in shares.instruments:
            if (getattr(share, "ticker", "") or "").upper() == t:
                _figi_cache[t] = share.figi
                return share.figi
    return None


def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 1:
        return None
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - (100 / (1 + rs)))


def calculate_sma(prices: List[float], period: int) -> Optional[float]:
    if len(prices) < period:
        return None
    return float(np.mean(prices[-period:]))


def calculate_ema(prices: List[float], period: int) -> List[Optional[float]]:
    ema: List[Optional[float]] = []
    k = 2 / (period + 1)
    for i in range(len(prices)):
        if i < period - 1:
            ema.append(None)
        elif i == period - 1:
            sma = float(np.mean(prices[:period]))
            ema.append(sma)
        else:
            prev = ema[-1]
            assert prev is not None
            ema.append(prices[i] * k + prev * (1 - k))
    return ema


def analyze_stock_from_prices(ticker: str, prices: List[float]) -> str:
    """
    Адаптивный анализ по списку close-цен (без Tinkoff API).
    Мягкие окна: короткая SMA до 50, длинная 60/120/200 по доступности.
    """
    n = len(prices)
    if n < 60:
        return "Недостаточно данных для анализа"  # абсолютный минимум

    short_win = min(50, n)
    if n >= 200:
        long_win = 200
    elif n >= 120:
        long_win = 120
    else:
        long_win = 60

    sma_short = float(np.mean(prices[-short_win:]))
    sma_long = float(np.mean(prices[-long_win:]))

    rsi = calculate_rsi(prices)

    signals = []
    if rsi is not None:
        if rsi < 30:
            signals.append("💎 RSI < 30 — перепроданность")
        elif rsi > 70:
            signals.append("⚠️ RSI > 70 — перекупленность")

    if sma_short > sma_long:
        signals.append("📈 SMA-короткая выше SMA-длинной — восходящий тренд")
    else:
        signals.append("📉 SMA-короткая ниже SMA-длинной — нисходящий тренд")

    if (rsi is not None and rsi < 35) and sma_short > sma_long:
        final = "✅ Покупать"
    elif (rsi is not None and rsi > 70) and sma_short < sma_long:
        final = "🔻 Продавать"
    else:
        final = "🤔 Подождать"

    return f"{final}\n" + "\n".join(signals)


async def analyze_stock(ticker: str, detailed: bool = False) -> str:
    """
    Основной анализ по свечам Tinkoff (дневки), с весовой моделью.
    """
    figi = await get_figi_by_ticker(ticker)
    if not figi:
        return f"❌ Не найден FIGI для {ticker}"

    async with AsyncClient(TINKOFF_TOKEN) as client:
        now_utc = datetime.utcnow()
        from_utc = now_utc - timedelta(days=300)

        candles = await client.market_data.get_candles(
            figi=figi,
            from_=from_utc,
            to=now_utc,
            interval=CandleInterval.CANDLE_INTERVAL_DAY
        )

    if not candles.candles:
        return f"❌ Нет данных по {ticker}"

    close_prices = [c.close.units + c.close.nano * 1e-9 for c in candles.candles]
    volumes = [c.volume for c in candles.candles]

    if len(close_prices) < 50:
        return f"❌ Недостаточно данных по {ticker}"

    weights = {
        "rsi": 2.0,
        "trend": 1.0,
        "volume": 0.5,
        "sma": 1.5,
        "macd": 1.0
    }

    # --- RSI
    rsi_score = 0
    rsi_text = "не рассчитан"
    rsi = calculate_rsi(close_prices)
    if rsi is not None:
        if rsi < 30:
            rsi_score = 1
            rsi_text = f"{rsi:.1f} — перепродан ✅"
        elif rsi > 70:
            rsi_score = -1
            rsi_text = f"{rsi:.1f} — перекуплен ❌"
        else:
            rsi_text = f"{rsi:.1f} — нейтрально ➖"

    # --- Тренд (5 дней)
    trend_score = 0
    if close_prices[-1] > close_prices[-5]:
        trend_score = 1
        trend_text = "рост ✅"
    elif close_prices[-1] < close_prices[-5]:
        trend_score = -1
        trend_text = "падение ❌"
    else:
        trend_text = "боковой ➖"

    # --- Объём
    avg_volume = sum(volumes[:-1]) / max(1, (len(volumes) - 1))
    current_volume = volumes[-1]
    volume_score = 0
    if current_volume > avg_volume * 1.5:
        volume_score = 1
        volume_text = "высокий ✅"
    elif current_volume < avg_volume * 0.5:
        volume_score = -1
        volume_text = "низкий ❌"
    else:
        volume_text = "нормальный ➖"

    # --- SMA 50/200 (с явной проверкой None)
    sma_score = 0
    sma_text = "недостаточно данных"
    sma50 = calculate_sma(close_prices, 50)
    sma200 = calculate_sma(close_prices, 200)
    if sma50 is not None and sma200 is not None:
        if sma50 > sma200:
            sma_score = 1
            sma_text = "золотой крест ✅"
        else:
            sma_score = -1
            sma_text = "мёртвый крест ❌"

    # --- MACD (выравнивание и None-safe)
    macd_score = 0
    macd_text = "недоступен"
    ema12 = calculate_ema(close_prices, 12)
    ema26 = calculate_ema(close_prices, 26)

    macd_line: List[Optional[float]] = [
        (e1 - e2) if (e1 is not None and e2 is not None) else None
        for e1, e2 in zip(ema12, ema26)
    ]
    macd_valid = [m for m in macd_line if m is not None]
    if len(macd_valid) >= 9:
        macd_signal_valid = calculate_ema(macd_valid, 9)
        pad = len(macd_line) - len(macd_signal_valid)
        macd_signal: List[Optional[float]] = [None] * pad + macd_signal_valid
        if macd_line[-1] is not None and macd_signal[-1] is not None:
            if macd_line[-1] > macd_signal[-1]:
                macd_score = 1
                macd_text = "импульс вверх ✅"
            else:
                macd_score = -1
                macd_text = "импульс вниз ❌"
    else:
        macd_text = "недоступен"

    # --- Весовой итог
    weighted_score = (
        rsi_score * weights["rsi"] +
        trend_score * weights["trend"] +
        volume_score * weights["volume"] +
        sma_score * weights["sma"] +
        macd_score * weights["macd"]
    )

    if weighted_score >= 2.5:
        conclusion = "💡 Вывод: ✅ Покупать"
    elif weighted_score <= -2.5:
        conclusion = "💡 Вывод: 🔻 Продавать"
    else:
        conclusion = "💡 Вывод: ⏸ Подождать"

    # --- SL/TP
    stop_loss_price = close_prices[-1] * 0.95
    take_profit_price = close_prices[-1] * 1.10
    stop_loss_text = f"🔻 *Стоп-лосс:* ~{stop_loss_price:.2f} ₽ (−5%)"
    take_profit_text = f"💰 *Тейк-профит:* ~{take_profit_price:.2f} ₽ (+10%)"

    if detailed:
        return (
            f"📊 Подробный анализ {ticker}:\n\n"
            f"• RSI: {rsi_text}\n"
            f"• Тренд: {trend_text}\n"
            f"• Объём: {volume_text}\n"
            f"• SMA-50/200: {sma_text}\n"
            f"• MACD: {macd_text}\n"
            f"\n🎯 Весовая оценка: {weighted_score:.1f}\n"
            f"{conclusion}\n\n"
            f"{stop_loss_text}\n"
            f"{take_profit_text}"
        )
    else:
        return f"{conclusion}\n"


# Пример локального запуска
async def _main():
    print(await analyze_stock("SBER", detailed=True))

# === Order plan builder for portfolio ===
from dataclasses import dataclass
from math import floor

@dataclass
class PlanLeg:
    kind: str           # "take_profit" | "stop_market" | "stop_limit"
    what: str           # описание, чтобы печатать человеку
    activation: float   # цена срабатывания
    limit: float | None # для stop_limit
    lots: int           # количество лотов
    note: str | None = None  # ремарка

def _lots_from_shares(qty_shares: int, lot_size: int) -> int:
    return max(1, qty_shares // max(1, lot_size))

def build_portfolio_order_plan(
    *,
    ticker: str,
    current_price: float,
    entry_price: float,
    qty_shares: int,
    lot_size: int,
    tp1_pct: float = 0.05,    # +5%
    tp2_pct: float = 0.10,    # +10%
    sl_pct: float  = 0.03,    # -3%
) -> dict:
    """
    Возвращает словарь с готовыми «ногами» плана:
      - один из вариантов TP: либо один общий TP2, либо 50/50 TP1+TP2 (если лотов >= 2)
      - стоп по выбору: stop-market или stop-limit (activation = SL, limit чуть ниже)
    Все цены считаются от ЦЕНЫ ВХОДА (консервативный вариант).
    """
    lots_total = _lots_from_shares(qty_shares, lot_size)

    tp1 = round(entry_price * (1 + tp1_pct), 2)
    tp2 = round(entry_price * (1 + tp2_pct), 2)
    sl  = round(entry_price * (1 - sl_pct),  2)

    legs: list[PlanLeg] = []

    # Вариант A: один общий тейк (всё по TP2)
    legs.append(PlanLeg(
        kind="take_profit",
        what="Тейк-профит (весь объём)",
        activation=tp2,
        limit=None,
        lots=lots_total,
        note="Простой вариант — зафиксировать прибыль одним тейком."
    ))

    # Вариант B: частичная фиксация 50/50, если хватает лотов
    if lots_total >= 2:
        half = lots_total // 2
        rest = lots_total - half
        legs.append(PlanLeg(
            kind="take_profit",
            what=f"Тейк-профит #1 (≈50% объёма)",
            activation=tp1,
            limit=None,
            lots=half,
            note="Частичная фиксация на TP1."
        ))
        legs.append(PlanLeg(
            kind="take_profit",
            what=f"Тейк-профит #2 (остаток)",
            activation=tp2,
            limit=None,
            lots=rest,
            note="Досрочно можно перенести в безубыток после TP1."
        ))

    # Стопы: два варианта на выбор
    # Stop-market — гарантированное срабатывание по рынку
    legs.append(PlanLeg(
        kind="stop_market",
        what="Стоп-маркет (весь объём)",
        activation=sl,
        limit=None,
        lots=lots_total,
        note="Проще всего: сработает по рынку при касании активации."
    ))

    # Stop-limit — активация SL, лимит на 0.2% ниже, чтобы повысить шанс исполнения
    sl_limit = round(sl * 0.998, 2)
    legs.append(PlanLeg(
        kind="stop_limit",
        what="Стоп-лимит (весь объём)",
        activation=sl,
        limit=sl_limit,
        lots=lots_total,
        note="Более контролируемая цена: лимит чуть ниже активации (~0.2%)."
    ))

    return {
        "ticker": ticker,
        "entry": round(entry_price, 2),
        "current": round(current_price, 2),
        "lot_size": lot_size,
        "qty_shares": qty_shares,
        "tp1": tp1,
        "tp2": tp2,
        "sl": sl,
        "legs": legs,
        # Краткая подсказка по типам заявок Тинькофф
        "help": (
            "TP — обычная заявка Take-Profit по цене.\n"
            "Stop-market — указываете только цену активации.\n"
            "Stop-limit — указываете цену активации и лимит-цену.\n"
            "Лимитная покупка на откате — можно выставлять вручную (живет ~13ч)."
        ),
    }

# --- Liquidity helpers ---
async def get_liquidity_metrics(ticker: str, days: int = 20) -> tuple[float, float, float]:
    """
    Возвращает (avg_close, avg_volume, avg_turnover_rub) за N последних торговых дней
    по дневным свечам Tinkoff.
    """
    figi = await get_figi_by_ticker(ticker)
    if not figi:
        return (0.0, 0.0, 0.0)

    async with AsyncClient(TINKOFF_TOKEN) as client:
        now_utc = datetime.utcnow()
        from_utc = now_utc - timedelta(days=days + 10)
        candles = await client.market_data.get_candles(
            figi=figi,
            from_=from_utc,
            to=now_utc,
            interval=CandleInterval.CANDLE_INTERVAL_DAY
        )

    if not candles.candles:
        return (0.0, 0.0, 0.0)

    closes = [c.close.units + c.close.nano * 1e-9 for c in candles.candles][-days:]
    vols   = [c.volume for c in candles.candles][-days:]
    if not closes or not vols:
        return (0.0, 0.0, 0.0)

    avg_close = float(np.mean(closes))
    avg_vol   = float(np.mean(vols))
    avg_turn  = avg_close * avg_vol
    return (avg_close, avg_vol, avg_turn)

if __name__ == "__main__":
    asyncio.run(_main())
