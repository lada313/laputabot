# notifier.py
import asyncio
import logging
import json
import os
import time
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from analysis import get_liquidity_metrics, analyze_stock
from save_json import enqueue_git_push

PARALLEL_LIMIT = int(os.getenv("PARALLEL_LIMIT", "8"))  # можно менять через ENV
ANALYSIS_LIMIT = int(os.getenv("ANALYSIS_LIMIT", "4"))
_analysis_sem = asyncio.Semaphore(ANALYSIS_LIMIT)
logger = logging.getLogger("notifier")

OPEN_TRADES_FILE = "open_trades.json"
SCAN_CANDIDATES = os.getenv("SCAN_CANDIDATES", "0") == "1"
MAX_NEW_SIGNALS_PER_CYCLE = int(os.getenv("MAX_NEW_SIGNALS_PER_CYCLE", "3"))
CANDIDATES_FILE = "candidates.json"
TP1_PCT = 0.05   # +5%
TP2_PCT = 0.10   # +10%
SL_PCT  = 0.03   # -3%
# --- риск/капитал/трейлинг ---
CAPITAL   = float(os.getenv("CAPITAL", "10000"))   # общий депозит, ₽
RISK_PCT  = float(os.getenv("RISK_PCT", "0.01"))    # риск на сделку, 1% по умолчанию
TRAIL_PCT = float(os.getenv("TRAIL_PCT", "0.03"))   # трейлинг-стоп 3%

async def _bounded_gather(coros, limit: int = PARALLEL_LIMIT):
    """Параллельно выполняет корутины, но не больше 'limit' одновременно."""
    sem = asyncio.Semaphore(limit)

    async def wrap(c):
        async with sem:
            try:
                return await c
            except Exception as e:
                return e  # вернём ошибку как результат, чтобы не падать всем циклом

    return await asyncio.gather(*(wrap(c) for c in coros), return_exceptions=False)

def _calc_position_size(entry: float, sl: float, lot_size: int) -> int:
    """
    Возвращает количество АКЦИЙ (не лотов!), чтобы риск (entry - sl) × qty ≤ CAPITAL × RISK_PCT.
    Округляет вниз до кратности лоту. Если даже 1 лот превышает риск — вернёт 0.
    """
    per_share = max(entry - sl, 0.01)       # защита от 0
    risk_bank = CAPITAL * RISK_PCT          # сколько ₽ готов терять на сделку

    raw_shares = int(risk_bank // per_share)
    # округляем вниз до кратности лоту
    shares = (raw_shares // lot_size) * lot_size
    if shares < lot_size:
        return 0
    return shares

def _load_open_trades() -> dict:
    try:
        if os.path.exists(OPEN_TRADES_FILE):
            if os.path.getsize(OPEN_TRADES_FILE) == 0:
                # пустой файл — инициализируем словарём
                _save_open_trades({})
                return {}
            with open(OPEN_TRADES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        else:
            _save_open_trades({})
        return {}
    except Exception as e:
        logger.warning(f"Не удалось прочитать {OPEN_TRADES_FILE}: {e}")
        return {}

def _save_open_trades(data: dict) -> None:
    try:
        if not isinstance(data, dict):
            raise ValueError("⚠️ Неверный формат данных open_trades")
        with open(OPEN_TRADES_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        enqueue_git_push("Update open_trades.json")
    except Exception as e:
        logger.warning(f"Не удалось сохранить {OPEN_TRADES_FILE}: {e}")

async def notify_price_changes(application, TICKERS, portfolio, last_signal, CHAT_ID,
                               get_moex_price_func, calculate_rsi_func,
                               lots_map=None, candidates_file="candidates.json"):
    first_run = True
    last_price_signal = {}
    last_alert_at = {}               # антиспам по времени для «отслежки»
    ALERT_COOLDOWN_SEC = 6 * 3600    # 6 часов

    open_trades = _load_open_trades()    # {ticker: {...}}

    def _load_candidates_dict() -> dict:
        try:
            if os.path.exists(CANDIDATES_FILE):
                with open(CANDIDATES_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data if isinstance(data, dict) else {}
        except Exception as e:
            logger.warning(f"Не удалось прочитать {CANDIDATES_FILE}: {e}")
        return {}

    def lot_size_for(ticker: str) -> int:
        if lots_map and ticker in lots_map:
            try:
                return int(lots_map[ticker])
            except Exception:
                pass
        try:
            if os.path.exists(candidates_file):
                with open(candidates_file, "r", encoding="utf-8") as f:
                    cands = json.load(f)
                lot = cands.get(ticker, {}).get("lot")
                if lot:
                    return int(lot)
        except Exception as e:
            logger.warning(f"lot_size_for: не удалось прочитать {candidates_file}: {e}")
        return 1

    def ensure_trade(ticker: str, name: str, entry_price: float, lot_size: int):
        """
        Если нет активной сделки — создаём.
        Сразу считаем размер позиции по риску 1% и заводим поля для трейлинга.
        """
        tr = open_trades.get(ticker)
        if tr and tr.get("status") in ("open", "tp1_hit"):
            return  # уже есть активная

        tp1 = round(entry_price * (1 + TP1_PCT), 6)
        tp2 = round(entry_price * (1 + TP2_PCT), 6)
        sl  = round(entry_price * (1 - SL_PCT), 6)

        qty_shares = _calc_position_size(entry_price, sl, lot_size)  # авто-расчёт размера
        open_trades[ticker] = {
            "name": name,
            "created_at": datetime.utcnow().isoformat(),
            "entry_price": float(entry_price),
            "tp1": tp1,
            "tp2": tp2,
            "sl": sl,
            "lot_size": int(lot_size),
            "qty": int(qty_shares),
            "status": "open",                # open -> tp1_hit -> closed
            # трейлинг
            "trail_active": False,
            "trail_anchor": float(entry_price),
            "trail_sl": float(sl),
            "last_notified": 0.0
        }
        _save_open_trades(open_trades)

    async def check_trade_exits():
        changed = False
        for ticker, tr in list(open_trades.items()):
            if tr.get("status") == "closed":
                continue
            try:
                px = await get_moex_price_func(ticker)
                if px is None:
                    continue

                name      = tr.get("name", ticker)
                status    = tr.get("status", "open")
                lot_size  = int(tr.get("lot_size", 1))
                entry     = float(tr.get("entry_price", 0.0))
                tp1       = float(tr["tp1"]); tp2 = float(tr["tp2"])
                sl        = float(tr["sl"])
                qty       = int(tr.get("qty", 0))

                trail_on  = bool(tr.get("trail_active", False))
                anchor    = float(tr.get("trail_anchor", entry))
                trail_sl  = float(tr.get("trail_sl", sl))

                lot_price = px * lot_size

                # 1) обычный SL (в т.ч. после перевода в безубыток)
                if px <= sl and status in ("open", "tp1_hit"):
                    msg = (
                        f"🛑 *{name}* ({ticker}) — достигнут Stop Loss\n"
                        f"• Текущая: ~{px:.2f} ₽\n"
                        f"• SL: {sl:.2f} ₽\n"
                        f"• Объём: {qty} акц. (~{qty//max(lot_size,1)} лот.)"
                        f"\n📦 Лот: {lot_size} шт · ~{lot_price:.2f} ₽"
                    )
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть в Тинькофф", url=f"https://www.tinkoff.ru/invest/stocks/{ticker}")]])
                    await application.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown", reply_markup=kb)
                    tr["status"] = "closed"
                    changed = True
                    continue

                # 2) TP2 — выход полностью
                if px >= tp2 and status in ("open", "tp1_hit"):
                    msg = (
                        f"🎉 *{name}* ({ticker}) — достигнут TP2, можно выйти полностью\n"
                        f"• Текущая: ~{px:.2f} ₽\n"
                        f"• TP2: {tp2:.2f} ₽\n"
                        f"• Объём: {qty} акц. (~{qty//max(lot_size,1)} лот.)"
                        f"\n📦 Лот: {lot_size} шт · ~{lot_price:.2f} ₽"
                    )
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть в Тинькофф", url=f"https://www.tinkoff.ru/invest/stocks/{ticker}")]])
                    await application.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown", reply_markup=kb)
                    tr["status"] = "closed"
                    changed = True
                    continue

                # 3) TP1 — безубыток + включаем трейлинг
                if px >= tp1 and status == "open":
                    tr["sl"] = round(entry, 6)                    # SL -> цена входа
                    tr["trail_active"] = True
                    tr["trail_anchor"] = float(px)
                    tr["trail_sl"] = round(px * (1 - TRAIL_PCT), 6)

                    msg = (
                        f"✅ *{name}* ({ticker}) — достигнут TP1\n"
                        f"• Безубыток активирован: SL → {tr['sl']:.2f} ₽\n"
                        f"• Включён трейлинг {int(TRAIL_PCT*100)}%: текущий trail SL {tr['trail_sl']:.2f} ₽\n"
                        f"• Следующая цель TP2: {tp2:.2f} ₽"
                        f"\n\nОбъём: {qty} акц. (~{qty//max(lot_size,1)} лот.)"
                    )
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть в Тинькофф", url=f"https://www.tinkoff.ru/invest/stocks/{ticker}")]])
                    await application.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown", reply_markup=kb)

                    tr["status"] = "tp1_hit"
                    changed = True

                # 4) обслуживание трейлинга
                if tr.get("trail_active") and tr.get("status") in ("open", "tp1_hit"):
                    if px > tr["trail_anchor"]:
                        tr["trail_anchor"] = float(px)
                        tr["trail_sl"] = round(px * (1 - TRAIL_PCT), 6)
                        changed = True
                    if px <= tr["trail_sl"]:
                        msg = (
                            f"📉 *{name}* ({ticker}) — сработал трейлинг-стоп\n"
                            f"• Текущая: ~{px:.2f} ₽\n"
                            f"• Trail SL: {tr['trail_sl']:.2f} ₽ (якорь ~{tr['trail_anchor']:.2f} ₽)\n"
                            f"• Объём: {qty} акц. (~{qty//max(lot_size,1)} лот.)"
                        )
                        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть в Тинькофф", url=f"https://www.tinkoff.ru/invest/stocks/{ticker}")]])
                        await application.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown", reply_markup=kb)
                        tr["status"] = "closed"
                        changed = True

            except Exception as e:
                logger.error(f"check_trade_exits {ticker}: {e}")

        if changed:
            _save_open_trades(open_trades)


    async def _fetch_one(ticker: str, name: str, in_portfolio: bool):
        """
        Быстро собрать всё нужное по одному тикеру (цена, лот, ликвидность, анализ).
        Ничего не отправляет и не пишет — только считает и возвращает данные.
        """
        result = {
            "ticker": ticker,
            "name": name,
            "price": None,
            "lot_size": 1,
            "avg_turn": None,
            "avg_vol": None,
            "signal_text": None,
            "in_portfolio": in_portfolio,
            "error": None,
        }
        try:
            # цена и лот
            price = await get_moex_price_func(ticker)
            if price is None:
                raise ValueError("Нет текущей цены")
            result["price"] = price
            result["lot_size"] = lot_size_for(ticker)
    
            # ликвидность — только для кандидатов (в портфеле не нужна)
            if not in_portfolio:
                try:
                    _, avg_vol, avg_turn = await get_liquidity_metrics(ticker, days=20)
                    result["avg_vol"] = avg_vol
                    result["avg_turn"] = avg_turn
                except Exception:
                    result["avg_vol"] = 0.0
                    result["avg_turn"] = 0.0
    
            # анализ ограничиваем отдельным семафором, чтобы не упереться в лимиты API
            async with _analysis_sem:
                result["signal_text"] = await analyze_stock(ticker)
    
        except Exception as e:
            result["error"] = str(e)
    
        return result
            
                while True:
                    # 1) КОГО СКАНИРУЕМ: watchlist + (опционально) кандидаты
                    scan_items = list(TICKERS.items())
                    if SCAN_CANDIDATES:
                        cands = _load_candidates_dict()
                        for t, info in cands.items():
                            if t in portfolio or t in TICKERS:
                                continue
                            nm = (info.get("name") or t)
                            scan_items.append((t, nm))
            
                    # 2) ПАРАЛЛЕЛЬНЫЙ СБОР ДАННЫХ (без отправки сообщений)
                    tasks = []
                    for ticker, name in scan_items:
                        tasks.append(_fetch_one(ticker, name, in_portfolio=(ticker in portfolio)))
            
                    results = await _bounded_gather(tasks)  # собираем пачками по PARALLEL_LIMIT
            
                    # 2.1) ПОСЛЕДОВАТЕЛЬНАЯ ОБРАБОТКА РЕЗУЛЬТАТОВ (отправки/запись/лимиты)
                    new_signals_sent = 0
                    for res in results:
                        if isinstance(res, Exception):
                            logger.error(f"Ошибка таска: {res}")
                            continue
                        if res.get("error"):
                            logger.error(f"Ошибка по {res['ticker']}: {res['error']}")
                            continue
            
                        ticker   = res["ticker"]
                        name     = res["name"]
                        price    = res["price"]
                        lot_size = res["lot_size"]
                        lot_price = price * lot_size
                        sig      = res["signal_text"]
                        in_pf    = res["in_portfolio"]

            try:
                if in_pf:
                    purchase_price = portfolio[ticker]["price"]
                    change = (price - purchase_price) / purchase_price * 100

                    # уведомление об изменении сигнала
                    if not first_run and sig != last_signal.get(ticker):
                        old = last_signal.get(ticker)
                        if old:
                            msg = (
                                f"🔄 *{name}* ({ticker}) — изменение сигнала\n\n"
                                f"Было: {old}\n"
                                f"Стало: {sig}\n"
                                f"\n📦 Лот: {lot_size} шт · ~{lot_price:.2f} ₽"
                            )
                        else:
                            msg = (
                                f"📌 *{name}* ({ticker}) — новый сигнал:\n\n"
                                f"{sig}\n"
                                f"\n📦 Лот: {lot_size} шт · ~{lot_price:.2f} ₽"
                            )
                        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть в Тинькофф", url=f"https://www.tinkoff.ru/invest/stocks/{ticker}")]])
                        await application.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown", reply_markup=kb)
                        last_signal[ticker] = sig

                    # алерты по прибыли/убытку
                    if change >= 10 and last_price_signal.get(ticker) != "take":
                        msg = (
                            f"💰 *{name}* ({ticker}) вырос на {change:.2f}% от цены покупки!\n"
                            f"🎯 Возможность зафиксировать прибыль (Take Profit)\n"
                            f"\n📦 Лот: {lot_size} шт · ~{lot_price:.2f} ₽"
                        )
                        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть в Тинькофф", url=f"https://www.tinkoff.ru/invest/stocks/{ticker}")]])
                        await application.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown", reply_markup=kb)
                        last_price_signal[ticker] = "take"
                    elif change <= -5 and last_price_signal.get(ticker) != "stop":
                        msg = (
                            f"⚠️ *{name}* ({ticker}) упал на {change:.2f}% от цены покупки!\n"
                            f"🔻 Подумай о защите капитала (Stop Loss)\n"
                            f"\n📦 Лот: {lot_size} шт · ~{lot_price:.2f} ₽"
                        )
                        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть в Тинькофф", url=f"https://www.tinkoff.ru/invest/stocks/{ticker}")]])
                        await application.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown", reply_markup=kb)
                        last_price_signal[ticker] = "stop"
                    elif -5 < change < 10:
                        last_price_signal[ticker] = None

                else:
                    # фильтр ликвидности
                    avg_turn = res["avg_turn"] or 0.0
                    avg_vol  = res["avg_vol"]  or 0.0
                    if avg_turn < 5_000_000 or avg_vol < 10_000:
                        continue

                    if not sig or "Покупать" not in sig:
                        continue

                    now_ts = time.time()
                    if now_ts - last_alert_at.get(ticker, 0) < ALERT_COOLDOWN_SEC:
                        continue
                    if SCAN_CANDIDATES and new_signals_sent >= MAX_NEW_SIGNALS_PER_CYCLE:
                        continue

                    ensure_trade(ticker, name, price, lot_size)
                    tr = open_trades.get(ticker, {})
                    lots_cnt = (tr.get("qty", 0) // max(lot_size, 1)) if tr.get("qty") else 0

                    msg = (
                        f"✅ *{name}* ({ticker}) — сигнал: Покупать\n\n"
                        f"{sig}\n"
                        f"\n🎯 Цели:\n"
                        f"• TP1: ~{tr['tp1']:.2f} ₽ (+{int(TP1_PCT*100)}%)\n"
                        f"• TP2: ~{tr['tp2']:.2f} ₽ (+{int(TP2_PCT*100)}%)\n"
                        f"• SL:  ~{tr['sl']:.2f} ₽ (−{int(SL_PCT*100)}%)\n"
                        f"\n📦 Лот: {lot_size} шт · ~{(price*lot_size):.2f} ₽"
                        f"\n📐 Риск {int(RISK_PCT*100)}% → объём: {tr.get('qty',0)} акц. (~{lots_cnt} лот.)"
                        f"\n💧 Ср. оборот (20д): ~{avg_turn:,.0f} ₽ / день"
                    )
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть в Тинькофф", url=f"https://www.tinkoff.ru/invest/stocks/{ticker}")]])
                    await application.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown", reply_markup=kb)

                    if SCAN_CANDIDATES:
                        new_signals_sent += 1
                    last_signal[ticker] = sig
                    last_alert_at[ticker] = now_ts

            except Exception as e:
                logger.error(f"Ошибка при уведомлении по {ticker}: {e}")

        # 3) ПРОВЕРЯЕМ ВЫХОДЫ ПО ОТКРЫТЫМ СДЕЛКАМ
        await check_trade_exits()

        first_run = False
        await asyncio.sleep(10 * 60)

