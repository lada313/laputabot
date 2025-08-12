import os, json
import asyncio
import aiohttp
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram import Chat
from tinkoff.invest import CandleInterval
from datetime import datetime, timedelta
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    CallbackQueryHandler, ConversationHandler, MessageHandler, filters
)
from telegram.error import BadRequest
from telegram.error import NetworkError
from telegram.request import HTTPXRequest
import numpy as np
from typing import List, Optional
from keep_alive import keep_alive
from tinkoff.invest import AsyncClient
from self_ping import self_ping
from notifier import notify_price_changes
from save_json import start_git_worker, enqueue_git_push
from analysis import analyze_stock, build_portfolio_order_plan  # NEW

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TINKOFF_TOKEN = os.getenv("TINKOFF_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID", "0"))

# ---- Общий клиент Tinkoff и семафор на весь процесс ----
TCS_CLIENT = None        # будет AsyncClient(...)
TCS_SEM = None           # будет asyncio.Semaphore(...)

TICKERS = {}
portfolio = {}
history = []
price_history = {}
last_signal = {}

LOTS = {}
LOTS_CACHE: dict[str, int] = {}

# Константы
BUY_PRICE, BUY_PRICE_TYPE, BUY_AMOUNT = range(3)
MAX_HISTORY_DAYS = 30
TICKERS_FILE = "tickers.json"
CANDIDATES_FILE = "candidates.json"
OPEN_TRADES_FILE = "open_trades.json"

async def safe_answer(query):
    try:
        await query.answer()
    except BadRequest as e:
        msg = str(e).lower()
        if "query is too old" in msg or "query id is invalid" in msg:
            logger.warning(f"Ignoring stale callback: {e}")
            return
        raise

async def run_forever(app):
    backoff = 2
    while True:
        try:
            await app.run_polling(
                poll_interval=1.0,
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
                stop_signals=None,
                close_loop=False,          # ← вот это ключевое
            )
        except NetworkError as e:
            logger.warning(f"NetworkError: {e}. Перезапуск через {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            logger.error(f"Критическая ошибка run_polling: {e}", exc_info=True)
            await asyncio.sleep(5)

# ====== AUTO-REFRESH CANDIDATES (TINKOFF first, MOEX fallback) ======
async def update_candidates_list_tinkoff() -> int:
    """
    Тянет список акций через Tinkoff Invest API и сохраняет candidates.json.
    Фильтр: только MOEX (class_code TQBR/TQTF/TQTD), валюта RUB,
    доступно к покупке, доступно через API, НЕ для квалифицированных инвесторов.
    Формат файла: { "SBER": {"name": "Сбербанк", "lot": 10}, ... }
    """
    try:
        client = TCS_CLIENT
        async with TCS_SEM:
            shares = await client.instruments.shares()
    except Exception as e:
        logger.error(f"❌ Tinkoff API: не удалось получить акции: {e}")
        return 0

    candidates = {}
    allowed_classes = {"TQBR", "TQTF", "TQTD"}

    for s in shares.instruments:
        try:
            ticker = (getattr(s, "ticker", None) or "").strip()
            if not ticker:
                continue

            class_code = (getattr(s, "class_code", None) or "").strip()
            if class_code not in allowed_classes:
                continue

            lot = int(getattr(s, "lot", 1) or 1)

            cur = getattr(s, "currency", None)
            cur = str(cur).lower() if cur is not None else "rub"
            if "rub" not in cur:
                continue

            buy_ok = bool(getattr(s, "buy_available_flag", True))
            api_ok = bool(getattr(s, "api_trade_available_flag", True))
            qual_only = bool(getattr(s, "for_qual_investor_flag", False))
            if not (buy_ok and api_ok):
                continue
            if qual_only:
                continue

            name = (getattr(s, "name", None) or getattr(s, "ticker", "")).strip()
            if not name:
                name = ticker

            if len(ticker) > 8:
                continue

            candidates[ticker] = {"name": name, "lot": lot}
        except Exception:
            continue

    try:
        with open(CANDIDATES_FILE, "w", encoding="utf-8") as f:
            json.dump(candidates, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Tinkoff: обновлён candidates.json — {len(candidates)} тикеров")
        return len(candidates)
    except Exception as e:
        logger.error(f"❌ Не удалось сохранить {CANDIDATES_FILE}: {e}")
        return 0


async def update_candidates_list_moex() -> int:
    """
    РЕЗЕРВ: тянем TQBR через MOEX ISS, если Tinkoff не дал список.
    После получения — фильтруем по флагам из Tinkoff (не для квалов, можно купить, доступно через API).
    """
    url = ("https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/"
           "securities.json?iss.meta=off&iss.only=securities")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=20) as resp:
                data = await resp.json()
    except Exception as e:
        logger.error(f"❌ MOEX: не удалось получить TQBR: {e}")
        return 0

    table = data.get("securities", {})
    cols = table.get("columns", [])
    rows = table.get("data", []) or []

    def col_index(name):
        try:
            return cols.index(name)
        except Exception:
            return None

    i_secid = col_index("SECID")
    i_short = col_index("SHORTNAME")
    i_lot = col_index("LOTSIZE")

    if i_secid is None:
        logger.error("❌ MOEX: нет SECID в ответе")
        return 0

    # 1) Черновой список из MOEX
    moex_raw = {}
    for r in rows:
        try:
            secid = (r[i_secid] or "").strip()
            if not secid:
                continue
            short = (r[i_short] if i_short is not None else None) or secid
            lot = r[i_lot] if i_lot is not None else 1
            try:
                lot = int(lot) if lot is not None else 1
            except Exception:
                lot = 1
            if len(secid) > 8:
                continue
            moex_raw[secid] = {"name": str(short).strip(), "lot": lot}
        except Exception:
            continue

    if not moex_raw:
        logger.warning("⚠️ MOEX: пустой список TQBR")
        return 0

    # 2) Фильтрация MOEX-списка по справочнику Tinkoff (не квал, можно купить, доступно через API, рубли, правильный класс)
    try:
        client = TCS_CLIENT
        async with TCS_SEM:
            shares = await client.instruments.shares()
    except Exception as e:
        logger.error(f"❌ Tinkoff API: не удалось получить акции для фильтра MOEX: {e}")
        return 0

    allowed_classes = {"TQBR", "TQTF", "TQTD"}
    filtered = {}
    # Подготовим быстрый справочник по тикерам
    for s in shares.instruments:
        try:
            ticker = (getattr(s, "ticker", None) or "").strip()
            if not ticker or ticker not in moex_raw:
                continue

            class_code = (getattr(s, "class_code", None) or "").strip()
            if class_code not in allowed_classes:
                continue

            cur = getattr(s, "currency", None)
            cur = str(cur).lower() if cur is not None else "rub"
            if "rub" not in cur:
                continue

            buy_ok = bool(getattr(s, "buy_available_flag", True))
            api_ok = bool(getattr(s, "api_trade_available_flag", True))
            qual_only = bool(getattr(s, "for_qual_investor_flag", False))
            if not (buy_ok and api_ok):
                continue
            if qual_only:
                continue

            lot = int(getattr(s, "lot", moex_raw[ticker]["lot"]) or moex_raw[ticker]["lot"])
            name = (getattr(s, "name", None) or moex_raw[ticker]["name"]).strip()
            filtered[ticker] = {"name": name, "lot": lot}
        except Exception:
            continue

    if not filtered:
        logger.warning("⚠️ MOEX→Tinkoff фильтр отсеял все тикеры")
        return 0

    try:
        with open(CANDIDATES_FILE, "w", encoding="utf-8") as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ MOEX (фильтр Tinkoff): обновлён candidates.json — {len(filtered)} тикеров")
        return len(filtered)
    except Exception as e:
        logger.error(f"❌ Не удалось сохранить {CANDIDATES_FILE}: {e}")
        return 0


async def update_candidates_list() -> int:
    """
    Универсальный апдейтер: сначала Tinkoff, если 0 — пробуем MOEX.
    """
    n = await update_candidates_list_tinkoff()
    if n == 0:
        logger.warning("⚠️ Переходим на резерв MOEX (Tinkoff вернул 0)")
        n = await update_candidates_list_moex()
    return n


async def refresh_candidates_periodically(interval_hours: int = 24):
    while True:
        try:
            n = await update_candidates_list()
            logger.info(f"🗓️ Автообновление кандидатов завершено: {n} тикеров")
        except Exception as e:
            logger.error(f"❌ Ошибка автообновления кандидатов: {e}")
        await asyncio.sleep(interval_hours * 3600)

def save_tickers():
    try:
        with open(TICKERS_FILE, "w", encoding="utf-8") as f:
            json.dump(TICKERS, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка при сохранении тикеров: {e}")

def load_tickers():
    global TICKERS
    try:
        with open(TICKERS_FILE, "r", encoding="utf-8") as f:
            TICKERS.update(json.load(f))
            print("✅ Загружены тикеры:", TICKERS)
    except FileNotFoundError:
        print("❌ Файл тикеров не найден — создаём новый.")
        save_tickers()
    except Exception as e:
        print(f"⚠️ Ошибка при загрузке тикеров: {e}")

async def update_candidates_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Обновляю список кандидатов...")
    try:
        count = await update_candidates_list()
        await update.message.reply_text(f"✅ Обновлено: {count} тикеров в candidates.json")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка обновления: {e}")

async def debug_aflt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        ticker = "AFLT"
        # попробуем взять лот из candidates.json; по умолчанию 10
        lot = 10
        name = "Аэрофлот"
        try:
            with open(CANDIDATES_FILE, "r", encoding="utf-8") as f:
                cands = json.load(f)
            if isinstance(cands, dict) and ticker in cands:
                lot = int(cands[ticker].get("lot", lot) or lot)
                name = cands[ticker].get("name", name) or name
        except Exception:
            pass

        await reply_safe(update, f"🔍 Проверяю {name} ({ticker})…")

        price = await get_moex_price(ticker)
        prices = await load_moex_history(ticker, days=250)

        if price is None:
            await reply_safe(update, "❌ Цена не получена с MOEX.")
            return
        if not prices:
            await reply_safe(update, "❌ История цен не получена.")
            return

        lot_price = price * lot
        score = score_from_prices_local(prices)
        signal = analyze_from_prices(ticker, prices)

        await reply_safe(update,
            "📊 *Debug AFLT*\n"
            f"• Название: {name}\n"
            f"• Лот: {lot} шт\n"
            f"• Цена 1 шт: {price:.2f} ₽\n"
            f"• Цена лота: *{lot_price:.2f} ₽*\n"
            f"• Точек истории: {len(prices)}\n"
            f"• Score: *{score:.2f}*\n"
            f"• Сигнал: {signal or '—'}"
        )
    except Exception as e:
        logger.error(f"debug_aflt error: {e}")
        await reply_safe(update, "⚠️ Ошибка в /debug_aflt — см. логи.")

async def _moex_fetch_lot_size(ticker: str) -> Optional[int]:
    boards = ["TQBR", "TQTF", "TQTD"]
    async with aiohttp.ClientSession() as session:
        for board in boards:
            try:
                url = (f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/"
                       f"{board}/securities/{ticker}.json?iss.meta=off&iss.only=securities")
                async with session.get(url, timeout=15) as resp:
                    data = await resp.json()
                tbl = data.get("securities", {})
                cols = tbl.get("columns", [])
                rows = tbl.get("data", [])
                if not cols or not rows:
                    continue
                idx = {c: i for i, c in enumerate(cols)}
                i_lot = idx.get("LOTSIZE")
                if i_lot is None:
                    continue
                lot = rows[0][i_lot]
                if lot:
                    return max(int(lot), 1)
            except Exception:
                continue
    return None

async def get_trade_price(ticker: str) -> float:
    t = ticker.upper()
    try:
        client = TCS_CLIENT
        async with TCS_SEM:
            shares = await client.instruments.shares()
            figi = next((getattr(s, "figi", None)
                         for s in shares.instruments
                         if (getattr(s, "ticker", "") or "").upper() == t), None)
            if figi:
                try:
                    ob = await client.market_data.get_order_book(figi=figi, depth=1)
                    if ob.asks:
                        a = ob.asks[0].price
                        return float(a.units + a.nano * 1e-9)
                except Exception:
                    pass
                lp = await client.market_data.get_last_prices(figi=[figi])
                if lp.last_prices:
                    p = lp.last_prices[0].price
                    return float(p.units + p.nano * 1e-9)
    except Exception as e:
        logger.warning(f"Tinkoff trade price failed for {t}: {e}")
    return await get_price(t)

async def _tinkoff_fetch_lot_size(ticker: str) -> Optional[int]:
    try:
        client = TCS_CLIENT
        async with TCS_SEM:
            shares = await client.instruments.shares()
        for s in shares.instruments:
            if (getattr(s, "ticker", "") or "").upper() == ticker.upper():
                lot = int(getattr(s, "lot", 1) or 1)
                return max(lot, 1)
    except Exception:
        pass
    return None

async def get_lot_size(ticker: str) -> int:
    t = ticker.upper()

    # 1) кэш
    if t in LOTS_CACHE:
        return LOTS_CACHE[t]

    # 2) candidates.json
    try:
        with open(CANDIDATES_FILE, "r", encoding="utf-8") as f:
            cands = json.load(f)
        if isinstance(cands, dict) and t in cands:
            lot = int(cands[t].get("lot", 0) or 0)
            if lot > 0:
                LOTS_CACHE[t] = lot
                return lot
    except Exception:
        pass

    # 3) Tinkoff API
    lot = await _tinkoff_fetch_lot_size(t)
    if lot:
        LOTS_CACHE[t] = lot
        return lot

    # 4) MOEX ISS
    lot = await _moex_fetch_lot_size(t)
    if lot:
        LOTS_CACHE[t] = lot
        return lot

    # 5) запасной вариант — старая карта LOTS или 1
    lot = int(LOTS.get(t, 1))
    LOTS_CACHE[t] = max(lot, 1)
    return LOTS_CACHE[t]

def save_portfolio():
    try:
        if not isinstance(portfolio, dict):
            raise ValueError("⚠️ Неверный формат портфеля: должен быть dict")

        for ticker, data in portfolio.items():
            if not isinstance(data, dict) or "price" not in data or "amount" not in data:
                raise ValueError(f"⚠️ Неверные данные по {ticker}: {data}")

        with open("portfolio.json", "w", encoding="utf-8") as f:
            json.dump(portfolio, f, ensure_ascii=False, indent=2)
        enqueue_git_push("Update portfolio.json")
        print("✅ Портфель сохранён:", portfolio)

    except Exception as e:
        print(f"❌ Ошибка при сохранении портфеля: {e}")

def load_portfolio():
    global portfolio
    try:
        if not os.path.exists("portfolio.json"):
            print("📂 Файл портфеля не найден. Создаём пустой.")
            save_portfolio()
            return

        with open("portfolio.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("❌ Формат файла портфеля повреждён (ожидался dict)")

        for ticker, item in data.items():
            if not isinstance(item, dict) or "price" not in item or "amount" not in item:
                raise ValueError(f"❌ Неверная структура для {ticker}: {item}")

        portfolio.clear()
        portfolio.update(data)
        print("✅ Портфель загружен:", portfolio)

    except json.JSONDecodeError:
        print("❌ Ошибка чтения JSON. Файл повреждён. Создаём новый.")
        portfolio.clear()
        save_portfolio()

    except Exception as e:
        print(f"❌ Ошибка при загрузке портфеля: {e}")
        portfolio.clear()
        save_portfolio()

def score_from_prices_local(prices: List[float]) -> float:
    # тот же алгоритм, что в suggest_ideas_by_budget
    if len(prices) < 30:
        return -1.0
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    period = 14
    if len(deltas) < period:
        return -1.0
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    rsi = 100.0 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / max(avg_loss, 1e-12))))
    bonus = 0.0
    if len(prices) >= 120:
        sma50 = float(np.mean(prices[-50:]))
        sma120 = float(np.mean(prices[-120:]))
        if sma50 > sma120:
            bonus = 10.0
    return float(round((100 - rsi) + bonus, 2))


def save_history():
    try:
        with open("history.json", "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        enqueue_git_push("Update portfolio.json")
    except Exception as e:
        print(f"Ошибка при сохранении истории: {e}")

def load_history():
  global history
  try:
      with open("history.json", "r", encoding="utf-8") as f:
          history.extend(json.load(f))
  except FileNotFoundError:
      save_history()
  except Exception as e:
      print(f"Ошибка при загрузке истории: {e}")

async def buy_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer(query)
    data = query.data

    if data.startswith("buy_"):
        ticker = query.data.split("_", 1)[1]
        context.user_data['buy_ticker'] = ticker
        await query.edit_message_text(
            f"Введите цену покупки для {ticker} (в рублях):\n\nДля отмены напишите /cancel"
        )
        return BUY_PRICE

async def get_moex_price(ticker: str) -> float:
    """Возвращает актуальную цену 1 акции (не лота) с MOEX.
    Приоритет: LAST → MARKETPRICETODAY → MARKETPRICE → LCURRENTPRICE →
               PREVLEGALCLOSEPRICE/LEGALCLOSEPRICE → PREVPRICE → (BID+ASK)/2."""
    boards = ["TQBR", "TQTF", "TQTD"]
    async with aiohttp.ClientSession() as session:
        last_err = None
        for board in boards:
            try:
                url = (
                    f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/"
                    f"{board}/securities/{ticker}.json?iss.only=marketdata&iss.meta=off"
                )
                async with session.get(url, timeout=15) as resp:
                    data = await resp.json()

                md = data.get("marketdata", {})
                cols = {c: i for i, c in enumerate(md.get("columns", []))}
                rows = md.get("data") or []
                if not cols or not rows:
                    continue
                row = rows[0]

                def val(name: str):
                    i = cols.get(name)
                    v = row[i] if i is not None else None
                    return float(v) if isinstance(v, (int, float)) and v > 0 else None

                candidates = [
                    val("LAST"),
                    val("MARKETPRICETODAY"),
                    val("MARKETPRICE"),
                    val("LCURRENTPRICE"),
                    val("PREVLEGALCLOSEPRICE") or val("LEGALCLOSEPRICE"),
                    val("PREVPRICE"),
                ]
                for v in candidates:
                    if v:
                        return v

                bid, ask = val("BID"), val("OFFER")
                if bid and ask:
                    return (bid + ask) / 2
            except Exception as e:
                last_err = e
                continue
    raise ValueError(f"Нет рыночных данных для {ticker} ({last_err})")

async def get_price(ticker: str) -> float:
    t = ticker.upper()

    # 1) Tinkoff Invest (почти realtime)
    try:
        client = TCS_CLIENT
        async with TCS_SEM:
            shares = await client.instruments.shares()
            figi = None
            for s in shares.instruments:
                if (getattr(s, "ticker", "") or "").upper() == t:
                    figi = getattr(s, "figi", None)
                    break
            if figi:
                lp = await client.market_data.get_last_prices(figi=[figi])
                if lp.last_prices:
                    p = lp.last_prices[0].price
                    return float(p.units + p.nano * 1e-9)
    except Exception as e:
        logger.warning(f"Tinkoff last price failed for {t}: {e}")

    # 2) MOEX ISS (может быть задержка до ~15 минут)
    boards = ["TQBR", "TQTF", "TQTD"]
    async with aiohttp.ClientSession() as session:
        last_err = None
        for board in boards:
            try:
                url = (
                    f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/"
                    f"{board}/securities/{t}.json?iss.only=marketdata&iss.meta=off"
                )
                async with session.get(url, timeout=15) as resp:
                    data = await resp.json()
                md = data.get("marketdata", {})
                cols = {c: i for i, c in enumerate(md.get("columns", []))}
                rows = md.get("data") or []
                if not cols or not rows:
                    continue
                row = rows[0]

                def val(name: str):
                    i = cols.get(name)
                    if i is None:
                        return None
                    v = row[i]
                    return float(v) if isinstance(v, (int, float)) and v > 0 else None

                for candidate in (
                    "LAST",
                    "MARKETPRICETODAY",
                    "MARKETPRICE",
                    "LCURRENTPRICE",
                    "PREVLEGALCLOSEPRICE",
                    "LEGALCLOSEPRICE",
                    "PREVPRICE",
                ):
                    v = val(candidate)
                    if v:
                        return v

                bid, ask = val("BID"), val("OFFER")
                if bid and ask:
                    return (bid + ask) / 2
            except Exception as e:
                last_err = e
                continue

    raise ValueError(f"Нет рыночных данных для {ticker} ({last_err})")

async def show_open_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer(query)
    try:
        data = _load_open_trades_safe("open_trades.json")
        if not data:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="main_menu")]])
            await query.edit_message_text("📭 Открытых сделок нет.", reply_markup=kb)
            return

        # формируем текст
        blocks = ["📑 *Открытые сделки:*", ""]
        for t, tr in data.items():
            if tr.get("status") == "closed":
                continue
            blocks.append(_fmt_trade_block(t, tr))

        # если все закрыты — тоже скажем
        text = "\n".join(blocks).strip()
        if text == "📑 *Открытые сделки:*":
            text = "📭 Открытых сделок нет."

        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="main_menu")]])
        await query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="main_menu")]])
        await query.edit_message_text(f"⚠️ Ошибка отображения сделок: {e}", reply_markup=kb)

def upsert_open_trade(ticker: str, name: str, entry_price: float, tp1: float, tp2: float, sl: float):
    """Добавляет или обновляет открытую сделку в файл open_trades.json"""
    try:
        if os.path.exists(OPEN_TRADES_FILE):
            with open(OPEN_TRADES_FILE, "r", encoding="utf-8") as f:
                trades = json.load(f)
        else:
            trades = {}

        trades[ticker] = {
            "name": name,
            "entry_price": entry_price,
            "tp1": tp1,
            "tp2": tp2,
            "sl": sl,
            "status": "open",
            "created_at": datetime.now().isoformat()
        }

        with open(OPEN_TRADES_FILE, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False, indent=2)

    except Exception as e:
        logger.error(f"Ошибка записи сделки {ticker}: {e}")

# --- helpers для экрана "Сделки" ---
def _load_open_trades_safe(path: str = "open_trades.json") -> dict:
    try:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        # файл может быть пуст/повреждён во время записи — просто вернём пусто
        return {}

def _fmt_trade_block(ticker: str, tr: dict) -> str:
    name = tr.get("name", ticker)
    status = tr.get("status", "open")
    entry = tr.get("entry_price")
    tp1 = tr.get("tp1"); tp2 = tr.get("tp2"); sl = tr.get("sl")
    lot_size = int(tr.get("lot_size", 1))
    qty = int(tr.get("qty", 0))
    lots = (qty // max(lot_size, 1)) if qty else 0

    # трейлинг
    trail_on = bool(tr.get("trail_active", False))
    trail_flag = "🟢 вкл" if trail_on else "⚪︎ выкл"
    trail_anchor = tr.get("trail_anchor")
    trail_sl = tr.get("trail_sl")

    created = tr.get("created_at", "")
    # красиво: "2025-08-10 12:34:56"
    created_hhmm = (created[:19].replace("T", " ")) if isinstance(created, str) else ""

    status_emoji = {"open": "🟩", "tp1_hit": "🟨", "closed": "🟥"}.get(status, "🟦")

    lines = [
        f"{status_emoji} *{name}* ({ticker}) — {status}",
        f"  Вход: {entry:.2f} ₽ | TP1: {tp1:.2f} ₽ | TP2: {tp2:.2f} ₽ | SL: {sl:.2f} ₽",
        f"  Объём: {qty} акц. (~{lots} лот., лот {lot_size})",
        f"  Трейлинг: {trail_flag}" + (f" | Anchor: {trail_anchor:.2f} ₽ | Trail SL: {trail_sl:.2f} ₽" if trail_on and trail_anchor and trail_sl else ""),
        f"  Сигнал от: {created_hhmm}",
        ""
    ]
    return "\n".join(lines)

async def trades_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = _load_open_trades_safe("open_trades.json")
        if not data:
            await update.message.reply_text("📭 Открытых сделок нет.")
            return

        blocks = ["📑 *Открытые сделки:*", ""]
        for t, tr in data.items():
            if tr.get("status") == "closed":
                continue
            blocks.append(_fmt_trade_block(t, tr))

        text = "\n".join(blocks).strip()
        if text == "📑 *Открытые сделки:*":
            text = "📭 Открытых сделок нет."
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка отображения сделок: {e}")

async def lot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /lot <TICKER>")
        return
    t = context.args[0].upper()
    lot = await get_lot_size(t)
    await update.message.reply_text(f"{t}: лот = {lot}")

async def load_moex_history(ticker: str, days: int = 250) -> List[float]:
    boards = ["TQBR", "TQTF", "TQTD"]
    async with aiohttp.ClientSession() as session:
        last_err = None
        for board in boards:
            try:
                url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/{board}/securities/{ticker}.json?limit={days}"
                async with session.get(url, timeout=20) as response:
                    data = await response.json()
                history_data = data.get("history", {})
                columns = history_data.get("columns", [])
                rows = history_data.get("data", [])
                if not columns or not rows:
                    continue
                close_idx = columns.index("CLOSE")
                prices = [row[close_idx] for row in rows if row[close_idx] is not None]
                if prices:
                    return prices
            except Exception as e:
                last_err = e
                continue
    raise ValueError(f"Нет исторических данных для {ticker} на MOEX ({last_err})")

async def load_history_any(ticker: str, days: int = 250) -> List[float]:
    # 1) Пытаемся MOEX с ретраями
    for attempt in range(3):
        try:
            prices = await load_moex_history(ticker, days=days)
            if prices:
                return prices
            break
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
            else:
                logger.info(f"MOEX history fail for {ticker}: {e}")

    # 2) Фолбэк: дневные свечи из Tinkoff (CLOSE)
    try:
        frm = datetime.utcnow() - timedelta(days=days + 5)
        to = datetime.utcnow()
        client = TCS_CLIENT
        async with TCS_SEM:
            shares = await client.instruments.shares()
            figi = next((getattr(s, "figi", None)
                         for s in shares.instruments
                         if (getattr(s, "ticker", "") or "").upper() == ticker.upper()), None)
            if not figi:
                return []
            candles = await client.market_data.get_candles(
                figi=figi, from_=frm, to=to, interval=CandleInterval.CANDLE_INTERVAL_DAY
            )
            closes = []
            for c in candles.candles:
                p = c.close
                closes.append(float(p.units + p.nano * 1e-9))
            return closes
    except Exception as e:
        logger.info(f"Tinkoff candles fail for {ticker}: {e}")
        return []

def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
     if len(prices) < period + 1:
         return None
     deltas = np.diff(prices)
     gains = np.where(deltas > 0, deltas, 0)
     losses = np.where(deltas < 0, -deltas, 0)
     avg_gain = np.mean(gains[:period])
     avg_loss = np.mean(losses[:period])
     for i in range(period, len(deltas)):
         gain = gains[i]
         loss = losses[i]
         avg_gain = (avg_gain * (period - 1) + gain) / period
         avg_loss = (avg_loss * (period - 1) + loss) / period
     if avg_loss == 0:
         return 100.0
     rs = avg_gain / avg_loss
     rsi = 100 - (100 / (1 + rs))
     return round(rsi, 2)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот для отслеживания акций.\nИспользуй кнопки ниже для управления.",
        reply_markup=main_menu_kb()
    )

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Портфель", callback_data="portfolio")],
        [InlineKeyboardButton("Анализ портфеля (план заявок)", callback_data="portfolio_plan")],  # NEW
        [InlineKeyboardButton("Отслеживаемые акции", callback_data="watchlist")],
        [InlineKeyboardButton("История", callback_data="history")],
        [InlineKeyboardButton("Сделки", callback_data="open_trades")],
        [InlineKeyboardButton("Добавить тикер", callback_data="add_ticker")],
        [InlineKeyboardButton("Инвестиционные идеи", callback_data="ideas_menu")]
    ])

async def fetch_accounts():
    client = TCS_CLIENT
    async with TCS_SEM:
        accounts = await client.users.get_accounts()
        for account in accounts.accounts:
            print(f"ID: {account.id}, Type: {account.type}")
async def check_api(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
    client = TCS_CLIENT
    async with TCS_SEM:
        accounts = await client.users.get_accounts()
            await update.message.reply_text(f"✅ Tinkoff API доступен. Счетов: {len(accounts.accounts)}")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка Tinkoff API: {str(e)}")


async def fetch_price_and_analysis(ticker, name):
    try:
        trade_price = await get_trade_price(ticker)
        if trade_price is None:
            raise RuntimeError("Нет котировки")

        lot = await get_lot_size(ticker)
        lot_price = trade_price * lot
        price_str = f"{trade_price:.2f} ₽ × {lot} = {lot_price:.2f} ₽ (лот)"
    except Exception:
        return f"{name} ({ticker}) — Ошибка получения цены", ticker

    try:
        analysis_text = await analyze_stock(ticker)
        if analysis_text:
            analysis_text = f" — {analysis_text}"
    except Exception as e:
        logger.warning(f"Ошибка анализа для {ticker}: {e}")
        analysis_text = " — анализ недоступен"

    return f"{name} ({ticker}) — {price_str}{analysis_text}", ticker

def analyze_from_prices(ticker: str, prices: list[float]) -> str:
    from analysis import analyze_stock_from_prices
    return analyze_stock_from_prices(ticker, prices)

async def show_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает список отслеживаемых акций с ценами и анализом"""
    query = update.callback_query
    await safe_answer(query)

    if not TICKERS:
        await query.edit_message_text("📭 Список отслеживаемых акций пуст.")
        return

    try:
        # Получаем данные параллельно для всех тикеров
        results = await asyncio.gather(
            *(fetch_price_and_analysis(ticker, name) for ticker, name in TICKERS.items()),
            return_exceptions=True  # Позволяет продолжить при ошибках отдельных запросов
        )

        msg = "📈 *Отслеживаемые акции:*\n\n"
        keyboard = []

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка при получении данных: {result}")
                continue

            line, ticker = result
            if not ticker:
                continue

            # Разбиваем строку: "<название> (<тикер>) — <цена> — <анализ>"
            try:
                name_part, rest = line.split(" — ", 1)
                msg += f"📌 *{name_part}*\n💲 {rest}\n\n"
            except Exception:
                msg += line + "\n\n"

            keyboard.append([
                InlineKeyboardButton(f"✔️ {ticker}", callback_data=f"buy_{ticker}"),
                InlineKeyboardButton("❌", callback_data=f"remove_{ticker}"),
                InlineKeyboardButton("📊", callback_data=f"signals_{ticker}"),
                InlineKeyboardButton("🔗", url=f"https://www.tinkoff.ru/invest/stocks/{ticker}")
            ])
        keyboard.append([InlineKeyboardButton("Назад", callback_data="main_menu")])

        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Критическая ошибка в show_watchlist: {e}")
        await query.edit_message_text("⚠️ Произошла ошибка при загрузке данных. Попробуйте позже.")

async def show_detailed_signals(update: Update, context: ContextTypes.DEFAULT_TYPE, ticker: str):
    try:
        text = await analyze_stock(ticker, detailed=True)
        keyboard = [[InlineKeyboardButton("Назад", callback_data="watchlist")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Ошибка анализа сигнала для {ticker}: {e}")
        await update.callback_query.message.reply_text("⚠️ Не удалось получить сигналы.")

async def get_moex_quote(ticker: str, board: str = "TQBR") -> dict:
    # вернёт: {"last": float|None, "bid": float|None, "ask": float|None}
    import aiohttp
    url = (f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/"
           f"{board}/securities/{ticker}.json?iss.meta=off&iss.only=marketdata")
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=15) as resp:
            data = await resp.json()

    md = data.get("marketdata", {})
    cols = md.get("columns", [])
    rows = md.get("data", [])
    if not cols or not rows:
        return {"last": None, "bid": None, "ask": None}

    i = {c: idx for idx, c in enumerate(cols)}
    row = rows[0]

    def take(field):
        j = i.get(field)
        if j is None:
            return None
        v = row[j]
        return float(v) if isinstance(v, (int, float)) and v > 0 else None

    return {
        "last": take("LAST"),
        "bid":  take("BID"),
        "ask":  take("OFFER")
    }

async def show_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает текущий портфель с детализацией по каждой позиции и итогами (позиции и история)"""
    query = update.callback_query
    await safe_answer(query)

    if not portfolio:
        await query.edit_message_text("📭 Ваш портфель пуст.")
        return

    try:
        msg = "📊 Ваш портфель:\n"
        keyboard = []
        tasks = []

        for ticker, data in portfolio.items():
            tasks.append(get_portfolio_position_info(ticker, data))

        position_infos = await asyncio.gather(*tasks, return_exceptions=True)

        total_invested = 0.0   # по текущим позициям (avg price × кол-во акций)
        total_current = 0.0    # рыночная стоимость позиций сейчас

        for info in position_infos:
            if isinstance(info, Exception):
                logger.error(f"Ошибка позиции: {info}")
                continue

            msg += info.get("message", "")
            keyboard.append(info.get("buttons", []))
            total_invested += float(info.get("invested", 0.0))
            total_current  += float(info.get("current", 0.0))

        total_profit = total_current - total_invested
        total_profit_pct = (total_profit / total_invested * 100) if total_invested > 0 else 0.0

        msg += (
            "\n"
            f"💰 Общий результат: {total_profit:+.2f} ₽ ({total_profit_pct:+.2f}%)\n"
            f"📦 Текущая стоимость (по рынку): {total_current:.2f} ₽\n"
            f"📥 Инвестировано (по текущим позициям): {total_invested:.2f} ₽"
        )

        keyboard.append([InlineKeyboardButton("Назад", callback_data="main_menu")])

        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    except Exception as e:
        logger.error(f"Критическая ошибка в show_portfolio: {e}")
        await query.edit_message_text("⚠️ Произошла ошибка при загрузке портфеля. Попробуйте позже.")

def calc_history_invested() -> float:
    """Сумма всех покупок из history (action == 'buy'). Игнорирует продажи и 'n/a'."""
    total = 0.0
    for r in history:
        try:
            if isinstance(r, dict) and r.get("action") == "buy":
                t = r.get("total")
                if isinstance(t, (int, float)):
                    total += float(t)
                else:
                    p = r.get("price")
                    a = r.get("amount")
                    if isinstance(p, (int, float)) and isinstance(a, (int, float)):
                        total += float(p) * float(a)
        except Exception:
            continue
    return round(total, 2)

async def get_portfolio_position_info(ticker: str, data: dict) -> dict:
    """Возвращает информацию о позиции в портфеле с анализом и суммами для итога"""
    try:
        trade_price = await get_trade_price(ticker)
        if trade_price is None:
            raise RuntimeError("Нет котировки")

        current_price = float(trade_price)
        purchase_price = float(data["price"])
        amount_shares = int(data["amount"])  # это ШТУКИ (акции), не лоты!
        lot_size = await get_lot_size(ticker)

        invested = purchase_price * amount_shares
        current_value = current_price * amount_shares

        if purchase_price > 0:
            profit_pct = ((current_price - purchase_price) / purchase_price) * 100
        else:
            profit_pct = 0.0
        profit_abs = current_value - invested

        emoji = "🟢" if current_price > purchase_price else ("🔻" if current_price < purchase_price else "➖")

        try:
            analysis_text = await analyze_stock(ticker)
            signal_line = (analysis_text or "").strip().split("\n")[0] or "⚠️ Анализ недоступен"
        except Exception as e:
            signal_line = "⚠️ Анализ недоступен"
            logger.error(f"Ошибка анализа для {ticker}: {e}")

        message = (
            f"\n📌 {TICKERS.get(ticker, ticker)} ({ticker})\n"
            f"├ Цена покупки: {purchase_price:.2f} ₽\n"
            f"├ Текущая цена: {current_price:.2f} ₽ {emoji}\n"
            f"├ Количество: {amount_shares} шт (лот {lot_size})\n"
            f"├ Прибыль/убыток: {profit_pct:+.2f}% ({profit_abs:+.2f} ₽)\n"
            f"└ {signal_line}\n"
        )

        buttons = [
            InlineKeyboardButton(f"✔️ {ticker}", callback_data=f"buy_{ticker}"),
            InlineKeyboardButton(f"❌", callback_data=f"sell_{ticker}"),
            InlineKeyboardButton("📊", callback_data=f"signals_{ticker}")
        ]
        return {
            "message": message,
            "buttons": buttons,
            "invested": invested,
            "current": current_value
        }

    except Exception as e:
        logger.error(f"Ошибка обработки позиции {ticker}: {e}")
        return {
            "message": f"\n⚠️ {ticker}: Ошибка получения данных\n",
            "buttons": [
                InlineKeyboardButton(f"❌ Ошибка {ticker}", callback_data="error"),
                InlineKeyboardButton(f"🔄 Обновить {ticker}", callback_data=f"refresh_{ticker}")
            ],
            "invested": 0.0,
            "current": 0.0
        }

async def show_portfolio_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer(query)

    if not portfolio:
        await query.edit_message_text("📭 Ваш портфель пуст.")
        return

    try:
        # собираем параллельно цены и лоты
        tasks = []
        for ticker, data in portfolio.items():
            tasks.append(get_trade_price(ticker))
        prices = await asyncio.gather(*tasks, return_exceptions=True)

        lines = ["🧭 *Анализ портфеля — план заявок*", ""]
        kb_rows = []

        for (ticker, data), px in zip(portfolio.items(), prices):
            if isinstance(px, Exception) or px is None:
                lines.append(f"• {ticker}: цена недоступна — пропускаю")
                continue

            lot_size = await get_lot_size(ticker)
            qty_shares = int(data["amount"])
            entry = float(data["price"])
            plan = build_portfolio_order_plan(
                ticker=ticker,
                current_price=float(px),
                entry_price=entry,
                qty_shares=qty_shares,
                lot_size=lot_size,
            )

            name = ticker  # если есть карта тикеров->имён — подставьте имя
            lines.append(f"🔹 *{name}* ({ticker})")
            lines.append(f"  Вход: {plan['entry']:.2f} ₽ | Текущая: {plan['current']:.2f} ₽")
            lines.append(f"  Лот: {plan['lot_size']} | Объём: {plan['qty_shares']} акц. "
                         f"(~{plan['qty_shares']//max(plan['lot_size'],1)} лот.)")
            lines.append(f"  Рекомендации:")
            # Выведем две «ветки» TP (единый и 50/50) и два стопа
            # Для компактности — только ключевые строки, без дублей
            tp_shown = set()
            for leg in plan["legs"]:
                if leg.kind == "take_profit":
                    key = (leg.kind, leg.activation, leg.lots)
                    if key in tp_shown:
                        continue
                    tp_shown.add(key)
                price_note = f" → лимит {leg.limit:.2f} ₽" if leg.limit else ""
                lines.append(
                    f"    • {leg.what}: {leg.activation:.2f} ₽ · {leg.lots} лот(а){price_note}"
                    + (f"  — {leg.note}" if leg.note else "")
                )
            lines.append("")  # пустая строка между бумагами

            kb_rows.append([
                InlineKeyboardButton("Открыть в Тинькофф", url=f"https://www.tinkoff.ru/invest/stocks/{ticker}")
            ])

        lines.append("_Подсказка по типам заявок указана выше в каждом пункте._")
        kb_rows.append([InlineKeyboardButton("Назад", callback_data="main_menu")])
        await query.edit_message_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(kb_rows),
            parse_mode="Markdown"
        )
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка формирования плана: {e}")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
  query = update.callback_query
  await safe_answer(query)
  data = query.data

  if data == "portfolio":
      await show_portfolio(update, context)

  elif data == "watchlist":
      await show_watchlist(update, context)

  elif data == "reset":
      await reset_portfolio(update, context)

  elif data == "add_ticker":
    await query.edit_message_text(
        "Чтобы добавить тикер, отправь команду в формате:\n"
        "/addticker <ТИКЕР> <НАЗВАНИЕ>\n\n"
        "Например:\n"
        "/addticker AAPL Apple"
    )
  elif data == "update_candidates":
      await safe_answer(query)
      await query.edit_message_text("⏳ Обновляю список кандидатов...")
      try:
         count = await update_candidates_list()
         await query.edit_message_text(f"✅ Обновлено: {count} тикеров.")
      except Exception as e:
         await query.edit_message_text(f"❌ Ошибка обновления: {e}")
  elif data == "history":
      if not history:
          await query.edit_message_text("История операций пуста.")
      else:
          msg = "📜 История операций:\n"
          for record in reversed(history[-10:]):
              msg += (f"\n🛒 {record['action'].upper()} {record['ticker']} — "
                      f"{record['amount']} шт по {record['price']} ₽ "
                      f"(на {record['total']} ₽)")
          keyboard = [[InlineKeyboardButton("Назад", callback_data="main_menu")]]
          reply_markup = InlineKeyboardMarkup(keyboard)
          await query.edit_message_text(msg, reply_markup=reply_markup)

  elif data == "ideas_menu":
    keyboard = [
        [InlineKeyboardButton("💰 1000 ₽", callback_data="ideas_1000")],
        [InlineKeyboardButton("💰 3000 ₽", callback_data="ideas_3000")],
        [InlineKeyboardButton("💰 5000 ₽", callback_data="ideas_5000")],
        [InlineKeyboardButton("🔄 Обновить список", callback_data="update_candidates")],
        [InlineKeyboardButton("Назад", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        "Выбери сумму, на которую хочешь найти акции:\n\n"
        "Или напиши вручную команду, например: `/ideas 1500`",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )
  elif data.startswith("add_"):
    ticker = data.split("_", 1)[1]
    # попробуем подтянуть имя из candidates.json
    name = ticker
    try:
        with open(CANDIDATES_FILE, "r", encoding="utf-8") as f:
            candidates = json.load(f)
        if isinstance(candidates, dict) and ticker in candidates:
            name = candidates[ticker].get("name", ticker)
    except Exception:
        pass

    if ticker in TICKERS:
        await query.edit_message_text(f"{ticker} уже в отслеживаемых.")
    else:
        TICKERS[ticker] = name
        save_tickers()
        await query.edit_message_text(
            f"➕ Добавлен {ticker} — {name} в список отслеживаемых.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Открыть отслеживаемые", callback_data="watchlist")]]
            )
        )

  elif data == "main_menu":
    await query.edit_message_text("Главное меню:", reply_markup=main_menu_kb())

  elif data == "portfolio_plan":
     # на случай, если общий handler поймает раньше спец-хендлера
    await show_portfolio_plan(update, context)
      
  elif data.startswith("ideas_"):
    budget = data.split("_")[1]
    await safe_answer(query)

    # быстрый отклик, чтобы Telegram получил ответ мгновенно
    try:
        await query.edit_message_text(f"⏳ Подбираю идеи под бюджет {budget} ₽...")
    except Exception:
        await query.message.reply_text(f"⏳ Подбираю идеи под бюджет {budget} ₽...")

    context.args = [budget]
    await suggest_ideas_by_budget(update, context)
    return  # важно: не продолжать обработку дальше

  elif data.startswith("sell_"):
      ticker = data.split("_", 1)[1]
      if ticker in portfolio:
          del portfolio[ticker]
          save_portfolio()
          price_history.pop(ticker, None)
          last_signal.pop(ticker, None)

          history.append({
              "ticker": ticker,
              "action": "sell",
              "amount": "всё",
              "price": "n/a",
              "total": "n/a"
          })
          save_history()

          await query.edit_message_text(f"❌ Проданы и удалены акции {ticker} из портфеля.")
      else:
          await query.edit_message_text("Акций с таким тикером нет в портфеле.")

  elif data == "open_trades":
    await show_open_trades(update, context)  

  elif data.startswith("remove_"):
    ticker = data.split("_", 1)[1]
    if ticker in TICKERS:
        del TICKERS[ticker]
        save_tickers()
        await query.edit_message_text(f"Тикер {ticker} удалён из списка отслеживаемых.")

  elif data.startswith("signals_"):
    ticker = data.split("_", 1)[1]
    await show_detailed_signals(update, context, ticker)

  else:
    await query.edit_message_text("Такого тикера нет в списке.")

  return ConversationHandler.END

async def reset_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
       query = update.callback_query
       await safe_answer(query)
       global portfolio, price_history, last_signal, TICKERS
       portfolio.clear()
       price_history.clear()
       last_signal.clear()
       save_portfolio()
       TICKERS = {
           "MTSS": "МТС",
           "PHOR": "ФосАгро",
           "SBER": "Сбербанк"
       }
       await query.edit_message_text("Данные портфеля и список тикеров очищены и сброшены к базовому набору.")

async def daily_portfolio_plan_notifier(application, chat_id: int, hours: int = 24):
    await asyncio.sleep(5)  # дать приложению подняться
    while True:
        try:
            if portfolio:
                # Сформируем простой текст без клавиатуры
                tasks = [get_trade_price(t) for t in portfolio.keys()]
                prices = await asyncio.gather(*tasks, return_exceptions=True)
                lines = ["🗓️ Автообновление плана заявок (ежедневно)", ""]
                for (ticker, data), px in zip(portfolio.items(), prices):
                    if isinstance(px, Exception) or px is None:
                        continue
                    lot_size = await get_lot_size(ticker)
                    plan = build_portfolio_order_plan(
                        ticker=ticker,
                        current_price=float(px),
                        entry_price=float(data["price"]),
                        qty_shares=int(data["amount"]),
                        lot_size=lot_size,
                    )
                    lines.append(f"• {ticker}: TP2 {plan['tp2']:.2f} ₽ | SL {plan['sl']:.2f} ₽ "
                                 f"({plan['qty_shares']//max(lot_size,1)} лот.)")
                if len(lines) > 2:
                    await application.bot.send_message(chat_id=chat_id, text="\n".join(lines))
        except Exception as e:
            logger.error(f"daily_portfolio_plan_notifier: {e}")
        await asyncio.sleep(hours * 3600)

async def buy_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip().lower()
    if text == "/cancel":
        return await buy_cancel(update, context)

    import re
    nums = re.findall(r"\d+[.,]?\d*", text)
    if not nums:
        await update.message.reply_text(
            "⚠️ Введите число. Пример:\n"
            "`126.40` — цена за 1 акцию\n"
            "`252.79` — затем выберите «общая сумма»",
            parse_mode="Markdown"
        )
        return BUY_PRICE

    price_val = float(nums[0].replace(',', '.'))
    context.user_data['buy_price_raw'] = price_val

    # спрашиваем тип цены кнопками
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Цена за 1 акцию", callback_data="price_type_single"),
            InlineKeyboardButton("Общая сумма", callback_data="price_type_total"),
        ]
    ])
    await update.message.reply_text(
        f"Вы ввели {price_val:.2f} ₽.\nУточните, что это за значение:",
        reply_markup=keyboard
    )
    return BUY_PRICE_TYPE

async def price_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer(query)
    data = query.data

    if data == "price_type_single":
        context.user_data['price_is_total'] = False
        note = "💡 Понял: это цена за 1 акцию."
    elif data == "price_type_total":
        context.user_data['price_is_total'] = True
        note = "💡 Понял: это общая сумма за весь объём."
    else:
        await query.edit_message_text("Неверный выбор. Попробуй ещё раз.")
        return BUY_PRICE_TYPE

    lot_size = await get_lot_size(context.user_data.get('buy_ticker'))
    await query.edit_message_text(
        f"{note}\nВведите количество акций (лот {lot_size}).\n\nДля отмены — /cancel"
    )
    return BUY_AMOUNT

async def reply_safe(update, text: str):
    if update.message:
        await update.message.reply_text(text)
    elif update.callback_query:
        await update.callback_query.message.reply_text(text)

async def send_kb(update: Update, kb: InlineKeyboardMarkup):
    """Безопасно шлём клавиатуру и при callback_query, и при команде."""
    try:
        if getattr(update, "callback_query", None):
            await update.callback_query.message.reply_text("Выбери действие:", reply_markup=kb)
        else:
            await update.message.reply_text("Выбери действие:", reply_markup=kb)
    except Exception as e:
        logger.warning(f"Не удалось отправить клавиатуру: {e}")

async def suggest_ideas_by_budget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подбор идей под бюджет: параллельно (батчами через семафор), c статистикой и сортировкой."""
    def _score_from_prices_local(prices: List[float]) -> float:
        if len(prices) < 30:
            return -1.0
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        period = 14
        if len(deltas) < period:
            return -1.0
        avg_gain = np.mean(gains[:period]); avg_loss = np.mean(losses[:period])
        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rsi = 100.0 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / max(avg_loss, 1e-12))))
        bonus = 0.0
        if len(prices) >= 120:
            sma50 = float(np.mean(prices[-50:])); sma120 = float(np.mean(prices[-120:]))
            if sma50 > sma120:
                bonus = 10.0
        return float(round((100 - rsi) + bonus, 2))

    async def _process_one(ticker: str, info: dict, sem: asyncio.Semaphore, budget: float):
        """Возвращает либо dict с полями (row), либо dict с причиной skip."""
        async with sem:
            try:
                if ticker in TICKERS or ticker in portfolio:
                    return {"skip": "tracked"}
                lot = int(info.get("lot", 1) or 1)
                name = info.get("name", ticker)

                price = await get_moex_price(ticker)
                if price is None:
                    return {"skip": "no_price"}
                lot_price = price * lot
                if lot_price > budget:
                    return {"skip": "budget"}

                prices = await load_history_any(ticker, days=250)
                if not prices or len(prices) < 50:
                    return {"skip": "nohist"}

                score = _score_from_prices_local(prices)
                signal = analyze_from_prices(ticker, prices)

                row = {
                    "ticker": ticker, "name": name, "price": price, "lot": lot,
                    "lot_price": lot_price, "score": score, "signal": signal
                }
                return {"row": row}
            except Exception as e:
                logger.info(f"_process_one skip {ticker}: {e}")
                return {"skip": "error"}

    try:
        # аргумент бюджета
        if not context.args:
            await reply_safe(update, "📌 Использование: /ideas <бюджет>\nПример: /ideas 1500")
            return
        try:
            budget = float(str(context.args[0]).replace(",", "."))
        except ValueError:
            await reply_safe(update, "❌ Бюджет должен быть числом. Пример: /ideas 1500")
            return
        if budget <= 0:
            await reply_safe(update, "❌ Укажи положительное значение бюджета")
            return

        # кандидаты
        if not os.path.exists(CANDIDATES_FILE) or os.path.getsize(CANDIDATES_FILE) == 0:
            await reply_safe(update, "⏳ Список кандидатов пуст — загружаю из Tinkoff/MOEX...")
            n = await update_candidates_list()
            if n == 0:
                await reply_safe(update, "⚠️ Не удалось получить список кандидатов. Попробуй позже.")
                return

        with open(CANDIDATES_FILE, "r", encoding="utf-8") as f:
            candidates = json.load(f)
        total = len(candidates)

        # параллельная обработка
        sem = asyncio.Semaphore(10)  # лимит одновременных запросов
        tasks = [ _process_one(t, info, sem, budget) for t, info in candidates.items() ]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        # агрегируем
        hard_hits, soft_hits = [], []
        checked = skipped_tracked = skipped_budget = skipped_nohist = skipped_other = 0

        for res in results:
            if "row" in res:
                checked += 1
                r = res["row"]
                signal = r["signal"] or ""
                if "Покупать" in signal:
                    hard_hits.append(r)
                elif "Подождать" in signal:
                    soft_hits.append(r)
            else:
                reason = res.get("skip")
                if reason == "tracked":
                    skipped_tracked += 1
                elif reason == "budget":
                    skipped_budget += 1
                elif reason == "nohist":
                    skipped_nohist += 1
                else:
                    skipped_other += 1

        def _format(rows: list, title: str):
            rows.sort(key=lambda x: (-x["score"], x["lot_price"]))
            rows = rows[:10]
    
            msg = [f"*💡 {title} под бюджет {budget:.2f} ₽:*",
                   f"Проверено: {checked} из {total}",
                   f"Исключено — в отслежке/портфеле: {skipped_tracked}, дороже бюджета: {skipped_budget}, "
                   f"нет истории: {skipped_nohist}, прочее: {skipped_other}",
                   ""]
            kb_rows = []
    
            for r in rows:
                mark = "⚠️ " if "Подождать" in (r['signal'] or "") else ""
                msg.extend([
                    f"{mark}*{r['name']}* ({r['ticker']})",
                    f"💲 {r['price']:.2f} ₽ × {r['lot']} = *{r['lot_price']:.2f} ₽*",
                    f"🧮 Score: {r['score']:.2f}",
                    f"{r['signal']}",
                    ""
                ])
                kb_rows.append([
                    InlineKeyboardButton(f"➕ {r['ticker']}", callback_data=f"add_{r['ticker']}"),
                    InlineKeyboardButton("Тинькофф", url=f"https://www.tinkoff.ru/invest/stocks/{r['ticker']}")
                ])
    
            # Кнопка «Назад» — ДО return и ВНЕ цикла
            kb_rows.append([InlineKeyboardButton("Назад", callback_data="ideas_menu")])
    
            return "\n".join(msg), InlineKeyboardMarkup(kb_rows) if kb_rows else None

        sent_any = False
        if hard_hits:
            text, kb = _format(hard_hits, "Сигнал: Покупать")
            await reply_safe(update, text); sent_any = True
            if kb: await send_kb(update, kb)
        if soft_hits:
            text, kb = _format(soft_hits, "Сигнал: Подождать")
            await reply_safe(update, text + "\n_(мягкий фильтр)_"); sent_any = True
            if kb: await send_kb(update, kb)

        if not sent_any:
            explain = (
                f"📭 Под бюджет {budget:.2f} ₽ идей не нашлось.\n"
                f"Проверено: {checked} из {total}, "
                f"в отслежке/портфеле: {skipped_tracked}, "
                f"дороже бюджета: {skipped_budget}, "
                f"недостаточно истории: {skipped_nohist}, "
                f"прочее: {skipped_other}.\n\n"
                "Попробуй увеличить бюджет или дождаться обновления рынка."
            )
            await send_kb(update, InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="ideas_menu")]]))

    except Exception as e:
        logger.error(f"Ошибка в suggest_ideas_by_budget: {e}")
        await reply_safe(update, "⚠️ Произошла ошибка при поиске идей.")


async def buy_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if text == "/cancel":
        return await buy_cancel(update, context)

    # количество
    try:
        amount = int(text)
        if amount <= 0:
            await update.message.reply_text("⚠️ Количество должно быть больше нуля.")
            return BUY_AMOUNT
    except ValueError:
        await update.message.reply_text("⚠️ Введите целое число.")
        return BUY_AMOUNT

    ticker = context.user_data.get('buy_ticker')
    if not ticker:
        await update.message.reply_text("⚠️ Не выбран тикер. Начните заново из меню портфеля/отслежки.")
        return ConversationHandler.END

    lot_size = await get_lot_size(ticker)
    if amount % lot_size != 0:
        await update.message.reply_text(
            f"❗ {ticker} покупается кратно {lot_size} (1 лот). "
            "Введите количество заново или /cancel."
        )
        return BUY_AMOUNT

    # цена
    price_raw = context.user_data.get('buy_price_raw')
    if price_raw is None:
        # пользователь перепрыгнул шаг, или контекст потерян
        await update.message.reply_text(
            "⚠️ Сначала введите цену. "
            "Напишите цену (за 1 акцию) или общую сумму сделки, и я уточню."
        )
        return BUY_PRICE

    if context.user_data.get('price_is_total'):
        price = round(price_raw / amount, 6)
        await update.message.reply_text(
            f"💡 Итог: {price_raw:.2f} ₽ за {amount} акц. → {price:.2f} ₽ за 1 акцию."
        )
    else:
        price = float(price_raw)
        await update.message.reply_text(f"💡 Цена за 1 акцию: {price:.2f} ₽.")

    # запись в портфель
    if ticker in portfolio and isinstance(portfolio[ticker], dict) and \
       portfolio[ticker].get("price") not in (None, 0) and portfolio[ticker].get("amount", 0) > 0:
        existing = portfolio[ticker]
        total_amount = existing["amount"] + amount
        avg_price = (existing["price"] * existing["amount"] + price * amount) / total_amount
        portfolio[ticker] = {"price": round(avg_price, 6), "amount": total_amount}
    else:
        portfolio[ticker] = {"price": price, "amount": amount}

    save_portfolio()
    price_history.pop(ticker, None)
    last_signal.pop(ticker, None)

    history.append({
        "ticker": ticker,
        "action": "buy",
        "amount": amount,
        "price": price,
        "total": round(price * amount, 2)
    })
    save_history()

    keyboard = [[InlineKeyboardButton("Назад", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"✅ Куплено {amount} акций {ticker} по {price:.2f} ₽ за 1 акцию.",
        reply_markup=reply_markup
    )
    return ConversationHandler.END


async def refresh_candidates_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await reply_safe(update, "⏳ Обновляю список кандидатов (Tinkoff → MOEX fallback)...")
    n = await update_candidates_list()
    if n > 0:
        await reply_safe(update, f"✅ Готово: {n} тикеров.")
    else:
        await reply_safe(update, "⚠️ Не удалось обновить кандидатов.")

async def auto_suggest_ideas_daily(budget: float = 3000.0, hour: int = 9):
    class FakeMessage:
        def __init__(self, chat_id):
            self.chat = Chat(id=chat_id, type="private")
        async def reply_text(self, text, **kwargs):
            print(f"[Бот отправил сообщение]: {text}")

    class FakeUpdate:
        def __init__(self, chat_id):
            self.message = FakeMessage(chat_id)
            self.effective_chat = self.message.chat
            self.callback_query = None

    class FakeContext:
        def __init__(self):
            self.args = [str(budget)]

    while True:
        now = datetime.now()
        if now.hour == hour:
            print(f"🕘 Запуск автоанализа на {hour}:00 — бюджет {budget} ₽")
            try:
                fake_update = FakeUpdate(chat_id=CHAT_ID)
                fake_context = FakeContext()
                await suggest_ideas_by_budget(fake_update, fake_context)
            except Exception as e:
                logger.error(f"❌ Ошибка автоподбора идей: {e}")
            await asyncio.sleep(3600)  # ждать 1 час
        else:
            await asyncio.sleep(60)   # проверка раз в минуту

            
async def debug_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (context.args[0] if context.args else "AFLT").upper()
    ti = None
    mx = None

    # tinkoff
    try:
        client = TCS_CLIENT
        async with TCS_SEM:
            shares = await client.instruments.shares()
            figi = next((getattr(s, "figi", None) for s in shares.instruments
                         if (getattr(s, "ticker", "") or "").upper() == t), None)
            if figi:
                lp = await client.market_data.get_last_prices(figi=[figi])
                if lp.last_prices:
                    p = lp.last_prices[0].price
                    ti = float(p.units + p.nano * 1e-9)
    except Exception as e:
        logger.warning(f"/debug_price tinkoff {t}: {e}")

    # moex
    try:
        boards = ["TQBR", "TQTF", "TQTD"]
        async with aiohttp.ClientSession() as session:
            for board in boards:
                url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/{board}/securities/{t}.json?iss.only=marketdata&iss.meta=off"
                async with session.get(url, timeout=15) as resp:
                    data = await resp.json()
                md = data.get("marketdata", {})
                cols = {c: i for i, c in enumerate(md.get("columns", []))}
                rows = md.get("data") or []
                if not rows:
                    continue
                row = rows[0]
                i = cols.get("LAST")
                if i is not None and row[i]:
                    mx = float(row[i])
                    break
    except Exception as e:
        logger.warning(f"/debug_price moex {t}: {e}")

    await update.message.reply_text(
        f"{t}\nTinkoff: {ti if ti is not None else '—'}\nMOEX: {mx if mx is not None else '—'}"
    )
async def buy_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("Назад", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Покупка отменена.", reply_markup=reply_markup)
    return ConversationHandler.END

async def add_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        keyboard = [[InlineKeyboardButton("Назад", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "Использование: /addticker <ТИКЕР> <НАЗВАНИЕ>\nПример: /addticker AAPL Apple",
            reply_markup=reply_markup
        )
        return

    ticker = args[0].upper()
    name = " ".join(args[1:])

    if ticker in TICKERS:
        keyboard = [[InlineKeyboardButton("Назад", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"Тикер {ticker} уже в списке отслеживания.",
            reply_markup=reply_markup
        )
        return

    TICKERS[ticker] = name
    save_tickers()

    keyboard = [[InlineKeyboardButton("Назад", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"Тикер {ticker} — {name} добавлен в список отслеживаемых.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Отслеживаемые акции", callback_data="watchlist")]]
        )
    )
    return

   # --- Финальный main с исправлением 3 ---
async def main():
    # Загрузка данных
    load_tickers()
    load_portfolio()
    load_history()

    print("📊 Загружаем исторические данные для SMA...")
    start_git_worker()  # включаем фонового "гита"

    # --- Инициализируем общий Tinkoff AsyncClient + семафор ---
    global TCS_CLIENT, TCS_SEM
    TCS_CLIENT = AsyncClient(TINKOFF_TOKEN)
    await TCS_CLIENT.__aenter__()  # открываем один gRPC-канал на весь процесс
    TCS_SEM = asyncio.Semaphore(int(os.getenv("TCS_CONCURRENCY", "4")))

    for ticker in TICKERS:
        try:
            prices = await load_moex_history(ticker, days=250)
            price_history[ticker] = prices
            print(f"✅ История загружена для {ticker}, дней: {len(prices)}")
        except Exception as e:
            print(f"❌ Не удалось загрузить историю для {ticker}: {e}")

    # Инициализация бота
    request = HTTPXRequest(
        connect_timeout=20.0,
        read_timeout=60.0,
        write_timeout=20.0,
        pool_timeout=20.0,
        http_version="1.1",
    )
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).request(request).build()

    # --- СНАЧАЛА: ConversationHandler ---
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(buy_from_button, pattern="^buy_")],
        states={
            BUY_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, buy_price)],
            BUY_PRICE_TYPE: [CallbackQueryHandler(price_type_handler, pattern="^price_type_")],
            BUY_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, buy_amount)],
        },
        fallbacks=[
            CommandHandler('cancel', buy_cancel),
            MessageHandler(filters.COMMAND, buy_cancel)
        ],
    )

    print("✅ ConversationHandler добавлен первым")
    application.add_handler(conv_handler)
    
    print("✅ Команды добавлены")
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("addticker", add_ticker))
    application.add_handler(CommandHandler("check_api", check_api))
    application.add_handler(CommandHandler("ideas", suggest_ideas_by_budget))
    application.add_handler(CommandHandler("refresh_candidates", refresh_candidates_command))

    application.add_handler(CommandHandler("debug_aflt", debug_aflt))
    application.add_handler(CommandHandler("lot", lot_cmd))
    application.add_handler(CommandHandler("debug_price", debug_price))
    application.add_handler(CommandHandler("trades", trades_cmd))
    
    application.add_handler(CallbackQueryHandler(show_portfolio_plan, pattern="^portfolio_plan$"))  # NEW
    print("✅ CallbackQueryHandler добавлен")
    application.add_handler(CallbackQueryHandler(button_handler, pattern="^(?!buy_).*"))



    # --- Обработка ошибок ---
    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.error(f"Ошибка: {context.error}", exc_info=context.error)
        if isinstance(update, Update) and update.callback_query:
            try:
                await update.callback_query.answer("⚠️ Произошла ошибка, попробуйте позже")
            except Exception as e:
                logger.error(f"Ошибка при обработке ошибки: {e}")

    application.add_error_handler(error_handler)
    asyncio.create_task(auto_suggest_ideas_daily(budget=3000, hour=9))

    # --- Фоновая задача: уведомления ---
    asyncio.create_task(notify_price_changes(
        application, TICKERS, portfolio, last_signal, CHAT_ID, get_price, calculate_rsi,
        lots_map=None, candidates_file=CANDIDATES_FILE
     ))
    asyncio.create_task(daily_portfolio_plan_notifier(application, CHAT_ID))  # NEW

    # --- Фоновая задача: самопинг ---
    asyncio.create_task(self_ping())

    try:
        need_bootstrap = (not os.path.exists(CANDIDATES_FILE)) or (os.path.getsize(CANDIDATES_FILE) == 0)
    except Exception:
        need_bootstrap = True

    if need_bootstrap:
        # можно await, чтобы гарантированно заполнить до старта подбора идей
        try:
            init_n = await update_candidates_list()
            logger.info(f"🔰 Инициализация кандидатов: {init_n} тикеров")
        except Exception as e:
            logger.error(f"❌ Инициализация кандидатов упала: {e}")

    # Ежедневное автообновление в фоне
    asyncio.create_task(refresh_candidates_periodically(interval_hours=24))

    # Перед запуском polling: убираем webhook и старые апдейты
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
        me = await application.bot.get_me()
        logger.info(f"Webhook off. Starting polling for @{me.username} (id={me.id})")
    except Exception as e:
        logger.warning(f"Не удалось удалить webhook или получить getMe: {e}")

    # --- Запуск бота ---
    logger.info("Бот запускается...")
    try:
        await run_forever(application)
    except asyncio.CancelledError:
        logger.info("Бот остановлен по запросу")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        logger.info("Бот завершает работу")
                # Закрываем общий Tinkoff-клиент корректно
        if TCS_CLIENT is not None:
            try:
                await TCS_CLIENT.__aexit__(None, None, None)
            finally:
                pass
        save_tickers()
        save_portfolio()
        save_history()




if __name__ == "__main__":
    from keep_alive import keep_alive
    keep_alive()

    import asyncio
    asyncio.run(main())
