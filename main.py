import os
import json
import asyncio
import aiohttp
import logging
import nest_asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    CallbackQueryHandler, ConversationHandler, MessageHandler, filters
)
import numpy as np
from typing import List, Optional
from keep_alive import keep_alive
from analysis import analyze_stock
from tinkoff.invest import AsyncClient
from self_ping import self_ping

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TINKOFF_TOKEN = os.getenv("TINKOFF_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID", "0"))

TICKERS = {}
portfolio = {}
history = []
price_history = {}
last_signal = {}

LOTS = {
    "SBER": 10,
    "MTSS": 10,
    "PHOR": 1
}

# Константы
BUY_PRICE, BUY_AMOUNT = range(2)
MAX_HISTORY_DAYS = 30
TICKERS_FILE = "tickers.json"

# Инициализация nest_asyncio
nest_asyncio.apply()

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

def save_portfolio():
    try:
        if not isinstance(portfolio, dict):
            raise ValueError("⚠️ Неверный формат портфеля: должен быть dict")

        for ticker, data in portfolio.items():
            if not isinstance(data, dict) or "price" not in data or "amount" not in data:
                raise ValueError(f"⚠️ Неверные данные по {ticker}: {data}")

        with open("portfolio.json", "w", encoding="utf-8") as f:
            json.dump(portfolio, f, ensure_ascii=False, indent=2)
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

def save_history():
  try:
      with open("history.json", "w", encoding="utf-8") as f:
          json.dump(history, f, ensure_ascii=False, indent=2)
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
    await query.answer()
    data = query.data

    if data.startswith("buy_"):
        ticker = query.data.split("_", 1)[1]
        context.user_data['buy_ticker'] = ticker
        await query.edit_message_text(
            f"Введите цену покупки для {ticker} (в рублях):\n\nДля отмены напишите /cancel"
        )
        return BUY_PRICE

async def get_moex_price(ticker: str) -> float:
       url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}.json"
       async with aiohttp.ClientSession() as session:
           async with session.get(url) as response:
               data = await response.json()
               marketdata = data.get("marketdata")
               if not marketdata:
                   raise ValueError(f"Нет данных для тикера {ticker}")
               columns = marketdata["columns"]
               data_rows = marketdata.get("data")
               if not data_rows or not data_rows[0]:
                   raise ValueError(f"Нет рыночных данных для тикера {ticker}")
               values = data_rows[0]
               price_idx = columns.index("LAST")
               last_price = values[price_idx]
               return float(last_price)

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
  keyboard = [
      [InlineKeyboardButton("Портфель", callback_data="portfolio")],
      [InlineKeyboardButton("Отслеживаемые акции", callback_data="watchlist")],
      [InlineKeyboardButton("История", callback_data="history")],  # изменено
      [InlineKeyboardButton("Добавить тикер", callback_data="add_ticker")],  # <- добавлено
  ]
  reply_markup = InlineKeyboardMarkup(keyboard)
  await update.message.reply_text(
      "Привет! Я бот для отслеживания российских акций.\n"
      "Используй кнопки ниже для управления.",
      reply_markup=reply_markup
  )

async def add_ticker_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Использование:\n"
            "/addticker <ТИКЕР> <НАЗВАНИЕ>\n"
            "Например:\n"
            "/addticker AAPL Apple"
        )
        return
    ticker = args[0].upper()
    name = " ".join(args[1:])
    if ticker in TICKERS:
        await update.message.reply_text(f"Тикер {ticker} уже есть в списке.")
        return
    TICKERS[ticker] = name
    save_tickers()
    await update.message.reply_text(f"Тикер {ticker} — {name} добавлен в список отслеживаемых.")

async def fetch_accounts():
    async with AsyncClient(token=TINKOFF_TOKEN) as client:
        accounts = await client.users.get_accounts()
        for account in accounts.accounts:
            print(f"ID: {account.id}, Type: {account.type}")
async def check_api(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with AsyncClient(TINKOFF_TOKEN) as client:
            accounts = await client.users.get_accounts()
            await update.message.reply_text(f"✅ Tinkoff API доступен. Счетов: {len(accounts.accounts)}")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка Tinkoff API: {str(e)}")


async def fetch_price_and_analysis(ticker, name):
    try:
        current_price = await get_moex_price(ticker)
        price_str = f"{current_price:.2f} ₽"
    except Exception:
        return f"{name} ({ticker}) — Ошибка получения цены", None

    try:
        analysis_text = await analyze_stock(ticker)
        if analysis_text:
            analysis_text = f" — {analysis_text}"
    except Exception as e:
        print(f"Ошибка анализа для {ticker}: {e}")
        analysis_text = " — анализ недоступен"

    return f"{name} ({ticker}) — {price_str}{analysis_text}", ticker

async def show_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает список отслеживаемых акций с ценами и анализом"""
    query = update.callback_query
    await query.answer()

    if not TICKERS:
        await query.edit_message_text("📭 Список отслеживаемых акций пуст.")
        return

    try:
        # Получаем данные параллельно для всех тикеров
        results = await asyncio.gather(
            *(fetch_price_and_analysis(ticker, name) for ticker, name in TICKERS.items()),
            return_exceptions=True  # Позволяет продолжить при ошибках отдельных запросов
        )

        msg = "📈 Отслеживаемые акции:\n"
        keyboard = []

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка при получении данных: {result}")
                continue

            line, ticker = result
            msg += "\n" + line

            if ticker:  # Только если тикер действителен
                keyboard.append([
                    InlineKeyboardButton(f"💰 Купить {ticker}", callback_data=f"buy_{ticker}"),
                    InlineKeyboardButton(f"❌ Удалить {ticker}", callback_data=f"remove_{ticker}")
                ])

        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="main_menu")])
        await query.edit_message_text(
            msg, 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )   
    except Exception as e:
        logger.error(f"Критическая ошибка в show_watchlist: {e}")
        await query.edit_message_text("⚠️ Произошла ошибка при загрузке данных. Попробуйте позже.")


async def show_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает текущий портфель с детализацией по каждой позиции"""
    query = update.callback_query
    await query.answer()

    if not portfolio:
        await query.edit_message_text("📭 Ваш портфель пуст.")
        return

    try:
        msg = "📊 Ваш портфель:\n"
        keyboard = []
        tasks = []

        # Подготавливаем задачи для параллельного выполнения
        for ticker, data in portfolio.items():
            tasks.append(get_portfolio_position_info(ticker, data))

        # Выполняем все задачи параллельно
        position_infos = await asyncio.gather(*tasks, return_exceptions=True)

        for info in position_infos:
            if isinstance(info, Exception):
                logger.error(f"Ошибка позиции: {info}")
                continue
            msg += info["message"]
            keyboard.append(info["buttons"])

        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="main_menu")])
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard))

    except Exception as e:
        logger.error(f"Критическая ошибка в show_portfolio: {e}")
        await query.edit_message_text("⚠️ Произошла ошибка при загрузке портфеля. Попробуйте позже.")


async def get_portfolio_position_info(ticker: str, data: dict) -> dict:
    """Возвращает информацию о позиции в портфеле"""
    try:
        current_price = await get_moex_price(ticker)
        purchase_price = data["price"]
        amount = data["amount"]
        profit_pct = ((current_price - purchase_price) / purchase_price) * 100
        lot_size = LOTS.get(ticker, 1)

        emoji = "🟢" if current_price > purchase_price else "🔻" if current_price < purchase_price else "➖"

        message = (
            f"\n📌 {TICKERS.get(ticker, ticker)} ({ticker})\n"
            f"├ Цена покупки: {purchase_price:.2f} ₽\n"
            f"├ Текущая цена: {current_price:.2f} ₽ {emoji}\n"
            f"├ Количество: {amount} (лот {lot_size})\n"
            f"└ Прибыль/убыток: {profit_pct:+.2f}%\n"
        )

        buttons = [
            InlineKeyboardButton(f"💸 Продать {ticker}", callback_data=f"sell_{ticker}"),
            InlineKeyboardButton(f"🔄 Купить ещё {ticker}", callback_data=f"buy_{ticker}"),
        ]

        return {"message": message, "buttons": buttons}

    except Exception as e:
        logger.error(f"Ошибка обработки позиции {ticker}: {e}")
        return {
            "message": f"\n⚠️ {ticker}: Ошибка получения данных\n",
            "buttons": [
                InlineKeyboardButton(f"❌ Ошибка {ticker}", callback_data="error"),
                InlineKeyboardButton(f"🔄 Обновить {ticker}", callback_data=f"refresh_{ticker}")
            ]
        }

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
  query = update.callback_query
  await query.answer()
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

  elif data == "main_menu":
      keyboard = [
          [InlineKeyboardButton("Портфель", callback_data="portfolio")],
          [InlineKeyboardButton("Отслеживаемые акции", callback_data="watchlist")],
          [InlineKeyboardButton("История", callback_data="history")],
          [InlineKeyboardButton("Добавить тикер", callback_data="add_ticker")]
      ]
      reply_markup = InlineKeyboardMarkup(keyboard)
      await query.edit_message_text("Главное меню:", reply_markup=reply_markup)

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

  elif data.startswith("remove_"):
    ticker = data.split("_", 1)[1]
    if ticker in TICKERS:
        del TICKERS[ticker]
        save_tickers()
        await query.edit_message_text(f"Тикер {ticker} удалён из списка отслеживаемых.")
  else:
        await query.edit_message_text("Такого тикера нет в списке.")

  return ConversationHandler.END

async def reset_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
       query = update.callback_query
       await query.answer()
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

async def buy_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("📩 Вызван buy_price")
    
    if not update.message or not update.message.text:
        await update.message.reply_text("⚠️ Пожалуйста, введите цену числом.")
        return BUY_PRICE

    text = update.message.text

    if text == "/cancel":
        return await buy_cancel(update, context)

    try:
        price = float(text.replace(',', '.'))
        context.user_data['buy_price'] = price
        await update.message.reply_text("Введите количество акций:\n\nДля отмены напишите /cancel")
        return BUY_AMOUNT
    except ValueError:
        await update.message.reply_text("Некорректная цена. Попробуйте снова или напишите /cancel для отмены.")
        return BUY_PRICE


async def buy_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "/cancel":
        return await buy_cancel(update, context)
    try:
        amount = int(text)
        ticker = context.user_data.get('buy_ticker')
        price = context.user_data.get('buy_price')
        lot_size = LOTS.get(ticker, 1)
        if amount % lot_size != 0:
            await update.message.reply_text(
                f"❗ {ticker} покупается только кратно {lot_size} акциям (1 лот). Введите количество заново или /cancel для отмены."
            )
            return BUY_AMOUNT

        if ticker in portfolio:
            existing = portfolio[ticker]
            total_amount = existing["amount"] + amount
            avg_price = (existing["price"] * existing["amount"] + price * amount) / total_amount
            portfolio[ticker] = {"price": round(avg_price, 2), "amount": total_amount}
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
            f"✅ Куплено {amount} акций {ticker} по цене {price} ₽.",
            reply_markup=reply_markup
        )
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("Некорректное количество. Попробуйте снова или напишите /cancel для отмены.")
        return BUY_AMOUNT


async def buy_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("Назад", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Покупка отменена.", reply_markup=reply_markup)
    return ConversationHandler.END

async def notify_price_changes(application):
       while True:
           for ticker, name in TICKERS.items():
               if ticker in portfolio:
                   try:
                       current_price = await get_moex_price(ticker)
                       purchase_price = portfolio[ticker]["price"]
                       change = (current_price - purchase_price) / purchase_price

                       if ticker not in price_history:
                           price_history[ticker] = []
                       price_history[ticker].append(current_price)
                       if len(price_history[ticker]) > MAX_HISTORY_DAYS:
                           price_history[ticker].pop(0)

                       rsi_note = ""
                       rsi = calculate_rsi(price_history[ticker])
                       if rsi is not None:
                           if rsi < 30:
                               rsi_note = f"\n📉 RSI = {rsi}. Перепроданность — цена может быть близка к дну."
                           elif rsi > 70:
                               rsi_note = f"\n📈 RSI = {rsi}. Перекупленность — возможен откат."

                       signal = None
                       msg = None
                       if change >= 0.10:
                           recommendation = await analyze_stock(ticker)
                           print(f"Recommendation for {ticker}: {recommendation}")
                           if recommendation:
                               msg = recommendation
                           else:
                               msg = None
                               if msg and CHAT_ID != 0:
                                   try:
                                       await application.bot.send_message(chat_id=CHAT_ID, text=msg)
                                       last_signal[ticker] = msg  # чтобы не спамить одно и то же
                                   except Exception as e:
                                       print(f"Ошибка отправки сообщения: {e}")
                           if price_history[ticker]:
                               min_price = min(price_history[ticker])
                               if current_price <= min_price:
                                   msg += "\n📉 Цена достигла минимума за период!"
                       elif change <= -0.05:
                           signal = "stop_loss"
                           msg = f"⚠️ Акция {ticker}: цена упала на 5%. Проверь необходимость продажи.{rsi_note}"

                       if signal and last_signal.get(ticker) != signal:
                           await application.bot.send_message(chat_id=CHAT_ID, text=msg)
                           last_signal[ticker] = signal
                       elif signal is None:
                           last_signal[ticker] = None

                   except Exception as e:
                       print(f"Ошибка при уведомлении по {ticker}: {e}")

           await asyncio.sleep(10 * 60)

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
        f"Добавлен тикер {ticker} — {name} в список отслеживания.",
        reply_markup=reply_markup
    )

   # --- Финальный main с исправлением 3 ---
async def main():
    # Загрузка данных
    load_tickers()
    load_portfolio()
    load_history()

    # Инициализация бота
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # --- СНАЧАЛА: ConversationHandler ---
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(buy_from_button, pattern="^buy_")],
        states={
            BUY_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, buy_price)],
            BUY_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, buy_amount)],
        },
        fallbacks=[
            CommandHandler('cancel', buy_cancel),
            MessageHandler(filters.COMMAND, buy_cancel)
        ],
        # ---per_message=True
    )

    print("✅ ConversationHandler добавлен первым")
    application.add_handler(conv_handler)
    
    print("✅ Команды добавлены")
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("addticker", add_ticker_command))
    application.add_handler(CommandHandler("check_api", check_api))
    
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

    # --- Фоновая задача: уведомления ---
    asyncio.create_task(notify_price_changes(application))

    # --- Фоновая задача: самопинг ---
    asyncio.create_task(self_ping())

    # --- Запуск бота ---
    try:
        logger.info("Бот запускается...")
        await application.run_polling()
    except asyncio.CancelledError:
        logger.info("Бот остановлен по запросу")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        logger.info("Бот завершает работу")
        save_tickers()
        save_portfolio()
        save_history()


if __name__ == "__main__":
    from keep_alive import keep_alive
    keep_alive()

    import asyncio
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
