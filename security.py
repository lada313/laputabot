"""Access policy, bounded inputs and redacted logs for the private bot."""
import asyncio
from decimal import Decimal, InvalidOperation
from functools import wraps
import logging
import os
import re
import time

MAX_AMOUNT = 1_000_000
MAX_MONEY = Decimal('1000000000')
MAX_TICKERS = 100
SESSION_TTL = 600


def allowed_user_ids():
    raw = os.getenv('ALLOWED_USER_IDS', '')
    parts = [p.strip() for p in raw.split(',') if p.strip()]
    if not parts or any(not p.isascii() or not p.isdigit() or int(p) <= 0 for p in parts):
        raise ValueError('Set ALLOWED_USER_IDS to positive numeric Telegram user IDs')
    return frozenset(map(int, parts))


def authorized(update, allowed):
    user = getattr(update, 'effective_user', None)
    chat = getattr(update, 'effective_chat', None)
    return bool(user and chat and not user.is_bot and user.id in allowed
                and chat.type == 'private' and chat.id == user.id)


def make_access_guard(allowed):
    # Loaded at startup, before any Telegram or broker connections.
    last_update = {}
    async def guard(update, context):
        from telegram.ext import ApplicationHandlerStop
        if not authorized(update, allowed):
            if getattr(update, 'callback_query', None):
                await update.callback_query.answer('Нет доступа', show_alert=True)
            raise ApplicationHandlerStop
        # A single policy covers commands, callback queries and conversation steps.
        now = time.monotonic()
        previous = last_update.get(update.effective_user.id, float('-inf'))
        if now - previous < 0.25:
            if getattr(update, 'callback_query', None):
                await update.callback_query.answer('Слишком часто. Повторите через секунду.')
            raise ApplicationHandlerStop
        last_update[update.effective_user.id] = now
    return guard


def parse_money(raw):
    text = str(raw).strip()
    if len(text) > 32 or not re.fullmatch(r'[0-9]+(?:[.,][0-9]{1,6})?', text):
        raise ValueError('Введите положительное число, максимум 6 знаков после запятой')
    try:
        value = Decimal(text.replace(',', '.'))
    except InvalidOperation:
        raise ValueError('Некорректная сумма') from None
    if not value.is_finite() or not Decimal('0.000001') <= value <= MAX_MONEY:
        raise ValueError('Сумма должна быть от 0.000001 до 1000000000')
    return float(value)


def parse_amount(raw, lot_size=1):
    text = str(raw).strip().lower()
    match = re.fullmatch(r'([0-9]{1,7})\s*(л|l|lot)?', text)
    if not match:
        raise ValueError('Введите целое количество акций или лотов')
    value = int(match[1]) * (max(1, lot_size) if match[2] else 1)
    if not 0 < value <= MAX_AMOUNT:
        raise ValueError(f'Количество должно быть от 1 до {MAX_AMOUNT}')
    return value


def valid_ticker(value):
    return isinstance(value, str) and re.fullmatch(r'[A-Z0-9]{1,8}', value) is not None


def clear_trade(context):
    for key in list(context.user_data):
        if key.startswith(('buy_', 'sell_', 'trade_')) or key == 'price_is_total':
            context.user_data.pop(key, None)


def trade_active(context, kind):
    return (context.user_data.get('trade_kind') == kind
            and 0 <= time.monotonic() - context.user_data.get('trade_started', 0) <= SESSION_TTL)


def valid_price_callback(query, context, kind):
    message = getattr(query, 'message', None)
    return bool(trade_active(context, kind) and message
                and context.user_data.get('trade_price_message') == (message.chat.id, message.message_id))


async def _notice(update, text):
    query = getattr(update, 'callback_query', None)
    message = query.message if query else getattr(update, 'message', None)
    if message:
        await message.reply_text(text)


def limited_action(cooldown=30, timeout=120):
    """One in-flight execution, a cooldown, and a deadline per decorated action."""
    def decorate(func):
        busy = False
        last_started = float('-inf')
        @wraps(func)
        async def wrapper(update, context):
            nonlocal busy, last_started
            now = time.monotonic()
            if busy or now - last_started < cooldown:
                await _notice(update, 'Операция уже выполняется или недавно запускалась. Повторите позже.')
                return
            busy, last_started = True, now
            try:
                return await asyncio.wait_for(func(update, context), timeout)
            except asyncio.TimeoutError:
                await _notice(update, 'Время выполнения истекло. Повторите позже.')
            finally:
                busy = False
        return wrapper
    return decorate


class RedactingFormatter(logging.Formatter):
    def format(self, record):
        text = super().format(record)
        for name in ('TELEGRAM_TOKEN', 'TINKOFF_TOKEN', 'GIT_TOKEN'):
            secret = os.getenv(name)
            if secret:
                text = text.replace(secret, '[REDACTED]')
        text = re.sub(r'https://[^\s/@]+:[^\s/@]+@', 'https://[REDACTED]@', text)
        return text


def configure_logging():
    root = logging.getLogger()
    if not root.handlers:
        root.addHandler(logging.StreamHandler())
    for handler in root.handlers:
        handler.setFormatter(RedactingFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.setLevel(logging.INFO)
    # HTTP client INFO logs can contain the bot token as part of the request URL.
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)
