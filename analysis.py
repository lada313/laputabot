import os
import asyncio
from datetime import datetime, timedelta
import numpy as np
from tinkoff.invest import (
    AsyncClient,
    CandleInterval,
)

TINKOFF_TOKEN = os.getenv("TINKOFF_TOKEN")

# Теперь ключи — числа (int), как в enum RealExchange (0..4)
EXCHANGE_MAP = {
    0: "Не указано",
    1: "Московская биржа (MOEX)",
    2: "СПБ биржа (SPB)",
    3: "Внебиржевой рынок (OTC)",
    4: "UNKNOWN/NEW (неизвестная биржа)"
}

async def get_raw_shares():
    async with AsyncClient(TINKOFF_TOKEN) as client:
        shares = await client.instruments.shares()

        print("Примеры акций и их биржи:")
        for instrument in shares.instruments[:20]:
            exchange_code = instrument.exchange  # это int
            exchange_name = EXCHANGE_MAP.get(exchange_code, f"UNKNOWN ({exchange_code})")
            print(f"{instrument.ticker}: exchange = {exchange_name}")

        unique_exchanges = set(instr.exchange for instr in shares.instruments)
        print("Уникальные значения exchange:", unique_exchanges)


async def get_figi_by_ticker(ticker: str) -> str | None:
    async with AsyncClient(TINKOFF_TOKEN) as client:
        shares = await client.instruments.shares()
        for share in shares.instruments:
            if share.ticker.upper() == ticker.upper():
                return share.figi
    return None


def calculate_rsi(prices: list[float], period: int = 14) -> float | None:
    if len(prices) < period + 1:
        return None
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


async def analyze_stock(ticker: str) -> str:
    figi = await get_figi_by_ticker(ticker)
    if not figi:
        return f"❌ Не найден FIGI для {ticker}"

    async with AsyncClient(TINKOFF_TOKEN) as client:
        now_utc = datetime.utcnow()
        from_utc = now_utc - timedelta(days=30)

        candles = await client.market_data.get_candles(
            figi=figi,
            from_=from_utc,
            to=now_utc,
            interval=CandleInterval.CANDLE_INTERVAL_DAY
        )

        if not candles.candles:
            return f"❌ Нет данных по {ticker}"

        close_prices = [
            c.close.units + c.close.nano * 1e-9 for c in candles.candles
        ]
        volumes = [c.volume for c in candles.candles]

        if len(close_prices) < 15:
            return f"❌ Недостаточно данных по {ticker}"

        rsi = calculate_rsi(close_prices)
        if rsi is None:
            rsi_status = "RSI: неизвестен"
        elif rsi < 30:
            rsi_status = f"RSI {rsi:.1f} — 💚 перепродан, надо брать"
        elif rsi > 70:
            rsi_status = f"RSI {rsi:.1f} — 🔺 перекуплен"
        else:
            rsi_status = f"RSI {rsi:.1f} — 🟡 нейтрально"

        avg_volume = sum(volumes[:-1]) / (len(volumes) - 1)
        current_volume = volumes[-1]
        if current_volume > avg_volume * 1.5:
            volume_status = "объем⬆"
        elif current_volume < avg_volume * 0.5:
            volume_status = "объем⬇"
        else:
            volume_status = "объем↔"

        if close_prices[-1] > close_prices[-5]:
            trend = "📈 рост"
        elif close_prices[-1] < close_prices[-5]:
            trend = "📉 падение"
        else:
            trend = "↔ тренд"

        return f"{trend} {ticker.upper()}: {rsi_status}, {volume_status}"


async def main():
    await get_raw_shares()
    # print(await analyze_stock("PHOR"))


if __name__ == "__main__":
    asyncio.run(main())