import requests

def get_moex_price(ticker):
    try:
        url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}.json"
        response = requests.get(url)
        data = response.json()
        market_data = data["marketdata"]["data"][0]
        price = market_data[12]  # Последняя цена сделки (LAST)
        return float(price) if price else None
    except Exception as e:
        print(f"Ошибка получения цены {ticker}: {e}")
        return None
