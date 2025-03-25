from fastapi import FastAPI, WebSocket
import requests
import psycopg2
import os
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi.middleware.cors import CORSMiddleware
from ta.trend import EMAIndicator, MACD, IchimokuIndicator
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands

# FastAPI inicializálás
app = FastAPI()

# CORS beállítások
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# PostgreSQL kapcsolat
def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/crypto"))

conn = get_db_connection()
cursor = conn.cursor()

# Táblázat létrehozása
cursor.execute('''
CREATE TABLE IF NOT EXISTS crypto_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    market_cap_total NUMERIC,
    btc_price NUMERIC,
    btc_market_cap NUMERIC,
    eth_price NUMERIC,
    eth_market_cap NUMERIC,
    doge_price NUMERIC,
    doge_market_cap NUMERIC,
    btc_dominance NUMERIC,
    liquidation NUMERIC,
    avg_rsi NUMERIC
);
''')
conn.commit()

# 🔹 API-ból való adatlekérés és adatbázisba mentés
def fetch_crypto_data():
    try:
        market_url = "https://api.coingecko.com/api/v3/global"
        price_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dogecoin&vs_currencies=usd&include_market_cap=true"

        market_data = requests.get(market_url).json()
        price_data = requests.get(price_url).json()

        btc_dominance = market_data['data'].get('market_cap_percentage', {}).get('btc', 0)
        market_cap_total = market_data['data'].get('total_market_cap', {}).get('usd', 0)

        btc_price = price_data.get('bitcoin', {}).get('usd', 0)
        btc_market_cap = price_data.get('bitcoin', {}).get('usd_market_cap', 0)

        eth_price = price_data.get('ethereum', {}).get('usd', 0)
        eth_market_cap = price_data.get('ethereum', {}).get('usd_market_cap', 0)

        doge_price = price_data.get('dogecoin', {}).get('usd', 0)
        doge_market_cap = price_data.get('dogecoin', {}).get('usd_market_cap', 0)

        total_liquidation = 0
        coinglass_key = os.getenv("COINGLASS_API_KEY", "")
        if coinglass_key:
            try:
                headers = {"coinglassSecret": coinglass_key}
                liquidation_url = "https://api.coinglass.com/api/futures/liquidations"
                liquidation_data = requests.get(liquidation_url, headers=headers).json()
                total_liquidation = liquidation_data.get("total", 0)
            except Exception as e:
                print(f"Hiba a likvidációs adatok lekérésekor: {e}")

        historical_url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=14"
        historical_data = requests.get(historical_url).json()
        prices = [point[1] for point in historical_data.get("prices", [])]
        
        avg_rsi = None
        if prices:
            df = pd.DataFrame({"price": prices})
            avg_rsi = RSIIndicator(df["price"]).rsi().mean()

        cursor.execute('''
        INSERT INTO crypto_data (market_cap_total, btc_price, btc_market_cap, eth_price, eth_market_cap, doge_price, doge_market_cap, btc_dominance, liquidation, avg_rsi)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (market_cap_total, btc_price, btc_market_cap, eth_price, eth_market_cap, doge_price, doge_market_cap, btc_dominance, total_liquidation, avg_rsi))
        conn.commit()
    
    except Exception as e:
        print(f"Hiba történt az API lekérdezésekor: {e}")

# 🔹 WebSocket élő adatokhoz
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        cursor.execute("SELECT * FROM crypto_data ORDER BY timestamp DESC LIMIT 1;")
        data = cursor.fetchone()
        if data:
            await websocket.send_json({
                "timestamp": data[1],
                "market_cap_total": data[2],
                "btc_price": data[3],
                "btc_market_cap": data[4],
                "eth_price": data[5],
                "eth_market_cap": data[6],
                "doge_price": data[7],
                "doge_market_cap": data[8],
                "btc_dominance": data[9],
                "liquidation": data[10],
                "avg_rsi": data[11]
            })

# 🔹 Hírek
@app.get("/crypto-news")
def get_crypto_news():
    try:
        url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN"
        response = requests.get(url).json()
        return response.get("Data", [])
    except Exception as e:
        return {"error": f"Hiba történt a hírek lekérésekor: {e}"}

# 🔹 Technikai indikátorok
@app.get("/crypto-indicators")
def get_crypto_indicators(coin: str = "bitcoin", days: int = 30):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart?vs_currency=usd&days={days}"
        response = requests.get(url).json()
        
        prices = [point[1] for point in response.get("prices", [])]
        if not prices:
            return {"error": "Nincsenek elérhető adatok"}

        df = pd.DataFrame({"high": prices, "low": prices, "close": prices})
        df["rsi"] = RSIIndicator(df["price"]).rsi()
        df["ema"] = EMAIndicator(df["price"], window=14).ema_indicator()
        df["macd"] = MACD(df["price"]).macd()
        df["bollinger_upper"] = BollingerBands(df["price"]).bollinger_hband()
        df["bollinger_lower"] = BollingerBands(df["price"]).bollinger_lband()

        ichi = IchimokuIndicator(high=df["high"], low=df["low"], close=df["close"])
        df["ichimoku_base"] = ichi.ichimoku_base_line()
        df["ichimoku_conversion"] = ichi.ichimoku_conversion_line()
        
        return df.to_dict(orient="records")
    
    except Exception as e:
        return {"error": f"Hiba történt az indikátorok kiszámításakor: {e}"}

# 🔹 ÚJ: Market Overview
@app.get("/market-overview")
def market_overview():
    try:
        cursor.execute("SELECT * FROM crypto_data ORDER BY timestamp DESC LIMIT 1;")
        data = cursor.fetchone()
        if data:
            return {
                "market_cap_total": float(data[2]),
                "btc_dominance": float(data[9]),
                "liquidation": float(data[10]),
                "avg_rsi": float(data[11])
            }
        return {"error": "Nincs elérhető adat"}
    except Exception as e:
        return {"error": f"Hiba történt: {e}"}

# 🔹 ÚJ: CoinGecko adat proxy
@app.get("/crypto-data")
def crypto_data():
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 50,
            "page": 1,
            "sparkline": False
        }
        response = requests.get(url, params=params)
        return response.json()
    except Exception as e:
        return {"error": f"Adatlekérés sikertelen: {e}"}

# 🔹 Ütemezett frissítés
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_crypto_data, 'interval', minutes=10)
scheduler.start()

# 🔹 Futás
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
