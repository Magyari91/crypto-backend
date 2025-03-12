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
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/crypto")
conn = psycopg2.connect(DATABASE_URL)
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

# API-ból való adatlekérés és mentés
def fetch_crypto_data():
    try:
        market_url = "https://api.coingecko.com/api/v3/global"
        market_data = requests.get(market_url).json()
        
        price_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dogecoin&vs_currencies=usd&include_market_cap=true"
        price_data = requests.get(price_url).json()

        btc_dominance = market_data['data']['market_cap_percentage']['btc']
        market_cap_total = market_data['data']['total_market_cap']['usd']
        
        btc_price = price_data['bitcoin']['usd']
        btc_market_cap = price_data['bitcoin']['usd_market_cap']
        
        eth_price = price_data['ethereum']['usd']
        eth_market_cap = price_data['ethereum']['usd_market_cap']
        
        doge_price = price_data['dogecoin']['usd']
        doge_market_cap = price_data['dogecoin']['usd_market_cap']

        # 🔹 Likvidációs adatok CoinGlass API-ból
        liquidation_url = "https://api.coinglass.com/api/futures/liquidations"
        headers = {"coinglassSecret": os.getenv("COINGLASS_API_KEY")}
        liquidation_data = requests.get(liquidation_url, headers=headers).json()
        total_liquidation = liquidation_data.get("total", 0)

        # 🔹 Átlag RSI számítása
        historical_url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=14"
        historical_data = requests.get(historical_url).json()
        prices = [point[1] for point in historical_data["prices"]]
        df = pd.DataFrame({"price": prices})
        avg_rsi = RSIIndicator(df["price"]).rsi().mean()

        # Adatok mentése adatbázisba
        cursor.execute('''
        INSERT INTO crypto_data (market_cap_total, btc_price, btc_market_cap, eth_price, eth_market_cap, doge_price, doge_market_cap, btc_dominance, liquidation, avg_rsi)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (market_cap_total, btc_price, btc_market_cap, eth_price, eth_market_cap, doge_price, doge_market_cap, btc_dominance, total_liquidation, avg_rsi))
        conn.commit()
    
    except Exception as e:
        print(f"Hiba történt az API lekérdezésekor: {e}")

# WebSocket élő adatokhoz
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

# CoinTelegraph hírek API
@app.get("/crypto-news")
def get_crypto_news():
    url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN"
    response = requests.get(url).json()
    return response["Data"]

# Technikai elemzések (Fibonacci, Ichimoku Cloud, RSI, MACD, Bollinger)
@app.get("/crypto-indicators")
def get_crypto_indicators(coin: str = "bitcoin", days: int = 30):
    url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart?vs_currency=usd&days={days}"
    response = requests.get(url).json()
    
    prices = [point[1] for point in response["prices"]]
    df = pd.DataFrame({"price": prices})

    df["rsi"] = RSIIndicator(df["price"]).rsi()
    df["ema"] = EMAIndicator(df["price"], window=14).ema_indicator()
    df["macd"] = MACD(df["price"]).macd()
    df["bollinger_upper"] = BollingerBands(df["price"]).bollinger_hband()
    df["bollinger_lower"] = BollingerBands(df["price"]).bollinger_lband()

    # 🔹 Ichimoku Cloud számítása
    ichi = IchimokuIndicator(df["price"])
    df["ichimoku_base"] = ichi.ichimoku_base_line()
    df["ichimoku_conversion"] = ichi.ichimoku_conversion_line()
    
    return df.to_dict(orient="records")

# Scheduler beállítása 10 percenkénti frissítésre
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_crypto_data, 'interval', minutes=10)
scheduler.start()

# Fő API elindítása
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
