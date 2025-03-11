from fastapi import FastAPI, WebSocket
import requests
import psycopg2
import os
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi.middleware.cors import CORSMiddleware
from ta.trend import EMAIndicator, MACD
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
    doge_market_cap NUMERIC
);
''')
conn.commit()

# API-ból való adatlekérés
def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/global"
    market_data = requests.get(url).json()

    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dogecoin&vs_currencies=usd&include_market_cap=true"
    price_data = requests.get(url).json()

    market_cap_total = market_data['data']['total_market_cap']['usd']
    
    btc_price = price_data['bitcoin']['usd']
    btc_market_cap = price_data['bitcoin']['usd_market_cap']
    
    eth_price = price_data['ethereum']['usd']
    eth_market_cap = price_data['ethereum']['usd_market_cap']
    
    doge_price = price_data['dogecoin']['usd']
    doge_market_cap = price_data['dogecoin']['usd_market_cap']
    
    cursor.execute('''
    INSERT INTO crypto_data (market_cap_total, btc_price, btc_market_cap, eth_price, eth_market_cap, doge_price, doge_market_cap)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ''', (market_cap_total, btc_price, btc_market_cap, eth_price, eth_market_cap, doge_price, doge_market_cap))
    conn.commit()

# WebSocket élő adatokhoz
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        cursor.execute("SELECT * FROM crypto_data ORDER BY timestamp DESC LIMIT 1;")
        data = cursor.fetchone()
        await websocket.send_json({
            "timestamp": data[1],
            "market_cap_total": data[2],
            "btc_price": data[3],
            "btc_market_cap": data[4],
            "eth_price": data[5],
            "eth_market_cap": data[6],
            "doge_price": data[7],
            "doge_market_cap": data[8]
        })

# CoinTelegraph hírek integrálása
@app.get("/crypto-news")
def get_crypto_news():
    url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN"
    response = requests.get(url).json()
    return response["Data"]

# Scheduler beállítása 10 percenkénti frissítésre
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_crypto_data, 'interval', minutes=10)
scheduler.start()

# Fő API elindítása
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
