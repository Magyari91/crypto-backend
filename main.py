from fastapi import FastAPI, WebSocket
import requests
import psycopg2
import os
import pandas as pd
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi.middleware.cors import CORSMiddleware
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands

# 🔹 **FastAPI inicializálás**
app = FastAPI()

# 🔹 **CORS Engedélyezése**
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ⚠️ Biztonsági okokból később szigorítható
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🔹 **PostgreSQL kapcsolat**
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/crypto")
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# 🔹 **Táblázat létrehozása az adatbázisban**
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

# 🔹 **API-ból való adatlekérés és adatbázisba mentés**
def fetch_crypto_data():
    try:
        url_market = "https://api.coingecko.com/api/v3/global"
        market_data = requests.get(url_market).json()

        url_price = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dogecoin&vs_currencies=usd&include_market_cap=true"
        price_data = requests.get(url_price).json()

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
    
    except Exception as e:
        print(f"Hiba az adatlekérés során: {e}")

# 🔹 **WebSocket valós idejű frissítéshez**
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
                "doge_market_cap": data[8]
            })
        await asyncio.sleep(10)  # 🔹 **Frissítés 10 másodpercenként**

# 🔹 **CoinGecko API Proxy a CORS hibák elkerülésére**
@app.get("/crypto-data")
def get_crypto_data():
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&sparkline=false"
        response = requests.get(url)
        return response.json()
    except Exception as e:
        return {"error": f"Nem sikerült lekérni az adatokat: {e}"}

# 🔹 **Történelmi árfolyamadatok lekérése (5 évig visszamenőleg)**
@app.get("/crypto-history")
def get_crypto_history(coin: str = "bitcoin", days: int = 1825):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart?vs_currency=usd&days={days}"
        response = requests.get(url).json()
        
        historical_data = [{"date": point[0], "price": point[1]} for point in response["prices"]]
        return {"coin": coin, "history": historical_data}
    except Exception as e:
        return {"error": f"Nem sikerült lekérni a történelmi adatokat: {e}"}

# 🔹 **RSI, EMA és Bollinger Bands indikátorok számítása**
@app.get("/crypto-indicators")
def get_crypto_indicators(coin: str = "bitcoin", days: int = 30):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart?vs_currency=usd&days={days}"
        response = requests.get(url).json()
        
        prices = [point[1] for point in response["prices"]]
        df = pd.DataFrame({"price": prices})

        df["rsi"] = RSIIndicator(df["price"]).rsi()
        df["ema"] = EMAIndicator(df["price"], window=14).ema_indicator()
        df["bollinger_upper"] = BollingerBands(df["price"]).bollinger_hband()
        df["bollinger_lower"] = BollingerBands(df["price"]).bollinger_lband()
        
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": f"Nem sikerült kiszámítani az indikátorokat: {e}"}

# 🔹 **Scheduler beállítása 10 percenkénti frissítésre**
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_crypto_data, 'interval', minutes=10)
scheduler.start()

# 🔹 **Fő API elindítása**
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
