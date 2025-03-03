from fastapi import FastAPI
import requests
import psycopg2
import os
from apscheduler.schedulers.background import BackgroundScheduler

# FastAPI inicializálás
app = FastAPI()

# PostgreSQL kapcsolat
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/crypto")
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Táblázat létrehozása az adatbázisban
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

# Adatok lekérése API endpoint
@app.get("/crypto")
def get_crypto_data():
    cursor.execute("SELECT * FROM crypto_data ORDER BY timestamp DESC LIMIT 1;")
    data = cursor.fetchone()
    return {
        "timestamp": data[1],
        "market_cap_total": data[2],
        "btc_price": data[3],
        "btc_market_cap": data[4],
        "eth_price": data[5],
        "eth_market_cap": data[6],
        "doge_price": data[7],
        "doge_market_cap": data[8]
    }

# Scheduler beállítása 10 percenkénti frissítésre
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_crypto_data, 'interval', minutes=10)
scheduler.start()

# Fő API elindítása
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
