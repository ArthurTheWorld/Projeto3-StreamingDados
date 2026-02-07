import json
import time
import requests
from datetime import datetime, timezone
from google.cloud import storage


BUCKET_NAME = "bucket-portfolio-projeto-3"
BRONZE_PATH = "bronze/yfinance"
CARTEIRA_FILE = "/app/carteira.txt"
API_KEY_FILE = "/app/api_key.txt"

FINNHUB_URL = "https://finnhub.io/api/v1/quote"
EXCHANGE_DEFAULT = "NASDAQ"

SERVICE_ACCOUNT_FILE = "/app/portfolio-projeto-3-486022-c80446521334.json"


class YFinanceCollector:
    def __init__(self):
        self.api_key = self.load_api_key()

        self.client = storage.Client.from_service_account_json(
            SERVICE_ACCOUNT_FILE
        )
        self.bucket = self.client.bucket(BUCKET_NAME)

    def load_api_key(self):
        with open(API_KEY_FILE) as f:
            return f.read().strip()

    def load_carteira(self):
        with open(CARTEIRA_FILE) as f:
            return [line.strip() for line in f if line.strip()]

    def get_quote(self, ticker):
        params = {
            "symbol": ticker,
            "token": self.api_key
        }

        r = requests.get(FINNHUB_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()

        
        price = data.get("c")

        if not price or price == 0:
            raise ValueError("Sem preço disponível")

        
        change = data.get("d") or 0.0
        change_percent = data.get("dp") or 0.0

        return {
            "ticker": ticker,
            "price": float(price),
            "change": float(change),
            "change_percent": float(change_percent),
            "exchange": EXCHANGE_DEFAULT,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def send_to_gcs(self, payload):
        filename = (
            f"{BRONZE_PATH}/"
            f"{payload['ticker']}_"
            f"{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}.json"
        )

        blob = self.bucket.blob(filename)
        blob.upload_from_string(
            json.dumps(payload),
            content_type="application/json"
        )

        print(
            f"📤 {payload['ticker']} | "
            f"price={payload['price']} | "
            f"change={payload['change']} | "
            f"dp={payload['change_percent']}"
        )

    def start(self):
        print("📊 Listener de ações iniciado (Finnhub via REST)")
        carteira = self.load_carteira()
        print(f"📌 Carteira atualizada: {carteira}")

        while True:
            for ticker in carteira:
                try:
                    data = self.get_quote(ticker)
                    self.send_to_gcs(data)
                except Exception as e:
                    print(f"⚠️ Erro para {ticker}: {e}")

                time.sleep(2)

            time.sleep(60)



if __name__ == "__main__":
    collector = YFinanceCollector()
    collector.start()
