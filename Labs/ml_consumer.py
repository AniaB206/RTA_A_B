from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, requests

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_URL = "http://localhost:8001/score"

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json
import requests

# === Kafka consumer ===
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# === Kafka producer (alerts) ===
alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "http://localhost:8001/score"

print("🚀 ML Consumer started...")

# === GŁÓWNA PĘTLA ===
for message in consumer:
    tx = message.value
    print(f"\nOtrzymano transakcję: {tx}")

    try:
        # 1. Wyciąganie cech
        amount = tx.get("amount", 0)
        is_electronics = tx.get("is_electronics", 0)

        # timestamp → godzina
        timestamp = tx.get("timestamp", None)
        if timestamp:
            hour = datetime.fromisoformat(timestamp).hour
        else:
            hour = 12  # fallback

        # uproszczenie (zgodnie z zadaniem)
        tx_per_minute = 5

        features = {
            "amount": amount,
            "is_electronics": is_electronics,
            "tx_per_minute": tx_per_minute
        }

        # 2. Wywołanie API
        response = requests.post(API_URL, json=features)

        if response.status_code != 200:
            print("Błąd API:", response.text)
            continue

        result = response.json()
        print("Wynik modelu:", result)

        # 3. Jeśli fraud → wysyłamy alert
        if result.get("is_fraud"):
            alert = {
                "timestamp": datetime.now().isoformat(),
                "transaction": tx,
                "fraud_probability": result.get("fraud_probability")
            }

            alert_producer.send('alerts', alert)
            alert_producer.flush()

            print("ALERT! Fraud wykryty:", alert)

    except Exception as e:
        print("❌ Błąd przetwarzania:", e)
