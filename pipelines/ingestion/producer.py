import requests
import json
from kafka import KafkaProducer
import time

API_URL = "https://fakestoreapi.com/products"
KAFKA_TOPIC = "products"
KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    client_id="product-producer",
    retries=5,
    acks="all",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_products():
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()

def send_to_kafka(products):
    for product in products:
        producer.send(KAFKA_TOPIC, product)
        print(f"[✓] Enviado: {product['id']}")
        time.sleep(0.2) 
    
if __name__ == "__main__":
    products = fetch_products()
    send_to_kafka(products)
    producer.flush()
