import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'sensor-kelembaban-gudang'
gudang_ids = ["G1", "G2", "G3"]

print(f"Mengirim data kelembaban ke topik: {topic_name}")

try:
    while True:
        for gudang_id in gudang_ids:
            kelembaban = round(random.uniform(60, 80), 2)
            data = {
                "gudang_id": gudang_id,
                "kelembaban": kelembaban,
                "timestamp": time.time()
            }
            print(f"Mengirim: {data}")
            producer.send(topic_name, value=data)
        producer.flush()
        time.sleep(1)
except KeyboardInterrupt:
    print("Pengiriman dihentikan.")
finally:
    producer.close()
    print("Producer kelembaban ditutup.")