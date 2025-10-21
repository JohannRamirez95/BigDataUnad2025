from kafka import KafkaConsumer
import json

# Conectarse al topic de Kafka
consumer = KafkaConsumer(
    'clima_facatativa',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

print("🌍 Esperando mensajes del clima en tiempo real...\n")

for mensaje in consumer:
    data = mensaje.value
    print(f"⏰ {data['hora']} | 🌡️ {data['temperatura']}°C | 💧 {data['humedad']}% | ☔ {data['precipitacion']} mm")
