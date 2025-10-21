from kafka import KafkaProducer
import requests, json, time

# Crear conexión con Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🌦️ Enviando datos del clima de Facatativá en tiempo real... (Ctrl + C para detener)")

# URL real de Open-Meteo (Facatativá)
url = "https://api.open-meteo.com/v1/forecast?latitude=4.81&longitude=-74.35&hourly=temperature_2m,relative_humidity_2m,precipitation&timezone=America/Bogota"

while True:
    try:
        data = requests.get(url).json()
        hora = data["hourly"]["time"][-1]
        temp = data["hourly"]["temperature_2m"][-1]
        hum = data["hourly"]["relative_humidity_2m"][-1]
        prec = data["hourly"]["precipitation"][-1]

        mensaje = {
            "hora": hora,
            "temperatura": temp,
            "humedad": hum,
            "precipitacion": prec
        }

        producer.send("clima_facatativa", mensaje)
        print("📤 Enviado:", mensaje)
        time.sleep(5)

    except Exception as e:
        print("❌ Error:", e)
        time.sleep(10)
