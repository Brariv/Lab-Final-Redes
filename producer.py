"""Productor: simula la estación meteorológica y publica a Kafka cada 15-30 s.

Uso:
    python3 producer.py            # JSON (sección 3.2)
    python3 producer.py --compact  # payload de 3 bytes (sección 3.4)
    python3 producer.py --fast     # intervalo de 2 s para pruebas
"""
import argparse
import random
import time

from kafka import KafkaProducer

from common import KAFKA_CFG, TOPIC, WINDS, encode, encode_json

MEDIA, SIGMA = 55.0, 15.0  # media 55, desviación estándar 15 (varianza 225)


def generar_data() -> dict:
    temp = min(110.0, max(0.0, random.gauss(MEDIA, SIGMA)))
    hum = int(round(min(100, max(0, random.gauss(MEDIA, SIGMA)))))
    return {
        "temperatura": round(temp, 2),
        "humedad": hum,
        "direccion_viento": random.choice(WINDS),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--compact", action="store_true", help="payload de 3 bytes")
    ap.add_argument("--fast", action="store_true", help="enviar cada 2 s (pruebas)")
    args = ap.parse_args()

    producer = KafkaProducer(**KAFKA_CFG)
    print(f"Productor -> topic {TOPIC} ({'3 bytes' if args.compact else 'JSON'}). Ctrl+C para detener.")
    try:
        while True:
            data = generar_data()
            value = encode(data) if args.compact else encode_json(data)
            producer.send(TOPIC, key=b"sensor1", value=value).get(timeout=15)
            print(f"enviado {data} -> {value!r} ({len(value)} bytes)")
            time.sleep(2 if args.fast else random.uniform(15, 30))
    except KeyboardInterrupt:
        print("Detenido.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
