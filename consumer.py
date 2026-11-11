"""Consumidor: lee del topic y grafica en vivo temperatura, humedad y viento.

Detecta automáticamente si el mensaje es JSON o de 3 bytes.
Uso: python3 consumer.py [--save grafica.png]
"""
import argparse

import matplotlib.pyplot as plt
from kafka import KafkaConsumer

from common import KAFKA_CFG, TOPIC, WINDS, decode_auto


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--save", default="grafica.png", help="archivo donde guardar la gráfica")
    ap.add_argument("--group", default="lab-iot-23088")
    ap.add_argument("--from-beginning", action="store_true")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        TOPIC,
        group_id=args.group,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        value_deserializer=None,
        **KAFKA_CFG,
    )

    all_temp, all_hume, all_wind = [], [], []
    plt.ion()
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
    fig.suptitle(f"Estación meteorológica IoT - topic {TOPIC}")

    def plot_all():
        n = range(1, len(all_temp) + 1)
        ax1.cla(); ax2.cla(); ax3.cla()
        ax1.plot(n, all_temp, "r-o", ms=3); ax1.set_ylabel("Temperatura (°C)"); ax1.set_ylim(0, 110)
        ax2.plot(n, all_hume, "b-o", ms=3); ax2.set_ylabel("Humedad (%)"); ax2.set_ylim(0, 100)
        ax3.plot(n, [WINDS.index(w) for w in all_wind], "g-o", ms=3)
        ax3.set_yticks(range(len(WINDS))); ax3.set_yticklabels(WINDS)
        ax3.set_ylabel("Dir. viento"); ax3.set_xlabel("Muestra")
        for a in (ax1, ax2, ax3):
            a.grid(alpha=0.3)
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        fig.canvas.draw_idle()
        plt.pause(0.05)

    print(f"Escuchando topic {TOPIC}... Ctrl+C para salir.")
    try:
        for mensaje in consumer:
            payload = decode_auto(mensaje.value)
            print(f"recibido ({len(mensaje.value)} bytes): {payload}")
            all_temp.append(payload["temperatura"])
            all_hume.append(payload["humedad"])
            all_wind.append(payload["direccion_viento"])
            plot_all()
            fig.savefig(args.save, dpi=150)
    except KeyboardInterrupt:
        print("Detenido.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
