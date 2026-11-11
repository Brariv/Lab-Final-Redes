"""Configuración compartida y codificación/decodificación del laboratorio IoT."""
import json

BOOTSTRAP = "161.35.11.141:9092"
TOPIC = "23088"
KAFKA_CFG = dict(
    bootstrap_servers=BOOTSTRAP,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="estudiante",
    sasl_plain_password="TryluRandomPass-2!@!",
)

WINDS = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]  # índice = 3 bits


# ---------- Formato JSON (secciones 3.1 - 3.3) ----------
def encode_json(data: dict) -> bytes:
    return json.dumps(data).encode("utf-8")


def decode_json(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))


# ---------- Formato compacto de 3 bytes (sección 3.4) ----------
# Layout de 24 bits:  [ temp: 14 bits | humedad: 7 bits | viento: 3 bits ]
# temp se guarda como entero = round(temperatura * 100)  -> 0..11000 < 2^14 = 16384
def encode(data: dict) -> bytes:
    t = int(round(data["temperatura"] * 100))
    t = max(0, min(11000, t))
    h = max(0, min(100, int(data["humedad"])))
    w = WINDS.index(data["direccion_viento"])
    word = (t << 10) | (h << 3) | w
    return word.to_bytes(3, "big")


def decode(raw: bytes) -> dict:
    assert len(raw) == 3, "payload debe ser de 3 bytes"
    word = int.from_bytes(raw, "big")
    return {
        "temperatura": round(((word >> 10) & 0x3FFF) / 100, 2),
        "humedad": (word >> 3) & 0x7F,
        "direccion_viento": WINDS[word & 0x7],
    }


def decode_auto(raw: bytes) -> dict:
    """3 bytes -> formato compacto; si no, JSON."""
    return decode(raw) if len(raw) == 3 else decode_json(raw)
