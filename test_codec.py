import random
from common import encode, decode, WINDS
random.seed(1)
for _ in range(100000):
    d = {"temperatura": round(random.uniform(0, 110), 2), "humedad": random.randint(0, 100),
         "direccion_viento": random.choice(WINDS)}
    b = encode(d)
    assert len(b) == 3 and decode(b) == d, (d, b, decode(b))
for d in [{"temperatura": 110.0, "humedad": 100, "direccion_viento": "NE"},
          {"temperatura": 0.0, "humedad": 0, "direccion_viento": "N"}]:
    print(d, encode(d).hex(), decode(encode(d)))
print("OK: 100000 round-trips exactos, siempre 3 bytes")
