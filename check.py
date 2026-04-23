import json

with open("cian_results.json", encoding="utf-8") as f:
    raw = json.load(f)

# Берём первые 3 записи и смотрим типы и значения
for offer_id, data in list(raw.items())[:3]:
    print(f"\n--- {offer_id} ---")
    print(f"price:  {repr(data.get('price'))}")
    print(f"square: {repr(data.get('square'))}")