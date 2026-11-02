from functools import lru_cache
from math import radians, sin, cos, sqrt, atan2

from geopy.geocoders import Nominatim

# Кремль
MOSCOW_CENTER = (55.7520, 37.6175)
# Те же границы, что при очистке данных в validate_dataset.py
MOSCOW_BOUNDS = [(55.0, 37.0), (56.0, 38.0)]

_geocoder = Nominatim(user_agent="cian-estimate-bot", timeout=5)


# Функция для расчёта расстояния между кремлем и квартирой по координатам
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


# Кэш: один адрес всегда даёт одну цену, а к Nominatim меньше запросов (лимит 1 в секунду)
@lru_cache(maxsize=1024)
def geocode_moscow(location: str) -> tuple[float, float] | None:
    """Координаты района, улицы или метро в Москве; None, если место не найдено."""
    found = _geocoder.geocode(
        f"{location}, Москва",
        exactly_one=False,
        limit=5,
        country_codes="ru",
        viewbox=MOSCOW_BOUNDS,
        bounded=True,
        language="ru",
    )
    # Границы захватывают и область, поэтому проверяем сам адрес
    for place in found or []:
        if "Москва," in place.address:
            return place.latitude, place.longitude
    return None


def nearby_points(lat: float, lon: float, radius_km: float = 1.0, steps: int = 5) -> list[tuple[float, float]]:
    """Сетка точек вокруг места: геокодер точен лишь примерно, а квартира может быть где угодно рядом."""
    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * cos(radians(lat)))
    offsets = [2 * i / (steps - 1) - 1 for i in range(steps)]
    return [(lat + a * dlat, lon + b * dlon) for a in offsets for b in offsets]
