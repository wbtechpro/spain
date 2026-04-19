"""
Centroids of all 52 Spanish provinces (50 on peninsula/islands + Ceuta + Melilla).

Coordinates are the geographic centroid of each province (not the capital city).
Derived from Natural Earth / Wikipedia data, rounded to 2 decimals.

`aliases` holds the exact forms used by different Spanish government PDFs,
which vary wildly in accents, spelling and provincial naming conventions.
"""
from __future__ import annotations

PROVINCES = [
    # (name_ru, name_es, ine_code, lat, lng, aliases)
    ("Алава",           "Araba/Álava",               "01", 42.84, -2.71, ["Araba", "Álava", "Alava", "Araba/Álava"]),
    ("Альбасете",       "Albacete",                  "02", 38.83, -1.86, ["Albacete"]),
    ("Аликанте",        "Alicante",                  "03", 38.48, -0.54, ["Alicante", "Alicante/Alacant", "Alacant"]),
    ("Альмерия",        "Almería",                   "04", 37.16, -2.33, ["Almería", "Almeria"]),
    ("Авила",           "Ávila",                     "05", 40.57, -4.95, ["Ávila", "Avila"]),
    ("Бадахос",         "Badajoz",                   "06", 38.80, -6.00, ["Badajoz"]),
    ("Балеары",         "Illes Balears",             "07", 39.50,  2.80, ["Balears (Illes)", "Baleares(Illes)", "Baleares (Illes)", "Illes Balears", "Baleares", "Islas Baleares"]),
    ("Барселона",       "Barcelona",                 "08", 41.60,  2.00, ["Barcelona"]),
    ("Бургос",          "Burgos",                    "09", 42.35, -3.60, ["Burgos"]),
    ("Касерес",         "Cáceres",                   "10", 39.70, -6.30, ["Cáceres", "Caceres"]),
    ("Кадис",           "Cádiz",                     "11", 36.50, -5.80, ["Cádiz", "Cadiz"]),
    ("Кастельон",       "Castellón",                 "12", 40.08, -0.11, ["Castellón", "Castellón/Castelló", "Castelló"]),
    ("Сьюдад-Реаль",    "Ciudad Real",               "13", 39.00, -3.90, ["Ciudad Real", "CiudadReal"]),
    ("Кордова",         "Córdoba",                   "14", 37.90, -4.85, ["Córdoba", "Cordoba"]),
    ("Ла-Корунья",      "A Coruña",                  "15", 43.17, -8.40, ["A Coruña", "Coruña (A)", "Coruña, A", "La Coruña"]),
    ("Куэнка",          "Cuenca",                    "16", 40.00, -2.20, ["Cuenca"]),
    ("Жирона",          "Girona",                    "17", 42.15,  2.55, ["Girona", "Gerona"]),
    ("Гранада",         "Granada",                   "18", 37.30, -3.10, ["Granada"]),
    ("Гвадалахара",     "Guadalajara",               "19", 40.80, -2.30, ["Guadalajara"]),
    ("Гипускоа",        "Gipuzkoa",                  "20", 43.17, -2.20, ["Gipuzkoa", "Guipuzkoa", "Guipúzcoa", "Guipuzcoa"]),
    ("Уэльва",          "Huelva",                    "21", 37.55, -6.95, ["Huelva"]),
    ("Уэска",           "Huesca",                    "22", 42.20, -0.10, ["Huesca"]),
    ("Хаэн",            "Jaén",                      "23", 38.00, -3.50, ["Jaén", "Jaen"]),
    ("Леон",            "León",                      "24", 42.67, -6.00, ["León", "Leon"]),
    ("Льейда",          "Lleida",                    "25", 42.00,  1.00, ["Lleida", "Lérida"]),
    ("Ла-Риоха",        "La Rioja",                  "26", 42.30, -2.50, ["La Rioja", "Rioja (La)", "Rioja, La"]),
    ("Луго",            "Lugo",                      "27", 43.00, -7.50, ["Lugo"]),
    ("Мадрид",          "Madrid",                    "28", 40.45, -3.70, ["Madrid", "Madrid (Comunidad de)", "Comunidad de Madrid"]),
    ("Малага",          "Málaga",                    "29", 36.75, -4.70, ["Málaga", "Malaga"]),
    ("Мурсия",          "Murcia",                    "30", 38.05, -1.60, ["Murcia", "Murcia (Región de)", "Región de Murcia"]),
    ("Наварра",         "Navarra",                   "31", 42.70, -1.65, ["Navarra", "Navarra (Com. Foral de)", "Comunidad Foral de Navarra"]),
    ("Оренсе",          "Ourense",                   "32", 42.20, -7.60, ["Ourense", "Orense"]),
    ("Астурия",         "Asturias",                  "33", 43.35, -6.00, ["Asturias", "Asturias (Principado de)", "Principado de Asturias"]),
    ("Паленсия",        "Palencia",                  "34", 42.30, -4.55, ["Palencia"]),
    ("Лас-Пальмас",     "Las Palmas",                "35", 28.20, -15.45, ["Las Palmas", "Palmas (Las)", "Palmas, Las"]),
    ("Понтеведра",      "Pontevedra",                "36", 42.50, -8.50, ["Pontevedra"]),
    ("Саламанка",       "Salamanca",                 "37", 40.80, -6.00, ["Salamanca"]),
    ("Санта-Крус",      "Santa Cruz de Tenerife",    "38", 28.50, -16.90, ["Santa Cruz de Tenerife", "SantaCruzdeTenerife", "SC Tenerife", "Santa Cruz De Tenerife"]),
    ("Кантабрия",       "Cantabria",                 "39", 43.20, -4.03, ["Cantabria"]),
    ("Сеговия",         "Segovia",                   "40", 41.05, -4.10, ["Segovia"]),
    ("Севилья",         "Sevilla",                   "41", 37.40, -5.60, ["Sevilla", "Seville"]),
    ("Сория",           "Soria",                     "42", 41.70, -2.70, ["Soria"]),
    ("Таррагона",       "Tarragona",                 "43", 41.20,  1.10, ["Tarragona"]),
    ("Теруэль",         "Teruel",                    "44", 40.60, -0.80, ["Teruel"]),
    ("Толедо",          "Toledo",                    "45", 39.80, -4.10, ["Toledo"]),
    ("Валенсия",        "Valencia",                  "46", 39.50, -0.85, ["Valencia", "Valencia/València", "València"]),
    ("Вальядолид",      "Valladolid",                "47", 41.70, -4.80, ["Valladolid"]),
    ("Бискайя",         "Bizkaia",                   "48", 43.25, -2.90, ["Bizkaia", "Vizcaya"]),
    ("Самора",          "Zamora",                    "49", 41.70, -6.00, ["Zamora"]),
    ("Сарагоса",        "Zaragoza",                  "50", 41.65, -0.90, ["Zaragoza", "Saragoza"]),
    ("Сеута",           "Ceuta",                     "51", 35.89, -5.31, ["Ceuta"]),
    ("Мелилья",         "Melilla",                   "52", 35.29, -2.94, ["Melilla"]),
]


def normalize(s: str) -> str:
    """Lowercase, strip accents, remove spaces/punctuation for fuzzy matching."""
    import unicodedata
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return "".join(c for c in s.lower() if c.isalnum())


# Build lookup: normalized_alias -> (name_ru, name_es, ine, lat, lng)
PROVINCE_LOOKUP: dict[str, tuple[str, str, str, float, float]] = {}
for ru, es, ine, lat, lng, aliases in PROVINCES:
    for alias in [es, *aliases, ru]:
        PROVINCE_LOOKUP[normalize(alias)] = (ru, es, ine, lat, lng)


def find(name: str):
    """Find a province by any name/alias. Returns (ru, es, ine, lat, lng) or None."""
    return PROVINCE_LOOKUP.get(normalize(name))
