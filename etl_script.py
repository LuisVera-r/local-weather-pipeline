import os
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# --- Configuración y Validación Temprana ---
load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
DB_URI = os.getenv("DB_URI")

if not API_KEY or not DB_URI:
    raise EnvironmentError("Faltan variables de entorno: OPENWEATHER_API_KEY y/o DB_URI")

CITIES = ["Mexico City", "Monterrey", "Queretaro"]
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
TIMEOUT_SECONDS = 10


# --- HTTP Session con Reintentos ---
def build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# --- Extract ---
def extract_weather_data(session: requests.Session) -> list[dict]:
    """Fase de Extracción: Obtiene datos crudos de la API."""
    weather_data = []
    logger.info("Iniciando extracción...")

    for city in CITIES:
        try:
            response = session.get(
                BASE_URL,
                params={"q": f"{city},mx", "appid": API_KEY},
                timeout=TIMEOUT_SECONDS
            )
            response.raise_for_status()
            data = response.json()

            weather_data.append({
                "city": city,
                "temp_kelvin": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather_desc": data["weather"][0]["description"],
                "timestamp": datetime.now(timezone.utc)
            })
            logger.info(f"Datos obtenidos para: {city}")

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout al consultar {city}, se omite.")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error HTTP para {city}: {e.response.status_code}")
        except (KeyError, IndexError) as e:
            logger.error(f"Respuesta inesperada de API para {city}: {e}")

    return weather_data


# --- Transform ---
def transform_data(raw_data: list[dict]) -> pd.DataFrame:
    """Fase de Transformación: Limpieza y lógica de negocio."""
    if not raw_data:
        logger.warning("No hay datos para transformar.")
        return pd.DataFrame()

    logger.info("Transformando datos...")
    df = pd.DataFrame(raw_data)

    df["temp_celsius"] = (df["temp_kelvin"] - 273.15).round(2)
    df = df.drop(columns=["temp_kelvin"])
    # Eliminar duplicados si corres el script muy rápido
    df = df.drop_duplicates(subset=["city", "timestamp"])

    return df


# --- Load ---
def load_data_to_postgres(df: pd.DataFrame) -> None:
    """Fase de Carga: Guardar en Base de Datos SQL."""
    if df.empty:
        logger.warning("DataFrame vacío, nada que guardar.")
        return

    logger.info("Guardando en PostgreSQL...")
    try:
        engine = create_engine(DB_URI, pool_pre_ping=True)
        with engine.begin() as conn:
            df.to_sql("weather_logs", conn, if_exists="append", index=False)
        logger.info("Carga exitosa en tabla 'weather_logs'.")

    except SQLAlchemyError as e:
        logger.exception(f"Error crítico en base de datos: {e}")
        raise


# --- Orquestador ---
if __name__ == "__main__":
    logger.info("--- INICIO DEL PIPELINE ETL ---")
    try:
        session = build_http_session()
        raw = extract_weather_data(session)
        clean_df = transform_data(raw)
        load_data_to_postgres(clean_df)
        logger.info("--- PIPELINE COMPLETADO ---")
    except Exception as e:
        logger.critical(f"Pipeline abortado: {e}")
        raise SystemExit(1)