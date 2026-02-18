#  Local Data Engineering Pipeline

##  Descripción del Proyecto
Pipeline de ingeniería de datos automatizado (ETL) diseñado para la ingesta, transformación y almacenamiento de datos meteorológicos en tiempo real. 

Este proyecto simula un entorno de producción utilizando **Docker** para la orquestación de bases de datos y **Python** para la lógica de negocio, implementando buenas prácticas como manejo de errores, logging estructurado y variables de entorno.

##  Arquitectura
`API OpenWeatherMap` ➡️ **Extract (Python)** ➡️ **Transform (Pandas)** ➡️ **Load (SQLAlchemy)** ➡️ `PostgreSQL (Docker)`

##  Tech Stack
* **Lenguaje:** Python 3.9+
* **Contenedores:** Docker (PostgreSQL Image)
* **Base de Datos:** PostgreSQL
* **Librerías Clave:** Pandas, SQLAlchemy, Requests, Python-Dotenv
* **Automatización:** Cron Jobs (Unix)

##  Características Técnicas
* **Idempotencia:** El pipeline puede ejecutarse múltiples veces sin duplicar registros erróneos.
* **Resiliencia:** Implementación de `Exponential Backoff` para manejar fallos de red o límites de API (Rate Limiting).
* **Seguridad:** Manejo de credenciales mediante variables de entorno (`.env`).
* **Observabilidad:** Sistema de logging detallado para monitoreo de ejecuciones.

##  Instalación y Uso

1.  **Clonar el repositorio:**
    ```bash
    git clone [https://github.com/TU_USUARIO/local-weather-pipeline.git](https://github.com/TU_USUARIO/local-weather-pipeline.git)
    cd local-weather-pipeline
    ```

2.  **Configurar entorno virtual:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Levantar Base de Datos (Docker):**
    ```bash
    docker run --name pg-docker -e POSTGRES_PASSWORD=password123 -e POSTGRES_DB=weather_db -p 5433:5432 -d postgres
    ```

4.  **Configurar Variables:**
    Crear un archivo `.env` basado en el ejemplo y añadir tu API Key.

5.  **Ejecutar ETL:**
    ```bash
    python3 etl_script.py
    ```

---
