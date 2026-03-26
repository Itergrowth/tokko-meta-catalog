"""
config.py — Configuración central del servidor Tokko Meta Catalog.
Lee variables de entorno (o .env) para parametrizar el comportamiento.
"""

import os
from dotenv import load_dotenv

# Carga el archivo .env si existe (útil en desarrollo local)
load_dotenv()

# ── Tokko Broker ──────────────────────────────────────────────────────────────
# Clave de API de Tokko Broker (obligatoria)
TOKKO_API_KEY = os.getenv("TOKKO_API_KEY", "")

# URL base de la API de Tokko Broker
TOKKO_BASE_URL = "https://www.tokkobroker.com/api/v1"

# ── Comportamiento del servidor ───────────────────────────────────────────────
# Cada cuántas horas se refresca el feed automáticamente
REFRESH_INTERVAL_HOURS = int(os.getenv("REFRESH_INTERVAL_HOURS", "24"))

# Puerto en que escucha uvicorn (Railway inyecta $PORT automáticamente)
PORT = int(os.getenv("PORT", "8000"))

# ── Filtros del feed ──────────────────────────────────────────────────────────
# Tipos de operación a incluir, separados por coma (Venta, Alquiler, etc.)
PROPERTY_TYPES = os.getenv("PROPERTY_TYPES", "Venta,Alquiler")

# Moneda en que se expresan los precios del feed
CURRENCY = os.getenv("CURRENCY", "USD")

# URL base opcional para construir las URLs de las propiedades.
# Si se deja vacío, se usa la URL de Tokko directamente.
SITE_BASE_URL = os.getenv("SITE_BASE_URL", "")
