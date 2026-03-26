"""
main.py — Servidor FastAPI para el catálogo Meta Ads de Tokko Broker.

Expone un feed XML de propiedades inmobiliarias que se auto-actualiza
cada REFRESH_INTERVAL_HOURS horas. Diseñado para deployarse en Railway.
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import Response, JSONResponse, RedirectResponse

import config
from tokko_client import fetch_all_properties
from feed_generator import generate_feed

# ── Configuración de logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Aplicación FastAPI ────────────────────────────────────────────────────────
app = FastAPI(
    title="Tokko Meta Catalog",
    description="Feed XML de propiedades Tokko Broker compatible con Meta Ads Home Listings.",
    version="1.0.0",
)

# ── Cache global del feed ─────────────────────────────────────────────────────
# Se actualiza en memoria cada vez que se ejecuta refresh_feed().
feed_cache: dict = {
    "xml": "",               # Contenido XML del feed
    "last_updated": None,    # datetime UTC de la última actualización exitosa
    "property_count": 0,     # Cantidad de propiedades en el feed
    "next_refresh": None,    # datetime UTC estimado del próximo refresh
    "error": None,           # Último error (si lo hubo), para diagnóstico
}


# ── Lógica de refresco ────────────────────────────────────────────────────────

async def refresh_feed() -> None:
    """
    Obtiene las propiedades de Tokko Broker, genera el feed XML y actualiza
    el cache global. Se ejecuta al inicio y luego en el loop periódico.
    """
    logger.info("Iniciando refresco del feed...")

    # Validación temprana de la API key
    if not config.TOKKO_API_KEY:
        msg = "TOKKO_API_KEY no configurada. El feed no puede generarse."
        logger.error(msg)
        feed_cache["error"] = msg
        return

    try:
        # Ejecutar la llamada HTTP (bloqueante) en un thread pool para no
        # bloquear el event loop de asyncio.
        loop = asyncio.get_event_loop()
        properties = await loop.run_in_executor(
            None,
            fetch_all_properties,
            config.TOKKO_API_KEY,
            config.TOKKO_BASE_URL,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Error al obtener propiedades de Tokko: %s", exc, exc_info=True)
        feed_cache["error"] = str(exc)
        return

    if not properties:
        logger.warning("No se obtuvieron propiedades. El feed anterior se mantiene sin cambios.")
        feed_cache["error"] = "Tokko devolvió 0 propiedades publicadas."
        return

    try:
        xml_content = generate_feed(
            properties=properties,
            property_types=config.PROPERTY_TYPES,
            currency=config.CURRENCY,
            site_base_url=config.SITE_BASE_URL,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Error al generar el feed XML: %s", exc, exc_info=True)
        feed_cache["error"] = str(exc)
        return

    # Actualizar el cache con el nuevo feed
    now_utc = datetime.now(timezone.utc)
    feed_cache["xml"] = xml_content
    feed_cache["last_updated"] = now_utc
    feed_cache["property_count"] = len(properties)
    feed_cache["next_refresh"] = now_utc + timedelta(hours=config.REFRESH_INTERVAL_HOURS)
    feed_cache["error"] = None

    logger.info(
        "Feed actualizado correctamente. Propiedades: %d. Próximo refresh: %s UTC",
        len(properties),
        feed_cache["next_refresh"].strftime("%Y-%m-%d %H:%M:%S"),
    )


async def _refresh_loop() -> None:
    """
    Loop de refresco periódico. Espera REFRESH_INTERVAL_HOURS entre cada
    actualización del feed.
    """
    interval_seconds = config.REFRESH_INTERVAL_HOURS * 3600
    logger.info(
        "Loop de refresco iniciado. Intervalo: %d horas (%d segundos).",
        config.REFRESH_INTERVAL_HOURS,
        interval_seconds,
    )
    while True:
        await asyncio.sleep(interval_seconds)
        logger.info("Ejecutando refresco programado del feed...")
        await refresh_feed()


# ── Eventos de ciclo de vida ──────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup() -> None:
    """
    Al arrancar el servidor:
    1. Ejecuta un refresco inmediato del feed.
    2. Lanza el loop de refresco periódico como tarea en background.
    """
    logger.info(
        "Servidor iniciando. API Key configurada: %s. Intervalo de refresh: %d h.",
        "SÍ" if config.TOKKO_API_KEY else "NO (¡falta TOKKO_API_KEY!)",
        config.REFRESH_INTERVAL_HOURS,
    )

    # Primer refresco inmediato
    await refresh_feed()

    # Lanzar el loop periódico en background (no bloquea el servidor)
    asyncio.create_task(_refresh_loop())
    logger.info("Loop de refresco periódico iniciado en background.")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    """Redirige la raíz al endpoint /health para conveniencia."""
    return RedirectResponse(url="/health")


@app.get("/feed.xml", response_class=Response, summary="Feed XML para Meta Ads")
async def get_feed() -> Response:
    """
    Devuelve el feed XML de propiedades compatible con Meta Ads Home Listings.

    El contenido se sirve desde cache en memoria y se actualiza automáticamente
    cada REFRESH_INTERVAL_HOURS horas.
    """
    xml_content = feed_cache.get("xml", "")

    if not xml_content:
        # Feed vacío: todavía no se generó o hubo un error
        error_msg = feed_cache.get("error") or "El feed aún no está disponible. Intente en unos instantes."
        logger.warning("Se solicitó /feed.xml pero el feed está vacío. Error: %s", error_msg)
        return Response(
            content=f"<!-- {error_msg} -->",
            media_type="application/xml",
            status_code=503,
        )

    logger.info(
        "Sirviendo feed.xml — %d propiedades, última actualización: %s UTC",
        feed_cache.get("property_count", 0),
        feed_cache["last_updated"].strftime("%Y-%m-%d %H:%M:%S") if feed_cache.get("last_updated") else "N/A",
    )
    return Response(content=xml_content, media_type="application/xml")


@app.get("/health", summary="Estado del servidor")
async def health_check() -> JSONResponse:
    """
    Devuelve información sobre el estado del servidor y el feed.

    Útil para monitoreo y debugging desde Railway o cualquier servicio externo.
    """
    last_updated: Optional[datetime] = feed_cache.get("last_updated")
    next_refresh: Optional[datetime] = feed_cache.get("next_refresh")

    return JSONResponse(
        content={
            "status": "ok" if feed_cache.get("xml") else "sin_datos",
            "last_updated": last_updated.isoformat() if last_updated else None,
            "property_count": feed_cache.get("property_count", 0),
            "next_refresh": next_refresh.isoformat() if next_refresh else None,
            "refresh_interval_hours": config.REFRESH_INTERVAL_HOURS,
            "tokko_api_key_configured": bool(config.TOKKO_API_KEY),
            "error": feed_cache.get("error"),
        }
    )


@app.get("/debug", summary="Muestra operaciones de las primeras propiedades")
async def debug_operations() -> JSONResponse:
    """
    Endpoint de diagnóstico: muestra los tipos de operación que devuelve
    Tokko para las primeras 5 propiedades. Útil para verificar los nombres
    exactos y corregir el filtro PROPERTY_TYPES.
    """
    loop = asyncio.get_event_loop()
    properties = await loop.run_in_executor(
        None, fetch_all_properties, config.TOKKO_API_KEY, config.TOKKO_BASE_URL
    )
    sample = []
    for prop in properties[:5]:
        ops = prop.get("operations") or []
        sample.append({
            "id": prop.get("id"),
            "title": prop.get("publication_title") or prop.get("address"),
            "operations": [
                {
                    "operation_type": op.get("operation_type"),
                    "prices": op.get("prices"),
                }
                for op in ops
            ],
        })
    return JSONResponse(content={
        "total_properties": len(properties),
        "property_types_config": config.PROPERTY_TYPES,
        "sample": sample,
    })


@app.get("/refresh", summary="Forzar refresco manual del feed (GET)")
async def force_refresh_get() -> JSONResponse:
    """Igual que POST /refresh pero accesible desde el navegador directamente."""
    logger.info("Refresco manual solicitado vía GET /refresh")
    await refresh_feed()
    last_updated: Optional[datetime] = feed_cache.get("last_updated")
    return JSONResponse(content={
        "status": "ok" if not feed_cache.get("error") else "error",
        "message": "Feed refrescado exitosamente." if not feed_cache.get("error") else feed_cache.get("error"),
        "last_updated": last_updated.isoformat() if last_updated else None,
        "property_count": feed_cache.get("property_count", 0),
    })


@app.post("/refresh", summary="Forzar refresco manual del feed")
async def force_refresh() -> JSONResponse:
    """
    Fuerza un refresco inmediato del feed sin esperar el intervalo programado.

    Útil durante el desarrollo o ante cambios urgentes en el inventario.
    """
    logger.info("Refresco manual solicitado vía POST /refresh")
    await refresh_feed()
    last_updated: Optional[datetime] = feed_cache.get("last_updated")
    return JSONResponse(
        content={
            "status": "ok" if not feed_cache.get("error") else "error",
            "message": "Feed refrescado exitosamente." if not feed_cache.get("error") else feed_cache.get("error"),
            "last_updated": last_updated.isoformat() if last_updated else None,
            "property_count": feed_cache.get("property_count", 0),
        }
    )
