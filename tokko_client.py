"""
tokko_client.py — Cliente HTTP para la API de Tokko Broker.

Maneja paginación automática, timeouts y filtrado de propiedades publicadas.
"""

import logging
import requests

logger = logging.getLogger(__name__)

# Timeout en segundos para cada request a la API de Tokko
REQUEST_TIMEOUT = 30

# Status 2 = publicada en Tokko Broker
PUBLISHED_STATUS = 2


def fetch_all_properties(api_key: str, base_url: str = "https://www.tokkobroker.com/api/v1") -> list[dict]:
    """
    Recupera TODAS las propiedades publicadas de Tokko Broker paginando
    automáticamente mediante el campo `meta.next` de cada respuesta.

    Args:
        api_key:  Clave de API de Tokko Broker.
        base_url: URL base de la API (se puede sobreescribir en tests).

    Returns:
        Lista de dicts con los datos de cada propiedad.
        Retorna lista vacía si ocurre algún error.
    """
    if not api_key:
        logger.error("TOKKO_API_KEY no está configurada. No se pueden obtener propiedades.")
        return []

    all_properties: list[dict] = []
    # Primera página: listado general de propiedades
    url = f"{base_url}/property/"
    params = {
        "key": api_key,
        "format": "json",
        "lang": "es",
        # Pedimos el máximo de resultados por página para reducir la cantidad de requests
        "limit": 100,
        "offset": 0,
    }

    page_number = 1

    while url:
        logger.info("Obteniendo página %d de propiedades desde Tokko: %s", page_number, url)
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error("Timeout al conectar con Tokko Broker (página %d). Se detiene la paginación.", page_number)
            break
        except requests.exceptions.ConnectionError as exc:
            logger.error("Error de conexión con Tokko Broker (página %d): %s", page_number, exc)
            break
        except requests.exceptions.HTTPError as exc:
            logger.error(
                "Error HTTP %s al obtener propiedades de Tokko (página %d): %s",
                response.status_code,
                page_number,
                exc,
            )
            break
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error inesperado al obtener propiedades (página %d): %s", page_number, exc)
            break

        try:
            data = response.json()
        except ValueError as exc:
            logger.error("Respuesta de Tokko no es JSON válido (página %d): %s", page_number, exc)
            break

        # La API de Tokko devuelve { "meta": { "next": "url|null", ... }, "objects": [...] }
        objects = data.get("objects", [])
        meta = data.get("meta", {})

        # Filtrar sólo propiedades con status=2 (publicadas)
        published = [p for p in objects if p.get("status") == PUBLISHED_STATUS]
        all_properties.extend(published)

        logger.info(
            "Página %d: %d propiedades totales, %d publicadas (acumulado: %d)",
            page_number,
            len(objects),
            len(published),
            len(all_properties),
        )

        # Avanzar a la siguiente página
        next_url = meta.get("next")
        if next_url:
            # La siguiente URL que devuelve Tokko ya viene completa; limpiamos los
            # params para no duplicarlos en el query string.
            url = next_url
            params = {}
            page_number += 1
        else:
            # No hay más páginas
            url = None

    logger.info("Descarga finalizada. Total de propiedades publicadas: %d", len(all_properties))
    return all_properties
