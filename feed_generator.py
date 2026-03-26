"""
feed_generator.py — Generador de feed XML compatible con Meta Ads Home Listings.

Usa el formato XML nativo de Meta (listings/listing), NO RSS 2.0.
Referencia: https://www.facebook.com/business/help/2524548294518685
"""

import logging
import xml.etree.ElementTree as ET
from xml.dom import minidom
from typing import Optional

logger = logging.getLogger(__name__)

# ── Mapeos de Tokko → Meta ─────────────────────────────────────────────────────

# Tipo de operación (case-insensitive): nombre en Tokko → availability en Meta
OPERATION_TYPE_MAP: dict[str, str] = {
    "sale": "for_sale",
    "rent": "for_rent",
    "temporary rent": "for_rent",
    "venta": "for_sale",
    "alquiler": "for_rent",
    "alquiler temporario": "for_rent",
}

# Tipo de propiedad: nombre en Tokko → property_type en Meta
PROPERTY_TYPE_MAP: dict[str, str] = {
    "Departamento": "apartment",
    "Casa": "house",
    "Terreno": "land",
    "Lote": "land",
    "Oficina": "condo",
    "Local comercial": "condo",
    "Local": "condo",
    "Cochera": "other",
    "Depósito": "other",
    "Galpón": "other",
    "Campo": "land",
    "Chacra": "land",
    "PH": "apartment",
}

MAX_IMAGES = 10


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_nested(data: dict, *keys, default=None):
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key, default)
        if current is None:
            return default
    return current


def _get_images(prop: dict) -> list[str]:
    photos = prop.get("photos") or []
    urls = []
    for photo in photos[:MAX_IMAGES]:
        url = photo.get("original") or photo.get("image") or ""
        if url:
            urls.append(url)
    return urls


def _get_area(prop: dict) -> Optional[float]:
    surface = (
        prop.get("total_surface") or
        prop.get("roofed_surface") or
        prop.get("surface") or
        None
    )
    try:
        return float(surface) if surface else None
    except (ValueError, TypeError):
        return None


def _get_year_built(prop: dict) -> Optional[int]:
    year = prop.get("year") or prop.get("year_built")
    try:
        return int(year) if year else None
    except (ValueError, TypeError):
        return None


def _get_price(prop: dict, operation_name: str) -> float:
    """Obtiene el precio en USD para la operación dada."""
    for op in (prop.get("operations") or []):
        if (op.get("operation_type") or "").lower() == operation_name.lower():
            prices = op.get("prices") or []
            # Preferir precio en USD
            for p in prices:
                if (p.get("currency") or "").upper() == "USD" and p.get("price"):
                    return float(p["price"])
            # Si no hay USD, tomar el primero disponible
            if prices and prices[0].get("price"):
                return float(prices[0]["price"])
    return 0.0


def _build_listing(parent: ET.Element, prop: dict, operation_name: str,
                   currency: str, site_base_url: str) -> None:
    """
    Agrega un elemento <listing> al XML raíz para una propiedad + operación.
    Formato: Meta Home Listings XML nativo.
    """
    prop_id = prop.get("id", "")
    meta_availability = OPERATION_TYPE_MAP.get(operation_name.lower(), "for_sale")
    listing_id = f"{prop_id}_sale" if meta_availability == "for_sale" else f"{prop_id}_rent"

    # ── Precio ────────────────────────────────────────────────────────────────
    price_value = _get_price(prop, operation_name)
    price_str = f"{int(price_value)} {currency}"
    availability = meta_availability if price_value > 0 else "off_market"

    # ── URL ───────────────────────────────────────────────────────────────────
    # Usar siempre la URL pública que devuelve Tokko (es la URL real del sitio)
    prop_url = prop.get("public_url") or prop.get("url") or \
               f"https://www.tokkobroker.com/propiedades/{prop_id}"

    # ── Textos ────────────────────────────────────────────────────────────────
    description = prop.get("description") or prop.get("description_es") or ""
    description = " ".join(description.split()) or f"Propiedad en {operation_name.lower()}"
    title = prop.get("publication_title") or prop.get("address") or f"Propiedad {prop_id}"

    # ── Dirección ─────────────────────────────────────────────────────────────
    location = prop.get("location") or {}
    street = prop.get("address") or prop.get("fake_address") or ""
    city = _get_nested(location, "short_display") or _get_nested(location, "name") or ""
    parent_loc = location.get("parent") or {}
    grandparent_loc = parent_loc.get("parent") or {}
    great_grandparent_loc = grandparent_loc.get("parent") or {}
    region = (
        great_grandparent_loc.get("name") or
        grandparent_loc.get("name") or
        parent_loc.get("name") or
        "Buenos Aires"  # fallback solo si no se puede identificar
    )
    postal_code = str(prop.get("postal_code") or "")

    # ── Extras ────────────────────────────────────────────────────────────────
    lat = str(prop.get("geo_lat") or prop.get("latitude") or "")
    lon = str(prop.get("geo_long") or prop.get("longitude") or "")
    neighborhood = _get_nested(prop, "location", "name") or prop.get("neighborhood") or ""
    num_beds = str(prop.get("bedrooms") or prop.get("suite_amount") or "")
    num_baths = str(prop.get("bathrooms") or prop.get("full_baths") or "")
    prop_type = PROPERTY_TYPE_MAP.get(_get_nested(prop, "type", "name") or "", "other")
    area = _get_area(prop)
    year_built = _get_year_built(prop)
    images = _get_images(prop)

    # ═══════════════════════════════════════════════════════════════════════════
    # Construcción del <listing> en formato nativo Meta
    # ═══════════════════════════════════════════════════════════════════════════
    listing = ET.SubElement(parent, "listing")

    def add(tag: str, value) -> None:
        if value is not None and str(value).strip():
            el = ET.SubElement(listing, tag)
            el.text = str(value).strip()

    # Campos requeridos
    add("home_listing_id", listing_id)
    add("name", title)
    add("availability", availability)
    add("description", description)
    add("price", price_str)
    add("url", prop_url)

    # Imágenes — formato <image><url>...</url></image> requerido por Meta
    for img_url in images:
        img_el = ET.SubElement(listing, "image")
        url_el = ET.SubElement(img_el, "url")
        url_el.text = img_url

    # Dirección — formato <address format="simple"><component name="...">
    addr_el = ET.SubElement(listing, "address")
    addr_el.set("format", "simple")

    def add_component(name: str, value: str) -> None:
        if value and value.strip():
            comp = ET.SubElement(addr_el, "component")
            comp.set("name", name)
            comp.text = value.strip()

    add_component("addr1", street)
    add_component("city", city)
    add_component("region", region)
    add_component("country", "AR")
    if postal_code:
        add_component("postal_code", postal_code)

    # Campos opcionales recomendados
    add("latitude", lat)
    add("longitude", lon)
    add("neighborhood", neighborhood)
    add("num_baths", num_baths)
    add("num_beds", num_beds)
    add("property_type", prop_type)
    add("listing_type", meta_availability)
    if area:
        area_el = ET.SubElement(listing, "area")
        area_el.set("unit", "square_meters")
        area_el.text = str(int(area))
    add("year_built", str(year_built) if year_built else None)


def generate_feed(
    properties: list[dict],
    property_types: str = "Sale,Rent",
    currency: str = "USD",
    site_base_url: str = "",
) -> str:
    """
    Genera el XML del catálogo Meta Ads Home Listings (formato nativo Meta).

    Args:
        properties:     Lista de dicts con propiedades de Tokko.
        property_types: No usado (se incluyen todas las ops del mapa).
        currency:       Código de moneda para los precios.
        site_base_url:  URL base del sitio para construir URLs de propiedades.

    Returns:
        String con el XML completo del feed.
    """
    logger.info("Generando feed Meta Home Listings (formato nativo)...")

    # Elemento raíz <listings>
    root = ET.Element("listings")

    item_count = 0
    skipped_count = 0

    for prop in properties:
        operations = prop.get("operations") or []

        ops_to_include = [
            op.get("operation_type")
            for op in operations
            if (op.get("operation_type") or "").lower() in OPERATION_TYPE_MAP
        ]

        if not ops_to_include:
            skipped_count += 1
            continue

        for op_name in ops_to_include:
            try:
                _build_listing(root, prop, op_name, currency, site_base_url)
                item_count += 1
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    "Error al generar listing para propiedad %s (%s): %s",
                    prop.get("id", "?"), op_name, exc, exc_info=True,
                )
                skipped_count += 1

    logger.info("Feed generado: %d listings, %d omitidos.", item_count, skipped_count)

    # Serializar con pretty-print
    raw_xml = ET.tostring(root, encoding="unicode", xml_declaration=False)
    dom = minidom.parseString(raw_xml)
    pretty_xml = dom.toprettyxml(indent="  ", encoding=None)

    lines = pretty_xml.splitlines()
    if lines and lines[0].startswith("<?xml"):
        lines[0] = '<?xml version="1.0" encoding="UTF-8"?>'
    return "\n".join(lines)
