"""
feed_generator.py — Generador de feed XML compatible con Meta Ads Home Listings.

Convierte la lista de propiedades de Tokko Broker al formato RSS 2.0 que
requiere Meta para el catálogo de Home Listings.

Referencia: https://www.facebook.com/business/help/2524548294518685
"""

import logging
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── Mapeos de Tokko → Meta ─────────────────────────────────────────────────────

# Tipo de operación: nombre en Tokko → valor esperado por Meta
OPERATION_TYPE_MAP: dict[str, str] = {
    "Venta": "for_sale",
    "Alquiler": "for_rent",
    "Alquiler temporario": "for_rent",
    "Alquiler Temporario": "for_rent",
}

# Tipo de propiedad: nombre en Tokko → valor esperado por Meta
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

# Máximo de imágenes a incluir por propiedad
MAX_IMAGES = 10


# ── Helpers ────────────────────────────────────────────────────────────────────

def _escape_xml(text: Optional[str]) -> str:
    """Escapa caracteres especiales para XML de forma segura."""
    if text is None:
        return ""
    # xml.etree ya escapa &, <, > al setear .text; esta función sirve para
    # valores que se inyectan como atributos o se construyen manualmente.
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _get_nested(data: dict, *keys, default=None):
    """Navega un dict anidado de forma segura. Ej: _get_nested(d, 'a', 'b', 'c')."""
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key, default)
        if current is None:
            return default
    return current


def _build_address(prop: dict) -> tuple[str, str, str, str, str]:
    """
    Extrae los componentes de la dirección de una propiedad de Tokko.

    Returns:
        Tupla (address_full, city, region, postal_code, country)
    """
    # Tokko tiene distintos niveles: location → parent_location → ...
    location = prop.get("location") or {}
    full_address = prop.get("address") or prop.get("fake_address") or ""

    # Intentar reconstruir la dirección si no viene completa
    street = prop.get("address") or ""
    city = (
        _get_nested(location, "short_display") or
        _get_nested(location, "name") or
        ""
    )

    # Navegar hacia la región / provincia
    parent = location.get("parent") or {}
    region = parent.get("name") or ""

    # Intentar conseguir país desde niveles superiores
    grandparent = parent.get("parent") or {}
    country = grandparent.get("name") or "Argentina"

    postal_code = prop.get("postal_code") or ""

    if not full_address:
        parts = [p for p in [street, city, region] if p]
        full_address = ", ".join(parts)

    return full_address, city, region, postal_code, country


def _get_property_type(prop: dict) -> str:
    """Mapea el tipo de propiedad de Tokko al valor de Meta."""
    prop_type = _get_nested(prop, "type", "name") or ""
    return PROPERTY_TYPE_MAP.get(prop_type, "other")


def _get_images(prop: dict) -> list[str]:
    """Extrae las URLs de imágenes de la propiedad (máximo MAX_IMAGES)."""
    photos = prop.get("photos") or []
    urls = []
    for photo in photos[:MAX_IMAGES]:
        # Tokko puede dar 'image' o 'original' como clave de la URL
        url = photo.get("original") or photo.get("image") or ""
        if url:
            urls.append(url)
    return urls


def _get_area(prop: dict) -> Optional[float]:
    """Retorna el área total de la propiedad en m²."""
    # Tokko puede traer total_surface, roofed_surface, etc.
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
    """Retorna el año de construcción si está disponible."""
    year = prop.get("year") or prop.get("year_built")
    try:
        return int(year) if year else None
    except (ValueError, TypeError):
        return None


def _build_item(
    parent: ET.Element,
    prop: dict,
    operation_name: str,
    currency: str,
    site_base_url: str,
) -> None:
    """
    Agrega un elemento <item> al canal RSS para una propiedad + operación.

    Args:
        parent:         Elemento <channel> al que se agrega el <item>.
        prop:           Dict con los datos de la propiedad de Tokko.
        operation_name: Nombre de la operación en Tokko (ej: "Venta").
        currency:       Código de moneda (ej: "USD").
        site_base_url:  URL base del sitio (opcional, para construir la URL).
    """
    prop_id = prop.get("id", "")
    meta_availability = OPERATION_TYPE_MAP.get(operation_name, "for_sale")

    # ID único por operación para evitar colisiones cuando una propiedad
    # aparece tanto en Venta como en Alquiler.
    if meta_availability == "for_sale":
        listing_id = f"{prop_id}_sale"
    else:
        listing_id = f"{prop_id}_rent"

    # ── Precio ───────────────────────────────────────────────────────────────
    # Buscar el precio correspondiente a la operación actual
    price_value = 0
    operations = prop.get("operations") or []
    for op in operations:
        op_type = _get_nested(op, "operation_type") or ""
        if op_type == operation_name:
            prices = op.get("prices") or []
            if prices:
                price_value = prices[0].get("price") or 0
            break

    try:
        price_value = float(price_value)
    except (ValueError, TypeError):
        price_value = 0.0

    price_str = f"{int(price_value)} {currency}"
    availability = meta_availability if price_value > 0 else "off_market"

    # ── URL de la propiedad ──────────────────────────────────────────────────
    prop_url = prop.get("public_url") or prop.get("url") or ""
    if site_base_url and prop_id:
        # Si el operador tiene su propio sitio, construir la URL con su dominio
        prop_url = f"{site_base_url.rstrip('/')}/propiedad/{prop_id}"
    if not prop_url:
        prop_url = f"https://www.tokkobroker.com/propiedades/{prop_id}"

    # ── Descripción ──────────────────────────────────────────────────────────
    description = prop.get("description") or prop.get("description_es") or ""
    # Limpiar saltos de línea excesivos
    description = " ".join(description.split())
    if not description:
        description = f"Propiedad en {operation_name.lower()}"

    # ── Título ───────────────────────────────────────────────────────────────
    title = prop.get("publication_title") or prop.get("address") or f"Propiedad {prop_id}"

    # ── Dirección ────────────────────────────────────────────────────────────
    full_address, city, region, postal_code, country = _build_address(prop)

    # ── Coordenadas ──────────────────────────────────────────────────────────
    lat = prop.get("geo_lat") or prop.get("latitude") or ""
    lon = prop.get("geo_long") or prop.get("longitude") or ""

    # ── Imágenes ─────────────────────────────────────────────────────────────
    images = _get_images(prop)

    # ── Barrio / Vecindario ───────────────────────────────────────────────────
    neighborhood = (
        _get_nested(prop, "location", "name") or
        prop.get("neighborhood") or
        ""
    )

    # ── Ambientes, baños, dormitorios ─────────────────────────────────────────
    num_beds = prop.get("bedrooms") or prop.get("suite_amount") or ""
    num_baths = prop.get("bathrooms") or prop.get("full_baths") or ""

    # ── Tipo de propiedad ─────────────────────────────────────────────────────
    prop_type_meta = _get_property_type(prop)

    # ── Área ──────────────────────────────────────────────────────────────────
    area = _get_area(prop)

    # ── Año de construcción ───────────────────────────────────────────────────
    year_built = _get_year_built(prop)

    # ═══════════════════════════════════════════════════════════════════════════
    # Construcción del elemento <item>
    # Meta requiere el namespace g: para los campos de Home Listings
    # ═══════════════════════════════════════════════════════════════════════════
    item = ET.SubElement(parent, "item")

    def add(tag: str, value: Optional[str]) -> None:
        """Agrega un sub-elemento con texto, omitiendo si el valor está vacío."""
        if value is not None and str(value).strip():
            el = ET.SubElement(item, tag)
            el.text = str(value).strip()

    # Campos requeridos por Meta Home Listings
    add("g:home_listing_id", str(listing_id))
    add("g:name", title)
    add("g:availability", availability)
    add("g:description", description)
    add("g:price", price_str)
    add("g:url", prop_url)

    # Dirección estructurada (requerida)
    addr_el = ET.SubElement(item, "g:address")
    add_addr = lambda tag, val: (
        ET.SubElement(addr_el, tag).__setattr__("text", str(val).strip())
        if val and str(val).strip() else None
    )
    if full_address:
        ET.SubElement(addr_el, "g:addr1").text = full_address
    if city:
        ET.SubElement(addr_el, "g:city").text = city
    if region:
        ET.SubElement(addr_el, "g:region").text = region
    if postal_code:
        ET.SubElement(addr_el, "g:postal_code").text = str(postal_code)
    ET.SubElement(addr_el, "g:country").text = country

    # Imágenes: la primera es el campo image principal, el resto son image_link adicionales
    if images:
        add("g:image", images[0])
        for extra_img in images[1:]:
            add("g:additional_image_link", extra_img)
    else:
        # Meta requiere al menos una imagen; si no hay, se deja el campo vacío
        # y la propiedad probablemente sea rechazada, pero no queremos romper el feed.
        logger.warning("Propiedad %s no tiene imágenes.", prop_id)

    # Campos opcionales pero recomendados
    if lat:
        add("g:latitude", str(lat))
    if lon:
        add("g:longitude", str(lon))
    if neighborhood:
        add("g:neighborhood", neighborhood)
    if num_baths:
        add("g:num_baths", str(num_baths))
    if num_beds:
        add("g:num_beds", str(num_beds))
    if area:
        add("g:area", f"{area} SQ_M")
    if prop_type_meta:
        add("g:property_type", prop_type_meta)
    if year_built:
        add("g:year_built", str(year_built))
    if listing_id:
        add("g:listing_type", meta_availability)


def generate_feed(
    properties: list[dict],
    property_types: str = "Venta,Alquiler",
    currency: str = "USD",
    site_base_url: str = "",
) -> str:
    """
    Genera el XML del catálogo Meta Ads Home Listings a partir de la lista
    de propiedades de Tokko Broker.

    Args:
        properties:     Lista de dicts con propiedades de Tokko.
        property_types: String con tipos de operación separados por coma.
        currency:       Código de moneda para los precios.
        site_base_url:  URL base del sitio para construir URLs de propiedades.

    Returns:
        String con el XML completo del feed, formateado con indentación.
    """
    # Tipos de operación habilitados
    enabled_types = [t.strip() for t in property_types.split(",") if t.strip()]
    logger.info("Generando feed para tipos de operación: %s", enabled_types)

    # ── Estructura RSS 2.0 ────────────────────────────────────────────────────
    # Meta requiere que el namespace g: esté declarado en el elemento raíz <rss>
    rss = ET.Element("rss")
    rss.set("version", "2.0")
    rss.set("xmlns:g", "http://base.google.com/ns/1.0")

    channel = ET.SubElement(rss, "channel")

    # Metadatos del canal
    ET.SubElement(channel, "title").text = "Tokko Broker — Home Listings para Meta Ads"
    ET.SubElement(channel, "link").text = site_base_url or "https://www.tokkobroker.com"
    ET.SubElement(channel, "description").text = (
        "Feed de propiedades inmobiliarias generado automáticamente para Meta Ads Home Listings."
    )
    ET.SubElement(channel, "lastBuildDate").text = (
        datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
    )

    item_count = 0
    skipped_count = 0

    for prop in properties:
        operations = prop.get("operations") or []

        # Determinar qué operaciones de esta propiedad deben aparecer en el feed
        ops_to_include: list[str] = []
        for op in operations:
            op_type = op.get("operation_type") or ""
            if op_type in enabled_types:
                ops_to_include.append(op_type)

        if not ops_to_include:
            skipped_count += 1
            continue

        # Si una propiedad tiene múltiples operaciones habilitadas (ej: Venta Y Alquiler),
        # se crea un <item> independiente por cada una con IDs únicos.
        for op_name in ops_to_include:
            try:
                _build_item(channel, prop, op_name, currency, site_base_url)
                item_count += 1
            except Exception as exc:  # pylint: disable=broad-except
                prop_id = prop.get("id", "desconocido")
                logger.error(
                    "Error al generar item para propiedad %s (operación: %s): %s",
                    prop_id,
                    op_name,
                    exc,
                    exc_info=True,
                )
                skipped_count += 1

    logger.info(
        "Feed generado: %d items incluidos, %d propiedades omitidas.",
        item_count,
        skipped_count,
    )

    # ── Serializar a string XML con indentación legible ───────────────────────
    raw_xml = ET.tostring(rss, encoding="unicode", xml_declaration=False)

    # minidom para pretty-print con declaración XML correcta
    dom = minidom.parseString(raw_xml)
    pretty_xml = dom.toprettyxml(indent="  ", encoding=None)

    # toprettyxml agrega su propia declaración XML; la reemplazamos por la estándar
    lines = pretty_xml.splitlines()
    # La primera línea es la declaración XML de minidom; la descartamos y ponemos la nuestra
    if lines and lines[0].startswith("<?xml"):
        lines[0] = '<?xml version="1.0" encoding="UTF-8"?>'
    return "\n".join(lines)
