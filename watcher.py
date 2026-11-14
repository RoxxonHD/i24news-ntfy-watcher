#!/usr/bin/env python3
"""
i24NEWS Real-Time News Feed Push Watcher (ntfy.sh + iOS Universal Links)

Monitorea cada 40 segundos el flujo en vivo "News Feed" de i24NEWS en inglés
(https://api.i24news.tv/v2/en/news), capturando única y exclusivamente el texto
largo de cada entrada del ticker (item["title"]). Filtra en silencio por las
10 secciones permitidas revisando categoría, subcategoría, ruta URL y tags, y
envía notificaciones push minimalistas (solo título, sin cuerpo/excerpt, sin
emojis y con el logo oficial de i24NEWS) vía ntfy.sh.
"""

from __future__ import annotations

import argparse
import asyncio
import html
import logging
import os
import re
import signal
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from dotenv import load_dotenv

# Cargar variables de entorno desde .env si existe
load_dotenv()

DEFAULT_API_URL = os.getenv("I24_API_URL", "https://api.i24news.tv/v2/en/news")
DEFAULT_CONTENTS_API_URL = os.getenv("I24_CONTENTS_API_URL", "https://api.i24news.tv/v2/en/contents")
DEFAULT_NTFY_URL = os.getenv("NTFY_URL", "https://ntfy.sh/i24news-alerts")
DEFAULT_NTFY_ICON_URL = os.getenv(
    "NTFY_ICON_URL",
    "https://www.i24news.tv/images/apple-touch-icon.png",
)
DEFAULT_POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "40"))
DEFAULT_DB_PATH = os.getenv("DB_PATH", "seen_news.db")
DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
DEFAULT_MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
DEFAULT_MAX_RUNTIME = int(os.getenv("MAX_RUNTIME_SECONDS", "0"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("i24_watcher")

# Headers de navegador realistas para evitar bloqueos de WAF / CDN
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.i24news.tv",
    "Referer": "https://www.i24news.tv/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# Caché en memoria de tags por content_id para enriquecer ítems del News Feed
# cuya categoría base sea genérica (p. ej. "Middle East") sin repetir peticiones HTTP.
_CONTENT_TAGS_CACHE: dict[int, list[dict[str, Any]]] = {}

# ---------------------------------------------------------------------------
# Configuración de Filtros Internos (10 secciones exactas + Blacklist)
# ---------------------------------------------------------------------------

BLACKLISTED_TOKENS = {
    "culture",
    "lifestyle",
    "sport",
    "sports",
    "innov",
    "innov-nation",
    "innov'nation",
    "economy",
    "technology-science",
    "technology & science",
    "tech",
    "archeology",
    "archaeology",
    "environment",
    "discover-galilee",
    "discover galilee",
    "analysis-opinion",
    "analysis & opinion",
    "health",
}


@dataclass(frozen=True)
class SectionRule:
    """Regla de mapeo interno para una de las 10 secciones permitidas."""

    canonical_name: str
    names: frozenset[str]
    slugs: frozenset[str]
    url_segments: frozenset[str]
    tag_keywords: frozenset[str]


WHITELIST_RULES: tuple[SectionRule, ...] = (
    SectionRule(
        canonical_name="Israel Elections 2026",
        names=frozenset({"israel elections 2026", "israel election 2026", "israel elections"}),
        slugs=frozenset({"israel-election-s-2026", "israel-elections-2026", "israeli-elections"}),
        url_segments=frozenset({"israel-election-s-2026", "israel-elections-2026", "israeli-elections"}),
        tag_keywords=frozenset({"israel elections 2026", "israeli elections", "knesset elections"}),
    ),
    SectionRule(
        canonical_name="Defense News",
        names=frozenset({"defense news", "defense", "diplomacy & defense"}),
        slugs=frozenset({"defense", "defense-news", "diplomacy-defense"}),
        url_segments=frozenset({"defense", "defense-news", "diplomacy-defense"}),
        tag_keywords=frozenset({"defense news", "idf", "israel defense forces", "israeli military"}),
    ),
    SectionRule(
        canonical_name="Iran & Eastern States",
        names=frozenset({"iran & eastern states", "iran and eastern states"}),
        slugs=frozenset({"iran-eastern-states"}),
        url_segments=frozenset({"iran-eastern-states"}),
        tag_keywords=frozenset({"iran", "tehran", "irgc", "iran & eastern states"}),
    ),
    SectionRule(
        canonical_name="West Bank & Gaza Strip",
        names=frozenset({"west bank & gaza strip", "west bank and gaza strip", "palestinian territories"}),
        slugs=frozenset({"palestinian-territories", "west-bank-gaza-strip"}),
        url_segments=frozenset({"palestinian-territories", "west-bank-gaza-strip"}),
        tag_keywords=frozenset({"gaza", "gaza strip", "west bank", "hamas", "palestinian territories", "rafah", "khan younis"}),
    ),
    SectionRule(
        canonical_name="US / Canada",
        names=frozenset({"us / canada", "us/canada", "us & canada", "americas"}),
        slugs=frozenset({"americas", "us-canada"}),
        url_segments=frozenset({"americas", "us-canada"}),
        tag_keywords=frozenset({"united states", "us / canada", "u.s.", "canada", "white house", "washington"}),
    ),
    SectionRule(
        canonical_name="Europe",
        names=frozenset({"europe"}),
        slugs=frozenset({"europe"}),
        url_segments=frozenset({"europe"}),
        tag_keywords=frozenset({"europe", "european union", "uk", "france", "germany", "ukraine", "russia"}),
    ),
    SectionRule(
        canonical_name="Latin America",
        names=frozenset({"latin america"}),
        slugs=frozenset({"latin-america"}),
        url_segments=frozenset({"latin-america"}),
        tag_keywords=frozenset({"latin america", "south america", "argentina", "brazil", "mexico", "colombia", "venezuela"}),
    ),
    SectionRule(
        canonical_name="Diaspora Affairs",
        names=frozenset({"diaspora affairs"}),
        slugs=frozenset({"diaspora-affairs"}),
        url_segments=frozenset({"diaspora-affairs"}),
        tag_keywords=frozenset({"diaspora", "diaspora affairs", "antisemitism", "jewish world"}),
    ),
    SectionRule(
        canonical_name="Israel",
        names=frozenset({"israel"}),
        slugs=frozenset({"israel"}),
        url_segments=frozenset({"israel"}),
        tag_keywords=frozenset({"israel", "jerusalem", "tel aviv", "netanyahu", "knesset"}),
    ),
    SectionRule(
        canonical_name="International",
        names=frozenset({"international"}),
        slugs=frozenset({"international"}),
        url_segments=frozenset({"international"}),
        tag_keywords=frozenset({"international"}),
    ),
)


@dataclass
class ParsedFeedItem:
    """Representa una entrada del News Feed validada y lista para notificar."""

    news_id: int
    content_id: int
    section: str
    feed_title: str
    frontend_url: str
    is_breaking: bool
    started_at: str


# ---------------------------------------------------------------------------
# Utilidades de Limpieza y Validación
# ---------------------------------------------------------------------------

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def clean_text(raw: Optional[str]) -> str:
    """Elimina etiquetas HTML, decodifica entidades HTML y normaliza espacios."""
    if not raw:
        return ""
    without_tags = _HTML_TAG_RE.sub(" ", raw)
    unescaped = html.unescape(without_tags)
    return _WHITESPACE_RE.sub(" ", unescaped).strip()


def normalize_frontend_url(raw_url: Optional[str]) -> Optional[str]:
    """
    Valida que la URL pertenezca a un artículo completo real en i24news.tv.
    Descarta entradas sin URL o que apunten únicamente a la portada/categoría.
    """
    if not raw_url or not isinstance(raw_url, str):
        return None

    url = raw_url.strip()
    if url.startswith("/"):
        url = f"https://www.i24news.tv{url}"

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return None
    if not parsed.netloc.endswith("i24news.tv"):
        return None

    path_parts = [p for p in parsed.path.split("/") if p]
    if len(path_parts) < 4 or path_parts[0] != "en" or path_parts[1] != "news":
        return None

    article_slug = path_parts[-1].lower()
    if not article_slug or article_slug in ("news", "en"):
        return None

    return url


def match_whitelisted_section(
    item: dict[str, Any],
    content: dict[str, Any],
    frontend_url: str,
    extra_tags: Optional[list[dict[str, Any]]] = None,
) -> Optional[str]:
    """
    Evalúa silenciosamente si el ítem del News Feed pertenece a alguna de las
    10 secciones permitidas revisando categoría, subcategoría, ruta en frontendUrl y tags.
    Devuelve el nombre canónico de la sección (solo para control/log interno) o None.
    """
    category = content.get("category") or {}
    parent = category.get("parent") or {}

    cat_name = clean_text(category.get("name")).lower()
    cat_slug = clean_text(category.get("slug")).lower()
    parent_name = clean_text(parent.get("name")).lower()
    parent_slug = clean_text(parent.get("slug")).lower()

    parsed_url = urlparse(frontend_url)
    path_parts = [p.lower() for p in parsed_url.path.split("/") if p]
    url_section_segments: list[str] = []
    if len(path_parts) >= 4 and path_parts[0] == "en" and path_parts[1] == "news":
        url_section_segments = path_parts[2:-1]

    raw_tags: list[Any] = []
    for source in (content.get("tags"), item.get("tags"), extra_tags):
        if isinstance(source, list):
            raw_tags.extend(source)

    tag_names: set[str] = set()
    tag_slugs: set[str] = set()
    for t in raw_tags:
        if isinstance(t, dict):
            if t.get("name"):
                tag_names.add(clean_text(str(t["name"])).lower())
            if t.get("slug"):
                tag_slugs.add(clean_text(str(t["slug"])).lower())
        elif isinstance(t, str):
            tag_names.add(clean_text(t).lower())

    # 1. Verificación estricta de Blacklist (Culture, Lifestyle, Sports, INNOV, Economy, etc.)
    all_section_tokens = {cat_name, cat_slug, parent_name, parent_slug} | set(url_section_segments)
    all_section_tokens.discard("")
    for token in all_section_tokens:
        if token in BLACKLISTED_TOKENS or any(
            token.startswith(b) for b in ("innov", "sport", "cultur", "lifestyl", "econom")
        ):
            return None

    if content.get("isSponsored") is True:
        return None

    # 2. Prioridad 1: Coincidencia directa con categoría hoja, segmento de URL o tags exactos de sección
    leaf_url_segment = url_section_segments[-1] if url_section_segments else ""
    for rule in WHITELIST_RULES:
        if (
            cat_name in rule.names
            or cat_slug in rule.slugs
            or leaf_url_segment in rule.url_segments
            or bool(tag_names & rule.names)
            or bool(tag_slugs & rule.slugs)
        ):
            return rule.canonical_name

    # 3. Prioridad 2: Coincidencia por categoría padre o segmento raíz de URL
    for rule in WHITELIST_RULES:
        if (
            parent_name in rule.names
            or parent_slug in rule.slugs
            or any(seg in rule.url_segments for seg in url_section_segments)
        ):
            return rule.canonical_name

    # 4. Prioridad 3: Coincidencia por tags temáticos del artículo (útil cuando la nota/live-blog
    #    está archivada bajo la categoría contenedora "Middle East" pero sus tags son Iran, Israel, Gaza, etc.)
    for rule in WHITELIST_RULES:
        if bool(tag_names & rule.tag_keywords) or bool(tag_slugs & rule.tag_keywords):
            return rule.canonical_name

    return None


async def fetch_content_tags(
    client: httpx.AsyncClient,
    content_id: int,
    contents_api_url: str = DEFAULT_CONTENTS_API_URL,
) -> list[dict[str, Any]]:
    """
    Obtiene y cachea los tags completos de un artículo desde /v2/en/contents/{id}
    cuando el resumen en /v2/en/news no incluye el array `tags`.
    """
    if content_id in _CONTENT_TAGS_CACHE:
        return _CONTENT_TAGS_CACHE[content_id]

    url = f"{contents_api_url.rstrip('/')}/{content_id}"
    try:
        resp = await client.get(url, headers=BROWSER_HEADERS)
        if resp.status_code == 200:
            data = resp.json()
            tags = data.get("tags") if isinstance(data, dict) else []
            if isinstance(tags, list):
                _CONTENT_TAGS_CACHE[content_id] = tags
                return tags
    except Exception as exc:
        logger.debug("No se pudieron obtener tags adicionales para content_id=%d: %s", content_id, exc)

    _CONTENT_TAGS_CACHE[content_id] = []
    return []


async def parse_news_feed_item(
    item: dict[str, Any],
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[ParsedFeedItem]:
    """
    Convierte un elemento del flujo News Feed (/v2/en/news) en un ParsedFeedItem:
    - Captura exclusivamente `item["title"]` (el texto largo y detallado del News Feed).
    - Descarta micro-flashes sin artículo completo (`content=None` o sin `frontendUrl`).
    - Aplica silenciosamente el filtro de las 10 secciones revisando metadatos y tags.
    """
    if not isinstance(item, dict):
        return None

    news_id = item.get("id")
    content = item.get("content")

    # Solo artículos reales con nota asociada (descartar flashes de una línea con content=None)
    if not news_id or not isinstance(content, dict):
        return None

    content_id = content.get("id")
    if not content_id:
        return None

    frontend_url = normalize_frontend_url(content.get("frontendUrl"))
    if not frontend_url:
        return None

    # Texto largo del News Feed (item["title"]), NO el titular corto del artículo (content["title"])
    feed_title = clean_text(item.get("title")) or clean_text(content.get("title"))
    if not feed_title:
        return None

    # Evaluar filtro interno de las 10 secciones
    section = match_whitelisted_section(item, content, frontend_url)
    if section is None and client is not None:
        extra_tags = await fetch_content_tags(client, int(content_id))
        if extra_tags:
            section = match_whitelisted_section(item, content, frontend_url, extra_tags=extra_tags)

    if not section:
        return None

    status_raw = str(item.get("status") or content.get("status") or "").strip().lower()
    is_breaking = (
        status_raw in ("breaking", "urgent", "alert", "important")
        or bool(item.get("isBreaking"))
        or bool(content.get("isBreaking"))
        or "breaking" in status_raw
    )

    started_at = str(
        item.get("startedAt")
        or content.get("publishedAt")
        or datetime.now(timezone.utc).isoformat()
    )

    return ParsedFeedItem(
        news_id=int(news_id),
        content_id=int(content_id),
        section=section,
        feed_title=feed_title,
        frontend_url=frontend_url,
        is_breaking=is_breaking,
        started_at=started_at,
    )


# ---------------------------------------------------------------------------
# Base de Datos SQLite (Deduplicación del News Feed y Arranque Limpio)
# ---------------------------------------------------------------------------


class SeenNewsStore:
    """Almacén SQLite local para deduplicar entradas del News Feed ya procesadas."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent and not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_news (
                    news_id INTEGER PRIMARY KEY,
                    content_id INTEGER,
                    frontend_url TEXT,
                    section TEXT,
                    title TEXT,
                    seen_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_seen_title
                ON seen_news(title)
                """
            )

    def is_empty(self) -> bool:
        with self._connect() as conn:
            cur = conn.execute("SELECT COUNT(1) FROM seen_news")
            row = cur.fetchone()
            return (row[0] if row else 0) == 0

    def is_seen(self, news_id: int, feed_title: Optional[str] = None) -> bool:
        with self._connect() as conn:
            if feed_title:
                cur = conn.execute(
                    "SELECT 1 FROM seen_news WHERE news_id = ? OR title = ? LIMIT 1",
                    (news_id, feed_title),
                )
            else:
                cur = conn.execute(
                    "SELECT 1 FROM seen_news WHERE news_id = ? LIMIT 1",
                    (news_id,),
                )
            return cur.fetchone() is not None

    def mark_seen(
        self,
        news_id: int,
        content_id: Optional[int] = None,
        frontend_url: Optional[str] = None,
        section: Optional[str] = None,
        title: Optional[str] = None,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO seen_news (news_id, content_id, frontend_url, section, title, seen_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (news_id, content_id, frontend_url, section, title, now_iso),
            )


# ---------------------------------------------------------------------------
# Cliente HTTP Asíncrono (i24NEWS News Feed API + ntfy.sh Push)
# ---------------------------------------------------------------------------


async def fetch_news_feed(
    client: httpx.AsyncClient,
    api_url: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> list[dict[str, Any]]:
    """
    Consulta el endpoint del News Feed (/v2/en/news) con manejo de timeouts y reintentos.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            response = await client.get(api_url, headers=BROWSER_HEADERS)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                raise ValueError(f"Se esperaba una lista JSON de {api_url}, se recibió {type(data).__name__}")
            return data
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                backoff = min(2 ** attempt, 10)
                logger.warning(
                    "Intento %d/%d fallido al consultar i24NEWS (%s). Reintentando en %ds...",
                    attempt,
                    max_retries,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
            else:
                logger.error(
                    "Error al consultar i24NEWS tras %d intentos: %s",
                    max_retries,
                    exc,
                )
    assert last_exc is not None
    raise last_exc


async def send_ntfy_notification(
    client: httpx.AsyncClient,
    ntfy_url: str,
    item: ParsedFeedItem,
    icon_url: str = DEFAULT_NTFY_ICON_URL,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> bool:
    """
    Envía una notificación push minimalista a ntfy.sh:
    - Header `Title`: ÚNICAMENTE el texto largo del News Feed (sin corchetes).
    - Body: Carácter invisible `\u200b` (cuerpo visualmente vacío sin el excerpt y sin que
      ntfy.sh inyecte el texto por defecto "triggered").
    - Sin header `Tags`: Cero emojis en la notificación.
    - Header `Icon`: Logo cuadrado oficial de i24NEWS.
    - Header `Click`: Universal Link completo hacia la nota en i24news.tv.
    """
    priority = "urgent" if item.is_breaking else "high"

    raw_headers = {
        "Title": item.feed_title,
        "Click": item.frontend_url,
        "Icon": icon_url,
        "Priority": priority,
    }
    utf8_headers = httpx.Headers(raw_headers, encoding="utf-8")
    # Cuerpo invisible para mostrar exclusivamente el título sin excerpt
    empty_body_bytes = "\u200b".encode("utf-8")

    for attempt in range(1, max_retries + 1):
        try:
            response = await client.post(
                ntfy_url,
                headers=utf8_headers,
                content=empty_body_bytes,
            )
            response.raise_for_status()
            logger.info(
                "Push enviado (filtro interno: %s) -> %s | URL: %s",
                item.section,
                item.feed_title,
                item.frontend_url,
            )
            return True
        except Exception as exc:
            if attempt < max_retries:
                backoff = min(2 ** attempt, 10)
                logger.warning(
                    "Intento %d/%d fallido enviando push a ntfy.sh (%s). Reintentando en %ds...",
                    attempt,
                    max_retries,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
            else:
                logger.error(
                    "No se pudo enviar la notificación a ntfy.sh tras %d intentos: %s",
                    max_retries,
                    exc,
                )
    return False


# ---------------------------------------------------------------------------
# Servidor HTTP Healthcheck Opcional (para despliegues Cloud gratuitos)
# ---------------------------------------------------------------------------


async def _handle_healthcheck(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        await reader.read(1024)
        body = b'{"status":"ok","service":"i24news-watcher"}'
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
            b"Connection: close\r\n\r\n" + body
        )
        writer.write(response)
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


async def start_healthcheck_server_if_needed() -> Optional[asyncio.AbstractServer]:
    """
    Si la plataforma en la nube define la variable de entorno PORT (p. ej. Render Web Service
    o Koyeb), levanta un micro-servidor HTTP en ese puerto para superar los healthchecks.
    """
    port_str = os.getenv("PORT")
    if not port_str:
        return None
    try:
        port = int(port_str)
        server = await asyncio.start_server(_handle_healthcheck, "0.0.0.0", port)
        logger.info("Servidor healthcheck activo en el puerto %d para despliegue Cloud.", port)
        return server
    except Exception as exc:
        logger.warning("No se pudo iniciar el servidor healthcheck en PORT=%s: %s", port_str, exc)
        return None


# ---------------------------------------------------------------------------
# Modos de Ejecución (--test y Monitoreo 24/7)
# ---------------------------------------------------------------------------


async def run_test_notification(api_url: str, ntfy_url: str, icon_url: str, timeout: float) -> int:
    """
    Obtiene el News Feed en vivo, busca la entrada más reciente que cumpla los 10 filtros
    y envía una notificación real de prueba a ntfy.sh (solo título, sin cuerpo, sin emojis, con logo).
    """
    logger.info("Ejecutando modo --test contra %s ...", api_url)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        items = await fetch_news_feed(client, api_url)
        logger.info("News Feed descargado correctamente (%d entradas totales).", len(items))

        for raw_item in items:
            parsed = await parse_news_feed_item(raw_item, client=client)
            if parsed is not None:
                logger.info(
                    "Última entrada válida del News Feed encontrada:\n"
                    "  - ID News Feed / Artículo : %d / %d\n"
                    "  - Filtro interno aplicado : %s (oculto en el push)\n"
                    "  - Title (News Feed texto) : %s\n"
                    "  - Body                    : (vacío)\n"
                    "  - Icon                    : %s\n"
                    "  - URL Universal Link      : %s",
                    parsed.news_id,
                    parsed.content_id,
                    parsed.section,
                    parsed.feed_title,
                    icon_url,
                    parsed.frontend_url,
                )
                ok = await send_ntfy_notification(client, ntfy_url, parsed, icon_url=icon_url)
                return 0 if ok else 1

        logger.warning("No se encontró ninguna entrada en la primera página que cumpla los filtros.")
        return 1


async def seed_initial_feed(
    store: SeenNewsStore,
    items: list[dict[str, Any]],
    client: httpx.AsyncClient,
) -> int:
    """
    Puebla la base SQLite en el primer arranque sin enviar notificaciones
    para evitar una avalancha inicial (incluso si el contenedor en la nube se reinicia).
    """
    seeded_count = 0
    for raw_item in items:
        if not isinstance(raw_item, dict) or not raw_item.get("id"):
            continue
        news_id = int(raw_item["id"])
        parsed = await parse_news_feed_item(raw_item, client=client)
        if parsed:
            store.mark_seen(
                news_id=parsed.news_id,
                content_id=parsed.content_id,
                frontend_url=parsed.frontend_url,
                section=parsed.section,
                title=parsed.feed_title,
            )
        else:
            content = raw_item.get("content") if isinstance(raw_item.get("content"), dict) else {}
            store.mark_seen(
                news_id=news_id,
                content_id=content.get("id") if content else None,
                frontend_url=content.get("frontendUrl") if content else None,
                section=None,
                title=clean_text(raw_item.get("title")),
            )
        seeded_count += 1
    return seeded_count


async def run_watcher_loop(
    api_url: str,
    ntfy_url: str,
    icon_url: str,
    db_path: str,
    poll_interval: int,
    timeout: float,
    max_runtime: int = 0,
) -> None:
    """Loop principal 24/7 con arranque limpio y tolerancia a fallos de red."""
    store = SeenNewsStore(db_path)
    stop_event = asyncio.Event()
    start_monotonic = asyncio.get_running_loop().time()

    def _request_stop() -> None:
        logger.info("Señal de parada recibida. Cerrando servicio de forma limpia...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _request_stop())

    health_server = await start_healthcheck_server_if_needed()

    logger.info(
        "Iniciando i24NEWS News Feed Watcher | Intervalo: %ds | Base de datos: %s",
        poll_interval,
        db_path,
    )

    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            while not stop_event.is_set():
                try:
                    items = await fetch_news_feed(client, api_url)

                    # Primer arranque (base vacía): poblar sin enviar notificaciones
                    if store.is_empty():
                        count = await seed_initial_feed(store, items, client)
                        logger.info(
                            "Primer arranque detectado: %d entradas actuales del News Feed registradas en %s sin enviar alertas.",
                            count,
                            db_path,
                        )
                    else:
                        # Procesar de más antigua a más reciente dentro del lote actual
                        new_entries: list[ParsedFeedItem] = []
                        for raw_item in reversed(items):
                            if not isinstance(raw_item, dict) or not raw_item.get("id"):
                                continue
                            news_id = int(raw_item["id"])
                            raw_title = clean_text(raw_item.get("title"))

                            if store.is_seen(news_id, raw_title):
                                continue

                            parsed = await parse_news_feed_item(raw_item, client=client)
                            if parsed is None:
                                store.mark_seen(news_id=news_id, title=raw_title)
                                continue

                            new_entries.append(parsed)

                        for entry in new_entries:
                            sent = await send_ntfy_notification(
                                client,
                                ntfy_url,
                                entry,
                                icon_url=icon_url,
                            )
                            if sent:
                                store.mark_seen(
                                    news_id=entry.news_id,
                                    content_id=entry.content_id,
                                    frontend_url=entry.frontend_url,
                                    section=entry.section,
                                    title=entry.feed_title,
                                )

                except Exception as exc:
                    logger.error("Fallo temporal en el ciclo de monitoreo (el loop continúa): %s", exc)

                if max_runtime > 0:
                    elapsed = loop.time() - start_monotonic
                    if elapsed + poll_interval >= max_runtime:
                        logger.info(
                            "Tiempo máximo de sesión alcanzado (%.0fs / %ds). Cerrando ciclo limpiamente...",
                            elapsed,
                            max_runtime,
                        )
                        break

                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    pass
    finally:
        if health_server is not None:
            health_server.close()
            await health_server.wait_closed()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Monitor 24/7 del News Feed de i24NEWS en inglés con notificaciones push vía ntfy.sh"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Toma la última entrada válida del News Feed que cumpla los 10 filtros y envía un push real a ntfy.sh",
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"URL del endpoint News Feed de i24NEWS (por defecto: {DEFAULT_API_URL})",
    )
    parser.add_argument(
        "--ntfy-url",
        default=DEFAULT_NTFY_URL,
        help="URL del tópico de ntfy.sh",
    )
    parser.add_argument(
        "--icon-url",
        default=DEFAULT_NTFY_ICON_URL,
        help=f"URL del ícono para ntfy.sh (por defecto: {DEFAULT_NTFY_ICON_URL})",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL,
        help=f"Segundos entre consultas al feed (por defecto: {DEFAULT_POLL_INTERVAL})",
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"Ruta al archivo SQLite de deduplicación (por defecto: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--max-runtime",
        type=int,
        default=DEFAULT_MAX_RUNTIME,
        help="Duración máxima en segundos antes de salir limpiamente (0 = infinito, útil para runners Cloud)",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        if args.test:
            return asyncio.run(
                run_test_notification(
                    args.api_url,
                    args.ntfy_url,
                    args.icon_url,
                    DEFAULT_TIMEOUT,
                )
            )

        asyncio.run(
            run_watcher_loop(
                api_url=args.api_url,
                ntfy_url=args.ntfy_url,
                icon_url=args.icon_url,
                db_path=args.db_path,
                poll_interval=args.interval,
                timeout=DEFAULT_TIMEOUT,
                max_runtime=args.max_runtime,
            )
        )
        return 0
    except KeyboardInterrupt:
        logger.info("Servicio detenido por el usuario.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
