"""
FI Sales Service — HTTP API поверх базы взаимодействий FI Sales.

Запуск:
    python3 fi_sales_service.py
    uvicorn fi_sales_service:app --host 127.0.0.1 --port 18003

Эндпоинты:
    Справочники:
        GET  /clients            — все клиенты с aliases
        POST /clients            — имя от сейлза → canonical
        GET  /products           — все продукты с aliases
    Записи взаимодействий:
        POST /records            — сохранить заметку
        POST /records/search     — записи по фильтрам
        POST /records/count      — подсчёт по фильтрам (без fallback)
        GET  /records/{record_id} — одна запись целиком
        PATCH /records/{record_id} — правка уже сохранённой заметки
        GET  /records/export       — Excel базы одной команды
    Служебное:
        GET  /health             — статус сервиса

Добавление: дата встречи только сегодня или вчера (окно 1 день).
Правка и удаление: только своя заметка и только 24 часа с created_at.

JSONL-зеркало (best-effort, не блокирует OpenSearch):
    FI_RECORDS_JSONL — путь к fi_records.jsonl
    (по умолчанию рядом с fi_calls.xlsx: .../calls_data/fi_records.jsonl)
"""
import hashlib
import json
import logging
import os
import re
import sys
import threading
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import openpyxl
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from opensearchpy import OpenSearch, helpers as os_helpers
from opensearchpy.exceptions import NotFoundError
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[1]
_PKG_PARENT = _REPO_ROOT.parent
if str(_PKG_PARENT) not in sys.path:
    sys.path.insert(0, str(_PKG_PARENT))

from synaptica.backend.streamlit_functions.calls_access import (  # noqa: E402
    NOTES_ALLOWED_USERS,
    normalize_username,
    NOTES_USER_FIO,
    notes_fi_desk_for_user,
    notes_logins_for_participant,
    notes_team_for_user,
    published_notes_username,
    verify_notes_token,
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BASE = "synaptica/synaptica/"
_SERVER_ROOT = Path("/home/synaptica/synaptica/synaptica")
_LOCAL_ROOT = Path(__file__).resolve().parents[1]


def _config_file(name: str) -> Path:
    server = _SERVER_ROOT / "config" / name
    local = _LOCAL_ROOT / "config" / name
    return server if server.is_file() else local


def _data_file(name: str) -> Path:
    if os.environ.get("NOTES_LOCAL_STORE") == "1":
        # Локальный стенд: Excel/JSONL пишем рядом с индексами, не в calls_data из git.
        return Path(os.environ.get("NOTES_LOCAL_DIR", str(_LOCAL_ROOT / ".local_s3" / "notes"))) / name
    if (_SERVER_ROOT / "calls_data").is_dir():
        return _SERVER_ROOT / "calls_data" / name
    return _LOCAL_ROOT / "calls_data" / name


@dataclass(frozen=True)
class NotesTeam:
    id: str
    index: str
    excel_file: str
    jsonl_file: str


def _teams() -> dict[str, NotesTeam]:
    return {
        "fi": NotesTeam(
            "fi",
            os.environ.get("FI_OS_INDEX", "fi_interaction_records"),
            "fi_calls.xlsx",
            "fi_records.jsonl",
        ),
        "corp": NotesTeam(
            "corp",
            os.environ.get("CORP_NOTES_OS_INDEX", "corp_notes_records"),
            "corp_notes.xlsx",
            "corp_notes.jsonl",
        ),
        "commodity": NotesTeam(
            "commodity",
            os.environ.get("COMMODITY_NOTES_OS_INDEX", "commodity_notes_records"),
            "commodity_notes.xlsx",
            "commodity_notes.jsonl",
        ),
    }


_TEAM_ALIASES = {
    "fi": "fi",
    "corp": "corp",
    "corps": "corp",
    "corporate": "corp",
    "commodity": "commodity",
    "comdty": "commodity",
    "cmd": "commodity",
    "cmdt": "commodity",
}


def resolve_team(name: str | None) -> NotesTeam:
    key = _TEAM_ALIASES.get(str(name or "fi").strip().lower())
    teams = _teams()
    if not key or key not in teams:
        raise ValueError(f"unknown team: {name}")
    return teams[key]


FI_CALLS_EXCEL = str(_data_file("fi_calls.xlsx"))
FI_CLIENTS_PATH = str(_config_file("fi_clients.json"))
FI_PRODUCTS_PATH = str(_config_file("notes_products.json"))

OPENSEARCH_HOST = "tsles-pkapg0006.esrt.sber.ru"
OPENSEARCH_PORT = 19200
OPENSEARCH_USER = "pvs_admin_user"
OPENSEARCH_PASSWORD = "Musthavedarkside777@"
OPENSEARCH_CA_CERTS = "/tmp/opensearch-certs/opensearch/ca"
OPENSEARCH_CLIENT_CERT = "/tmp/opensearch-certs/opensearch/elastic.cert"
OPENSEARCH_CLIENT_KEY = "/tmp/opensearch-certs/opensearch/elastic.key"

FI_INDEX_NAME = os.environ.get("FI_OS_INDEX", "fi_interaction_records")

_NOTES_INDEX_BODY = {
    "mappings": {
        "properties": {
            "record_id": {"type": "keyword"},
            "record_type": {"type": "keyword"},
            "team": {"type": "keyword"},
            "client_name_norm": {"type": "keyword"},
            "client_name_raw": {"type": "keyword"},
            "products": {"type": "keyword"},
            "currencies": {"type": "keyword"},
            "interaction_date": {"type": "date", "ignore_malformed": True},
            "meeting_start": {"type": "keyword"},
            "meeting_end": {"type": "keyword"},
            "meeting_duration": {"type": "keyword"},
            "raw_text": {"type": "text"},
            "short_summary": {"type": "text"},
            "next_step": {"type": "text"},
            "status": {"type": "keyword"},
            "created_by": {"type": "keyword"},
        }
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fi_sales_svc")

# Best-effort JSONL-зеркало (сбой не влияет на OpenSearch).
FI_RECORDS_JSONL = os.environ.get(
    "FI_RECORDS_JSONL",
    str(Path(FI_CALLS_EXCEL).with_name("fi_records.jsonl")),
)
_jsonl_lock = threading.Lock()


def _jsonl_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def mirror_upsert(doc: dict, path: str | None = None) -> None:
    if not doc or not doc.get("record_id"):
        return
    try:
        path = Path(path or FI_RECORDS_JSONL)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(doc, ensure_ascii=False, default=str)
        with _jsonl_lock:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(payload + "\n")
    except Exception as exc:
        logger.warning("JSONL mirror upsert failed (non-critical): %s", exc)


def mirror_delete(record_id: str, path: str | None = None) -> None:
    if not record_id:
        return
    try:
        path = Path(path or FI_RECORDS_JSONL)
        path.parent.mkdir(parents=True, exist_ok=True)
        tombstone = {
            "record_id": record_id,
            "_deleted": True,
            "deleted_at": _jsonl_now_iso(),
        }
        payload = json.dumps(tombstone, ensure_ascii=False)
        with _jsonl_lock:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(payload + "\n")
    except Exception as exc:
        logger.warning("JSONL mirror delete failed (non-critical): %s", exc)

# ---------------------------------------------------------------------------
# OpenSearch client
# ---------------------------------------------------------------------------

_os_client: OpenSearch = None


def get_os_client() -> OpenSearch:
    global _os_client
    if _os_client is None and os.environ.get("NOTES_LOCAL_STORE") == "1":
        # Локальный стенд без корп. сети: индексы — JSON-файлы.
        from synaptica.services.local_notes_store import LocalNotesStore

        _os_client = LocalNotesStore(
            Path(os.environ.get("NOTES_LOCAL_DIR", str(_LOCAL_ROOT / ".local_s3" / "notes"))),
            seed_jsonl={cfg.index: _LOCAL_ROOT / "calls_data" / cfg.jsonl_file for cfg in _teams().values()},
        )
    if _os_client is None:
        _os_client = OpenSearch(
            hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
            http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=False,
            timeout=30,
            ca_certs=OPENSEARCH_CA_CERTS,
            client_cert=OPENSEARCH_CLIENT_CERT,
            client_key=OPENSEARCH_CLIENT_KEY,
        )
    return _os_client


# ---------------------------------------------------------------------------
# Справочники
# ---------------------------------------------------------------------------

def _load_json(path: str | Path) -> dict:
    p = Path(path)
    if not p.is_file():
        raise RuntimeError(f"Справочник не найден: {p}")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def get_clients(team: str = "fi") -> dict:
    """Один справочник на FI, корпов и Comdty. team оставлен для совместимости API."""
    del team
    path = _config_file("fi_clients.json")
    if not path.is_file():
        raise RuntimeError(f"Справочник не найден: {path}")
    return _load_json(path)


_FI_DESKS = frozenset({"derivatives", "credit", "liquidity"})


def _record_fi_desk(rec: dict) -> str:
    """Старые заметки FI без поля — деривативы."""
    raw = str(rec.get("fi_desk") or "").strip().lower()
    if raw in _FI_DESKS:
        return raw
    return "derivatives"


def _visible_on_fi_desk(rec: dict, fi_desk: str | None) -> bool:
    if not fi_desk or fi_desk == "master":
        return True
    return _record_fi_desk(rec) == fi_desk


def _filter_fi_desk(records: list[dict], fi_desk: str | None) -> list[dict]:
    if not fi_desk or fi_desk == "master":
        return records
    return [rec for rec in records if _visible_on_fi_desk(rec, fi_desk)]


def _reject_if_other_fi_desk(rec: dict, actor: str, team_id: str) -> None:
    if team_id != "fi":
        return
    desk = notes_fi_desk_for_user(actor)
    if not desk or desk == "master" or _visible_on_fi_desk(rec, desk):
        return
    raise HTTPException(status_code=403, detail="заметка другого направления FI")


def _desk_to_store(actor: str, team_id: str, requested: str | None) -> str:
    if team_id != "fi":
        return ""
    desk = notes_fi_desk_for_user(actor) or "derivatives"
    if desk != "master":
        return desk
    asked = str(requested or "").strip().lower()
    if asked in _FI_DESKS:
        return asked
    return "derivatives"


def get_products(team: str | None = None, fi_desk: str | None = None) -> dict:
    """Продукты своей команды. Направление внутри FI справочник не режет."""
    del fi_desk
    path = _config_file("notes_products.json")
    if not path.is_file():
        path = _config_file("fi_products.json")
        data = _load_json(path)
        if team and resolve_team(team).id != "fi":
            return {}
        return data
    data = _load_json(path)
    if not team:
        return data
    tid = resolve_team(team).id
    out: dict = {}
    for key, info in data.items():
        info = info or {}
        teams = info.get("team") or info.get("teams") or ["fi"]
        if isinstance(teams, str):
            teams = [teams]
        if tid in teams:
            out[key] = info
    return out


_LEGAL_TOKENS = frozenset({"пао", "ао", "ооо", "зао", "оао", "ск"})


def _client_tokens(value: str) -> list[str]:
    text = str(value).lower().replace("ё", "е")
    tokens: list[str] = []
    buf: list[str] = []
    for ch in text:
        if ch.isalnum():
            buf.append(ch)
        elif buf:
            tokens.append("".join(buf))
            buf = []
    if buf:
        tokens.append("".join(buf))
    return [token for token in tokens if token not in _LEGAL_TOKENS]


def _normalize_client_name(value: str) -> str:
    tokens = _client_tokens(value)
    if len(tokens) > 1:
        tokens = [token for token in tokens if token not in {"банк", "bank"}]
    return "".join(tokens)


def _tokens_cover(query: list[str], client: list[str]) -> bool:
    """Составное имя: каждое слово запроса есть у клиента. Одиночный «сбер» не раскрываем."""
    if len(query) < 2 or not client:
        return False
    pool = list(client)
    for token in query:
        found = None
        for i, other in enumerate(pool):
            same = token == other
            sber = token == "сбер" and other.startswith("сбер") and len(other) > len(token)
            prefix = len(token) >= 5 and other.startswith(token) and other != token
            if same or sber or prefix:
                found = i
                break
        if found is None:
            return False
        pool.pop(found)
    return True


def _wordset_candidates(raw: str, team: str, limit: int = 8) -> list[str]:
    query = _client_tokens(raw)
    if len(query) > 1:
        query = [token for token in query if token not in {"банк", "bank"}]
    if len(query) < 2:
        return []
    found: list[str] = []
    for canonical, info in get_clients(team).items():
        info = info or {}
        names = [canonical, info.get("full_name_ru") or "", info.get("full_name_en") or ""]
        names.extend(info.get("aliases") or [])
        if any(_tokens_cover(query, _client_tokens(name)) for name in names if str(name).strip()):
            found.append(canonical)
        if len(found) >= limit:
            break
    return found


_MIN_HINT = 4
_WEAK_CLIENT_KEYS = frozenset({"банк", "банок", "bank", "ао", "пао", "ооо", "ук", "нпф", "зао"})
def _exact_canonical(key: str, index: dict[str, str]) -> str | None:
    if not key:
        return None
    return index.get(key)


def _client_candidates(key: str, index: dict[str, str], limit: int = 15) -> list[str]:
    """Подсказки агенту, не автовыбор. «владимир» ≠ «БУРМИСТРОВ ВЛАДИМИР»."""
    if not key or key in _WEAK_CLIENT_KEYS or len(key) < 2:
        return []
    found: list[str] = []
    seen: set[str] = set()
    for alias, canonical in sorted(index.items(), key=lambda item: item[0]):
        if canonical in seen:
            continue
        if alias == key:
            continue
        if len(alias) < _MIN_HINT:
            continue
        hint = (
            alias.startswith(key)
            or (len(key) >= _MIN_HINT and key.startswith(alias))
            or (len(key) >= _MIN_HINT and key in alias)
        )
        if not hint:
            continue
        seen.add(canonical)
        found.append(canonical)
        if len(found) >= limit:
            break
    return found


def _match_client_key(
    key: str,
    index: dict[str, str],
    notes: dict[str, str] | None = None,
    raw: str = "",
    team: str = "fi",
) -> tuple[str | None, list[str]]:
    """Точное имя справочника — matched. Имя только из заметок и похожий набор слов — candidates."""
    if not key or key in {"сбер", "сбербанк", "sber", "sberbank"}:
        return None, []
    exact = _exact_canonical(key, index)
    if exact:
        return exact, []
    found = _client_candidates(key, index)
    for name in _wordset_candidates(raw or key, team):
        if name not in found:
            found.append(name)
    if notes and notes.get(key):
        note_name = notes[key]
        if note_name not in found:
            found.insert(0, note_name)
        return None, found
    return None, found


def _note_client_index(team: str) -> dict[str, str]:
    """Имена, которые уже лежат в заметках, даже если их нет в справочнике."""
    try:
        cfg = resolve_team(team)
        resp = get_os_client().search(
            index=cfg.index,
            body={
                "size": 0,
                "query": {"match_all": {}},
                "aggs": {"by_client": {"terms": {"field": "client_name_norm", "size": 500}}},
            },
        )
    except Exception:
        return {}
    out: dict[str, str] = {}
    buckets = ((resp.get("aggregations") or {}).get("by_client") or {}).get("buckets") or []
    for bucket in buckets:
        name = str(bucket.get("key") or "").strip()
        key = _normalize_client_name(name)
        if key and name:
            out.setdefault(key, name)
    return out


def _alias_index(team: str = "fi") -> dict[str, str]:
    """Нормализованное имя (canonical, полное название, алиас) → canonical."""
    index: dict[str, str] = {}
    for canonical, info in get_clients(team).items():
        info = info or {}
        names = [canonical, info.get("full_name_ru") or "", info.get("full_name_en") or ""]
        names.extend(info.get("aliases") or [])
        for name in names:
            key = _normalize_client_name(name)
            if key:
                index.setdefault(key, canonical)
    return index


def resolve_client_names(names: list[str] | None, team: str = "fi") -> list[str]:
    """Точный алиас или имя из заметок → canonical. Остальное оставляем как написали."""
    if not names:
        return []
    index = _alias_index(team)
    notes = _note_client_index(team)
    out: list[str] = []
    seen: set[str] = set()
    for raw in names:
        name = str(raw).strip()
        if not name:
            continue
        key = _normalize_client_name(name)
        if not key:
            continue
        chosen, _cands = _match_client_key(key, index, notes, raw=name, team=team)
        if not chosen:
            chosen = name
        if chosen not in seen:
            seen.add(chosen)
            out.append(chosen)
    return out


# ---------------------------------------------------------------------------
# Поиск
# ---------------------------------------------------------------------------

_CURRENCY_PAIRS = {"CNH": "CNY", "CNY": "CNH"}
_LEGACY_PRODUCT_LABELS = {
    "XCCY": ["swap"],
    "MBK": ["Loans & Deposits (МБК)"],
    "CAP": ["option"],
    "FLOOR": ["option"],
}
_DIRECTION_ALIASES = {
    "client_places": "client_places",
    "client_attracts": "client_attracts",
    "hedge": "hedge",
    "размещение": "client_places",
    "размещал": "client_places",
    "размещали": "client_places",
    "привлечение": "client_attracts",
    "привлекал": "client_attracts",
    "привлекали": "client_attracts",
    "хедж": "hedge",
    "хеджирование": "hedge",
}


def _expand_currencies(currencies: list[str]) -> list[str]:
    out: list[str] = []
    for raw in currencies:
        token = str(raw).strip().upper()
        if not token:
            continue
        if token not in out:
            out.append(token)
        pair = _CURRENCY_PAIRS.get(token)
        if pair and pair not in out:
            out.append(pair)
    return out


def _product_catalog(team: str) -> tuple[dict[str, str], dict[str, list[str]]]:
    alias_to_canon: dict[str, str] = {}
    labels: dict[str, list[str]] = {}
    for canon, info in get_products(team).items():
        names = [canon, *list((info or {}).get("aliases") or [])]
        labels[canon] = [str(name) for name in names if str(name).strip()]
        for name in labels[canon]:
            alias_to_canon.setdefault(name.strip().lower(), canon)
    return alias_to_canon, labels


def _expand_products(products: list[str], team: str) -> list[str]:
    alias_to_canon, labels = _product_catalog(team)
    out: list[str] = []
    for raw in products:
        key = str(raw).strip()
        if not key:
            continue
        canon = alias_to_canon.get(key.lower(), key)
        for label in labels.get(canon, [canon]):
            if label not in out:
                out.append(label)
        for extra in _LEGACY_PRODUCT_LABELS.get(str(canon), []):
            if extra not in out:
                out.append(extra)
        if key not in out:
            out.append(key)
    return out


def _canonical_products(products: list[str], team: str) -> list[str]:
    alias_to_canon, _labels = _product_catalog(team)
    out: list[str] = []
    for raw in products:
        key = str(raw).strip()
        if not key:
            continue
        canon = alias_to_canon.get(key.lower(), key)
        if canon not in out:
            out.append(canon)
    return out


_CLIENT_TYPE_ALIASES = {
    "bank": "bank",
    "банк": "bank",
    "банки": "bank",
    "insurance": "insurance",
    "страховая": "insurance",
    "страховые": "insurance",
    "fund": "fund",
    "фонд": "fund",
    "фонды": "fund",
    "ук": "fund",
    "нпф": "fund",
    "broker": "broker",
    "брокер": "broker",
    "брокеры": "broker",
    "company": "company",
    "corporate": "corporate",
    "корпорации": "company",
}


def _client_type_tokens(values: list[str] | None) -> set[str]:
    out: set[str] = set()
    for raw in values or []:
        key = " ".join(str(raw).strip().lower().replace("ё", "е").split())
        if not key:
            continue
        token = _CLIENT_TYPE_ALIASES.get(key)
        if not token:
            raise ValueError(f"unknown client type: {raw}")
        out.add(token)
        if token == "company":
            out.add("corporate")
    return out


def _client_types_index(team: str) -> dict[str, str]:
    return {
        canonical: str((info or {}).get("type") or "")
        for canonical, info in get_clients(team).items()
    }


def _direction_token(value: str) -> str:
    key = " ".join(str(value or "").strip().lower().replace("ё", "е").split())
    if not key:
        return ""
    token = _DIRECTION_ALIASES.get(key)
    if not token:
        raise ValueError(f"unknown direction: {value}")
    return token


def _backfill_directions() -> None:
    """Пустое направление и кривые метки продуктов в уже лежащих заметках."""
    if os.environ.get("NOTES_LOCAL_STORE") != "1":
        return
    for cfg in _teams().values():
        records, _total = search_records(
            [], [], [], None, None, None, team=cfg.id,
        )
        for rec in records:
            raw = str(rec.get("raw_text") or "")
            changed = False
            if not rec.get("economic_direction"):
                inferred = _infer_direction(raw)
                if inferred:
                    rec["economic_direction"] = inferred
                    changed = True
            products = _canonicalize_stored_products(list(rec.get("products") or []), raw)
            if products != list(rec.get("products") or []):
                rec["products"] = products
                changed = True
            if changed and rec.get("record_id"):
                _index_record(str(rec["record_id"]), rec, cfg.index)


def _infer_direction(text: str) -> list[str]:
    """Направление с точки зрения клиента. «Мы привлекли» — клиент размещал."""
    low = str(text or "").lower().replace("ё", "е")
    found: list[str] = []
    sber_attracts = any(phrase in low for phrase in (
        "сбер привлек", "сбер привлека", "мы привлек", "мы привлека",
    ))
    sber_places = any(phrase in low for phrase in (
        "сбер размест", "сбер размещ", "мы размест", "мы размещ",
    ))
    if sber_places or (("привлек" in low or "привлека" in low) and not sber_attracts):
        found.append("client_attracts")
    if sber_attracts or (("размест" in low or "размещ" in low) and not sber_places):
        found.append("client_places")
    if "хедж" in low or "хеджир" in low:
        found.append("hedge")
    return found


def _canonicalize_stored_products(products: list[str], raw_text: str) -> list[str]:
    """Старые метки к канону, если текст это подтверждает. FX swap Атона не становится XCCY."""
    text = str(raw_text or "").lower().replace("ё", "е")
    xccy = any(word in text for word in ("xccy", "хссу", "валютно-процент", "валютно процентный"))
    out: list[str] = []
    for raw in products:
        label = str(raw).strip()
        if not label:
            continue
        if label == "swap" and xccy:
            label = "XCCY"
        elif label == "Loans & Deposits (МБК)":
            label = "MBK"
        elif label == "option" and "floor" in text:
            label = "FLOOR"
        elif label == "option" and "cap" in text and "floor" not in text:
            label = "CAP"
        if label not in out:
            out.append(label)
    return out


def _build_query(
    clients: list[str],
    products: list[str],
    currencies: list[str],
    date_from: Optional[str],
    date_to: Optional[str],
    record_type: str,
    limit: int,
    exclude_clients: list[str] | None = None,
    created_by: str | None = None,
    direction: str | None = None,
    sort: str = "date_desc",
) -> dict:
    must = []
    must_not = []

    if record_type and record_type != "all":
        must.append({"terms": {"record_type": _record_type_search_values(record_type)}})

    if clients:
        must.append({"terms": {"client_name_norm": clients}})

    if exclude_clients:
        must_not.append({"terms": {"client_name_norm": exclude_clients}})

    if products:
        must.append({"terms": {"products": products}})

    if currencies:
        must.append({"terms": {"currencies": currencies}})

    if created_by:
        must.append({"terms": {"created_by": [created_by]}})

    if direction:
        must.append({"terms": {"economic_direction": [_direction_token(direction)]}})

    if date_from or date_to:
        date_range: dict = {}
        if date_from:
            date_range["gte"] = date_from
        if date_to:
            date_range["lte"] = date_to
        must.append({"range": {"interaction_date": date_range}})

    bool_q: dict = {}
    if must:
        bool_q["must"] = must
    if must_not:
        bool_q["must_not"] = must_not
    order = "asc" if sort == "date_asc" else "desc"
    return {
        "query": {"bool": bool_q} if bool_q else {"match_all": {}},
        "sort": [{"interaction_date": {"order": order, "missing": "_last"}}],
        "size": limit,
        "track_total_hits": True,
    }


def _extract_total(resp: dict) -> int:
    total = resp.get("hits", {}).get("total", 0)
    if isinstance(total, dict):
        return int(total.get("value", 0) or 0)
    return int(total or 0)


def _hit_to_record(hit: dict) -> dict:
    """OpenSearch hit → dict для _format_record; record_id из _source или _id документа."""
    src = hit.get("_source")
    if not isinstance(src, dict):
        src = {}
    else:
        src = dict(src)
    doc_id = hit.get("_id") or ""
    if doc_id and not str(src.get("record_id") or "").strip():
        src["record_id"] = doc_id
    return src


def _search_all(body: dict, index: str = FI_INDEX_NAME) -> tuple[list[dict], int]:
    """Все хиты по запросу, без пользовательского лимита."""
    client = get_os_client()
    page = 500
    body = dict(body)
    body["size"] = page
    body["track_total_hits"] = True
    sorts = list(body.get("sort") or [])
    sorts.append({"_id": {"order": "asc"}})
    body["sort"] = sorts
    records: list[dict] = []
    search_after = None
    total = 0
    while True:
        req = dict(body)
        if search_after is not None:
            req["search_after"] = search_after
        resp = client.search(index=index, body=req)
        total = _extract_total(resp)
        hits = resp.get("hits", {}).get("hits", []) or []
        if not hits:
            break
        records.extend(_hit_to_record(h) for h in hits)
        search_after = hits[-1].get("sort")
        if len(hits) < page or search_after is None:
            break
    return records, total


def search_records(
    clients: list[str],
    products: list[str],
    currencies: list[str],
    date_from: Optional[str],
    date_to: Optional[str],
    text: Optional[str],
    record_type: str = "all",
    limit: int = 0,
    team: str = "fi",
    fi_desk: str | None = None,
    exclude_clients: list[str] | None = None,
    created_by: str | None = None,
    direction: str | None = None,
    sort: str = "date_desc",
    client_types: list[str] | None = None,
) -> tuple[list[dict], int]:
    """Совпадения из индекса. limit > 0 обрезает уже отфильтрованный список, total не режет."""
    cfg = resolve_team(team)
    clients = resolve_client_names(clients, cfg.id)
    excluded = resolve_client_names(exclude_clients, cfg.id)
    products = _expand_products(products, cfg.id)
    currencies = _expand_currencies(currencies)
    actor = normalize_username(created_by) if created_by else ""
    if date_from:
        date_from = _normalize_date(date_from)
    if date_to:
        date_to = _normalize_date(date_to)
    body = _with_text(
        _build_query(
            clients, products, currencies, date_from, date_to, record_type, 0,
            exclude_clients=excluded,
            created_by=actor or None,
            direction=direction,
            sort=sort,
        ),
        text,
    )
    records, total = _search_all(body, cfg.index)
    if not records and text and not (
        products or currencies or clients or excluded or date_from or date_to or actor or direction
    ):
        body3 = {
            "query": {
                "multi_match": {
                    "query": text,
                    "fields": ["raw_text", "short_summary", "client_name_raw"],
                    "type": "best_fields",
                }
            },
            "sort": [{"interaction_date": {"order": "asc" if sort == "date_asc" else "desc", "missing": "_last"}}],
            "track_total_hits": True,
        }
        records, total = _search_all(body3, cfg.index)
    records = _filter_fi_desk(records, fi_desk)
    wanted_types = _client_type_tokens(client_types)
    if wanted_types:
        types = _client_types_index(cfg.id)
        records = [
            rec for rec in records
            if types.get(str(rec.get("client_name_norm") or rec.get("client_name_raw") or "")) in wanted_types
        ]
    total = len(records) if (fi_desk and fi_desk != "master") or wanted_types else total
    if sort == "date_asc":
        records = list(reversed(records))
    if limit and limit > 0:
        records = records[: int(limit)]
    return records, total


def _with_text(body: dict, text: Optional[str]) -> dict:
    qtext = str(text or "").strip()
    if not qtext:
        return body
    mm = {
        "multi_match": {
            "query": qtext,
            "fields": ["raw_text", "short_summary", "client_name_raw"],
            "type": "best_fields",
        }
    }
    query = body.get("query") or {}
    if "match_all" in query:
        body["query"] = mm
        return body
    bool_q = query.get("bool")
    if isinstance(bool_q, dict):
        must = list(bool_q.get("must") or [])
        must.append(mm)
        bool_q["must"] = must
    return body


def count_records(
    clients: list[str],
    products: list[str],
    currencies: list[str],
    date_from: Optional[str],
    date_to: Optional[str],
    text: Optional[str] = None,
    record_type: str = "all",
    team: str = "fi",
    fi_desk: str | None = None,
    exclude_clients: list[str] | None = None,
    created_by: str | None = None,
    direction: str | None = None,
    client_types: list[str] | None = None,
) -> dict:
    """Точный total по фильтрам. Без fallback на text/ослабление — иначе число плывёт."""
    if (fi_desk and fi_desk != "master") or client_types:
        records, total = search_records(
            clients, products, currencies, date_from, date_to, text, record_type,
            team=team, fi_desk=fi_desk, exclude_clients=exclude_clients,
            created_by=created_by, direction=direction, client_types=client_types,
        )
        by_type: dict[str, int] = {}
        by_client: dict[str, int] = {}
        for rec in records:
            kind = _display_record_type(rec.get("record_type"))
            by_type[kind] = by_type.get(kind, 0) + 1
            client = str(rec.get("client_name_norm") or rec.get("client_name_raw") or "").strip()
            if client:
                by_client[client] = by_client.get(client, 0) + 1
        return {"total": total, "by_record_type": by_type, "by_client": by_client}
    cfg = resolve_team(team)
    clients = resolve_client_names(clients, cfg.id)
    excluded = resolve_client_names(exclude_clients, cfg.id)
    actor = normalize_username(created_by) if created_by else ""
    if date_from:
        date_from = _normalize_date(date_from)
    if date_to:
        date_to = _normalize_date(date_to)
    body = _build_query(
        clients, _expand_products(products, cfg.id), _expand_currencies(currencies),
        date_from, date_to, record_type, limit=0,
        exclude_clients=excluded, created_by=actor or None, direction=direction,
    )
    body["size"] = 0
    body["track_total_hits"] = True
    body["aggs"] = {
        "by_type": {"terms": {"field": "record_type", "size": 16}},
        "by_client": {"terms": {"field": "client_name_norm", "size": 300}},
    }
    _with_text(body, text)
    resp = get_os_client().search(index=cfg.index, body=body)
    by_type: dict[str, int] = {}
    for bucket in ((resp.get("aggregations") or {}).get("by_type") or {}).get("buckets") or []:
        key = _display_record_type(bucket.get("key"))
        by_type[key] = by_type.get(key, 0) + int(bucket.get("doc_count") or 0)
    by_client: dict[str, int] = {}
    for bucket in ((resp.get("aggregations") or {}).get("by_client") or {}).get("buckets") or []:
        key = str(bucket.get("key") or "").strip()
        if key:
            by_client[key] = int(bucket.get("doc_count") or 0)
    return {"total": _extract_total(resp), "by_record_type": by_type, "by_client": by_client}


def _format_record(rec: dict) -> dict:
    rt = _display_record_type(rec.get("record_type"))
    return {
        "record_id": str(rec.get("record_id") or "").strip(),
        "date": rec.get("interaction_date", ""),
        "meeting_start": rec.get("meeting_start") or "",
        "meeting_end": rec.get("meeting_end") or "",
        "meeting_duration": rec.get("meeting_duration") or "",
        "client": rec.get("client_name_norm") or rec.get("client_name_raw", ""),
        "products": rec.get("products") or [],
        "currencies": rec.get("currencies") or [],
        "summary": rec.get("short_summary") or "",
        "raw_text": rec.get("raw_text") or "",
        "status": rec.get("status") or "",
        "next_step": rec.get("next_step") or "",
        "participants": rec.get("participants") or [],
        "record_type": rt,
        "event_kind": (
            _normalize_event_kind(rec.get("event_kind"))
            if _display_record_type(rec.get("record_type")) == RECORD_TYPE_EVENT
            else (rec.get("event_kind") or "")
        ),
        "created_by": rec.get("created_by") or "",
        "created_at": rec.get("created_at") or "",
        "fi_desk": _record_fi_desk(rec) if (rec.get("team") or "fi") == "fi" else "",
        "team": rec.get("team") or "",
    }


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _clean_actor(value: str | None) -> str:
    return str(value or "").strip() or "unknown"


def ensure_notes_index(index: str) -> None:
    """Создаёт пустой индекс команды. Индекс FI не трогает."""
    if index == FI_INDEX_NAME:
        return
    client = get_os_client()
    if client.indices.exists(index=index):
        return
    client.indices.create(index=index, body=_NOTES_INDEX_BODY)


def get_record(record_id: str, team: str = "fi") -> dict | None:
    client = get_os_client()
    try:
        resp = client.get(index=resolve_team(team).index, id=record_id)
    except NotFoundError:
        return None
    src = resp.get("_source")
    if not isinstance(src, dict):
        return None
    src.setdefault("record_id", record_id)
    return src


def _index_record(record_id: str, rec: dict, index: str = FI_INDEX_NAME) -> None:
    ensure_notes_index(index)
    os_client = get_os_client()
    action = {"_index": index, "_id": record_id, "_source": rec}
    _success, errors = os_helpers.bulk(os_client, [action], raise_on_error=False)
    if errors:
        logger.error("bulk index error: %s", errors)
        raise RuntimeError(f"Index error: {errors[0]}")


# def update_note(record_id: str, patches: dict[str, Any]) -> dict:
#     rec = get_record(record_id)
#     if rec is None:
#         raise KeyError(record_id)
#
#     if patches.get("raw_text") is not None:
#         text = str(patches["raw_text"]).strip()
#         if not text:
#             raise ValueError("raw_text cannot be empty")
#         rec["raw_text"] = text
#         rec["splitter_raw_block"] = text
#
#     if patches.get("client_canonical") is not None:
#         client = str(patches["client_canonical"]).strip()
#         if not client:
#             raise ValueError("client_canonical cannot be empty")
#         rec["client_name_raw"] = client
#         rec["client_name_norm"] = client
#         rec["client_group_id"] = f"{client}_intake"
#
#     if patches.get("date") is not None:
#         rec["interaction_date"] = str(patches["date"]).strip()
#
#     if patches.get("products") is not None:
#         rec["products"] = list(patches["products"])
#
#     if patches.get("currencies") is not None:
#         rec["currencies"] = list(patches["currencies"])
#
#     if patches.get("summary") is not None:
#         rec["short_summary"] = str(patches["summary"])
#
#     if patches.get("counterparty_type") is not None:
#         rec["counterparty_type"] = str(patches["counterparty_type"]) or "unknown"
#
#     if patches.get("status") is not None:
#         rec["status"] = str(patches["status"])
#
#     if patches.get("next_step") is not None:
#         rec["next_step"] = str(patches["next_step"])
#
#     if patches.get("created_by") is not None:
#         rec["created_by"] = _clean_actor(patches["created_by"])
#
#     if patches.get("record_type") is not None:
#         rec["record_type"] = _normalize_record_type(patches["record_type"])
#
#     rec["record_id"] = record_id
#     rec["updated_at"] = _now_iso()
#     rec.setdefault("created_at", rec["updated_at"])
#
#     _index_record(record_id, rec)
#     mirror_upsert(rec)
#     return rec
#
#
# def delete_note(record_id: str) -> None:
#     client = get_os_client()
#     try:
#         client.delete(index=FI_INDEX_NAME, id=record_id, refresh="wait_for")
#     except NotFoundError as exc:
#         raise KeyError(record_id) from exc
#     mirror_delete(record_id)


# ---------------------------------------------------------------------------
# Сохранение заметки
# ---------------------------------------------------------------------------

# Окно добавления — дата встречи не старше суток.
# Правка и удаление — 24 часа с момента записи.
EDIT_WINDOW_HOURS = 24

RECORD_TYPE_NEED = "потребность"
RECORD_TYPE_GENERAL = "общая_информация"
RECORD_TYPE_EVENT = "мероприятие"
VALID_RECORD_TYPES = frozenset({RECORD_TYPE_NEED, RECORD_TYPE_GENERAL, RECORD_TYPE_EVENT})
# Старый ярлык в индексе; для чтения это потребность.
_LEGACY_NEED_TYPES = frozenset({"взаимодействие", "interaction", "Interaction"})
_EVENT_TYPE_ALIASES = frozenset({
    RECORD_TYPE_EVENT, "конференция", "гемба", "gemba", "event",
})
EVENT_KINDS = frozenset({"звонок", "встреча"})
INTERNAL_EVENT_CLIENT = "внутренняя"


def _normalize_record_type(value: str | None) -> str:
    rt = "_".join(str(value or RECORD_TYPE_NEED).strip().lower().split())
    if rt in _EVENT_TYPE_ALIASES:
        return RECORD_TYPE_EVENT
    if rt not in VALID_RECORD_TYPES:
        raise ValueError(
            "record_type must be one of: "
            f"«{RECORD_TYPE_NEED}», «{RECORD_TYPE_GENERAL}», «{RECORD_TYPE_EVENT}»"
        )
    return rt


def _normalize_event_kind(value: str | None) -> str:
    """Только звонок или встреча. Пустое и неизвестное — пустая строка."""
    raw = "_".join(str(value or "").strip().lower().split())
    if not raw:
        return ""
    aliases = {
        "звонок": "звонок",
        "call": "звонок",
        "созвон": "звонок",
        "встреча": "встреча",
        "митинг": "встреча",
        "meeting": "встреча",
        "конференция": "встреча",
        "гемба": "встреча",
        "gemba": "встреча",
        "кетчап": "встреча",
        "кетчуп": "встреча",
        "catch-up": "встреча",
        "catchup": "встреча",
        "catch_up": "встреча",
        "мероприятие": "встреча",
    }
    return aliases.get(raw, "")


def _display_record_type(value: str | None) -> str:
    rt = "_".join(str(value or "").strip().lower().split())
    if rt == RECORD_TYPE_GENERAL:
        return RECORD_TYPE_GENERAL
    if rt in _EVENT_TYPE_ALIASES:
        return RECORD_TYPE_EVENT
    return RECORD_TYPE_NEED


def _record_type_search_values(value: str) -> list[str]:
    rt = "_".join(str(value or "").strip().lower().split())
    if not rt or rt in {"all", "заметки", "заметка", "записи", "запись"}:
        return []
    if rt in {RECORD_TYPE_NEED, *_LEGACY_NEED_TYPES}:
        return [RECORD_TYPE_NEED, *_LEGACY_NEED_TYPES]
    if rt == RECORD_TYPE_GENERAL:
        return [RECORD_TYPE_GENERAL]
    if rt in _EVENT_TYPE_ALIASES:
        return [RECORD_TYPE_EVENT, *_EVENT_TYPE_ALIASES]
    return []


_MONTHS_RU = {
    "января": 1, "январь": 1, "янв": 1,
    "февраля": 2, "февраль": 2, "фев": 2,
    "марта": 3, "март": 3, "мар": 3,
    "апреля": 4, "апрель": 4, "апр": 4,
    "мая": 5, "май": 5,
    "июня": 6, "июнь": 6, "июн": 6,
    "июля": 7, "июль": 7, "июл": 7,
    "августа": 8, "август": 8, "авг": 8,
    "сентября": 9, "сентябрь": 9, "сен": 9, "сент": 9,
    "октября": 10, "октябрь": 10, "окт": 10,
    "ноября": 11, "ноябрь": 11, "ноя": 11,
    "декабря": 12, "декабрь": 12, "дек": 12,
}


def _parse_day(value: str) -> datetime:
    raw = " ".join(str(value or "").strip().lower().replace("ё", "е").split())
    raw = re.sub(r"\s+\d{1,2}[:.]\d{2}.*$", "", raw)
    if not raw:
        raise ValueError("спроси дату")
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if raw in {"сегодня", "today", "сейчас"}:
        return today
    if raw in {"вчера", "yesterday"}:
        return today - timedelta(days=1)
    if raw in {"позавчера"}:
        return today - timedelta(days=2)
    month_hit = re.match(r"^(\d{1,2})\s+([а-я]+)\.?\s*(\d{2,4})?$", raw)
    if month_hit:
        month = _MONTHS_RU.get(month_hit.group(2))
        if month:
            year = int(month_hit.group(3) or today.year)
            if year < 100:
                year += 2000
            parsed = datetime(year, month, int(month_hit.group(1)))
            if not month_hit.group(3):
                parsed = _prefer_past_year(parsed, today)
            return parsed
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 8:
        if digits.startswith(("19", "20")):
            try:
                return datetime.strptime(digits, "%Y%m%d")
            except ValueError:
                pass
        return datetime.strptime(digits, "%d%m%Y")
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    for fmt in ("%d.%m.%y", "%d/%m/%y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    for fmt in ("%d.%m", "%d/%m"):
        try:
            parsed = datetime.strptime(raw, fmt).replace(year=today.year)
            return _prefer_past_year(parsed, today)
        except ValueError:
            continue
    raise ValueError("дату не понял")


def _prefer_past_year(parsed: datetime, today: datetime) -> datetime:
    """«28.12» и «16 декабря» без года: если дата ещё впереди — это прошлый год."""
    if parsed.date() <= today.date():
        return parsed
    year = parsed.year - 1
    try:
        return parsed.replace(year=year)
    except ValueError:
        return parsed.replace(year=year, day=28)


def _normalize_date(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return _parse_day(raw).strftime("%Y-%m-%d")


def _parse_created_at(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt, size in (("%Y-%m-%dT%H:%M:%S", 19), ("%Y-%m-%d", 10)):
        try:
            return datetime.strptime(raw[:size], fmt)
        except ValueError:
            continue
    return None


def _hhmm(value: str) -> str:
    raw = str(value or "").strip().replace(".", ":").replace("-", ":")
    if ":" in raw:
        hour_s, minute_s = raw.split(":", 1)
    elif raw.isdigit() and len(raw) in {1, 2}:
        hour_s, minute_s = raw, "00"
    elif raw.isdigit() and len(raw) in {3, 4}:
        raw = raw.zfill(4)
        hour_s, minute_s = raw[:2], raw[2:]
    else:
        raise ValueError("время укажи как ЧЧ:ММ")
    if not (hour_s.isdigit() and minute_s.isdigit()):
        raise ValueError("время укажи как ЧЧ:ММ")
    hour, minute = int(hour_s), int(minute_s)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("время укажи как ЧЧ:ММ")
    return f"{hour:02d}:{minute:02d}"


def _normalize_meeting_phrase(duration: str) -> tuple[str, str, str]:
    """полдня = 4 часа; первая половина 10:00–14:00, вторая с 14:00."""
    raw = " ".join(str(duration or "").split())
    t = raw.lower().replace("-", " ")
    compact = t.replace(" ", "")
    if not t:
        return "", "", ""
    if "перв" in t and "половин" in t:
        return "10:00", "14:00", "4 часа"
    if "втор" in t and "половин" in t:
        return "14:00", "18:00", "4 часа"
    if compact in {"полдня", "половинудня"} or "полдня" in compact:
        return "", "", "4 часа"
    return "", "", raw


def _duration_from_span(start: str, end: str) -> str:
    sh, sm = (int(x) for x in start.split(":"))
    eh, em = (int(x) for x in end.split(":"))
    delta = (eh * 60 + em) - (sh * 60 + sm)
    if delta <= 0:
        delta += 24 * 60
    hours, minutes = divmod(delta, 60)
    if hours and minutes:
        return f"{hours} ч {minutes} мин"
    if hours:
        return f"{hours} ч"
    return f"{minutes} мин"


def _need_contact_kind(value: str | None) -> str:
    raw = "_".join(str(value or "").strip().lower().split())
    kind = {
        "звонок": "звонок",
        "call": "звонок",
        "созвон": "звонок",
        "встреча": "встреча",
        "митинг": "встреча",
        "meeting": "встреча",
    }.get(raw, "")
    if kind not in {"звонок", "встреча"}:
        raise ValueError("спроси: встреча или звонок")
    return kind


def _meeting_slot(
    record_type: str,
    start: str,
    end: str,
    duration: str,
) -> tuple[str, str, str]:
    """Потребность: начало, конец и длительность. Мероприятие: начало и конец или только длительность."""
    if record_type not in {RECORD_TYPE_NEED, RECORD_TYPE_EVENT}:
        return "", "", ""
    start_raw = str(start or "").strip()
    end_raw = str(end or "").strip()
    imp_start, imp_end, duration_n = _normalize_meeting_phrase(duration)
    start_n = _hhmm(start_raw) if start_raw else imp_start
    end_n = _hhmm(end_raw) if end_raw else imp_end
    if record_type == RECORD_TYPE_NEED:
        if not (start_n and end_n):
            raise ValueError("спроси время начала и время конца")
        return start_n, end_n, duration_n or _duration_from_span(start_n, end_n)
    if duration_n or (start_n and end_n):
        return start_n, end_n, duration_n
    raise ValueError("спроси начало и конец или только длительность")


def _check_add_date(date_str: str) -> None:
    """Пустая дата (общая информация) допустима: запись создаётся сейчас."""
    raw = str(date_str or "").strip()
    if not raw:
        return
    day = _parse_day(raw).date()
    today = datetime.now().date()
    if day > today:
        raise ValueError("нельзя записать заметку будущей датой")


def _check_edit_window(rec: dict) -> None:
    """24 часа с момента внесения (created_at), не с даты встречи."""
    created = _parse_created_at(rec.get("created_at"))
    age = None if created is None else (datetime.now() - created).total_seconds()
    if age is None or age > EDIT_WINDOW_HOURS * 3600:
        raise HTTPException(
            status_code=403,
            detail="править и удалять можно только в течение 24 часов с момента внесения",
        )


def _record_id(client_canonical: str, date: str, raw_text: str, actor: str) -> str:
    """Один и тот же текст двух встреч — две записи. Автор и случайный суффикс входят в id."""
    nonce = uuid.uuid4().hex
    h = hashlib.sha256(
        f"{client_canonical}|{date}|{raw_text}|{actor}|{nonce}".encode()
    ).hexdigest()[:16]
    return f"intake_{h}"


def _clean_participants(names: list[str] | None) -> list[str]:
    out: list[str] = []
    for name in names or []:
        text = " ".join(str(name or "").split())
        if text and text not in out:
            out.append(text)
    return out


def _copy_note_to_participants(
    participants: list[str],
    actor: str,
    save_kwargs: dict,
) -> list[str]:
    """Фамилия из нашей базы — вторая такая же встреча на этого человека. Имя не сверяем."""
    copied: list[str] = []
    seen = {normalize_username(actor)}
    for name in participants:
        for login in notes_logins_for_participant(name):
            if login in seen:
                continue
            seen.add(login)
            their_team = notes_team_for_user(login)
            save_note(
                **save_kwargs,
                created_by=login,
                team=their_team,
                fi_desk=_desk_to_store(login, their_team, None),
                participants=participants,
            )
            copied.append(NOTES_USER_FIO.get(login, login))
            logger.info(
                "Note copied to %s (%s) from %s",
                login, their_team, actor,
            )
    return copied


def save_note(
    raw_text: str,
    client_canonical: str,
    date: str,
    products: list[str],
    currencies: list[str],
    summary: str,
    created_by: str | None = None,
    directions: list[str] | None = None,
    record_type: str = RECORD_TYPE_NEED,
    next_step: str = "",
    team: str = "fi",
    meeting_start: str = "",
    meeting_end: str = "",
    meeting_duration: str = "",
    fi_desk: str = "",
    event_kind: str = "",
    participants: list[str] | None = None,
) -> str:
    cfg = resolve_team(team)
    rt_early = _normalize_record_type(record_type)
    if rt_early in {RECORD_TYPE_NEED, RECORD_TYPE_EVENT} and not str(date or "").strip():
        raise ValueError("спроси дату")
    date = _normalize_date(date) if str(date or "").strip() else ""
    _check_add_date(date)
    start_n, end_n, duration_n = _meeting_slot(
        rt_early, meeting_start, meeting_end, meeting_duration,
    )
    if rt_early == RECORD_TYPE_EVENT:
        kind = _normalize_event_kind(event_kind)
        if kind not in EVENT_KINDS:
            raise ValueError("спроси: встреча или звонок")
    elif rt_early == RECORD_TYPE_NEED:
        kind = _need_contact_kind(event_kind)
    else:
        kind = ""
    name = str(client_canonical or "").strip()
    if rt_early == RECORD_TYPE_EVENT and not name:
        client_canonical = INTERNAL_EVENT_CLIENT
    else:
        known = _exact_canonical(_normalize_client_name(name), _alias_index(cfg.id))
        if known:
            client_canonical = known
        # иначе как написал агент — нового не подменяем на «похожее»
    rt = rt_early
    actor = _clean_actor(created_by)
    people = _clean_participants(participants)
    products = _canonicalize_stored_products(_canonical_products(products, cfg.id), raw_text)
    stored_direction: list[str] = []
    for item in directions or []:
        token = _direction_token(item)
        if token and token not in stored_direction:
            stored_direction.append(token)
    if not stored_direction:
        stored_direction = _infer_direction(raw_text)
    record_id = _record_id(client_canonical, date, raw_text, actor)

    rec = {
        "record_id": record_id,
        "team": cfg.id,
        "record_type": rt,
        "client_group_id": f"{client_canonical}_intake",
        "source_row_id": f"intake_{record_id}",
        "interaction_seq": 0,
        "interaction_date": date,
        "meeting_start": start_n,
        "meeting_end": end_n,
        "meeting_duration": duration_n,
        "client_name_raw": client_canonical,
        "client_name_norm": client_canonical,
        "source_sheet": "intake_chat",
        "direction_column": None,
        "splitter_raw_block": raw_text,
        "raw_text": raw_text,
        "short_summary": "",
        "currencies": currencies or [],
        "products": products or [],
        "economic_direction": stored_direction,
        "status": (
            "discussed" if rt == RECORD_TYPE_NEED
            else "event" if rt == RECORD_TYPE_EVENT
            else "knowledge"
        ),
        "event_kind": kind,
        "next_step": (next_step or "").strip(),
        "blockers": [],
        "team_name": None,
        "quotes": [],
        "facts": [],
        "extractor_version": "fi_sales_svc_v1",
        "extractor_raw_json": "{}",
        "extraction_confidence": 0.0,
        "enriched": False,
        "participants": people,
        "created_by": actor,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "fi_desk": fi_desk if cfg.id == "fi" else "",
    }

    _index_record(record_id, rec, cfg.index)
    mirror_upsert(rec, str(_data_file(cfg.jsonl_file)))
    _append_to_excel(rec, _data_file(cfg.excel_file))
    return record_id


_EXPORT_HEADERS = [
    "record_id", "record_type", "interaction_date",
    "meeting_start", "meeting_end", "meeting_duration",
    "client_name_norm", "products", "currencies",
    "short_summary", "raw_text", "next_step", "created_by", "team",
]


def export_team_excel(team: str, fi_desk: str | None = None) -> tuple[Path, int]:
    """Свежий Excel только по индексу одной команды."""
    cfg = resolve_team(team)
    records, total = _search_all(
        {
            "query": {"match_all": {}},
            "sort": [{
                "interaction_date": {
                    "order": "desc",
                    "missing": "_last",
                    "unmapped_type": "date",
                },
            }],
        },
        index=cfg.index,
    )
    records = _filter_fi_desk(records, fi_desk)
    total = len(records) if fi_desk and fi_desk != "master" else total
    export_name = cfg.excel_file.replace(".xlsx", "_export.xlsx")
    if cfg.id == "fi":
        desk_label = (fi_desk or "derivatives").strip().lower() or "derivatives"
        export_name = cfg.excel_file.replace(".xlsx", f"_{desk_label}_export.xlsx")
    path = _data_file(export_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = cfg.id
    ws.append(_EXPORT_HEADERS)
    for rec in records:
        products = rec.get("products") or []
        currencies = rec.get("currencies") or []
        ws.append([
            rec.get("record_id", ""),
            rec.get("record_type", ""),
            rec.get("interaction_date", ""),
            rec.get("meeting_start", ""),
            rec.get("meeting_end", ""),
            rec.get("meeting_duration", ""),
            rec.get("client_name_norm") or rec.get("client_name_raw", ""),
            ", ".join(str(x) for x in products),
            ", ".join(str(x) for x in currencies),
            rec.get("short_summary") or "",
            rec.get("raw_text") or "",
            rec.get("next_step") or "",
            rec.get("created_by") or "",
            cfg.id,
        ])
    tmp = path.with_suffix(".part")
    wb.save(str(tmp))
    tmp.replace(path)
    return path, total


def _append_to_excel(rec: dict, excel_path: Path | None = None) -> None:
    excel_path = excel_path or Path(FI_CALLS_EXCEL)
    excel_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if excel_path.is_file():
            wb = openpyxl.load_workbook(str(excel_path))
            ws = wb.active
        else:
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.append(["record_id", "record_type", "info_source", "interaction_date",
                        "meeting_start", "meeting_end", "meeting_duration",
                        "client_name_norm", "products", "currencies", "short_summary",
                        "raw_text", "counterparty_type", "source_sheet", "created_by"])

        ws.append([
            rec["record_id"],
            rec.get("record_type", RECORD_TYPE_NEED),
            "",
            rec["interaction_date"],
            rec.get("meeting_start", ""),
            rec.get("meeting_end", ""),
            rec.get("meeting_duration", ""),
            rec["client_name_norm"],
            ",".join(rec["products"]),
            ",".join(rec["currencies"]),
            rec["short_summary"][:500],
            rec["raw_text"][:1000],
            "",
            rec["source_sheet"],
            rec.get("created_by", ""),
        ])
        wb.save(str(excel_path))
        logger.info("Appended note to %s", excel_path)
    except Exception as e:
        logger.warning("Excel append failed (non-critical): %s", e)


def _delete_excel_row(record_id: str, excel_path: Path) -> None:
    try:
        if not excel_path.is_file():
            return
        wb = openpyxl.load_workbook(str(excel_path))
        ws = wb.active
        for row in range(ws.max_row, 1, -1):
            if str(ws.cell(row, 1).value or "") == record_id:
                ws.delete_rows(row, 1)
        wb.save(str(excel_path))
    except Exception as e:
        logger.warning("Excel delete failed (non-critical): %s", e)


def _update_excel_row(rec: dict, excel_path: Path) -> None:
    fields = {
        "record_type": rec.get("record_type", ""),
        "interaction_date": rec.get("interaction_date", ""),
        "meeting_start": rec.get("meeting_start", ""),
        "meeting_end": rec.get("meeting_end", ""),
        "meeting_duration": rec.get("meeting_duration", ""),
        "client_name_norm": rec.get("client_name_norm", ""),
        "client_name_raw": rec.get("client_name_raw", ""),
        "products": ",".join(rec.get("products") or []),
        "currencies": ",".join(rec.get("currencies") or []),
        "short_summary": (rec.get("short_summary") or "")[:500],
        "raw_text": (rec.get("raw_text") or "")[:1000],
        "next_step": rec.get("next_step") or "",
    }
    try:
        if not excel_path.is_file():
            return
        wb = openpyxl.load_workbook(str(excel_path))
        ws = wb.active
        headers = [str(ws.cell(1, col).value or "") for col in range(1, ws.max_column + 1)]
        target = None
        for row in range(2, ws.max_row + 1):
            if str(ws.cell(row, 1).value or "") == rec.get("record_id"):
                target = row
                break
        if target is None:
            return
        for col, name in enumerate(headers, 1):
            if name in fields:
                ws.cell(target, col).value = fields[name]
        wb.save(str(excel_path))
    except Exception as e:
        logger.warning("Excel update failed (non-critical): %s", e)


def _reject_if_not_owner(rec: dict, actor: str) -> None:
    if normalize_username(rec.get("created_by")) != normalize_username(actor):
        raise HTTPException(status_code=403, detail="можно править и удалять только свои заметки")


def update_note(record_id: str, team: str, patches: dict, actor: str) -> dict:
    cfg = resolve_team(team)
    rec = get_record(record_id, cfg.id)
    if rec is None:
        raise KeyError(record_id)
    _reject_if_other_fi_desk(rec, actor, cfg.id)
    _reject_if_not_owner(rec, actor)
    _check_edit_window(rec)
    patches.pop("created_by", None)
    patches.pop("created_at", None)
    patches.pop("fi_desk", None)

    if patches.get("client_canonical") is not None:
        name = str(patches["client_canonical"]).strip()
        if not name:
            raise ValueError("клиент не может быть пустым")
        resolved = resolve_client_names([name], cfg.id)
        if resolved:
            name = resolved[0]
        rec["client_name_raw"] = name
        rec["client_name_norm"] = name
    if patches.get("date") is not None:
        rec["interaction_date"] = _normalize_date(str(patches["date"]))
    if patches.get("raw_text") is not None:
        text = str(patches["raw_text"]).strip()
        if not text:
            raise ValueError("текст заметки не может быть пустым")
        rec["raw_text"] = text
        rec["splitter_raw_block"] = text
        rec["short_summary"] = ""
    patches.pop("summary", None)
    if patches.get("next_step") is not None:
        rec["next_step"] = str(patches["next_step"]).strip()
    if patches.get("products") is not None:
        rec["products"] = [str(x).strip() for x in patches["products"] if str(x).strip()]
    if patches.get("currencies") is not None:
        rec["currencies"] = [str(x).strip() for x in patches["currencies"] if str(x).strip()]
    for key in ("meeting_start", "meeting_end", "meeting_duration"):
        if patches.get(key) is not None:
            rec[key] = str(patches[key]).strip()

    if patches.get("event_kind") is not None and rec.get("record_type") == RECORD_TYPE_EVENT:
        kind = _normalize_event_kind(patches.get("event_kind"))
        if kind not in EVENT_KINDS:
            raise ValueError("спроси: встреча или звонок")
        rec["event_kind"] = kind
    if rec.get("record_type") == RECORD_TYPE_NEED:
        if not str(rec.get("interaction_date") or "").strip():
            raise ValueError("спроси дату встречи")
        if not rec.get("products"):
            raise ValueError("products required for потребность")
        if not rec.get("currencies"):
            raise ValueError("currencies required for потребность")
        start_n, end_n, duration_n = _meeting_slot(
            RECORD_TYPE_NEED,
            rec.get("meeting_start") or "",
            rec.get("meeting_end") or "",
            rec.get("meeting_duration") or "",
        )
        rec["meeting_start"] = start_n
        rec["meeting_end"] = end_n
        rec["meeting_duration"] = duration_n
    elif rec.get("record_type") == RECORD_TYPE_EVENT:
        if not str(rec.get("interaction_date") or "").strip():
            raise ValueError("спроси дату")
        start_n, end_n, duration_n = _meeting_slot(
            RECORD_TYPE_EVENT,
            rec.get("meeting_start") or "",
            rec.get("meeting_end") or "",
            rec.get("meeting_duration") or "",
        )
        rec["meeting_start"] = start_n
        rec["meeting_end"] = end_n
        rec["meeting_duration"] = duration_n
        rec["event_kind"] = _normalize_event_kind(rec.get("event_kind"))
    else:
        if str(rec.get("meeting_start") or "").strip():
            rec["meeting_start"] = _hhmm(rec["meeting_start"])
        if str(rec.get("meeting_end") or "").strip():
            rec["meeting_end"] = _hhmm(rec["meeting_end"])

    rec["updated_at"] = _now_iso()
    _index_record(record_id, rec, cfg.index)
    try:
        get_os_client().indices.refresh(index=cfg.index)
    except Exception as exc:
        logger.warning("refresh after update failed: %s", exc)
    mirror_upsert(rec, str(_data_file(cfg.jsonl_file)))
    _update_excel_row(rec, _data_file(cfg.excel_file))
    return rec


def delete_note(record_id: str, team: str, actor: str) -> None:
    cfg = resolve_team(team)
    rec = get_record(record_id, cfg.id)
    if rec is None:
        raise KeyError(record_id)
    _reject_if_other_fi_desk(rec, actor, cfg.id)
    _reject_if_not_owner(rec, actor)
    _check_edit_window(rec)
    client = get_os_client()
    try:
        client.delete(index=cfg.index, id=record_id, refresh="wait_for")
    except NotFoundError as exc:
        raise KeyError(record_id) from exc
    mirror_delete(record_id, str(_data_file(cfg.jsonl_file)))
    _delete_excel_row(record_id, _data_file(cfg.excel_file))


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("FI Sales Service starting, index=%s", FI_INDEX_NAME)
    try:
        for cfg in _teams().values():
            if cfg.id != "fi":
                ensure_notes_index(cfg.index)
            count = get_os_client().count(index=cfg.index).get("count", 0)
            logger.info("Index %s: %d records", cfg.index, count)
        _backfill_directions()
    except Exception as e:
        logger.warning("Could not connect to OpenSearch: %s", e)
    yield
    logger.info("FI Sales Service stopped")


app = FastAPI(title="FI Sales Service", version="1.0.0", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def _reject_unknown_search_field(request: Request, exc: RequestValidationError):
    if request.url.path == "/records/search":
        return JSONResponse(status_code=400, content={"detail": "неизвестное поле поиска"})
    return await request_validation_exception_handler(request, exc)


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    clients: list[str] = []
    exclude_clients: list[str] = []
    products: list[str] = []
    currencies: list[str] = []
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    text: Optional[str] = None
    record_type: str = "all"
    created_by: Optional[str] = None
    direction: Optional[str] = None
    client_types: list[str] = []
    limit: int = 0
    sort: str = "date_desc"
    team: Optional[str] = None


class ClientLookupRequest(BaseModel):
    names: list[str] = []
    team: Optional[str] = None


class NoteRequest(BaseModel):
    raw_text: str
    client_canonical: str = ""
    date: Optional[str] = None
    products: list[str] = []
    currencies: list[str] = []
    summary: str = ""
    created_by: Optional[str] = None
    # потребность | общая_информация | мероприятие
    record_type: str = RECORD_TYPE_NEED
    next_step: str = ""
    team: Optional[str] = None
    meeting_start: str = ""
    meeting_end: str = ""
    meeting_duration: str = ""
    fi_desk: Optional[str] = None
    event_kind: str = ""
    economic_direction: list[str] = []
    participants: list[str] = []


class NotePatchRequest(BaseModel):
    team: Optional[str] = None
    client_canonical: Optional[str] = None
    date: Optional[str] = None
    meeting_start: Optional[str] = None
    meeting_end: Optional[str] = None
    meeting_duration: Optional[str] = None
    products: Optional[list[str]] = None
    currencies: Optional[list[str]] = None
    summary: Optional[str] = None
    next_step: Optional[str] = None
    raw_text: Optional[str] = None
    event_kind: Optional[str] = None


# class NoteUpdateRequest(BaseModel):
#     raw_text: Optional[str] = None
#     client_canonical: Optional[str] = None
#     date: Optional[str] = None
#     products: Optional[list[str]] = None
#     currencies: Optional[list[str]] = None
#     summary: Optional[str] = None
#     counterparty_type: Optional[str] = None
#     status: Optional[str] = None
#     next_step: Optional[str] = None
#     created_by: Optional[str] = None
#     record_type: Optional[str] = None
#
#     @model_validator(mode="after")
#     def _at_least_one_field(self) -> "NoteUpdateRequest":
#         if not any(
#             v is not None
#             for v in (
#                 self.raw_text,
#                 self.client_canonical,
#                 self.date,
#                 self.products,
#                 self.currencies,
#                 self.summary,
#                 self.counterparty_type,
#                 self.status,
#                 self.next_step,
#                 self.created_by,
#                 self.record_type,
#             )
#         ):
#             raise ValueError("at least one field must be provided")
#         return self


def _notes_token_from_request(request: Request) -> str | None:
    token = str(request.headers.get("x-notes-token") or "").strip()
    if token:
        return token
    token = str(request.query_params.get("notes_token") or "").strip()
    return token or None


def _gate(request: Request, requested: str | None) -> tuple[NotesTeam, str]:
    """Команда только из подписанного логина.

    Заголовок агент иногда не копирует или копирует обрезанным — тогда берём актора,
    которого чат записал в начале этого сообщения. Чужой старый файл не подходит.
    """
    username = verify_notes_token(_notes_token_from_request(request))
    if not username or username not in NOTES_ALLOWED_USERS:
        username = published_notes_username()
        if username:
            logger.info("notes token missing or invalid, using published actor %s", username)
    if not username or username not in NOTES_ALLOWED_USERS:
        raise HTTPException(status_code=403, detail="notes access denied")
    team_id = notes_team_for_user(username)
    asked = str(requested or "").strip()
    if asked:
        try:
            asked_id = resolve_team(asked).id
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        if asked_id != team_id:
            raise HTTPException(status_code=403, detail="team locked")
    return resolve_team(team_id), username


@app.get("/clients")
def list_clients(request: Request, team: str | None = None, q: str | None = None):
    cfg, _actor = _gate(request, team)
    try:
        data = get_clients(cfg.id)
        needle = _normalize_client_name(q or "")
        if len(needle) < 2:
            return {
                "clients": [],
                "total": len(data),
                "hint": "без q справочник не отдаю — ищи через POST /clients",
            }
        clients = []
        for canonical, info in sorted(data.items()):
            blob = _normalize_client_name(
                " ".join(
                    [canonical, info.get("full_name_ru") or "", info.get("type") or ""]
                    + list(info.get("aliases") or [])
                )
            )
            if needle not in blob:
                continue
            clients.append({
                "canonical": canonical,
                "type": info.get("type", ""),
                "full_name_ru": info.get("full_name_ru", ""),
                "aliases": info.get("aliases", []),
            })
            if len(clients) >= 20:
                break
        return {"clients": clients, "total": len(data), "returned": len(clients)}
    except Exception as e:
        logger.exception("list_clients failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/clients")
def lookup_clients(req: ClientLookupRequest, request: Request):
    """Имя от сейлза → canonical. Сопоставление здесь, агенту не нужен весь справочник."""
    cfg, _actor = _gate(request, req.team)
    try:
        index = _alias_index(cfg.id)
        notes = _note_client_index(cfg.id)
        matched: dict[str, str] = {}
        candidates: dict[str, list[str]] = {}
        unknown: list[str] = []
        for raw in req.names:
            name = str(raw).strip()
            key = _normalize_client_name(name)
            if not key:
                continue
            chosen, hits = _match_client_key(key, index, notes, raw=name, team=cfg.id)
            if chosen:
                matched[name] = chosen
            elif hits:
                candidates[name] = hits
            else:
                unknown.append(name)
        return {"matched": matched, "candidates": candidates, "unknown": unknown}
    except Exception as e:
        logger.exception("lookup_clients failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/products")
def list_products(request: Request, team: str | None = None):
    cfg, _actor = _gate(request, team)
    try:
        data = get_products(cfg.id)
        products = []
        for canonical, info in sorted(data.items()):
            products.append({
                "canonical": canonical,
                "description": info.get("description", ""),
                "aliases": info.get("aliases", []),
            })
        return {"products": products, "total": len(products)}
    except Exception as e:
        logger.exception("list_products failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/records/search")
def search(req: SearchRequest, request: Request):
    cfg, actor = _gate(request, req.team)
    desk = notes_fi_desk_for_user(actor) if cfg.id == "fi" else None
    try:
        records, total = search_records(
            clients=req.clients,
            products=req.products,
            currencies=req.currencies,
            date_from=req.date_from,
            date_to=req.date_to,
            text=req.text,
            record_type=req.record_type,
            # Список всегда целиком: «последних N» не бывает, limit агента игнорируем.
            limit=0,
            team=cfg.id,
            fi_desk=desk,
            exclude_clients=req.exclude_clients,
            created_by=req.created_by,
            direction=req.direction,
            sort=req.sort,
            client_types=req.client_types,
        )
        return {
            "total": total,
            "returned": len(records),
            "records": [_format_record(r) for r in records],
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("search failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/records/count")
def count(req: SearchRequest, request: Request):
    cfg, actor = _gate(request, req.team)
    desk = notes_fi_desk_for_user(actor) if cfg.id == "fi" else None
    try:
        data = count_records(
            clients=req.clients,
            products=req.products,
            currencies=req.currencies,
            date_from=req.date_from,
            date_to=req.date_to,
            text=req.text,
            record_type=req.record_type,
            team=cfg.id,
            exclude_clients=req.exclude_clients,
            created_by=req.created_by,
            direction=req.direction,
            client_types=req.client_types,
            fi_desk=desk,
        )
        return {
            "total": data["total"],
            "by_record_type": data["by_record_type"],
            "by_client": data["by_client"],
        }
    except Exception as e:
        logger.exception("count failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/records")
def add_note(req: NoteRequest, request: Request):
    if not req.raw_text.strip():
        raise HTTPException(status_code=400, detail="raw_text is required")

    try:
        record_type = _normalize_record_type(req.record_type)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    if record_type != RECORD_TYPE_EVENT and not req.client_canonical.strip():
        raise HTTPException(status_code=400, detail="client_canonical is required")

    if record_type == RECORD_TYPE_NEED and not [x for x in req.products if str(x).strip()]:
        raise HTTPException(status_code=400, detail="products required for потребность")
    if record_type == RECORD_TYPE_NEED and not [x for x in req.currencies if str(x).strip()]:
        raise HTTPException(status_code=400, detail="currencies required for потребность")
    if record_type == RECORD_TYPE_NEED and not _clean_participants(req.participants):
        raise HTTPException(status_code=400, detail="participants required for потребность")

    # Дату встречи не подставляем: для потребности её должен назвать сейлз.
    date = (req.date or "").strip()

    cfg, actor = _gate(request, req.team)
    people = _clean_participants(req.participants)
    save_kwargs = dict(
        raw_text=req.raw_text,
        client_canonical=req.client_canonical,
        date=date,
        products=req.products,
        currencies=req.currencies,
        summary=req.summary,
        record_type=record_type,
        next_step=req.next_step,
        meeting_start=req.meeting_start,
        meeting_end=req.meeting_end,
        meeting_duration=req.meeting_duration,
        event_kind=req.event_kind,
        directions=req.economic_direction,
    )
    try:
        record_id = save_note(
            **save_kwargs,
            created_by=actor,
            team=cfg.id,
            fi_desk=_desk_to_store(actor, cfg.id, req.fi_desk),
            participants=people,
        )
        copied_to = _copy_note_to_participants(people, actor, save_kwargs)
        logger.info(
            "Note saved: client=%s type=%s date=%s id=%s copies=%s",
            req.client_canonical, record_type, date, record_id, copied_to,
        )
        return JSONResponse({
            "status": "ok",
            "record_id": record_id,
            "record_type": record_type,
            "participants": people,
            "copied_to": copied_to,
        })
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("save_note failed for client=%r", req.client_canonical)
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/records/export")
def export_records(request: Request, team: str | None = None):
    cfg, actor = _gate(request, team)
    desk = notes_fi_desk_for_user(actor) if cfg.id == "fi" else None
    try:
        path, total = export_team_excel(cfg.id, desk)
        return {"status": "ok", "team": cfg.id, "total": total, "path": str(path)}
    except Exception as e:
        logger.exception("export failed for team=%s", cfg.id)
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/records/{record_id}")
def get_record_api(record_id: str, request: Request, team: str | None = None):
    cfg, actor = _gate(request, team)
    try:
        rec = get_record(record_id, cfg.id)
        if rec is None:
            raise HTTPException(status_code=404, detail="record not found")
        _reject_if_other_fi_desk(rec, actor, cfg.id)
        return {"status": "ok", "record": rec}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_record failed for id=%r", record_id)
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.patch("/records/{record_id}")
def patch_note(record_id: str, req: NotePatchRequest, request: Request):
    cfg, actor = _gate(request, req.team)
    try:
        rec = update_note(
            record_id,
            cfg.id,
            req.model_dump(exclude_none=True, exclude={"team"}),
            actor,
        )
        logger.info("Note updated: id=%s team=%s", record_id, cfg.id)
        return JSONResponse({"status": "ok", "record_id": record_id, "record": _format_record(rec)})
    except HTTPException:
        raise
    except KeyError:
        raise HTTPException(status_code=404, detail="record not found") from None
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("update_note failed for id=%r", record_id)
        raise HTTPException(status_code=500, detail=str(e)) from e


# @app.patch("/records/{record_id}")
# def patch_note(record_id: str, req: NoteUpdateRequest):
#     try:
#         rec = update_note(
#             record_id,
#             req.model_dump(exclude_none=True),
#         )
#         logger.info("Note updated: id=%s client=%s", record_id, rec.get("client_name_norm"))
#         return JSONResponse({"status": "ok", "record_id": record_id, "record": rec})
#     except KeyError:
#         raise HTTPException(status_code=404, detail="record not found") from None
#     except ValueError as e:
#         raise HTTPException(status_code=400, detail=str(e)) from e
#     except Exception as e:
#         logger.exception("update_note failed for id=%r", record_id)
#         raise HTTPException(status_code=500, detail=str(e)) from e


@app.delete("/records/{record_id}")
def remove_note(record_id: str, request: Request, team: str | None = None):
    cfg, actor = _gate(request, team)
    try:
        delete_note(record_id, cfg.id, actor)
        logger.info("Note deleted: id=%s team=%s", record_id, cfg.id)
        return JSONResponse({"status": "ok", "record_id": record_id, "deleted": True})
    except HTTPException:
        raise
    except KeyError:
        raise HTTPException(status_code=404, detail="record not found") from None
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("delete_note failed for id=%r", record_id)
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fi_sales_service:app", host="127.0.0.1", port=18003, reload=False)
