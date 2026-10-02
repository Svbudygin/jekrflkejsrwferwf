from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import time
from pathlib import Path

import yaml


def normalize_username(username: str | None) -> str:
    return str(username or "").strip().lower()


def _access_yaml_path() -> Path:
    """Рядом с config.yaml. Если файла там нет — копия из репозитория."""
    repo = Path(__file__).resolve().parents[2]
    candidates: list[Path] = []
    env = os.environ.get("CONFIG_PATH", "").strip()
    if env:
        candidates.append(Path(env).expanduser().resolve().parent / "access.yaml")
    candidates.append(Path("/data/s3/projects/synaptica/access.yaml"))
    candidates.append(repo / ".local_s3" / "projects" / "synaptica" / "access.yaml")
    candidates.append(repo / "config" / "access.yaml")
    for path in candidates:
        if path.is_file():
            return path
    return candidates[-1]


def _load_access() -> dict:
    path = _access_yaml_path()
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"access.yaml должен быть словарём: {path}")
    return data


def _as_set(value) -> frozenset[str]:
    if not value:
        return frozenset()
    return frozenset(str(item).strip() for item in value if str(item).strip())


_ACCESS = _load_access()
_NOTES = _ACCESS.get("notes") or {}
_NOTES_FI = _NOTES.get("fi") or {}
_OPENCLAW = _ACCESS.get("openclaw") or {}


def _openclaw_users(key: str) -> frozenset[str]:
    node = _OPENCLAW.get(key)
    if isinstance(node, dict):
        return _as_set(node.get("users"))
    return _as_set(node)


def _openclaw_open(key: str) -> bool:
    node = _OPENCLAW.get(key)
    return bool(node.get("open_to_all")) if isinstance(node, dict) else False


OPENCLAW_FI_SALES_ALLOWED_USERS: frozenset[str] = _openclaw_users("fi_sales")
OPENCLAW_CORP_SALES_ALLOWED_USERS: frozenset[str] = _openclaw_users("corp_sales")

# Деривативы, кредит, фандинг (в коде liquidity). Без группы и те, кого нет в письме — мастер.
_NOTES_FI_DERIVATIVES_USERS: frozenset[str] = _as_set(_NOTES_FI.get("derivatives"))
_NOTES_FI_CREDIT_USERS: frozenset[str] = _as_set(_NOTES_FI.get("credit"))
_NOTES_FI_LIQUIDITY_USERS: frozenset[str] = _as_set(_NOTES_FI.get("liquidity"))
_NOTES_FI_MASTER_USERS: frozenset[str] = _as_set(_NOTES_FI.get("master"))
_NOTES_FI_USERS: frozenset[str] = (
    _NOTES_FI_MASTER_USERS
    | _NOTES_FI_DERIVATIVES_USERS
    | _NOTES_FI_CREDIT_USERS
    | _NOTES_FI_LIQUIDITY_USERS
)
_NOTES_CORP_USERS: frozenset[str] = _as_set(_NOTES.get("corp"))
_NOTES_COMMODITY_USERS: frozenset[str] = _as_set(_NOTES.get("commodity"))
_NOTES_TEST_USERS: frozenset[str] = _as_set(_NOTES.get("test_users"))
NOTES_ALLOWED_USERS: frozenset[str] = (
    _NOTES_FI_USERS | _NOTES_CORP_USERS | _NOTES_COMMODITY_USERS | _NOTES_TEST_USERS
)

# Фамилия Имя Отчество. Имя в заметке может не совпасть — узнаём по фамилии.
NOTES_USER_FIO: dict[str, str] = {
    str(login): str(fio)
    for login, fio in (_NOTES.get("fio") or {}).items()
    if str(login).strip() and str(fio).strip()
}

_NAME_PARTICLES = frozenset({
    "с", "со", "и", "от", "у", "к", "на", "в", "во", "по", "за", "из", "до", "для", "или",
})
_SURNAME_ENDINGS = ("ою", "ею", "ой", "ей", "ым", "им", "ом", "ем", "ах", "ях", "у", "ю", "е", "а", "я", "ы", "и")


def _name_key(word: str) -> str:
    return re.sub(r"[^a-zа-я-]", "", str(word or "").strip().casefold().replace("ё", "е"))


def _surname_stem(word: str) -> str:
    w = _name_key(word)
    for end in _SURNAME_ENDINGS:
        if w.endswith(end) and len(w) - len(end) >= 4:
            return w[: -len(end)]
    return w


def _feminine_name(word: str) -> bool:
    w = _name_key(word)
    return w.endswith(("а", "я", "ой", "ою", "ей", "ею"))


def notes_logins_for_participant(name: str) -> list[str]:
    """Логины, чья фамилия совпала. Имя не проверяем. Одна фамилия на двоих — этого слова нет."""
    tokens = [
        t for t in re.split(r"[\s,;/]+", str(name or "").strip())
        if _name_key(t) and _name_key(t) not in _NAME_PARTICLES
    ]
    found: list[str] = []
    for token in tokens:
        exact = [
            login
            for login, fio in NOTES_USER_FIO.items()
            if _name_key(fio.split()[0]) == _name_key(token)
        ]
        if len(exact) == 1:
            if exact[0] not in found:
                found.append(exact[0])
            continue
        if exact:
            continue
        stem = _surname_stem(token)
        if len(stem) < 4:
            continue
        hits = [
            (login, fio.split()[0])
            for login, fio in NOTES_USER_FIO.items()
            if _surname_stem(fio.split()[0]) == stem
        ]
        if len(hits) > 1:
            feminine = _feminine_name(token)
            hits = [(login, surname) for login, surname in hits if _feminine_name(surname) == feminine]
        if len(hits) == 1 and hits[0][0] not in found:
            found.append(hits[0][0])
    return found
OPENCLAW_CALLS_ALLOWED_USERS: frozenset[str] = _openclaw_users("calls")


def is_openclaw_calls_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in OPENCLAW_CALLS_ALLOWED_USERS


# Единственный источник по звонкам: пользователь (ключ) → банк для фильтра (ALL / СЗТБ / ДВТБ / …).
# Пустая строка — доступ есть, без привязки к ТБ. Набор допущенных логинов = ключи словаря.
CALLS_USER_BANK: dict[str, str] = {
    str(login): str(bank if bank is not None else "")
    for login, bank in ((_ACCESS.get("calls") or {}).get("user_bank") or {}).items()
    if str(login).strip()
}


def is_calls_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in CALLS_USER_BANK


def resolve_calls_bank(username: str | None) -> str:
    return CALLS_USER_BANK.get(normalize_username(username), "")


# /openclaw1_coder, /openclaw1_helper, /openclaw — отдельный allowlist для каждой кнопки.
OPENCLAW_CODER_ALLOWED_USERS: frozenset[str] = _openclaw_users("coder")
OPENCLAW_HELPER_ALLOWED_USERS: frozenset[str] = _openclaw_users("helper")
OPENCLAW_CLIENT_ALLOWED_USERS: frozenset[str] = _openclaw_users("client")

OPENCLAW_CLIENT_OPEN_TO_ALL: bool = _openclaw_open("client")

# Алиасы для обратной совместимости.
OPENCLAW_DEV_ALLOWED_USERS = OPENCLAW_CODER_ALLOWED_USERS
OPENCLAW_ALLOWED_USERS = OPENCLAW_CODER_ALLOWED_USERS


def is_openclaw_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in OPENCLAW_CODER_ALLOWED_USERS


def is_openclaw_dev_user(username: str | None) -> bool:
    return normalize_username(username) in OPENCLAW_CODER_ALLOWED_USERS


def is_openclaw_coder_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in OPENCLAW_CODER_ALLOWED_USERS


def is_openclaw_helper_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in OPENCLAW_HELPER_ALLOWED_USERS


def is_openclaw_client_allowed_user(username: str | None) -> bool:
    normalized = normalize_username(username)
    if OPENCLAW_CLIENT_OPEN_TO_ALL:
        return bool(normalized)
    return normalized in OPENCLAW_CLIENT_ALLOWED_USERS


def is_openclaw_fi_sales_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in OPENCLAW_FI_SALES_ALLOWED_USERS


def is_notes_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in NOTES_ALLOWED_USERS


def notes_team_for_user(username: str | None) -> str:
    """fi / corp / commodity. Чужая команда в контекст не попадает."""
    name = normalize_username(username)
    if name in _NOTES_COMMODITY_USERS:
        return "commodity"
    if name in _NOTES_CORP_USERS:
        return "corp"
    return "fi"


def notes_roster_by_team() -> dict[str, frozenset[str]]:
    """Сотрудники команд для среднего и отклонений. Мастера и тестовый логин не считаем."""
    skip = set(_NOTES_FI_MASTER_USERS) | {"h"}
    return {
        "fi": frozenset(name for name in _NOTES_FI_USERS if name not in skip),
        "corp": _NOTES_CORP_USERS,
        "commodity": _NOTES_COMMODITY_USERS,
    }


def notes_members_by_team() -> dict[str, frozenset[str]]:
    """Все логины команды, включая мастеров."""
    return {
        "fi": _NOTES_FI_USERS,
        "corp": _NOTES_CORP_USERS,
        "commodity": _NOTES_COMMODITY_USERS,
    }


def notes_fi_desk_for_user(username: str | None) -> str | None:
    """Направление внутри FI: master / derivatives / credit / liquidity. У корпов и Comdty нет."""
    name = normalize_username(username)
    if name not in _NOTES_FI_USERS:
        return None
    if name in _NOTES_FI_MASTER_USERS or name in _NOTES_TEST_USERS:
        return "master"
    if name in _NOTES_FI_CREDIT_USERS:
        return "credit"
    if name in _NOTES_FI_LIQUIDITY_USERS:
        return "liquidity"
    return "derivatives"


_NOTES_TOKEN_TTL_SEC = 12 * 3600


_NOTES_TOKEN_SECRET = b"notes-gate-v1-7c4e9a2b6d1f0835"


def _notes_token_secret() -> bytes:
    """На сервере секрета в env нет — остаётся прежний встроенный ключ."""
    raw = os.environ.get("NOTES_TOKEN_SECRET", "").strip()
    if raw:
        return raw.encode()
    return _NOTES_TOKEN_SECRET


def mint_notes_token(username: str | None, ttl_sec: int = _NOTES_TOKEN_TTL_SEC) -> str:
    name = normalize_username(username)
    if not name:
        raise ValueError("username required")
    exp = int(time.time()) + int(ttl_sec)
    msg = f"{name}|{exp}".encode()
    sig = hmac.new(_notes_token_secret(), msg, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(msg + b"|" + sig).decode().rstrip("=")


_NOTES_ACTOR_PATH = Path("/tmp/synaptica-notes-actor.json")


def publish_notes_actor(username: str | None) -> str:
    """Токен для заголовка и локальный актор, если агент заголовок забыл."""
    token = mint_notes_token(username)
    try:
        _NOTES_ACTOR_PATH.write_text(
            json.dumps({
                "username": normalize_username(username),
                "token": token,
                "ts": time.time(),
            }),
            encoding="utf-8",
        )
    except OSError:
        pass
    return token


def published_notes_username() -> str | None:
    try:
        data = json.loads(_NOTES_ACTOR_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    try:
        age = time.time() - float(data.get("ts") or 0)
    except (TypeError, ValueError):
        return None
    if age > _NOTES_TOKEN_TTL_SEC or age < 0:
        return None
    verified = verify_notes_token(str(data.get("token") or ""))
    if verified:
        return verified
    name = normalize_username(data.get("username"))
    if name and name in NOTES_ALLOWED_USERS:
        return name
    return None


def verify_notes_token(token: str | None) -> str | None:
    raw = str(token or "").strip()
    if not raw:
        return None
    pad = "=" * (-len(raw) % 4)
    try:
        blob = base64.urlsafe_b64decode(raw + pad)
        name_b, exp_b, sig = blob.split(b"|", 2)
        name = name_b.decode()
        exp = int(exp_b.decode())
        msg = name_b + b"|" + exp_b
    except (ValueError, UnicodeDecodeError):
        return None
    try:
        expected = hmac.new(_notes_token_secret(), msg, hashlib.sha256).digest()
    except RuntimeError:
        return None
    if not hmac.compare_digest(sig, expected):
        return None
    if exp < int(time.time()):
        return None
    if not name or name != normalize_username(name):
        return None
    return name


def is_openclaw_corp_sales_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in OPENCLAW_CORP_SALES_ALLOWED_USERS


# /vnd — allowlist (нормализация логина: lower, strip).
VND_ALLOWED_USERS: frozenset[str] = _as_set(_ACCESS.get("vnd"))

OPENCLAW_VND_RAG_ALLOWED_USERS = VND_ALLOWED_USERS
OPENCLAW_VND_RAG_OPEN_TO_ALL: bool = _openclaw_open("vnd_rag")
OPENCLAW_PRES_GEN_ALLOWED_USERS: frozenset[str] = _openclaw_users("pres_gen")


def is_vnd_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in VND_ALLOWED_USERS


def is_openclaw_vnd_rag_allowed_user(username: str | None) -> bool:
    normalized = normalize_username(username)
    if OPENCLAW_VND_RAG_OPEN_TO_ALL:
        return bool(normalized)
    return normalized in OPENCLAW_VND_RAG_ALLOWED_USERS


def is_openclaw_pres_gen_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in OPENCLAW_PRES_GEN_ALLOWED_USERS


# OpenClaw-роутер — дефолтный оркестратор свободного текста (кнопки /openclaw1 нет).
OPENCLAW_ROUTER_ALLOWED_USERS: frozenset[str] = _openclaw_users("router")
OPENCLAW_ROUTER_OPEN_TO_ALL: bool = _openclaw_open("router")


def is_openclaw_router_allowed_user(username: str | None) -> bool:
    normalized = normalize_username(username)
    if OPENCLAW_ROUTER_OPEN_TO_ALL:
        return bool(normalized)
    return normalized in OPENCLAW_ROUTER_ALLOWED_USERS


PRESENTATION_GEN_ALLOWED_USERS: frozenset[str] = _as_set(_ACCESS.get("presentation_gen"))

COMMENTS_VIEW_ALLOWED_USERS: frozenset[str] = _as_set(_ACCESS.get("comments_view"))


def is_presentation_gen_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in PRESENTATION_GEN_ALLOWED_USERS


def is_logs_view_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in COMMENTS_VIEW_ALLOWED_USERS


CALL_UPLOAD_ALLOWED_USERS: frozenset[str] = _as_set(_ACCESS.get("call_upload"))

SPECIAL_NEWS_ALLOWED_USERS: frozenset[str] = _as_set(_ACCESS.get("special_news"))


def is_call_upload_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in CALL_UPLOAD_ALLOWED_USERS


def is_special_news_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in SPECIAL_NEWS_ALLOWED_USERS


OPTIMIZER_ALLOWED_USERS: frozenset[str] = _as_set(_ACCESS.get("optimizer"))


def is_optimizer_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in OPTIMIZER_ALLOWED_USERS


# /scenario — сценарный аналитик (Hedge Desk), без OpenClaw.
SCENARIO_ALLOWED_USERS: frozenset[str] = _as_set(_ACCESS.get("scenario"))


def is_scenario_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in SCENARIO_ALLOWED_USERS


# /brokerka — брокерский StructuredAnswerDraft + products RAG ДГР.
BROKER_ALLOWED_USERS: frozenset[str] = _as_set(_ACCESS.get("broker"))


def is_broker_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in BROKER_ALLOWED_USERS


# /rates_pricing — OpenClaw-агент по rates-сделкам и прайсингу.
OPENCLAW_RATES_PRICING_ALLOWED_USERS: frozenset[str] = _openclaw_users("rates_pricing")


def is_openclaw_rates_pricing_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in OPENCLAW_RATES_PRICING_ALLOWED_USERS

# /openclaw_products — OpenClaw-агент по RAG продуктов ДГР.
OPENCLAW_PRODUCTS_ALLOWED_USERS: frozenset[str] = _openclaw_users("products")


def is_openclaw_products_allowed_user(username: str | None) -> bool:
    return normalize_username(username) in OPENCLAW_PRODUCTS_ALLOWED_USERS


# Основной дашборд метрик: логин, имя или локальная часть почты из config.yaml Синаптики.
DASHBOARD_ALLOWED_USERS: frozenset[str] = _as_set(_ACCESS.get("dashboard"))


def is_dashboard_allowed_user(
    username: str | None,
    name: str | None = None,
    email: str | None = None,
) -> bool:
    candidates = {
        normalize_username(username),
        normalize_username(name),
    }
    name_norm = normalize_username(name)
    if name_norm:
        for token in name_norm.replace(".", " ").replace("_", " ").replace("-", " ").split():
            candidates.add(token)
    email_norm = normalize_username(email)
    if "@" in email_norm:
        candidates.add(email_norm.split("@", 1)[0])
    candidates.discard("")
    return bool(candidates & DASHBOARD_ALLOWED_USERS)