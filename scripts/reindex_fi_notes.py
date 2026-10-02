"""Новый индекс FI и перенос выгрузки.

Создаёт ``fi_notes_records``, кладёт туда заметки FI из JSONL.
Логины из ``notes.test_users`` не переносятся.
Старый ``fi_interaction_records`` не удаляется и не перезаписывается.
Строки ``corp_notes_records`` из той же выгрузки обновляются на месте:
у них тоже появляется ``activity_side``.

``activity_side``: ``внутренний`` (мероприятие или клиент «внутренняя»)
или ``внешний``.

Запуск на сервере, где есть сертификаты OpenSearch:

    python scripts/reindex_fi_notes.py
    python scripts/reindex_fi_notes.py --dry-run calls_data/fi_notes_reindex_source.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path

import yaml

_SCRIPTS = Path(__file__).resolve().parent
_REPO = _SCRIPTS.parent
_PKG_PARENT = _REPO.parent
if str(_PKG_PARENT) not in sys.path:
    sys.path.insert(0, str(_PKG_PARENT))
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from synaptica.services.fi_sales_service import (  # noqa: E402
    _NOTES_INDEX_BODY,
    _activity_side,
    _display_record_type,
    _normalize_event_kind,
)

NEW_FI_INDEX = "fi_notes_records"
OLD_FI_INDEX = "fi_interaction_records"
CORP_INDEX = "corp_notes_records"
_INDEX_TEAM = {
    OLD_FI_INDEX: "fi",
    NEW_FI_INDEX: "fi",
    CORP_INDEX: "corp",
    "commodity_notes_records": "commodity",
}


def load_test_users(path: Path) -> frozenset[str]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    raw = ((data.get("notes") or {}).get("test_users")) or []
    users = frozenset(str(item).strip().lower() for item in raw if str(item).strip())
    if not users:
        raise RuntimeError(f"notes.test_users пуст: {path}")
    return users


def _repair_tokens(value) -> list[str]:
    """Склеивает список, который когда-то разрезали по буквам: U,S,D -> USD."""
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if not isinstance(value, list):
        return []
    items = [str(item) for item in value]
    if len(items) >= 3 and all(len(item) <= 1 for item in items):
        items = "".join(items).split(",")
    return [item.strip() for item in items if item.strip()]


def _contact_from_text(raw_text) -> str:
    """Если тип пустой, звонок или встреча берётся из текста заметки."""
    text = str(raw_text or "").lower().replace("ё", "е")

    def _at(words: tuple[str, ...]) -> int | None:
        positions = [text.find(word) for word in words if word in text]
        return min(positions) if positions else None

    call_at = _at(("звонок", "созвон", "созванив", "позвон"))
    meet_at = _at(("встреч", "ужин", "митинг"))
    if call_at is None and meet_at is None:
        return ""
    if meet_at is None or (call_at is not None and call_at < meet_at):
        return "звонок"
    return "встреча"


def _filled_event_kind(event_kind, raw_text, doc_id: str) -> str:
    """Всегда звонок или встреча. Если в тексте нет — стабильный случайный выбор по id."""
    kind = _normalize_event_kind(event_kind) or _contact_from_text(raw_text)
    if kind in {"звонок", "встреча"}:
        return kind
    return random.Random(doc_id).choice(("звонок", "встреча"))


def _read_jsonl(path: Path) -> list[dict]:
    docs = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        docs.append(json.loads(line))
    return docs


def transform(doc: dict, skip: frozenset[str]) -> tuple[str, str, dict] | None:
    created_by = str(doc.get("created_by") or "").strip().lower()
    if created_by in skip:
        return None
    source_index = str(doc.get("_index") or OLD_FI_INDEX)
    doc_id = str(doc.get("_id") or doc.get("record_id") or "").strip()
    if not doc_id:
        raise RuntimeError("документ без _id")
    body = {
        key: value
        for key, value in doc.items()
        if key not in {"_index", "_id", "short_summary", "summary"}
    }
    team = str(body.get("team") or "").strip().lower()
    if not team:
        team = _INDEX_TEAM.get(source_index, "fi")
        body["team"] = team
    target = CORP_INDEX if team == "corp" or source_index == CORP_INDEX else NEW_FI_INDEX
    if target == OLD_FI_INDEX:
        raise RuntimeError("отказ писать в старый индекс")
    body["record_type"] = _display_record_type(body.get("record_type"))
    body["event_kind"] = _filled_event_kind(body.get("event_kind"), body.get("raw_text"), doc_id)
    body["products"] = _repair_tokens(body.get("products"))
    body["currencies"] = _repair_tokens(body.get("currencies"))
    client = body.get("client_name_norm") or body.get("client_name_raw") or ""
    body["activity_side"] = _activity_side(body.get("record_type"), client)
    body["record_id"] = str(body.get("record_id") or doc_id)
    return target, doc_id, body


def _prepare(path: Path, access_path: Path) -> tuple[list[tuple[str, str, dict]], Counter]:
    skip = load_test_users(access_path)
    skipped: Counter = Counter()
    ready: list[tuple[str, str, dict]] = []
    for doc in _read_jsonl(path):
        created_by = str(doc.get("created_by") or "").strip().lower()
        item = transform(doc, skip)
        if item is None:
            skipped[created_by or "(пусто)"] += 1
            continue
        ready.append(item)
    return ready, skipped


def _print_plan(ready: list[tuple[str, str, dict]], skipped: Counter) -> None:
    by_index = Counter(index for index, _doc_id, _body in ready)
    by_side = Counter(body.get("activity_side") for _index, _doc_id, body in ready)
    print(f"к записи: {len(ready)}")
    for index, count in sorted(by_index.items()):
        print(f"  {index}: {count}")
    for side, count in sorted(by_side.items()):
        print(f"  {side}: {count}")
    print(f"пропущено test_users: {sum(skipped.values())}")
    for login, count in skipped.most_common():
        print(f"  {login}: {count}")


def load(ready: list[tuple[str, str, dict]]) -> None:
    from opensearchpy import helpers

    from dump_fi_index_jsonl import get_client

    client = get_client()
    if client.indices.exists(index=OLD_FI_INDEX):
        print(f"старый индекс {OLD_FI_INDEX} не трогаю")
    if client.indices.exists(index=NEW_FI_INDEX):
        client.indices.delete(index=NEW_FI_INDEX)
    client.indices.create(index=NEW_FI_INDEX, body=_NOTES_INDEX_BODY)

    corp_docs = [item for item in ready if item[0] == CORP_INDEX]
    if corp_docs:
        if client.indices.exists(index=CORP_INDEX):
            client.indices.put_mapping(
                index=CORP_INDEX,
                body={"properties": {"activity_side": {"type": "keyword"}}},
            )
        else:
            client.indices.create(index=CORP_INDEX, body=_NOTES_INDEX_BODY)

    actions = [
        {"_op_type": "index", "_index": index, "_id": doc_id, "_source": body}
        for index, doc_id, body in ready
    ]
    helpers.bulk(client, actions, raise_on_error=True)
    client.indices.refresh(index=NEW_FI_INDEX)
    if corp_docs:
        client.indices.refresh(index=CORP_INDEX)
    print(f"записано: {len(actions)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Перенос заметок в fi_notes_records")
    parser.add_argument(
        "source",
        nargs="?",
        default=str(_REPO / "calls_data" / "fi_notes_reindex_source.jsonl"),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--access",
        default=str(_REPO / "config" / "access.yaml"),
    )
    args = parser.parse_args()
    ready, skipped = _prepare(Path(args.source), Path(args.access))
    _print_plan(ready, skipped)
    if args.dry_run:
        return
    load(ready)


if __name__ == "__main__":
    main()
