"""Выгрузка всей базы заметок (FI, корпы, Comdty) в текстовый файл со случайным именем.

Одна строка — один документ: ``{"_index": ..., "_id": ..., ...поля _source}``.
Сверяет число строк с count() каждого индекса.

Запуск на сервере:
    python scripts/dump_notes_txt.py                 # ./notes_dump_<случайное>.txt
    python scripts/dump_notes_txt.py /data/s3/tmp    # в эту папку

Локально (без OpenSearch, из .local_s3/notes):
    NOTES_LOCAL_STORE=1 python scripts/dump_notes_txt.py
"""

import json
import os
import sys
import uuid
from pathlib import Path

INDICES = ("fi_notes_records", "corp_notes_records", "commodity_notes_records")
ROOT = Path(__file__).resolve().parents[1]


def _local_docs(index: str):
    path = Path(os.environ.get("NOTES_LOCAL_DIR", str(ROOT / ".local_s3" / "notes"))) / f"{index}.json"
    if not path.exists():
        return [], 0
    data = json.loads(path.read_text(encoding="utf-8") or "{}")
    items = data.items() if isinstance(data, dict) else enumerate(data)
    docs = [(str(doc.get("record_id") or key), doc) for key, doc in items]
    return docs, len(docs)


def _opensearch_docs(client, index: str):
    from opensearchpy import helpers

    if not client.indices.exists(index=index):
        return [], 0
    expected = int(client.count(index=index).get("count", 0))
    hits = helpers.scan(client, index=index, query={"query": {"match_all": {}}}, size=500)
    return ((h.get("_id"), h.get("_source") or {}) for h in hits), expected


def dump(out_dir: str = ".") -> tuple[Path, dict]:
    local = os.environ.get("NOTES_LOCAL_STORE") == "1"
    client = None
    if not local:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from dump_fi_index_jsonl import get_client

        client = get_client()
    out = Path(out_dir) / f"notes_dump_{uuid.uuid4().hex[:8]}.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    counts = {}
    with tmp.open("w", encoding="utf-8") as fh:
        for index in INDICES:
            docs, expected = _local_docs(index) if local else _opensearch_docs(client, index)
            n = 0
            for doc_id, src in docs:
                fh.write(json.dumps({"_index": index, "_id": doc_id, **src}, ensure_ascii=False) + "\n")
                n += 1
            if n != expected:
                tmp.unlink(missing_ok=True)
                raise RuntimeError(f"dump mismatch index={index} count={expected} dumped={n}")
            counts[index] = n
    tmp.replace(out)
    return out, counts


def main() -> None:
    out, counts = dump(sys.argv[1] if len(sys.argv) > 1 else ".")
    for index, n in counts.items():
        print(f"{index}: {n}")
    print(f"OK: {sum(counts.values())} docs -> {out}")


if __name__ == "__main__":
    main()
