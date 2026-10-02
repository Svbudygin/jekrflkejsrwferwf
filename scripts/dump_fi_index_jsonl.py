"""Полный дамп индекса FI из OpenSearch в JSONL (бэкап для восстановления базы).

Источник правды — тот же OpenSearch, что у fi_sales_service (не fi_calls.xlsx).
Каждая строка JSONL — один документ: ``{"_id": <id>, ...поля _source}``.

Запуск на сервере:
    python scripts/dump_fi_index_jsonl.py /data/s3/projects/synaptica/fi_backup.jsonl
"""

import json
import os
import sys

from opensearchpy import OpenSearch, helpers


def get_client() -> OpenSearch:
    return OpenSearch(
        hosts=[{"host": "tsles-pkapg0006.esrt.sber.ru", "port": 19200}],
        http_auth=("pvs_admin_user", "Musthavedarkside777@"),
        use_ssl=True,
        verify_certs=True,
        ssl_assert_hostname=False,
        ca_certs="/tmp/opensearch-certs/opensearch/ca",
        client_cert="/tmp/opensearch-certs/opensearch/elastic.cert",
        client_key="/tmp/opensearch-certs/opensearch/elastic.key",
        timeout=60,
    )


def dump_index(out_path: str, batch: int = 500) -> int:
    """Выгружает все документы индекса. Падает, если число строк ≠ count() в OS."""
    client = get_client()
    expected = int(client.count(index="fi_notes_records").get("count", 0))
    tmp_path = out_path + ".tmp"
    total = 0
    with open(tmp_path, "w", encoding="utf-8") as f:
        for h in helpers.scan(
            client,
            index="fi_notes_records",
            query={"query": {"match_all": {}}},
            size=batch,
        ):
            rec = {"_id": h.get("_id"), **(h.get("_source") or {})}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            total += 1
    if total != expected:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise RuntimeError(
            f"dump mismatch index=fi_notes_records os_count={expected} dumped={total}"
        )
    os.replace(tmp_path, out_path)
    return total


def main() -> None:
    out = sys.argv[1] if len(sys.argv) > 1 else "/data/s3/projects/synaptica/fi_backup.jsonl"
    parent = os.path.dirname(out)
    if parent:
        os.makedirs(parent, exist_ok=True)
    n = dump_index(out)
    print(f"OK: {n} docs -> {out}")


if __name__ == "__main__":
    main()
