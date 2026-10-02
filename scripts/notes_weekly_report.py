"""Сводка заметок FI / корпов / Comdty.

JSON для письма (сводная таблица) и дашборда на :3333 (разбивка по сейлзам).
Считает заметки по дате создания, если её нет — по дате встречи.

Запуск:
    python scripts/notes_weekly_report.py
    python dashboard/notes_weekly_app.py
"""

import json
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from opensearchpy import helpers

from dump_fi_index_jsonl import get_client

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = _REPO.parent
if str(_PKG_PARENT) not in sys.path:
    sys.path.insert(0, str(_PKG_PARENT))
from synaptica.backend.streamlit_functions.calls_access import (
    NOTES_USER_FIO as USER_FIO,
    notes_logins_for_participant,
)


TEAMS = (
    ("fi", "FI", "fi_interaction_records"),
    ("corp", "Корпы", "corp_notes_records"),
    ("commodity", "Comdty", "commodity_notes_records"),
)
SUMMARY_LABELS = {
    "fi": "FI Sales",
    "corp": "Corporate sales",
    "commodity": "Commodities",
}
WINDOW_DAYS = 7


def last_week_wed_tue(today=None):
    """Последняя закрытая неделя: среда → вторник."""
    if isinstance(today, datetime):
        today = today.date()
    today = today or date.today()
    end = today - timedelta(days=(today.weekday() - 1) % 7)
    start = end - timedelta(days=6)
    return start, end


def person_name(created_by: str) -> str:
    raw = str(created_by or "").strip()
    if not raw:
        return "не указан"
    return USER_FIO.get(raw.lower(), raw)


def has_person_name(login: str) -> bool:
    """Нет ФИО — служебный логин, в сводку и статистику не входит."""
    return str(login or "").strip().lower() in USER_FIO


def _parse_when(src: dict):
    raw = str(src.get("created_at") or src.get("interaction_date") or "").strip()
    if not raw:
        return None
    text = raw[:19] if "T" in raw else raw[:10]
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _clock_minutes(value: str):
    raw = str(value or "").strip().replace(".", ":")
    if ":" not in raw:
        return None
    hour_s, minute_s = raw.split(":", 1)
    if not (hour_s.isdigit() and minute_s.isdigit()):
        return None
    hour, minute = int(hour_s), int(minute_s)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour * 60 + minute


def meeting_minutes(src: dict):
    """Длительность встречи в минутах. Нет времени — None."""
    start = _clock_minutes(src.get("meeting_start"))
    end = _clock_minutes(src.get("meeting_end"))
    if start is not None and end is not None and start != end:
        delta = end - start
        if delta < 0:
            delta += 24 * 60
        if 0 < delta <= 12 * 60:
            return delta

    text = str(src.get("meeting_duration") or "").strip().lower().replace(",", ".")
    if not text:
        return None
    if ":" in text and not any(ch.isalpha() for ch in text):
        hour_s, minute_s = text.split(":", 1)
        if hour_s.isdigit() and minute_s.isdigit():
            total = int(hour_s) * 60 + int(minute_s)
            return total if total > 0 else None
    hours = re.search(r"(\d+(?:\.\d+)?)\s*(?:час|ч\b)", text)
    minutes = re.search(r"(\d+(?:\.\d+)?)\s*(?:мин|м\b)", text)
    if hours or minutes:
        total = 0.0
        if hours:
            total += float(hours.group(1)) * 60
        if minutes:
            total += float(minutes.group(1))
        total = int(round(total))
        return total if total > 0 else None
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        number = float(text)
        if number <= 0:
            return None
        if number <= 8:
            return int(round(number * 60))
        return int(round(number))
    return None


def _avg(values):
    nums = [v for v in values if v]
    if not nums:
        return None
    return round(sum(nums) / len(nums), 1)


def fmt_minutes(value) -> str:
    if value is None:
        return "—"
    if value in (0, 0.0):
        return "0 мин"
    total = int(round(float(value)))
    hours, minutes = divmod(total, 60)
    if hours and minutes:
        return f"{hours} ч {minutes} мин"
    if hours:
        return f"{hours} ч"
    return f"{minutes} мин"


def fmt_avg(value) -> str:
    if value in (None, 0, 0.0):
        return "0"
    number = float(value)
    if abs(number - round(number)) < 1e-9:
        return str(int(round(number)))
    return f"{number:.2f}".replace(".", ",")


def _record_kind(src: dict) -> str:
    rt = str(src.get("record_type") or "").strip().lower()
    if rt == "мероприятие":
        return "adjacent"
    if rt == "потребность":
        return "client"
    return "other"


def _team_staff(team_id: str) -> frozenset:
    """Кого считаем в среднем и отклонениях: только логины с ФИО."""
    members = frozenset()
    for importer in (
        "synaptica.backend.streamlit_functions.calls_access",
        "backend.streamlit_functions.calls_access",
    ):
        try:
            if importer.startswith("backend") and str(Path(__file__).resolve().parents[1]) not in sys.path:
                sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
            module = __import__(importer, fromlist=["notes_members_by_team"])
            members = module.notes_members_by_team().get(team_id, frozenset())
            break
        except Exception:
            continue
    return frozenset(login for login in members if has_person_name(login))


def _login(src: dict) -> str:
    return str(src.get("created_by") or "").strip().lower()


def _summary_row(label: str, client: int, adjacent: int, staff_n: int, external_minutes: int, deviations: int, team_id: str = "") -> dict:
    return {
        "label": label,
        "team_id": team_id,
        "client": client,
        "adjacent": adjacent,
        "avg_client": round(client / staff_n, 2) if staff_n else 0,
        "avg_adjacent": round(adjacent / staff_n, 2) if staff_n else 0,
        "external_minutes": external_minutes,
        "deviations": deviations,
        "staff": staff_n,
    }


def _bank_people(src: dict) -> str:
    raw = src.get("participants") or []
    if isinstance(raw, str):
        raw = [part.strip() for part in raw.split(",") if part.strip()]
    names = []
    seen = set()
    for item in raw:
        for login in notes_logins_for_participant(str(item)):
            if login in seen:
                continue
            seen.add(login)
            names.append(USER_FIO.get(login, str(item).strip()))
    return ", ".join(names)


def _note_item(src: dict, minutes) -> dict:
    kind = _record_kind(src)
    products = src.get("products")
    if isinstance(products, list):
        products = ", ".join(str(x) for x in products if x)
    when = str(src.get("interaction_date") or src.get("date") or src.get("created_at") or "")
    contact = str(src.get("event_kind") or "").strip().lower()
    if contact not in {"звонок", "встреча"}:
        contact = ""
    return {
        "date": when[:10],
        "start": str(src.get("meeting_start") or "").strip()[:5],
        "kind": {"client": "клиентская", "adjacent": "смежники"}.get(kind, "прочее"),
        "bucket": kind,
        "contact": contact,
        "minutes": int(minutes or 0),
        # В индексе клиент лежит в client_name_norm / client_name_raw; client — только в ответе API.
        "client": str(
            src.get("client_name_norm") or src.get("client_name_raw")
            or src.get("client") or src.get("client_canonical") or "—"
        ).strip() or "—",
        "bank": _bank_people(src),
        "products": str(products or "—"),
        "duration": fmt_minutes(minutes),
        "text": str(src.get("raw_text") or src.get("summary") or "").strip(),
    }


def _empty_person(name: str, login: str, team: str, team_id: str) -> dict:
    return {
        "name": name,
        "login": login,
        "team": team,
        "team_id": team_id,
        "notes": 0,
        "durations": [],
        "client": 0,
        "adjacent": 0,
        "external_minutes": 0,
        "items": [],
    }


def _iter_index(client, index: str):
    if not client.indices.exists(index=index):
        return
    for hit in helpers.scan(
        client,
        index=index,
        query={"query": {"match_all": {}}},
        size=500,
    ):
        src = hit.get("_source") or {}
        if isinstance(src, dict):
            yield src


def build_report(now=None, date_from=None, date_to=None) -> dict:
    now = now or datetime.now()
    if date_from is None or date_to is None:
        start_d, end_d = last_week_wed_tue(now)
        start = datetime.combine(start_d, datetime.min.time())
        end = datetime.combine(end_d, datetime.max.time()).replace(microsecond=0)
    else:
        end = date_to if isinstance(date_to, datetime) else datetime.combine(date_to, datetime.max.time()).replace(microsecond=0)
        start = date_from if isinstance(date_from, datetime) else datetime.combine(date_from, datetime.min.time())
    client = get_client()
    teams = []
    people = {}
    summary = []
    tot_client = tot_adjacent = tot_ext = tot_dev = tot_staff = 0
    for team_id, label, index in TEAMS:
        notes = []
        try:
            for src in _iter_index(client, index):
                when = _parse_when(src)
                if when is None or when < start or when > end:
                    continue
                notes.append(src)
        except Exception as exc:
            print(f"index {index}: {exc}", file=sys.stderr)
            notes = []
        notes = [src for src in notes if has_person_name(_login(src))]
        durations = [meeting_minutes(src) for src in notes]
        teams.append({
            "id": team_id,
            "label": label,
            "notes": len(notes),
            "meetings": sum(1 for v in durations if v),
            "avg_minutes": _avg(durations),
        })
        authors = set()
        client_n = adjacent_n = ext_min = 0
        met = set()
        for src, minutes in zip(notes, durations):
            login = _login(src)
            name = person_name(login or src.get("created_by"))
            authors.add(login)
            key = (login or name, team_id)
            bucket = people.setdefault(key, _empty_person(name, login, label, team_id))
            bucket["notes"] += 1
            if minutes:
                bucket["durations"].append(minutes)
            bucket["items"].append(_note_item(src, minutes))
            kind = _record_kind(src)
            if kind == "client":
                client_n += 1
                bucket["client"] += 1
                met.add(login)
                if minutes:
                    ext_min += minutes
                    bucket["external_minutes"] += minutes
            elif kind == "adjacent":
                adjacent_n += 1
                bucket["adjacent"] += 1
                met.add(login)
        staff = _team_staff(team_id) or frozenset(x for x in authors if x)
        for login in staff:
            people.setdefault((login, team_id), _empty_person(person_name(login), login, label, team_id))
        staff_n = len(staff)
        deviations = len(staff - met) if staff else 0
        summary.append(_summary_row(
            SUMMARY_LABELS.get(team_id, label),
            client_n, adjacent_n, staff_n, ext_min, deviations, team_id,
        ))
        tot_client += client_n
        tot_adjacent += adjacent_n
        tot_ext += ext_min
        tot_dev += deviations
        tot_staff += staff_n

    people_rows = [
        {
            "name": row["name"],
            "login": row["login"],
            "team": row["team"],
            "team_id": row["team_id"],
            "notes": row["notes"],
            "client": row["client"],
            "adjacent": row["adjacent"],
            "external_minutes": row["external_minutes"],
            "meetings": len(row["durations"]),
            "avg_minutes": _avg(row["durations"]),
            "items": row["items"],
        }
        for row in people.values()
    ]
    people_rows.sort(key=lambda row: (-row["notes"], row["team"], row["name"]))
    summary.append(_summary_row("Итого", tot_client, tot_adjacent, tot_staff, tot_ext, tot_dev))
    return {
        "from": start.strftime("%Y-%m-%d"),
        "to": end.strftime("%Y-%m-%d"),
        "teams": teams,
        "people": people_rows,
        "summary": summary,
    }


def main() -> None:
    report = build_report()
    print("NOTES_REPORT_JSON " + json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
