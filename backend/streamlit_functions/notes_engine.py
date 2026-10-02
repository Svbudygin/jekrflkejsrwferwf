"""
/notes — FI Notes без OpenClaw.

Цикл: GigaChat(Qwen) → JSON {reply, calls} → FI service → TOOL_RESULTS → снова LLM.
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Callable

import requests
import streamlit as st
from langchain_community.chat_models.gigachat import GigaChat
from langchain_core.messages import HumanMessage, SystemMessage

from synaptica.backend.streamlit_functions.calls_access import mint_notes_token
from synaptica.config.settings import synaptica_root

log = logging.getLogger("notes_engine")

DIALOG_KEY = "notes_dialog"
DRAFT_KEY = "notes_draft"
USER_CONFUSED = "Ой, что-то я запутался. Попробуй ещё раз или переформулируй."
SERVICE_BUSY = "Модель сейчас перегружена, я не дождался ответа. Повтори запрос через минуту."
NOTES_WELCOME = "📋 **FI Notes** — режим включён.\n\nДобавляю заметки и ищу историю по клиентам."
NOTES_AGENT_LOG_PATH = os.environ.get(
    "NOTES_AGENT_LOG_PATH", str(synaptica_root() / "notes_agent.jsonl")
)
_NOTES_LIST_RE = re.compile(r"<!--\s*NOTES_LIST(?:\s+ids=[0-9A-Za-z_,.\-]*)?\s*-->", re.IGNORECASE)


def _notes_list_marker(record_ids: list[str]) -> str:
    ids = [rid for rid in record_ids if rid]
    if not ids:
        return ""
    return "<!-- NOTES_LIST ids=" + ",".join(ids) + " -->"


def _attach_notes_list_marker(answer: str, user_text: str, did_search: bool, record_ids: list[str]) -> str:
    """Маркер только с id последнего поиска. На «кроме» и отказ маркер убирается."""
    text = str(answer or "").strip()
    if not text:
        return text
    low_user = f" {str(user_text or '').lower()} "
    low_answer = text.lower()
    drop = (
        "кроме" in low_user
        or " без " in low_user
        or "не могу" in low_answer
        or "верно?" in low_answer
    )
    if drop:
        return _NOTES_LIST_RE.sub("", text).strip()
    marker = _notes_list_marker(record_ids)
    if _NOTES_LIST_RE.search(text):
        if not marker:
            return _NOTES_LIST_RE.sub("", text).strip()
        return _NOTES_LIST_RE.sub(marker, text, count=1)
    if did_search and marker:
        return text.rstrip() + "\n\n" + marker
    return text

SYSTEM_PROMPT = """
===== IDENTITY =====
Ты — FI Notes. Внутренний ассистент команды FI Sales Сбербанка для дневника
взаимодействий с клиентами. Два режима: поиск и заметка. Распознавай по контексту.

===== USER / КОНТЕКСТ =====
Сейлз — сотрудник FI Sales Сбербанка. Продаёт производные внешним контрагентам
(банки, корпораты, NBFI). Всегда говорит от лица Сбера. Обращайся на «ты».
Пишет коротко: «встретились с Райфом, смотрят IRS на 3Y USD 50M»,
«юник спрашивал по XCCY», «что там с Юникредитом», «запиши встречу с Газпромом».

Интерпретация:
- «встретились с Райфом» = Сбер встретился с Raiffeisen → клиент Raiffeisen
- «клиент смотрит IRS» = внешний контрагент, не Сбер
- «наш клиент» / «мы предложили» = контрагент Сбера / Сбер предложил
- «юник спрашивал» = Юникредит спрашивал у нас
Не путай Сбер с контрагентом — в базе только встречи с внешними клиентами.

===== SOUL / СТИЛЬ =====
Точный и лаконичный. Без вступлений и «конечно, помогу!».
Жаргон: IRS, XCCY, NDF, DCD, bp, tenor, indicative, bid/offer.
«Райф смотрит 3Y USD 50M» — понимай без лишних вопросов.
Неясность → все дыры одним списком, не по одному вопросу за ход.
Уже сказанное (позавчера, 3 часа, клиент, валюта) не переспрашивай.

===== ОБЩИЕ ПРАВИЛА =====
- Данные только из TOOL_RESULTS / API. Не отвечай из памяти, не выдумывай
  клиентов, ставки, объёмы, даты.
- Имя клиента прогоняй через fi_clients и бери canonical из matched.
  Список продуктов — в PRODUCTS ниже.
- Не показывай record_id, технические поля и сырой JSON.
- Если данных нет — скажи прямо (после нескольких попыток поиска).
- raw_text — первичный источник; если там есть то, чего нет в summary — используй raw_text.
- Не смешивай данные разных клиентов/инструментов в одном выводе.
- Indicative / bid / offer — индикатив, не сделка.
  Различай: обсуждали / показали индикатив / сделали сделку / ждём ответа.
- Не ищи «Сбер», «Сбербанк», «СберCIB» как контрагента в clients или text.

===== ПОИСК =====
Сначала пойми запрос: число, список или содержание.

Назвали клиента («Райф», «ВТБ», «юник») — сначала fi_clients с этим именем.
Сервис не угадывает регулярками. matched — точное имя, бери его.
candidates — единственные имена, которые можно назвать.
Не выдумывай банк (CDB, China Development Bank, ТБанк), если его нет в ответе.
Кандидаты — даже один — спроси «имел в виду …?», не подставляй молча.
unknown при сохранении — «Такого клиента в базе нет. Как записать?» Без своих вариантов.
unknown на вопрос — всё равно ищи заметки по этому имени. Пусто — «Заметок нет».
«Сбер» — это мы, не клиент. Сейлзу не говори team и коды ошибок.
summary не переиначивай: «с ними» не заменяй на валюту или продукт.
Имя пиши как в выбранной карточке, не склоняй.
Без клиента в запросе fi_clients не нужен.

Сколько всего, по компаниям или по типу — один fi_count.
Цифры только из ответа: total, by_client, by_record_type.
Не считай записи руками и не ищи по text: слово «ВТБ» в тексте бывает и в чужих заметках.

«По каждой компании» — обычный вопрос. Возьми by_client и перескажи.
Не говори, что отчёта нет или нет доступа.

«Заметки» / «записи» / «все такие» — потребность и общая_информация вместе.
fi_search без record_type (или record_type=all).
Тип в фильтр — только если сейлз сам сказал «потребность» или «общая информация».

«Покажи / выведи / какие заметки / уже в базе / список» — fi_search, не fi_count.
В reply одна фраза и <!-- NOTES_LIST ids=id1,id2 --> — только record_id этого поиска. Без ids карточек нет. Если поиск шире ответа, маркер не ставь. Не пиши поля заметки текстом.
«Сколько» — fi_count без маркера. Потом «покажи их» — снова fi_search + маркер.
Пусто — скажи, что не нашёл. Не выдумывай запреты.

Заметки клиента — fi_search, clients = canonical из fi_clients, без record_type.

Что обсуждали / последние / история — fi_search по смыслу.
Для содержания можно 2–3 поиска с разными фильтрами. Для чисел так не делай.
Пусто — ослабь дату, потом продукт, клиента оставь. В text его не подставляй.
Фильтры, которых сейлз не называл, не добавляй.

В ответе: дата, клиент, тип, продукты, summary (raw_text — если нужен).
«Последний» — без ограничения по дате. «За месяц» = TODAY−30д.

Примеры фильтров:
| Вопрос | Фильтры |
| «что там с Юникредитом?» | clients, без record_type |
| «покажи все по ВТБ» / «список по ВТБ» | clients, без record_type. Одна фраза + <!-- NOTES_LIST ids=… --> |
| «все заметки по юаню» | currencies/text. Одна фраза + <!-- NOTES_LIST ids=… -->, не таблица |
| «Юник по IRS за прошлый год» | clients+products+dates |
| «кто смотрел XCCY в марте?» | products+dates |
| «USD размещения» | currencies=USD + text=размещение |
| «что любит ВТБ?» / предпочтения | clients + общая_информация |
| «последняя потребность» | clients + потребность |

Синонимы продуктов (в products — canonical из PRODUCTS):
- DCD / бивалютный депозит → DCD
- XCCY / валютно-процентный своп → XCCY
- IRS / процентный своп → IRS
- cap → CAP, floor → FLOOR
- простой валютный опцион → option
- золото спот с юрлицом → pm_spot
- товарный опцион → commodity_option
- МБК → MBK
- конверсия по генеральному соглашению → FX_spot

Направление (в text):
- «клиент размещает» / «Сбер привлекает» / sber_attracts
- «клиент привлекает» / «Сбер размещает» / sber_places
- hedge = хеджирование

===== ЗАМЕТКИ =====
record_type:
- потребность — конкретный интерес/обсуждение с датой
- общая_информация — фон: предпочтения, как лучше работать

Сигналы потребность: встретились, звонил, сегодня обсудили, на митинге,
конкретная дата, показали уровни, ждём ответа, интересуется, next step.
Сигналы общая_информация: обычно любит, лучше говорить с…, предпочитает,
всегда хеджирует, важно помнить, на будущее, как правило — без конкретного события.
Неочевидно → спроси: «Это потребность или общая информация по клиенту?»
При полной неясности («записать это?») → «Записать как заметку?»
Тип определяй сам по тексту.

Поля не выдумывай. Чего не хватает для потребности (клиент / дата /
длительность / продукт / валюта) — спроси **всё разом** списком.
Не спрашивай по одному полю за сообщение. Не переспрашивай то, что
уже было в этом диалоге: «позавчера 3 часа» = дата и длительность есть.
«дирхам» = AED, не спрашивай «это USD?». fx hedge — направление, не продукт:
если нет инструмента — спроси его в том же списке, что и валюту.
Пока дыры не закрыты — fi_prepare_note не вызывай.
Дальнейшие шаги пиши только словами сейлза. Не додумывай «ждём ответа» и подобное.
Клиент только из fi_clients. Не в списке → «Это новый клиент? Как записать?»

Поля: client_canonical, record_type, summary, raw_text.
потребность: date, время встречи, products, currencies, next_step.
  Время — либо meeting_start и meeting_end, либо только meeting_duration.
  Если сейлз назвал длительность, начало и конец не спрашивай.
  «полдня» = 4 часа (первая половина 10–14, вторая с 14). Пиши meeting_duration «4 часа».
  Сегодня сам не подставляй. Прошлую дату («22 сентября») принимай. Будущую не ставь.
  Время начала и конца — ЧЧ:ММ.
  «нет» / «пока нет» в шагах — пустой next_step, так можно.
общая_информация: клиент обязателен, дату/продукты/валюту не подставляй.
мероприятие: date, meeting_start и meeting_end, event_kind (только звонок или встреча).
  Клиент, продукт и валюта не обязательны. Гемба — не вид, а внутреннее мероприятие.
  «кетчап» / «кетчуп» / catch-up — внутренняя встреча, event_kind=встреча.
  Лондон, Сан-Паулу, Москва, Франкфурт, Нью-Дели, Дублин, Алматы, Цюрих, Мумбаи,
  Люксембург, Минск, Париж, Кейптаун, Дубай, Токио, Гонконг — чаще всего переговорки, не клиенты и не поездки.
  Такая фраза — мероприятие, event_kind=встреча, client пустой.

Удаление: сейлз явно просит удалить → найди заметку → спроси
«Удалить заметку от ДАТА по КЛИЕНТ?» → «да» → fi_delete с record_id.
record_id сейлзу не показывай. Старше 24 часов с момента внесения не правится и не удаляется:
если ok=false — скажи, что срок вышел. Не говори «удалил», пока ok не true.

Поток: fi_prepare_note → твой reply с превью «Верно?» → «да»/«ок»/«подтверждаю» → fi_note.
После успеха: «Записал.» / «Записал как общую информацию.»
«да»+правка → новый fi_prepare_note. ok=false — не рапортуй успех.

После успешного fi_prepare_note превью пишешь ТЫ в reply (не копируй JSON).
Каждый параметр с новой строки, markdown-список. Не склеивай в одну строку.
Строго такой формат:

**Я выделил из заметки:**

- **Тип:** Потребность
- **Клиент:** Юникредит
- **Дата:** 2025-05-05
- **Начало:** 11:00
- **Конец:** 12:00
- **Продукты:** IRS
- **Валюты:** USD
- **Дальнейшие шаги:** пришлём индикатив
- **Резюме:** Обсудили IRS $50M 3Y, клиент смотрит на уровень 6.5%

Верно?

Для общей_информации — **Тип:** Общая информация; дату не выдумывай.
Превью — только из того, что сейлз сказал или уточнил. Ничего не додумывай.

===== RUNTIME =====
Дата = строка TODAY ниже (это уже сегодня с сервера). «вчера» = TODAY−1, «позавчера» = TODAY−2.
Другой год не выдумывай. Продукты — в PRODUCTS ниже, клиентов спрашивай у fi_clients.
Инструменты только через JSON.calls:
fi_clients | fi_search | fi_count | fi_prepare_note | fi_note | fi_delete.

Формат ответа — строго один JSON:
{"reply": "...", "calls": [{"tool": "fi_search", "args": {...}}]}

fi_clients: names — имена как сказал сейлз. Ответ: matched, candidates, unknown.
fi_search args: clients?, products?, currencies?, date_from?, date_to?, text?,
  record_type? (all|потребность|общая_информация). По умолчанию all.
fi_count: те же фильтры. Ответ: total + by_record_type + by_client. Для «сколько» и «по компаниям».
fi_prepare_note: raw_text, client_canonical, record_type, summary, date?,
  meeting_start?, meeting_end?, meeting_duration?, products?, currencies?, next_step?
fi_note: draft_id?
fi_delete: record_id из fi_search. Только после подтверждения сейлза.

calls=[] — финальный reply / уточнение.
Пока есть calls — reply пустой. Уточнения — все недостающие поля разом.
Не пиши «ищу…», не начинай ответ до финала. Один финальный ответ по TOOL_RESULTS.
Отвечай строго на текущий USER; не смешивай фильтры из чужих батчей.
""".strip()


def _parse_turn(raw: str, allow_calls: bool) -> tuple[str, list[dict]]:
    text = re.sub(r"<think>[\s\S]*?</think>", "", raw or "", flags=re.I).strip()
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.I).strip()
    obj = None
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        a, b = text.find("{"), text.rfind("}")
        if 0 <= a < b:
            try:
                obj = json.loads(text[a : b + 1])
            except json.JSONDecodeError:
                pass
    if not isinstance(obj, dict):
        return ("" if text.startswith("{") else text), []

    reply = str(obj.get("reply") or "").strip()
    calls = []
    if allow_calls:
        for item in obj.get("calls") or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("tool") or "").strip()
            if name not in {"fi_clients", "fi_search", "fi_count", "fi_prepare_note", "fi_note", "fi_delete"}:
                continue
            args = item.get("args") if isinstance(item.get("args"), dict) else {}
            calls.append({"tool": name, "args": args})
    return reply, calls


def _draft_kind(text: str) -> str:
    s = re.sub(r"[\s,.!?]+", " ", (text or "").strip().lower()).strip()
    if any(w in s for w in ("не надо", "не нужно", "не сохраняй", "отмена", "отмени", "забей", "cancel", "стоп")):
        return "cancel"
    yes = {
        "да", "ок", "ok", "yes", "верно", "подтверждаю", "согласен",
        "сохраняй", "записывай", "сохрани", "ага", "угу", "давай",
        "окей", "okay", "хорошо", "ладно", "+",
        "всё верно", "все верно", "всё ок", "все ок",
    }
    if s in yes:
        return "confirm"
    first, _, rest = s.partition(" ")
    if first in yes:
        extra = yes | {"пожалуйста", "так", "всё", "все"}
        return "confirm" if not rest or set(rest.split()) <= extra else "amend"
    return "other"


def _append_log(event: dict) -> None:
    try:
        path = Path(NOTES_AGENT_LOG_PATH)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.open("a", encoding="utf-8").write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
    except Exception as e:
        log.warning("[Notes] agent_log write failed: %s", e)


def _new_log(user_query: str, username: str) -> dict:
    try:
        run_id = str(st.session_state.get("run_id") or "")
    except Exception:
        run_id = ""
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user": username or "unknown",
        "run_id": run_id,
        "took_sec": None,
        "type": "уточнение",
        "verdict": None,
        "audit_issues": 0,
        "n_records": 0,
        "n_searches": 0,
        "rounds": 0,
        "agents_called": [],
        "user_query": user_query,
        "transformed_user_input": "",
        "draft_action": "",
        "filters": {
            "clients": [], "products": [], "currencies": [],
            "date_from": [], "date_to": [], "text": [], "record_type": [],
        },
        "note": {},
        "saved": False,
        "final_answer": None,
        "error": "",
    }


def _track(agent_log: dict, name: str, args: dict, result: dict) -> None:
    agent_log["agents_called"].append(name)
    if not result.get("ok"):
        agent_log["audit_issues"] += 1
        return
    if name in {"fi_search", "fi_count"}:
        agent_log["n_searches"] += 1
        agent_log["n_records"] += len(result.get("records") or [])
        filters = agent_log["filters"]
        for key in ("clients", "products", "currencies"):
            for value in args.get(key) or []:
                s = str(value).strip()
                if s and s not in filters[key]:
                    filters[key].append(s)
        for key in ("date_from", "date_to", "text", "record_type"):
            s = str(args.get(key) or "").strip()
            if s and s not in filters[key]:
                filters[key].append(s)
        if agent_log["type"] == "уточнение":
            agent_log["type"] = "поиск"
    elif name in {"fi_prepare_note", "fi_note"}:
        agent_log["type"] = "заметка"
        if name == "fi_prepare_note":
            agent_log["note"] = dict(result.get("fields") or {})
        else:
            agent_log["saved"] = True


class FiService:
    def __init__(self):
        self.base = "http://127.0.0.1:18003"

    def _req(self, method: str, path: str, body: dict | None = None) -> dict:
        headers = {}
        user = str(st.session_state.get("username") or "").strip()
        if user:
            headers["X-Notes-Token"] = mint_notes_token(user)
        r = requests.request(method, f"{self.base}{path}", json=body, headers=headers, timeout=60)
        if r.status_code != 200:
            detail = ""
            try:
                detail = str((r.json() or {}).get("detail") or "")
            except Exception:
                detail = (r.text or "")[:200]
            raise RuntimeError(f"{path} {r.status_code} {detail}".strip())
        return r.json() or {}

    def lookup_clients(self, names: list[str]) -> dict:
        return self._req("POST", "/clients", {"names": names})

    def products(self) -> list[dict]:
        return list(self._req("GET", "/products").get("products") or [])

    def _filters(self, plan: dict) -> dict:
        rt = "_".join(str(plan.get("record_type") or "all").strip().lower().split())
        if rt in {"", "заметки", "заметка", "записи", "запись"}:
            rt = "all"
        if rt not in {"all", "потребность", "общая_информация"}:
            rt = "all"
        return {
            "clients": [str(x).strip() for x in (plan.get("clients") or []) if str(x).strip()],
            "products": [str(x).strip() for x in (plan.get("products") or []) if str(x).strip()],
            "currencies": [str(x).strip() for x in (plan.get("currencies") or []) if str(x).strip()],
            "date_from": plan.get("date_from") or None,
            "date_to": plan.get("date_to") or None,
            "text": str(plan["text"]).strip() if plan.get("text") else None,
            "record_type": rt,
        }

    def search(self, plan: dict) -> dict:
        return self._req("POST", "/records/search", self._filters(plan))

    def count(self, plan: dict) -> dict:
        return self._req("POST", "/records/count", self._filters(plan))

    def save_note(self, payload: dict) -> dict:
        return self._req("POST", "/records", payload)

    def delete_note(self, record_id: str) -> dict:
        return self._req("DELETE", f"/records/{record_id}")


class NotesLLM:
    def __init__(self):
        self._client: GigaChat | None = None

    def complete(self, system: str, user: str, deadline: float) -> str:
        if self._client is None:
            self._client = GigaChat(
                base_url="https://gigachat-ift.sberdevices.delta.sbrf.ru/v1/",
                cert_file="/tmp/giga/giga_cert.pem",
                key_file="/tmp/giga/giga_key.key",
                model="GigaChat-3-Ultra",
                temperature=1e-15,
                profanity_check=False,
                timeout=180,
                max_tokens=32768,
            )
        last_err: Exception | None = None
        for attempt in range(8):
            if time.monotonic() >= deadline:
                raise RuntimeError(f"llm deadline exceeded: {last_err}")
            try:
                resp = self._client.invoke([
                    SystemMessage(content=system or ""),
                    HumanMessage(content=user or ""),
                ])
                return str(getattr(resp, "content", "") or "")
            except Exception as e:
                last_err = e
                if any(c in str(e).lower() for c in (" 400", " 401", " 403", " 404", " 422")):
                    raise RuntimeError(f"llm failed: {e}") from e
                delay = min(2.0 * (2 ** attempt) + random.uniform(0, 2.0), 30.0)
                log.warning("[NotesLLM] attempt=%s/8 retry_in=%.1fs: %s", attempt + 1, delay, e)
                if time.monotonic() + delay >= deadline:
                    raise RuntimeError(f"llm failed: {last_err}") from e
                time.sleep(delay)
        raise RuntimeError(f"llm failed: {last_err}")


class NotesEngine:
    def __init__(self):
        self.svc = FiService()
        self.llm = NotesLLM()

    def _apply_draft(self, text: str, agent_log: dict) -> str:
        draft = st.session_state.get(DRAFT_KEY)
        if not isinstance(draft, dict):
            return ""
        if draft.get("status") == "confirmed":
            draft["status"] = "awaiting_confirm"
        kind = _draft_kind(text)
        agent_log["draft_action"] = kind
        if kind == "cancel":
            st.session_state.pop(DRAFT_KEY, None)
        elif kind == "confirm":
            draft["status"] = "confirmed"
            st.session_state[DRAFT_KEY] = draft
        elif kind == "amend":
            draft["status"] = "needs_reprepare"
            st.session_state[DRAFT_KEY] = draft
            return (
                f"AMENDMENT: правка сейлза, draft_id={draft.get('draft_id')} не подтверждён. "
                "Сделай fi_prepare_note заново. Не зови fi_note."
            )
        return ""

    def _system(self, username: str, products: list[dict], amend: str) -> str:
        catalog = "\n".join(
            f"- {it.get('canonical') or ''} | {it.get('description') or ''} | {', '.join(it.get('aliases') or [])}"
            for it in products
        ) or "(пусто)"
        parts = [
            SYSTEM_PROMPT,
            f"TODAY: {date.today().isoformat()}",
            f"current_username: {username or 'unknown'}",
            "PRODUCTS (canonical | description | aliases):\n" + catalog,
        ]
        draft = st.session_state.get(DRAFT_KEY)
        if isinstance(draft, dict):
            parts.append(
                f"ACTIVE_DRAFT status={draft.get('status')} draft_id={draft.get('draft_id')}.\n"
                f"fields:\n{json.dumps(draft.get('payload') or {}, ensure_ascii=False, indent=2)}"
            )
        if amend:
            parts.append(amend)
        return "\n\n".join(parts)

    def _user_prompt(self, dialog, user_text, results, force_final: bool) -> str:
        hist = [
            f"{t['role'].upper()}: {t['content']}"
            for t in dialog[:-1][-16:]
            if t.get("role") in {"user", "assistant"} and t.get("content")
        ]
        parts = ["===== DIALOG =====", "\n".join(hist) or "(пусто)", f"USER: {user_text}"]
        if results:
            blob = results if len(results) <= 24000 else "…\n" + results[-24000:]
            parts += ["===== TOOL_RESULTS (ground truth) =====", blob]
        if force_final:
            parts.append("===== INSTRUCTION =====\nПоследний раунд: JSON с calls=[] и финальным reply по TOOL_RESULTS.")
        else:
            parts.append("===== INSTRUCTION =====\nВерни JSON {reply, calls}. Мало данных — ещё fi_search в calls.")
        return "\n\n".join(parts)

    def _run_tool(self, name: str, args: dict, username: str) -> dict:
        args = args if isinstance(args, dict) else {}

        if name == "fi_clients":
            raw = args.get("names") or args.get("name") or args.get("clients")
            names = [str(x).strip() for x in (raw if isinstance(raw, list) else [raw]) if str(x or "").strip()]
            if not names:
                raise ValueError("names required")
            data = self.svc.lookup_clients(names)
            return {
                "ok": True,
                "matched": data.get("matched") or {},
                "candidates": data.get("candidates") or {},
                "unknown": data.get("unknown") or [],
            }

        if name == "fi_search":
            records = []
            for rec in self.svc.search(args).get("records") or []:
                records.append({
                    "record_id": rec.get("record_id") or "",
                    "date": rec.get("date") or "",
                    "meeting_start": rec.get("meeting_start") or "",
                    "meeting_end": rec.get("meeting_end") or "",
                    "meeting_duration": rec.get("meeting_duration") or "",
                    "client": rec.get("client") or "",
                    "products": rec.get("products") or [],
                    "currencies": rec.get("currencies") or [],
                    "summary": rec.get("summary") or "",
                    "raw_text": str(rec.get("raw_text") or "").strip(),
                    "status": rec.get("status") or "",
                    "next_step": rec.get("next_step") or "",
                    "record_type": rec.get("record_type") or "",
                })
            return {"ok": True, "total": len(records), "records": records}

        if name == "fi_count":
            data = self.svc.count(args)
            return {
                "ok": True,
                "total": int(data.get("total") or 0),
                "by_record_type": data.get("by_record_type") or {},
                "by_client": data.get("by_client") or {},
            }

        if name == "fi_prepare_note":
            rt = str(args.get("record_type") or "").strip()
            if rt not in {"потребность", "общая_информация", "мероприятие"}:
                raise ValueError("record_type must be потребность, общая_информация or мероприятие")
            date_val = str(args.get("date") or "").strip()
            payload = {
                "raw_text": str(args.get("raw_text") or "").strip(),
                "client_canonical": str(args.get("client_canonical") or "").strip(),
                "date": date_val,
                "meeting_start": str(args.get("meeting_start") or "").strip(),
                "meeting_end": str(args.get("meeting_end") or "").strip(),
                "meeting_duration": str(args.get("meeting_duration") or "").strip(),
                "products": [str(x).strip() for x in (args.get("products") or []) if str(x).strip()],
                "currencies": [str(x).strip() for x in (args.get("currencies") or []) if str(x).strip()],
                "summary": str(args.get("summary") or "").strip(),
                "next_step": str(args.get("next_step") or "").strip(),
                "created_by": username or "unknown",
                "record_type": rt,
            }
            if not payload["raw_text"]:
                raise ValueError("raw_text is required")
            if rt != "мероприятие" and not payload["client_canonical"]:
                raise ValueError("raw_text and client_canonical are required")
            if rt in {"потребность", "мероприятие"} and not payload["date"]:
                raise ValueError("date required")
            if rt == "потребность" and not (
                payload["meeting_duration"]
                or (payload["meeting_start"] and payload["meeting_end"])
            ):
                raise ValueError("нужны начало и конец встречи или только длительность")
            if rt == "мероприятие" and not (payload["meeting_start"] and payload["meeting_end"]):
                raise ValueError("нужны начало и конец мероприятия")
            if rt == "потребность" and not payload["products"]:
                raise ValueError("products required for потребность")
            if rt == "потребность" and not payload["currencies"]:
                raise ValueError("currencies required for потребность")
            draft_id = uuid.uuid4().hex[:12]
            st.session_state[DRAFT_KEY] = {"status": "awaiting_confirm", "payload": payload, "draft_id": draft_id}
            fields = {k: payload[k] for k in (
                "record_type", "client_canonical", "date", "meeting_start", "meeting_end",
                "meeting_duration", "products", "currencies", "next_step", "summary",
            )}
            return {"ok": True, "draft_id": draft_id, "fields": fields}

        if name == "fi_delete":
            record_id = str(args.get("record_id") or "").strip()
            if not record_id:
                raise ValueError("record_id required")
            try:
                self.svc.delete_note(record_id)
            except RuntimeError as e:
                return {"ok": False, "message": str(e)}
            return {"ok": True}

        if name == "fi_note":
            draft = st.session_state.get(DRAFT_KEY)
            if not isinstance(draft, dict) or not draft.get("payload"):
                return {"ok": False, "message": "нет черновика, сначала fi_prepare_note"}
            if draft.get("status") != "confirmed":
                return {"ok": False, "message": "сейлз ещё не подтвердил", "fields": draft["payload"]}
            payload = draft["payload"]
            saved = self.svc.save_note(payload)
            st.session_state.pop(DRAFT_KEY, None)
            return {"ok": True, "result": {"status": saved.get("status"), "record_type": payload.get("record_type")}}

        return {"ok": False, "message": f"unknown tool: {name}"}

    def _unconfirm(self) -> None:
        draft = st.session_state.get(DRAFT_KEY)
        if isinstance(draft, dict) and draft.get("status") == "confirmed":
            draft["status"] = "awaiting_confirm"
            st.session_state[DRAFT_KEY] = draft

    def handle(self, user_msg: str, username: str, on_partial: Callable | None = None) -> str:
        text = (user_msg or "").strip()
        if not text:
            return "Напиши вопрос к базе или заметку по клиенту."

        dialog = list(st.session_state.get(DIALOG_KEY) or [])
        dialog.append({"role": "user", "content": text})
        agent_log = _new_log(text, username)
        t0 = time.time()
        saved = False
        shown = ""
        deadline = time.monotonic() + 420.0

        try:
            amend = self._apply_draft(text, agent_log)
            try:
                products = self.svc.products()
            except Exception as e:
                raise RuntimeError("FI service недоступен") from e

            system = self._system(username, products, amend)
            user_text = f"{text}\n\n[system_note: {amend}]" if amend else text
            agent_log["transformed_user_input"] = user_text if amend else ""

            results, answer, n_tools, did_search = "", "", 0, False
            last_search_ids: list[str] = []
            for round_i in range(1, 7):
                if time.monotonic() >= deadline:
                    break
                agent_log["rounds"] = round_i
                force_final = round_i == 6
                raw = self.llm.complete(
                    system, self._user_prompt(dialog, user_text, results, force_final), deadline,
                )
                reply, calls = _parse_turn(raw, allow_calls=not force_final)
                log.info("[Notes] round=%s calls=%s", round_i, [c["tool"] for c in calls])

                if reply and calls:
                    low = reply.lower()
                    asking = any(m in low for m in (
                        "верно?", "это потребность", "записать как заметку", "это новый клиент",
                        "общая информация по клиенту", "с каким клиентом", "какой продукт", "какой клиент",
                        "какая валюта", "какие валюты", "в какой валюте",
                        "дальнейшие шаги", "следующие шаги",
                        "какая дата", "дата встречи", "во сколько", "время начала",
                        "время конца", "длительность",
                    ))
                    if asking and not {c["tool"] for c in calls} & {"fi_prepare_note", "fi_note"}:
                        if on_partial:
                            on_partial(reply)
                        answer = shown = reply
                        break

                if calls:
                    batch = []
                    for call in calls:
                        name, args = call["tool"], call["args"]
                        try:
                            result = self._run_tool(name, args, username)
                        except ValueError as e:
                            result = {"ok": False, "message": str(e)}
                        except Exception:
                            log.exception("[Notes] tool %s failed", name)
                            result = {"ok": False, "message": "tool failed"}
                        if name == "fi_search" and result.get("ok"):
                            did_search = True
                            last_search_ids = [
                                str(rec.get("record_id") or "").strip()
                                for rec in (result.get("records") or [])
                                if str(rec.get("record_id") or "").strip()
                            ]
                        if name == "fi_note" and result.get("ok"):
                            saved = True
                        _track(agent_log, name, args, result)
                        batch.append({"tool": name, "args": args, "result": result})
                    n_tools += len(batch)
                    chunk = f"----- tool_batch round={round_i} -----\n" + json.dumps(batch, ensure_ascii=False, indent=2)
                    results = f"{results}\n\n{chunk}".strip() if results else chunk
                    continue

                if reply:
                    answer = reply
                    break

            if not answer and results and time.monotonic() < deadline:
                raw = self.llm.complete(
                    system, self._user_prompt(dialog, user_text, results, True), deadline,
                )
                answer, _ = _parse_turn(raw, allow_calls=False)

            if not answer:
                answer = USER_CONFUSED if n_tools else "Уточни, пожалуйста: это вопрос к базе или заметка?"

            answer = _attach_notes_list_marker(answer, user_text, did_search, last_search_ids)
            if not saved:
                self._unconfirm()
            dialog.append({"role": "assistant", "content": answer})
            st.session_state[DIALOG_KEY] = dialog[-20:]
            st.session_state["_notes_partial_shown"] = bool(shown and shown.strip() == answer.strip())
            agent_log["final_answer"] = answer
            agent_log["verdict"] = "FAIL" if answer == USER_CONFUSED else "PASS"
            return answer

        except Exception as e:
            log.exception("[Notes] handle failed: %s", e)
            self._unconfirm()
            st.session_state[DIALOG_KEY] = dialog[-20:]
            st.session_state["_notes_partial_shown"] = False
            answer = SERVICE_BUSY if "429" in str(e) else USER_CONFUSED
            agent_log["final_answer"] = answer
            agent_log["verdict"] = "RATE_LIMIT" if answer == SERVICE_BUSY else "FAIL"
            agent_log["error"] = str(e)
            return answer

        finally:
            agent_log["took_sec"] = round(time.time() - t0, 3)
            _append_log(agent_log)


def reset_to_zero_notes_pipeline() -> None:
    st.session_state["notes_mode"] = False
    st.session_state["notes_chat_history"] = []
    for key in (DIALOG_KEY, DRAFT_KEY, "notes_engine", "_notes_partial_shown"):
        st.session_state.pop(key, None)


def write_notes_start(all_messages_memory, last_messages_memory, save_fn, stream_fn, reset_all_fn) -> None:
    reset_all_fn()
    st.session_state.notes_mode = True
    st.session_state.notes_chat_history = []
    st.session_state.pop(DIALOG_KEY, None)
    st.session_state.pop(DRAFT_KEY, None)
    st.chat_message("user").write("/notes")
    stream_fn(NOTES_WELCOME)
    save_fn(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input="/notes",
        transformed_user_input="",
        chat_answer=NOTES_WELCOME,
        chat_history_last="",
        pipeline="notes",
    )


def write_notes_answer(
    original_user_input, all_messages_memory, last_messages_memory,
    chat_history_last, save_fn, stream_fn,
) -> None:
    if "notes_engine" not in st.session_state:
        st.session_state.notes_engine = NotesEngine()
    engine: NotesEngine = st.session_state.notes_engine
    partial = {"text": ""}

    def on_partial(text: str) -> None:
        t = (text or "").strip()
        if t and t != partial["text"]:
            partial["text"] = t
            stream_fn(t)

    with st.spinner("FI Notes…"):
        answer = engine.handle(
            original_user_input,
            str(st.session_state.get("username") or "unknown"),
            on_partial=on_partial,
        )

    if not (
        st.session_state.pop("_notes_partial_shown", False)
        and partial["text"].strip() == (answer or "").strip()
    ):
        stream_fn(answer)

    hist = st.session_state.get("notes_chat_history")
    if not isinstance(hist, list):
        hist = []
    hist += [
        {"role": "user", "content": original_user_input},
        {"role": "assistant", "content": answer},
    ]
    st.session_state.notes_chat_history = hist
    save_fn(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=original_user_input,
        transformed_user_input="",
        chat_answer=answer,
        chat_history_last=str(chat_history_last) if chat_history_last else "",
        pipeline="notes",
    )

