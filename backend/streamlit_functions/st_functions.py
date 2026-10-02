import logging
import os
import re
from pathlib import Path
import importlib
import time
import uuid
from typing import Optional
from datetime import datetime, timedelta

import csv
import fcntl
import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit_antd_components as sac
import streamlit_authenticator as stauth
import yaml
from langchain_core.tracers.context import collect_runs
from langchain.memory import ConversationBufferMemory, ConversationBufferWindowMemory
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from streamlit_authenticator.utilities.exceptions import RegisterError
from streamlit_feedback import streamlit_feedback

from synaptica.backend.backend_logic import create_multivector_chain_with_chat_history, PERS_create_multivector_chain_with_chat_history
from synaptica.backend.streamlit_functions.calls_access import (
    is_calls_allowed_user,
    is_openclaw_allowed_user,
    is_openclaw_coder_allowed_user,
    is_openclaw_helper_allowed_user,
    is_openclaw_client_allowed_user,
    is_openclaw_fi_sales_allowed_user,
    is_notes_allowed_user,
    notes_fi_desk_for_user,
    notes_team_for_user,
    normalize_username,
    mint_notes_token,
    NOTES_USER_FIO,
    publish_notes_actor,
    is_broker_allowed_user,
    is_openclaw_corp_sales_allowed_user,
    is_openclaw_vnd_rag_allowed_user,
    is_openclaw_pres_gen_allowed_user,
    is_openclaw_rates_pricing_allowed_user,
    is_openclaw_products_allowed_user,
    is_openclaw_calls_allowed_user,
    is_openclaw_router_allowed_user,
    is_vnd_allowed_user,
    is_presentation_gen_allowed_user,
    is_logs_view_allowed_user,
    is_call_upload_allowed_user,
    is_special_news_allowed_user,
    resolve_calls_bank,
)
from synaptica.backend.streamlit_functions.agent_catalog import (
    caption_for as _agent_catalog_caption,
    format_agents_block,
    load_agent_routes,
)
from synaptica.backend.streamlit_functions.openclaw_router import (
    MAX_TARGETS as _ROUTER_MAX_TARGETS,
    OPENCLAW_ROUTER_AGENT_ID,
    call_router as _call_openclaw_router,
    classify as _classify_openclaw,
    parse_router_labels,
)
from synaptica.backend.streamlit_functions.st_reseting_functions import (
    reset_to_zero_cmdt_pipeline,
    reset_to_zero_fx_pipeline,
    reset_to_zero_indicatives_pipeline,
    reset_to_zero_input_pipeline,
    reset_to_zero_potential_pipeline,
    reset_to_zero_products_pipeline,
    reset_to_zero_sales_pipeline,
    reset_to_zero_pers_pipeline,
    reset_to_zero_calls_pipeline,
    reset_to_zero_openclaw_pipeline,
    reset_to_zero_openclaw_vnd_rag_pipeline,
    reset_to_zero_openclaw_router_pipeline,
    reset_to_zero_presentation_gen_pipeline,
    reset_to_zero_logs_view_pipeline as _reset_to_zero_logs_view_pipeline_orig,
    reset_to_zero_call_upload_pipeline,
    reset_to_zero_special_news_pipeline,
    reset_to_zero_ALL_pipelines,
    reset_to_zero_notes_pipeline,
    reset_to_zero_broker_pipeline,
)
from synaptica.backend.streamlit_functions.vnd_class import handle_vnd_question, get_vnd_doc_num_from_text, get_vnd_document_path
from synaptica.backend.analytics_rag import (
    get_dashborard_rag_response,
)
from synaptica.backend.rates_agent import (
    process_save_rates_deal_request,
    process_delete_rates_deal_request,
    process_rates_updates,
    process_rates_restruct,
    process_get_rates_deal_params,
    process_change_rates_params,
)
from synaptica.backend.utility_functions_giga import (
    choose_case,
    choose_case_rates_agent,
    do_fuzzy_search,
    get_products_from_user_input,
    paraphrase_bot_message,
    select_company,
)
from synaptica.config.settings import DefaultSettings
from synaptica.data.get_data import (
    get_alternative_products_data,
    get_commodities_dict,
    get_data_per_period_for_graph,
    get_eco_data,
    get_faq_data,
    get_fx_dict,
    get_hello_data,
    get_rates_help_data,
    get_potential_data,
    get_products_with_descriptions_data,
)
from synaptica.utils.helpers import seed_all
from synaptica.utils.templates import GENERAL_SYNAPTICA_SYSTEM_PROMPT_TEMPLATE, USER_1, USER_2, USER_3, PERSONIFICATION_SCRIPTS, SESSION_SYSTEM_PROMPT, ADDITIONAL_INSTRUCTION_RAW, PERSONIFICATION_CLIENT_INFO
from synaptica.backend.streamlit_functions.call_classes import (
    CALLS_RENDER_EMBED_KEY,
    CALL_TABLE_COMMAND,
    CALL_TABLE_DF_SELECTION_WIDGET_KEY,
    CALL_TABLE_INTERACTIVE_CTX_KEY,
    CALL_TABLE_PENDING_DETAIL_KEY,
    CallIntelligenceService,
    ensure_call_intelligence_service,
    build_call_table_operator_detail,
    format_call_table_detail_user_message,
    render_call_sales_table,
    resolve_call_data_path,
    resolve_call_table_pending_window,
)
from synaptica.backend.streamlit_functions.notes_engine import (
    write_notes_answer as _notes_answer,
)
from synaptica.backend.streamlit_functions.broker_engine import (
    BROKER_COMMAND,
    write_broker_start as _broker_start,
    write_broker_answer as _broker_answer,
)
from synaptica.backend.streamlit_functions.calls_tag_with_gigachat import run_call_upload_pipeline

SPECIAL_NEWS_COMMAND = "/special_news"
from synaptica.backend.streamlit_functions.comments_analytics import run_comments_analytics
from synaptica.backend.streamlit_functions.comments_analytics import (
    _load_comments_df,
    _load_classific_df,
    merge_class_names,
)


_logger = logging.getLogger(__name__)
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

settings = DefaultSettings()


def reset_to_zero_vnd_pipeline() -> None:
    st.session_state.vnd_mode = False
    st.session_state.vnd_chat_history = []


_reset_to_zero_ALL_pipelines_orig = reset_to_zero_ALL_pipelines


def reset_to_zero_ALL_pipelines() -> None:
    _reset_to_zero_ALL_pipelines_orig()
    reset_to_zero_vnd_pipeline()
    reset_to_zero_comments_pipeline()

def start_vnd_pipeline_2(
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last,
) -> None:
    if not is_vnd_allowed_user(str(st.session_state.get("username", ""))):
        _vnd_denied = "⛔ У вас нет доступа к режиму /vnd."
        stream_md_message(_vnd_denied)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            chat_answer=_vnd_denied,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="vnd",
        )
        return

    reset_to_zero_ALL_pipelines()
    st.session_state.vnd_mode = False

    hist = st.session_state.get("vnd_chat_history", [])
    if not isinstance(hist, list):
        hist = []

    if len(hist) > 5:
        hist = hist[-2:]

    with st.spinner("Подождите, обрабатываем запрос по ВНД…"):
        chat_answer = handle_vnd_question(original_user_input, hist)
    doc_num = get_vnd_doc_num_from_text(chat_answer)

    hist.append({"role": "user", "content": original_user_input})
    hist.append({"role": "assistant", "content": chat_answer})
    st.session_state.vnd_chat_history = hist

    st.session_state.pending_feedback_meta = {"pipeline": "vnd"}

    stream_md_message(chat_answer, unsafe_allow_html=True)
    if doc_num:
        doc_path = get_vnd_document_path(doc_num)
        if doc_path and os.path.isfile(doc_path):
            with open(doc_path, "rb") as f:
                st.download_button(
                    label="Скачать документ",
                    data=f.read(),
                    file_name=f"{doc_num}.docx",
                    mime="application/octet-stream",
                    key=f"vnd_download_{doc_num}_{uuid.uuid4()}",
                )

    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=original_user_input,
        transformed_user_input=transformed_user_input,
        chat_answer=chat_answer,
        chat_history_last=str(chat_history_last),
        pipeline="vnd",
    )



def start_vnd_pipeline(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    if not is_vnd_allowed_user(str(st.session_state.get("username", ""))):
        _vnd_denied = "⛔ У вас нет доступа к режиму /vnd."
        if show_user_msg:
            st.chat_message("user").write("/vnd")
        stream_md_message(_vnd_denied)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input="/vnd",
            transformed_user_input="",
            chat_answer=_vnd_denied,
            chat_history_last="",
            pipeline="vnd",
        )
        return

    reset_to_zero_ALL_pipelines()
    st.session_state.vnd_mode = True
    st.session_state.vnd_chat_history = []

    user_prompt = "/vnd"
    if show_user_msg:
        st.chat_message("user").write(user_prompt)
    chat_answer = (
        "**Режим /vnd включён.**\n\n"
        "Дальше ваши сообщения будут обрабатываться отдельной VND-моделью. "
        "Чтобы выйти — отправьте любую другую команду (например, /start, /products и т.д.)."
    )
    stream_md_message(chat_answer)

    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=user_prompt,
        transformed_user_input="",
        chat_answer=chat_answer,
        chat_history_last="",
        pipeline="vnd",
    )


def write_vnd_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
) -> None:
    if not is_vnd_allowed_user(str(st.session_state.get("username", ""))):
        reset_to_zero_vnd_pipeline()
        _vnd_denied = "⛔ У вас нет доступа к режиму /vnd."
        stream_md_message(_vnd_denied)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input=original_user_input,
            chat_answer=_vnd_denied,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="vnd",
        )
        return

    hist = st.session_state.get("vnd_chat_history", [])
    
    if len(hist) > 5:
        hist = hist[-2:]

    with st.spinner("Подождите, обрабатываем запрос по ВНД…"):
        chat_answer = handle_vnd_question(original_user_input, hist)
    doc_num = get_vnd_doc_num_from_text(chat_answer)

    hist.append({"role": "user", "content": original_user_input})
    hist.append({"role": "assistant", "content": chat_answer})
    st.session_state.vnd_chat_history = hist

    st.session_state.pending_feedback_meta = {"pipeline": "vnd"}

    stream_md_message(chat_answer, unsafe_allow_html=True)
    if doc_num:
        doc_path = get_vnd_document_path(doc_num)
        if doc_path and os.path.isfile(doc_path):
            with open(doc_path, "rb") as f:
                st.download_button(
                    label="Скачать документ",
                    data=f.read(),
                    file_name=f"{doc_num}.docx",
                    mime="application/octet-stream",
                    key=f"vnd_download_{doc_num}_{uuid.uuid4()}",
                )
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=original_user_input,
        transformed_user_input="",
        chat_answer=chat_answer,
        chat_history_last=str(chat_history_last),
        pipeline="vnd",
    )


def _openclaw_start_impl(
    command: str,
    welcome_msg: str,
    mode_key: str,
    hist_key: str,
    pipeline: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    access_check=None,
    show_user_msg: bool = True,
) -> None:
    username = str(st.session_state.get("username", ""))
    if access_check is not None and not access_check(username):
        msg = f"🦞 ⛔ У вас нет доступа к режиму {command}."
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=command,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last="",
            pipeline=pipeline,
        )
        return
    reset_to_zero_ALL_pipelines()
    st.session_state[mode_key] = True
    st.session_state[hist_key] = []
    if show_user_msg:
        st.chat_message("user").write(command)
    stream_md_message(welcome_msg)
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=command,
        transformed_user_input="",
        chat_answer=welcome_msg,
        chat_history_last="",
        pipeline=pipeline,
    )


def write_openclaw_coder_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    _openclaw_start_impl(
        command="/openclaw1_coder",
        welcome_msg=(
            "💻 🦞 **Synaptica Coder — режим включён.**\n\n"
            "Пишу код, разбираюсь в структуре проекта, добавляю фичи. "
            "Чтобы выйти — любая другая команда."
        ),
        mode_key="openclaw_coder_mode",
        hist_key="openclaw_coder_chat_history",
        pipeline="openclaw_coder",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_openclaw_coder_allowed_user,
        show_user_msg=show_user_msg,
    )


def write_openclaw_helper_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    _openclaw_start_impl(
        command="/openclaw1_helper",
        welcome_msg=(
            "📖 🦞 **Synaptica Helper — режим включён.**\n\n"
            "Расскажу где что находится в проекте, какой функционал есть, "
            "как что вызывать. Чтобы выйти — любая другая команда."
        ),
        mode_key="openclaw_helper_mode",
        hist_key="openclaw_helper_chat_history",
        pipeline="openclaw_helper",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_openclaw_helper_allowed_user,
        show_user_msg=show_user_msg,
    )


def write_openclaw_client_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    _openclaw_start_impl(
        command="/openclaw",
        welcome_msg=(
            "📋 🦞 **Анализ фин отчётности — режим включён.**\n\n"
            "Помогаю с анализом финансовой отчётности. "
            "Чтобы выйти — любая другая команда."
        ),
        mode_key="openclaw_client_mode",
        hist_key="openclaw_client_chat_history",
        pipeline="openclaw_client",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_openclaw_client_allowed_user,
        show_user_msg=show_user_msg,
    )


# Оставляем старое имя для совместимости — ведёт на coder.
def write_openclaw_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    write_openclaw_coder_start(all_messages_memory, last_messages_memory)


def _openclaw_merge_tool_delta(acc: dict[int, dict[str, str]], tool_calls_delta: list) -> None:
    """Склеивает частичные tool_calls из SSE (OpenAI-стрим)."""
    if not isinstance(tool_calls_delta, list):
        return
    for tc in tool_calls_delta:
        if not isinstance(tc, dict):
            continue
        idx = int(tc.get("index", 0))
        if idx not in acc:
            acc[idx] = {"id": "", "name": "", "arguments": ""}
        if tc.get("id"):
            acc[idx]["id"] = str(tc["id"])
        fn = tc.get("function")
        if isinstance(fn, dict):
            if fn.get("name"):
                acc[idx]["name"] = str(fn["name"])
            if fn.get("arguments") is not None:
                acc[idx]["arguments"] += str(fn.get("arguments", ""))


def _openclaw_parse_args(args_json: str) -> dict:
    import json as _json
    try:
        return _json.loads(args_json) if args_json.strip().endswith("}") else {}
    except Exception:
        return {}


FIN_REPORT_PDF = Path("/home/synaptica/synaptica/synaptica/calls_data/fin_report.pdf")
FIN_REPORT_PDF_MARKER = f"<!-- PDF: {FIN_REPORT_PDF} -->"


def _collect_pdf_paths(text: str) -> list[str]:
    return list(dict.fromkeys(
        re.findall(r'<!--\s*PDF:\s*(/[\w/.\-]+\.pdf)\s*-->', text or "")
        + re.findall(r'(/[\w/.\-]+\.pdf)', text or "")
    ))


def _strip_pdf_from_display(text: str) -> str:
    text = re.sub(r'<!--\s*PDF:\s*/[\w/.\-]+\.pdf\s*-->\n?', '', text or "").strip()
    if _collect_pdf_paths(text):
        text = re.sub(r'[^\n]*(/[\w/.\-]+\.pdf)[^\n]*\n?', '', text).strip()
    return text


def _render_pdf_was_called(text: str, tool_events: list[dict] | None = None) -> bool:
    if "/render_pdf" in (text or ""):
        return True
    for ev in tool_events or []:
        detail = str(ev.get("detail") or "")
        if "/render_pdf" in detail:
            return True
        if ev.get("event") == "tool_call" and ev.get("tool") == "exec":
            try:
                import json as _json
                args = _json.loads(detail) if detail.strip().startswith("{") else {}
                cmd = str(args.get("command") or args.get("cmd") or "")
                if "/render_pdf" in cmd:
                    return True
            except Exception:
                pass
    return False


def _ensure_fin_report_marker(text: str, tool_events: list[dict] | None = None) -> str:
    """Как PPTX: маркер в тексте сообщения → кнопка под этим сообщением в истории."""
    if not _render_pdf_was_called(text, tool_events):
        return text
    if FIN_REPORT_PDF_MARKER in text or str(FIN_REPORT_PDF) in _collect_pdf_paths(text):
        return text
    return text.rstrip() + "\n" + FIN_REPORT_PDF_MARKER


def _render_pdf_download_buttons(pdf_paths: list[str], key_prefix: str) -> None:
    for idx, pdf_path in enumerate(pdf_paths):
        p = Path(pdf_path)
        if not p.is_file():
            continue
        st.download_button(
            label=f"⬇ Скачать {p.name}",
            data=p.read_bytes(),
            file_name=p.name,
            mime="application/pdf",
            key=f"{key_prefix}_pdf_{idx}_{hash(pdf_path)}",
        )


_PPTX_MIME = "application/vnd.openxmlformats-officedocument.presentationml.presentation"


def _render_pptx_download_buttons(
    pptx_paths: list[str], pptx_urls: list[str], key_prefix: str
) -> None:
    """Нативная кнопка скачивания .pptx. Сначала пробуем локальный файл,
    иначе тянем файл по http-ссылке сервиса (имя файла = имя шаблона)."""
    import requests

    rendered: set[str] = set()
    for idx, pptx_path in enumerate(pptx_paths):
        p = Path(pptx_path)
        if not p.is_file():
            continue
        st.download_button(
            label=f"⬇ Скачать {p.name}",
            data=p.read_bytes(),
            file_name=p.name,
            mime=_PPTX_MIME,
            key=f"{key_prefix}_pptx_{idx}_{hash(pptx_path)}",
        )
        rendered.add(p.name)

    if rendered:
        return

    for idx, url in enumerate(pptx_urls):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except Exception:
            _logger.warning("не удалось скачать pptx по ссылке %s", url, exc_info=True)
            continue
        cd = resp.headers.get("Content-Disposition", "")
        m = re.search(r"filename\*?=(?:UTF-8\'\')?\"?([^\";]+)", cd)
        fname = m.group(1) if m else (url.rstrip("/").split("/")[-1] or "presentation") + ".pptx"
        st.download_button(
            label=f"⬇ Скачать {fname}",
            data=resp.content,
            file_name=fname,
            mime=_PPTX_MIME,
            key=f"{key_prefix}_pptx_url_{idx}_{hash(url)}",
        )


# UI fin reporting (/openclaw) и pres-gen (/pres_gen): True — простой спиннер
# с понятным текстом по шагам; False — полный лог tool calls как раньше.
OPENCLAW_CLIENT_SIMPLE_UI = True


_OPENCLAW_SIMPLE_SPINNER_STYLE = (
    "<style>@keyframes fin-oc-spin{to{transform:rotate(360deg)}}</style>"
)


def _render_openclaw_simple_spinner(text: str) -> str:
    safe = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return (
        f"{_OPENCLAW_SIMPLE_SPINNER_STYLE}"
        '<div style="display:flex;align-items:center;gap:0.55rem;color:#6b7280;'
        'font-size:0.95rem;margin:0.15rem 0 0.5rem;">'
        '<div style="width:18px;height:18px;border:2px solid #e5e7eb;'
        'border-top-color:#6b7280;border-radius:50%;animation:fin-oc-spin .75s linear infinite;'
        'flex-shrink:0;"></div>'
        f"<span>{safe}</span></div>"
    )


_FINRAG_SKILL_LABELS: dict[str, str] = {
    "finrag": "Выбираю способ запроса данных",
    "finanalysis": "Готовлю сценарий финансового анализа",
    "company_card": "Готовлю сценарий карточки компании",
    "competitive_brief": "Готовлю сценарий конкурентного анализа",
    "file": "Читаю инструкцию по загрузке файла",
}


def _finrag_exec_status_label(cmd: str) -> str | None:
    """Если cmd — curl к finrag API, возвращает понятный лейбл. Иначе None."""
    import re
    url_match = re.search(r'https?://\S+', cmd)
    if not url_match:
        return None
    url = url_match.group()
    path = url.split("?")[0]
    if path.endswith("/companies"):
        return "Получаю список компаний"
    if re.search(r"/documents/[^/]+$", path):
        if "-X DELETE" in cmd or "--request DELETE" in cmd:
            return "Удаляю документ из базы"
    if path.endswith("/documents"):
        if "--data-urlencode" in cmd or "?" in url:
            return "Ищу документы в базе"
        return "Получаю каталог документов"
    if re.search(r"/document/[^/]+/full", path):
        return "Читаю текст финансового документа"
    if path.endswith("/search"):
        q_match = re.search(r'query[= ]([^\s&"\']+)', cmd)
        if q_match:
            q = q_match.group(1)[:30]
            return f"Ищу: `{q}`"
        return "Ищу по финансовым данным"
    if path.endswith("/facts/aggregate"):
        codes_match = re.search(r'codes[= ]([^\s&"\']+)', cmd)
        if codes_match:
            codes = codes_match.group(1)[:25]
            return f"Запрашиваю показатели `{codes}`"
        return "Запрашиваю финансовые показатели"
    if path.endswith("/render_pdf"):
        return "Формирую PDF-отчёт"
    if path.endswith("/upload"):
        file_match = re.search(r'@([^\s"\']+)', cmd)
        if file_match:
            fname = file_match.group(1).split("/")[-1]
            return f"Загружаю `{fname}` в базу"
        return "Загружаю документ в базу"
    return None


def _pres_exec_json_field(cmd: str, field: str, *, max_len: int = 30) -> str | None:
    """Короткий фрагмент строкового поля из JSON в curl -d."""
    import re

    m = re.search(rf'"{field}"\s*:\s*"([^"]{{1,{max_len}}})', cmd)
    return m.group(1) if m else None


def _pres_exec_status_label(cmd: str) -> str | None:
    """Если cmd — curl к presentation API (18005), возвращает понятный лейбл."""
    import re

    url_match = re.search(r"https?://\S+", cmd)
    if not url_match:
        return None
    url = url_match.group()
    path = url.split("?")[0]
    if "/presentation/" not in path and ":18005" not in url:
        return None

    request = _pres_exec_json_field(cmd, "request")
    query = _pres_exec_json_field(cmd, "query")

    if path.endswith("/health"):
        return "Проверяю сервис презентаций"
    if path.endswith("/products"):
        return "Получаю список продуктов"
    if path.endswith("/generate"):
        if request:
            return f"Генерирую презентацию: `{request}`"
        return "Генерирую презентацию"
    if path.endswith("/session/reset"):
        return "Начинаю новую презентацию"
    if path.endswith("/edit/text"):
        if request:
            return f"Правлю тексты: `{request}`"
        return "Правлю тексты слайдов"
    if path.endswith("/edit/pricing"):
        if request:
            return f"Обновляю прайсинг: `{request}`"
        return "Обновляю параметры и прайсинг"
    if path.endswith("/price"):
        if request:
            return f"Считаю прайсинг: `{request}`"
        return "Считаю прайсинг продукта"
    if path.endswith("/slide-data"):
        if request:
            return f"Готовлю слайды: `{request}`"
        return "Готовлю данные для слайдов"
    if path.endswith("/knowledge"):
        if query:
            return f"Ищу в базе знаний: `{query}`"
        return "Ищу в базе знаний по продукту"
    if path.endswith("/plot-static"):
        return "Строю график продукта"
    if path.endswith("/product-name"):
        return "Определяю продукт по запросу"
    if path.endswith("/classify/intent"):
        return "Определяю намерение пользователя"
    if path.endswith("/classify/edit-type"):
        return "Определяю тип правки"
    if path.endswith("/extract/pricing-params"):
        return "Извлекаю параметры прайсинга"
    if path.endswith("/extract/text-instruction"):
        return "Извлекаю правку текста"
    if re.search(r"/presentation/session/[^/]+/history", path):
        return "Читаю историю сессии"
    if re.search(r"/presentation/session/[^/]+$", path):
        return "Проверяю состояние сессии"
    if path.endswith("/sessions/stats"):
        return "Получаю статистику сессий"
    if path.endswith("/sessions/cleanup"):
        return "Очищаю старые сессии"
    if re.search(r"/presentation/download/[^/]+$", path):
        return "Готовлю файл для скачивания"
    if re.search(r"/presentation/slide/[^/]+$", path):
        if "-X DELETE" in cmd or "--request DELETE" in cmd:
            return "Удаляю слайд"
        return "Загружаю состояние слайда"
    return "Работаю с сервисом презентаций"


def _pres_exec_log_label(cmd: str) -> str | None:
    """Короткая подпись для лога (без иконки). Возвращает None если не presentation."""
    import re

    url_match = re.search(r"https?://\S+", cmd)
    if not url_match:
        return None
    url = url_match.group()
    path = url.split("?")[0]
    if "/presentation/" not in path and ":18005" not in url:
        return None
    _ENDPOINT_LABELS = {
        "/generate": "генерация презентации",
        "/price": "прайсинг",
        "/slide-data": "данные слайдов",
        "/knowledge": "база знаний",
        "/edit/text": "правка текстов",
        "/edit/pricing": "правка прайсинга",
        "/products": "список продуктов",
        "/health": "проверка сервиса",
    }
    for ep, label in _ENDPOINT_LABELS.items():
        if path.endswith(ep):
            return f" — {label}"
    if path.endswith("/session/reset"):
        return " — сброс сессии"
    if path.endswith("/plot-static"):
        return " — построение графика"
    return " — presentation API"


def _finrag_exec_log_label(cmd: str) -> str | None:
    """Короткая подпись для лога (без икноки). Возвращает None если не finrag."""
    import re
    url_match = re.search(r'https?://\S+', cmd)
    if not url_match:
        return None
    url = url_match.group()
    path = url.split("?")[0]
    _ENDPOINT_LABELS = {
        "/companies": "список компаний",
        "/documents": "каталог документов",
        "/search": "поиск по данным",
        "/facts/aggregate": "финансовые показатели",
        "/upload": "загрузка документа",
    }
    for ep, label in _ENDPOINT_LABELS.items():
        if path.endswith(ep):
            return f" — {label}"
    if re.search(r"/document/[^/]+/full", path):
        return " — полный текст документа"
    if re.search(r"/documents/[^/]+$", path):
        return " — удаление документа"
    return None


def _openclaw_tool_label(name: str, args_json: str) -> str:
    """Короткая подпись — путь файла или начало команды."""
    args = _openclaw_parse_args(args_json)
    if name in ("read", "edit", "write"):
        path = args.get("path") or args.get("file_path") or args.get("filename") or ""
        if name == "read" and "skills/" in path and path.endswith("SKILL.md"):
            skill = path.split("skills/")[1].split("/")[0]
            return f" — сценарий `{skill}`"
        fname = path.split("/")[-1] if path else ""
        return f" — `{fname}`" if fname else (f" — `{path}`" if path else "")
    if name == "exec":
        cmd = str(args.get("command") or args.get("cmd") or "")
        finrag_label = _finrag_exec_log_label(cmd)
        if finrag_label:
            return finrag_label
        pres_label = _pres_exec_log_label(cmd)
        if pres_label:
            return pres_label
        parts = cmd.split()
        short = " ".join(parts[:5]) + ("…" if len(parts) > 5 else "")
        return f" — `{short}`" if short else ""
    return ""


def _openclaw_tool_status_label(name: str, args_json: str) -> str:
    """Текст спиннера: что сейчас происходит (5–6 слов)."""
    args = _openclaw_parse_args(args_json)
    if name == "read":
        path = args.get("path") or args.get("file_path") or ""
        if "skills/" in path and path.endswith("SKILL.md"):
            skill = path.split("skills/")[1].split("/")[0]
            return _FINRAG_SKILL_LABELS.get(skill, f"Читаю сценарий `{skill}`")
        fname = path.split("/")[-1] if path else "файл"
        return f"Читаю файл `{fname}`"
    if name == "exec":
        cmd = str(args.get("command") or args.get("cmd") or "")
        if not cmd and args_json:
            import re as _re
            m = _re.search(r'"command"\s*:\s*"(.*)', args_json, _re.DOTALL)
            if m:
                cmd = m.group(1).rstrip('\\"').replace('\\"', '"')
        finrag_label = _finrag_exec_status_label(cmd)
        if finrag_label:
            return finrag_label
        pres_label = _pres_exec_status_label(cmd)
        if pres_label:
            return pres_label
        parts = cmd.split()
        short = " ".join(parts[:4]) + ("…" if len(parts) > 4 else "")
        return f"Выполняю `{short}`" if short else "Выполняю команду"
    if name == "edit":
        path = args.get("path") or ""
        fname = path.split("/")[-1] if path else "файл"
        return f"Редактирую `{fname}`"
    if name == "write":
        path = args.get("path") or args.get("file_path") or ""
        fname = path.split("/")[-1] if path else "файл"
        return f"Создаю файл `{fname}`"
    return f"Вызываю `{name}`"


def _openclaw_tool_log_md(
    tool_acc: dict[int, dict[str, str]], current_idx: int | None
) -> str:
    """Markdown-лог tool calls: ⏳ для текущего, ✅ для завершённых."""
    lines = []
    for idx in sorted(tool_acc.keys()):
        t = tool_acc[idx]
        name = t.get("name") or "…"
        args = (t.get("arguments") or "").strip()
        label = _openclaw_tool_label(name, args)
        icon = "⏳" if idx == current_idx else "✅"
        lines.append(f"{icon} **`{name}`**{label}")
    return "\n\n".join(lines)


def _json_objects_from_text(text: str) -> list[dict]:
    import json as _json

    body = str(text or "").strip()
    if not body:
        return []
    out: list[dict] = []
    candidates = [body]
    start = 0
    while True:
        first, last = body.find("{", start), body.rfind("}")
        if first < 0 or last <= first:
            break
        candidates.append(body[first : last + 1])
        start = first + 1
        if start >= last:
            break
    for raw in candidates:
        try:
            data = _json.loads(raw)
        except Exception:
            continue
        if isinstance(data, dict):
            out.append(data)
    return out


def _extract_fi_records_from_tool_log(events: list[dict]) -> list[dict]:
    """Последний ответ /records/search из tool_result."""
    records: list[dict] = []
    for ev in events or []:
        if ev.get("event") != "tool_result":
            continue
        for data in _json_objects_from_text(ev.get("detail") or ""):
            recs = data.get("records")
            if not isinstance(recs, list):
                continue
            found = [
                r for r in recs
                if isinstance(r, dict) and str(r.get("record_id") or "").strip()
            ]
            if found:
                records = found
    return records


def _has_md_table(text: str) -> bool:
    return bool(re.search(r"(?m)^\s*\|.+\|", str(text or "")))


_NOTES_SEARCH_KEYS = (
    "clients", "products", "currencies", "date_from", "date_to", "record_type", "text",
    "exclude_clients", "created_by", "direction", "client_types",
)


def _dicts_from_blob(text: str) -> list[dict]:
    import json as _json

    found = list(_json_objects_from_text(text))
    extra: list[dict] = []
    for obj in found:
        for val in obj.values():
            if isinstance(val, str) and "{" in val:
                extra.extend(_json_objects_from_text(val))
    found.extend(extra)
    for match in re.finditer(r"-d\s+['\"](\{.*?\})['\"]", str(text or ""), re.S):
        try:
            data = _json.loads(match.group(1))
        except Exception:
            continue
        if isinstance(data, dict):
            found.append(data)
    return found


def _as_search_filters(data: dict) -> dict | None:
    if not isinstance(data, dict):
        return None
    if isinstance(data.get("records"), list):
        return None
    if "raw_text" in data or "client_canonical" in data or "names" in data:
        return None
    picked = {
        key: data[key]
        for key in _NOTES_SEARCH_KEYS
        if data.get(key) not in (None, "", [])
    }
    if picked or ("team" in data and any(key in data for key in ("limit", "record_type", "clients", "products"))):
        return picked
    return None


def _notes_search_from_log(events: list[dict]) -> tuple[dict, list[dict] | None]:
    """Последний /records/search агента: его фильтры и records."""
    pending: dict = {}
    last_filters: dict = {}
    last_recs: list[dict] | None = None
    last_filtered: tuple[dict, list[dict]] | None = None
    for ev in events or []:
        blob = str(ev.get("detail") or "")
        search_url = "/records/search" in blob
        for data in _dicts_from_blob(blob):
            if isinstance(data.get("records"), list):
                recs = [
                    rec for rec in data["records"]
                    if isinstance(rec, dict) and str(rec.get("record_id") or "").strip()
                ]
                last_recs = recs
                if pending:
                    last_filtered = (dict(pending), recs)
                continue
            filt = _as_search_filters(data)
            if filt is not None or search_url:
                pending = filt or {}
                last_filters = pending
    if last_filtered:
        return last_filtered
    return last_filters, last_recs


_NOTES_RETURNED_RE = re.compile(
    r'\\?"total\\?"\s*:\s*\d+\s*,\s*\\?"returned\\?"\s*:\s*(\d+)\s*,\s*\\?"records\\?"'
)


def _notes_search_returned(events: list[dict]) -> int | None:
    """returned последнего /records/search. Лог режется по длине, а это поле в начале JSON."""
    returned = None
    for ev in events or []:
        if ev.get("event") != "tool_result":
            continue
        for match in _NOTES_RETURNED_RE.finditer(str(ev.get("detail") or "")):
            returned = int(match.group(1))
    return returned


def _notes_full_list(team: str, ids: list[str], filters: dict, events: list[dict]) -> list[dict]:
    """Агент переписывает в маркер не все id. Берём выборку целиком, если она та же, что видел агент."""
    returned = _notes_search_returned(events)
    if not returned or len(ids) >= returned:
        return []
    notes = _notes_fetch(team, filters)
    got = {str(rec.get("record_id") or "") for rec in notes}
    if len(notes) != returned or not set(ids) <= got:
        return []
    return notes


# Раньше: таблица источников FI под чатом (/fi_notes). Отключена по запросу.
FI_NOTES_TABLE_CTX_KEY = "fi_notes_table_ctx"


def _current_synaptica_username() -> str:
    username = str(st.session_state.get("username", "") or "").strip()
    return username or "unknown"


def _with_fi_actor_context(user_input: str) -> str:
    username = _current_synaptica_username()
    team = notes_team_for_user(username)
    token = publish_notes_actor(username)
    desk = notes_fi_desk_for_user(username)
    desk_line = ""
    if desk == "master":
        desk_line = (
            "fi_desk: master. Видишь все заметки FI: деривативы, кредитные, ликвидность. "
            "Чужое направление в запрос не подставляй — сервис отдаёт все само.\n"
        )
    elif desk:
        names = {"derivatives": "деривативы", "credit": "кредитные", "liquidity": "ликвидность"}
        desk_line = (
            f"fi_desk: {desk} ({names.get(desk, desk)}). "
            "Видишь и пишешь только заметки этого направления. "
            "Сервис отклоняет чужое направление.\n"
        )
    return (
        f"<synaptica_context>\n"
        f"current_username: {username}\n"
        f"notes_team: {team}\n"
        f"notes_token: {token}\n"
        f"{desk_line}"
        f"Сегодня: сначала в терминале `date +%Y-%m-%d`; год/день бери только оттуда, не из памяти.\n"
        f"Каждый запрос к 127.0.0.1:18003 — заголовок X-Notes-Token: {token} и team=\"{team}\". "
        f"Сервис открывает базу по токену и отклоняет другой team.\n"
        f"Список заметок («покажи/выведи/какие/список/уже в базе») — POST /records/search, не count, "
        f"и отдельной строкой <!-- NOTES_LIST ids=id1,id2 --> только с record_id, которые показываешь "
        f"(без ids карточек не будет; маркер не ставь, если выборку сузил сам — например «только банки»).\n"
        f"Клиента не угадывай: бери только matched из POST /clients; candidates, даже один, — спроси "
        f"«имел в виду …?». «Сбер» — это мы, не клиент.\n"
        f"Остальные правила (формат превью, типы заметок, гемба, длительность, правка/удаление, "
        f"таблица «кто», дыры одним списком, синонимы) — в твоих инструкциях агента, следуй им.\n"
        f"</synaptica_context>\n\n"
        f"{user_input}"
    )


def _with_calls_context(user_input: str) -> str:
    """Жёстко прокидываем ТБ-охват юзера в запрос агента звонков.

    bank выбирает не LLM, а вызывающий код — иначе юзер мог бы расширить охват
    за пределы своего ТБ. Источник правды — CALLS_USER_BANK (resolve_calls_bank).
    """
    username = _current_synaptica_username()
    bank = resolve_calls_bank(username) or "ALL"
    return (
        f"<synaptica_context>\n"
        f"current_username: {username}\n"
        f"user_bank_scope: {bank}\n"
        f"ACCESS RULE: в КАЖДОМ вызове /calls/* передавай user_bank_scope=\"{bank}\". "
        f"Сервис сам ограничит выборку доступным ТБ; ты не можешь его расширить.\n"
        f"</synaptica_context>\n\n"
        f"{user_input}"
    )


# Директива агента звонков на отрисовку сводной таблицы /calls_tb под ответом.
# Ожидаемый формат (как <!--CHART:...-->): <!--CALLS_TABLE: {"need_statistics": true, ...}-->
# Но агент может оформить иначе (код-блок / просто JSON) — поэтому ищем сам JSON с need_statistics.
_CALLS_TABLE_JSON_RE = re.compile(r'\{[^{}]*"need_statistics"[^{}]*\}', re.DOTALL)
# Для вырезания из показа: и комментарий-обёртку, и возможные код-форсы/метку вокруг JSON.
_CALLS_TABLE_STRIP_RE = re.compile(
    r'`{0,3}\s*(?:json)?\s*(?:<!--)?\s*(?:CALLS_TABLE:)?\s*\{[^{}]*"need_statistics"[^{}]*\}\s*(?:-->)?\s*`{0,3}',
    re.DOTALL,
)


def _parse_calls_table_directive(text: str) -> dict | None:
    """Извлекает JSON-директиву таблицы из ответа агента (или None). Устойчиво к обёртке."""
    import json
    if not text:
        return None
    for m in _CALLS_TABLE_JSON_RE.finditer(text):
        try:
            data = json.loads(m.group(0))
        except Exception:
            continue
        if not isinstance(data, dict) or not data.get("need_statistics"):
            continue
        if not str(data.get("dt_from") or "").strip() or not str(data.get("dt_to") or "").strip():
            continue
        return data
    return None


def _strip_calls_table_marker(text: str) -> str:
    """Убирает директиву таблицы из видимого/сохраняемого текста."""
    return _CALLS_TABLE_STRIP_RE.sub("", text or "").strip()


# Таблица источников Corp под чатом (/corp_notes).
CORP_NOTES_TABLE_CTX_KEY = "corp_notes_table_ctx"
CORP_NOTES_TABLE_LIMIT = 50


def _store_corp_notes_table_ctx(events: list[dict], query_preview: str = "") -> None:
    """Кладёт records из /corp/search (tool log) в session_state для таблицы внизу."""
    notes = _extract_fi_records_from_tool_log(events)
    if not notes:
        return
    st.session_state[CORP_NOTES_TABLE_CTX_KEY] = {
        "records": notes[:CORP_NOTES_TABLE_LIMIT],
        "query_preview": (query_preview or "")[:120],
    }


def _with_corp_actor_context(user_input: str) -> str:
    username = _current_synaptica_username()
    return (
        f"<synaptica_context>\n"
        f"current_username: {username}\n"
        f"Corp authoring rule: when creating a note, pass created_by=\"{username}\".\n"
        f"</synaptica_context>\n\n"
        f"{user_input}"
    )


def render_corp_notes_interactive_dataframe() -> None:
    """Таблица заметок Corp под чатом (источники последнего /corp/search)."""
    if not getattr(st.session_state, "openclaw_corp_sales_mode", False):
        st.session_state.pop(CORP_NOTES_TABLE_CTX_KEY, None)
        return

    ctx = st.session_state.get(CORP_NOTES_TABLE_CTX_KEY)
    if not ctx or not ctx.get("records"):
        return

    records = ctx["records"]
    rows: list[dict[str, str]] = []
    for r in records:
        products = r.get("products") or []
        if isinstance(products, list):
            prod_str = ", ".join(str(p) for p in products if p)
        else:
            prod_str = str(products or "")
        raw_text = str(r.get("raw_text") or "").strip()
        summary = str(r.get("summary") or "").strip()
        note_text = raw_text or summary or "—"
        author = str(r.get("created_by") or "—")
        rows.append({
            "Дата": str(r.get("date") or "—"),
            "Клиент": str(r.get("client") or "—"),
            "Продукты": prod_str,
            "Автор": author,
            "Заметка": note_text,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return

    qp = str(ctx.get("query_preview") or "").strip()
    st.divider()
    st.markdown("**📋 Источники из базы Corp**")
    if qp:
        st.caption(f"По запросу: «{qp}»")

    try:
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Дата": st.column_config.TextColumn("Дата", width="small"),
                "Клиент": st.column_config.TextColumn("Клиент", width="small"),
                "Продукты": st.column_config.TextColumn("Продукты", width="small"),
                "Автор": st.column_config.TextColumn("Автор", width="small"),
                "Заметка": st.column_config.TextColumn("Заметка", width="small"),
            },
        )
    except Exception:
        st.dataframe(df, use_container_width=True, hide_index=True)


NOTES_LIST_CTX_KEY = "notes_list_ctx"
NOTES_EDIT_ID_KEY = "notes_edit_id"
NOTES_DELETE_ID_KEY = "notes_delete_id"
NOTES_EDIT_FLASH_KEY = "notes_edit_flash"
NOTES_PAGE_KEY = "notes_list_page"
NOTES_PAGE_SIZE = 10


def _active_notes_team() -> str | None:
    if st.session_state.get("openclaw_fi_sales_mode"):
        return notes_team_for_user(_current_synaptica_username())
    return None


_NOTES_LIST_RE = re.compile(
    r"<!--\s*NOTES_LIST(?:\s+ids=([0-9A-Za-z_,.\-]*))?\s*-->",
    re.IGNORECASE,
)


def _notes_list_marked(text: str) -> bool:
    return _NOTES_LIST_RE.search(str(text or "")) is not None


def _notes_list_ids(text: str) -> list[str]:
    found = _NOTES_LIST_RE.search(str(text or ""))
    if not found or not found.group(1):
        return []
    ids: list[str] = []
    for part in found.group(1).split(","):
        rid = part.strip()
        if rid and rid not in ids:
            ids.append(rid)
    return ids


def _strip_notes_list_marker(text: str) -> str:
    return _NOTES_LIST_RE.sub("", str(text or "")).strip()


def _notes_is_preview(text: str) -> bool:
    low = str(text or "").lower()
    return "верно?" in low and any(
        word in low for word in ("потребность", "мероприятие", "общая информация")
    )


_NOTES_LIST_NOISE = frozenset({
    "покажи", "выведи", "список", "все", "всё", "заметки", "заметку", "заметка",
    "заметках", "какие", "какая", "какой", "уже", "базе", "база", "есть", "что",
    "у", "по", "за", "мне", "мои", "мой", "в", "на", "и", "или", "из", "от",
    "для", "про", "это", "эти", "их", "её", "ее", "нам", "сегодня", "вчера",
})


def _notes_user_wants_list(user_text: str) -> bool:
    text = " ".join(str(user_text or "").lower().split())
    if not text:
        return False
    show = any(
        word in text
        for word in (
            "покажи",
            "выведи",
            "список",
            "все заметк",
            "заметки за",
            "заметки по",
            "какие заметк",
            "уже в базе",
        )
    )
    count_only = any(word in text for word in ("сколько", "количество", "число"))
    if count_only and not show:
        return False
    return show or text in {"её", "ее", "их", "все"}


def _notes_question_words(user_text: str) -> list[str]:
    import re

    return [
        word for word in re.findall(r"[0-9a-zа-яё]{2,}", str(user_text or "").lower().replace("ё", "е"))
        if word not in _NOTES_LIST_NOISE
    ]


def _notes_question_clients(team: str, user_text: str) -> list[str]:
    """Клиент из фразы, если лог поиска пуст. «по ПСБ» не превращается во всю базу."""
    import requests

    words = _notes_question_words(user_text)
    if not words:
        return []
    names = words[:8]
    try:
        resp = requests.post(
            "http://127.0.0.1:18003/clients",
            json={"team": team, "names": names},
            headers={"X-Notes-Token": mint_notes_token(_current_synaptica_username())},
            timeout=30,
        )
        data = resp.json() if resp.status_code < 400 else {}
    except Exception:
        return []
    matched = data.get("matched") if isinstance(data, dict) else None
    if not isinstance(matched, dict):
        return []
    out: list[str] = []
    for name in matched.values():
        if name and name not in out:
            out.append(name)
    return out


def _notes_fetch(team: str, filters: dict | None = None, limit: int = 0) -> list[dict]:
    import requests

    body = {"team": team, "limit": limit}
    for key in _NOTES_SEARCH_KEYS:
        value = (filters or {}).get(key)
        if value not in (None, "", []):
            body[key] = value
    body.setdefault("record_type", "all")
    try:
        resp = requests.post(
            "http://127.0.0.1:18003/records/search",
            json=body,
            headers={"X-Notes-Token": mint_notes_token(_current_synaptica_username())},
            timeout=30,
        )
    except Exception:
        return []
    if resp.status_code >= 400:
        return []
    try:
        data = resp.json()
    except Exception:
        return []
    recs = data.get("records") if isinstance(data, dict) else None
    if not isinstance(recs, list):
        return []
    return [rec for rec in recs if isinstance(rec, dict) and str(rec.get("record_id") or "").strip()]


def _notes_card_record(rec: dict) -> dict:
    """Сырая запись сервиса и уже сжатая карточка — одни и те же поля для UI."""
    if rec.get("client") or rec.get("date"):
        return rec
    return {
        "record_id": str(rec.get("record_id") or "").strip(),
        "date": rec.get("interaction_date") or "",
        "meeting_start": rec.get("meeting_start") or "",
        "meeting_end": rec.get("meeting_end") or "",
        "meeting_duration": rec.get("meeting_duration") or "",
        "client": rec.get("client_name_norm") or rec.get("client_name_raw") or "",
        "products": rec.get("products") or [],
        "currencies": rec.get("currencies") or [],
        "summary": rec.get("short_summary") or rec.get("summary") or "",
        "raw_text": rec.get("raw_text") or "",
        "status": rec.get("status") or "",
        "next_step": rec.get("next_step") or "",
        "record_type": rec.get("record_type") or "",
        "event_kind": rec.get("event_kind") or "",
        "created_by": rec.get("created_by") or "",
        "created_at": rec.get("created_at") or "",
    }


def _notes_fetch_ids(team: str, record_ids: list[str]) -> list[dict]:
    import requests

    token = mint_notes_token(_current_synaptica_username())
    out: list[dict] = []
    for rid in record_ids:
        try:
            resp = requests.get(
                f"http://127.0.0.1:18003/records/{rid}",
                params={"team": team},
                headers={"X-Notes-Token": token},
                timeout=30,
            )
        except Exception:
            continue
        if resp.status_code >= 400:
            continue
        try:
            data = resp.json()
        except Exception:
            continue
        rec = data.get("record") if isinstance(data, dict) else None
        if isinstance(rec, dict) and str(rec.get("record_id") or "").strip():
            out.append(_notes_card_record(rec))
    return out


def _notes_filters_specific(filters: dict | None) -> bool:
    data = filters or {}
    for key in _NOTES_SEARCH_KEYS:
        if key == "record_type":
            continue
        if data.get(key) not in (None, "", []):
            return True
    record_type = str(data.get("record_type") or "all").strip().lower()
    return record_type not in {"", "all"}


def _notes_query_negated(user_text: str) -> bool:
    text = f" {str(user_text or '').lower()} "
    return "кроме" in text or " без " in text


def _store_notes_list_ctx(
    events: list[dict],
    team: str | None,
    assistant_text: str = "",
    user_text: str = "",
) -> None:
    if not team:
        return
    if _has_md_table(assistant_text) or _notes_is_preview(assistant_text):
        had = NOTES_LIST_CTX_KEY in st.session_state
        st.session_state.pop(NOTES_LIST_CTX_KEY, None)
        st.session_state.pop(NOTES_EDIT_ID_KEY, None)
        st.session_state.pop(NOTES_DELETE_ID_KEY, None)
        if had:
            st.session_state["_notes_cards_pending_rerun"] = True
        return
    filters, _recs = _notes_search_from_log(events)
    marked = _notes_list_marked(assistant_text)
    ids = _notes_list_ids(assistant_text)
    found = any(
        phrase in str(assistant_text or "").lower()
        for phrase in ("нашёл заметк", "нашел заметк", "нашёл запис", "нашел запис")
    )
    count_only = any(
        word in str(user_text or "").lower()
        for word in ("сколько", "количество", "число")
    )
    wants = _notes_user_wants_list(user_text) or (found and not count_only)
    if not marked and not wants:
        return
    if count_only and not _notes_user_wants_list(user_text):
        st.session_state.pop(NOTES_LIST_CTX_KEY, None)
        st.session_state.pop(NOTES_EDIT_ID_KEY, None)
        st.session_state.pop(NOTES_DELETE_ID_KEY, None)
        return
    subject_words = _notes_question_words(user_text)
    bare = " ".join(str(user_text or "").lower().split())
    notes: list[dict] = []
    if ids:
        notes = _notes_full_list(team, ids, filters, events)
        if not notes:
            notes = _notes_fetch_ids(team, ids)
            filters = {"record_ids": ids}
    elif _notes_query_negated(user_text):
        notes = []
    elif subject_words and not _notes_filters_specific(filters):
        subject = _notes_question_clients(team, user_text)
        if subject:
            filters = {"clients": subject, "record_type": "all"}
    if not ids and not notes and _notes_filters_specific(filters) and not _notes_query_negated(user_text):
        notes = _notes_fetch(team, filters)
    if not ids and not notes and bare in {"её", "ее", "их"}:
        prev = st.session_state.get(NOTES_LIST_CTX_KEY) or {}
        prev_filters = prev.get("filters") or {}
        if _notes_filters_specific(prev_filters):
            notes = _notes_fetch(team, prev_filters)
            filters = prev_filters
    if not ids and not notes and wants and not subject_words and not _notes_query_negated(user_text):
        notes = _notes_fetch(team)
        filters = {}
    if not notes:
        st.session_state.pop(NOTES_LIST_CTX_KEY, None)
        st.session_state.pop(NOTES_EDIT_ID_KEY, None)
        st.session_state.pop(NOTES_DELETE_ID_KEY, None)
        return
    st.session_state[NOTES_LIST_CTX_KEY] = {
        "team": team,
        "records": notes,
        "filters": filters,
        "anchor_user": str(user_text or "").strip(),
    }
    st.session_state[NOTES_PAGE_KEY] = 0
    st.session_state.pop(NOTES_EDIT_ID_KEY, None)
    st.session_state.pop(NOTES_DELETE_ID_KEY, None)
    st.session_state["_notes_cards_pending_rerun"] = True


def notes_list_anchor_message_idx(messages) -> int | None:
    ctx = st.session_state.get(NOTES_LIST_CTX_KEY) or {}
    if not ctx.get("records"):
        return None
    user_n = str(ctx.get("anchor_user") or "").strip()
    found = None
    last_user = ""
    for i, msg in enumerate(messages or []):
        content = str(getattr(msg, "content", "") or "")
        kind = str(getattr(msg, "type", "") or "")
        if kind in ("human", "user"):
            last_user = content.strip()
            continue
        if kind not in ("ai", "assistant"):
            continue
        if _notes_list_marked(content) and not _has_md_table(content):
            found = i
        elif user_n and last_user == user_n and not _has_md_table(content):
            found = i
    return found


def _notes_author_short(login: str) -> str:
    """Фамилия И. О. того, кто записал заметку."""
    raw = str(login or "").strip()
    if not raw:
        return ""
    fio = NOTES_USER_FIO.get(raw.lower(), "")
    parts = [part for part in fio.replace(".", " ").split() if part]
    if not parts:
        return raw
    initials = " ".join(f"{part[0].upper()}." for part in parts[1:3])
    return f"{parts[0]} {initials}".strip()


def _notes_join(value) -> str:
    if isinstance(value, list):
        return ", ".join(str(x) for x in value if str(x).strip())
    return str(value or "")


def _notes_http_detail(resp) -> str:
    detail = (resp.text or "").strip()
    try:
        payload = resp.json()
        if isinstance(payload, dict) and payload.get("detail"):
            detail = str(payload["detail"])
    except Exception:
        pass
    return detail or f"HTTP {resp.status_code}"


def _notes_patch(team: str, record_id: str, body: dict) -> dict:
    import requests

    resp = requests.patch(
        f"http://127.0.0.1:18003/records/{record_id}",
        json={**body, "team": team},
        headers={"X-Notes-Token": mint_notes_token(_current_synaptica_username())},
        timeout=30,
    )
    if resp.status_code >= 400:
        raise RuntimeError(_notes_http_detail(resp))
    data = resp.json()
    record = data.get("record") if isinstance(data, dict) else None
    return record if isinstance(record, dict) else {}


def _notes_delete(team: str, record_id: str) -> None:
    import requests

    resp = requests.delete(
        f"http://127.0.0.1:18003/records/{record_id}",
        params={"team": team},
        headers={"X-Notes-Token": mint_notes_token(_current_synaptica_username())},
        timeout=30,
    )
    if resp.status_code >= 400:
        raise RuntimeError(_notes_http_detail(resp))


def _notes_edit_form(record: dict, team: str) -> None:
    rid = str(record.get("record_id") or "")
    text_key = f"notes_edit_text_{rid}"
    if text_key not in st.session_state:
        st.session_state[text_key] = str(record.get("raw_text") or "")
    st.text_area("Текст", key=text_key, height=160)
    save_col, cancel_col, _pad = st.columns([1.5, 1.2, 5], gap="small")
    save = save_col.button("Сохранить", key=f"notes_edit_save_{rid}", type="primary")
    cancel = cancel_col.button("Отмена", key=f"notes_edit_cancel_{rid}")
    if cancel:
        st.session_state.pop(NOTES_EDIT_ID_KEY, None)
        st.session_state.pop(text_key, None)
        st.rerun()
    if not save:
        return
    new_text = str(st.session_state.get(text_key) or "").strip()
    if not new_text:
        st.error("Текст не может быть пустым")
        return
    try:
        updated = _notes_patch(team, rid, {"raw_text": new_text})
    except Exception as exc:
        st.error(str(exc))
        return
    saved = str(updated.get("raw_text") or new_text).strip()
    summary = str(
        updated.get("summary") or updated.get("short_summary") or record.get("summary") or ""
    ).strip()
    ctx = st.session_state.get(NOTES_LIST_CTX_KEY) or {}
    records = list(ctx.get("records") or [])
    for i, item in enumerate(records):
        if str(item.get("record_id") or "") == rid:
            records[i] = {**item, "raw_text": saved, "summary": item.get("summary") or summary}
            break
    st.session_state[NOTES_LIST_CTX_KEY] = {**ctx, "records": records}
    st.session_state.pop(NOTES_EDIT_ID_KEY, None)
    st.session_state.pop(text_key, None)
    st.session_state[NOTES_EDIT_FLASH_KEY] = "Сохранено."
    st.rerun()


def render_notes_records_panel() -> None:
    """Карточки записей последнего поиска с кнопкой «Редактировать»."""
    team = _active_notes_team()
    if not team:
        return
    ctx = st.session_state.get(NOTES_LIST_CTX_KEY)
    if not ctx or ctx.get("team") != team or not ctx.get("records"):
        return

    records = list(ctx["records"])
    pages = max(1, (len(records) + NOTES_PAGE_SIZE - 1) // NOTES_PAGE_SIZE)
    page = int(st.session_state.get(NOTES_PAGE_KEY) or 0)
    if page < 0 or page >= pages:
        page = 0 if page < 0 else pages - 1
        st.session_state[NOTES_PAGE_KEY] = page
    chunk = records[page * NOTES_PAGE_SIZE : (page + 1) * NOTES_PAGE_SIZE]
    flash = st.session_state.pop(NOTES_EDIT_FLASH_KEY, None)
    edit_id = str(st.session_state.get(NOTES_EDIT_ID_KEY) or "")
    delete_id = str(st.session_state.get(NOTES_DELETE_ID_KEY) or "")
    if flash:
        st.success(flash)
    _render_notes_record_cards(chunk, team, edit_id, delete_id)
    if pages > 1:
        prev_col, label_col, next_col = st.columns([1, 2, 1])
        if prev_col.button("Назад", key="notes_page_prev", disabled=page == 0):
            st.session_state[NOTES_PAGE_KEY] = page - 1
            st.session_state.pop(NOTES_EDIT_ID_KEY, None)
            st.session_state.pop(NOTES_DELETE_ID_KEY, None)
            st.rerun()
        label_col.markdown(
            f"<div style='text-align:center'>{page + 1} / {pages}</div>",
            unsafe_allow_html=True,
        )
        if next_col.button("Дальше", key="notes_page_next", disabled=page >= pages - 1):
            st.session_state[NOTES_PAGE_KEY] = page + 1
            st.session_state.pop(NOTES_EDIT_ID_KEY, None)
            st.session_state.pop(NOTES_DELETE_ID_KEY, None)
            st.rerun()


_NOTES_EDIT_HOURS = 24


def _notes_can_edit(record: dict) -> bool:
    if normalize_username(record.get("created_by")) != normalize_username(_current_synaptica_username()):
        return False
    raw = str(record.get("created_at") or "").strip()
    created = None
    for fmt, size in (("%Y-%m-%dT%H:%M:%S", 19), ("%Y-%m-%d", 10)):
        try:
            created = datetime.strptime(raw[:size], fmt)
            break
        except ValueError:
            continue
    if created is None:
        return False
    return (datetime.now() - created).total_seconds() <= _NOTES_EDIT_HOURS * 3600


def _notes_md_text(value: str) -> str:
    text = str(value)
    for char in ("\\", "*", "_", "`", "[", "]"):
        text = text.replace(char, "\\" + char)
    return text


def _render_notes_record_cards(records: list, team: str, edit_id: str, delete_id: str = "") -> None:
    st.markdown(
        """
        <style>
        div[class*="st-key-notes_edit_btn_"] button,
        div[class*="st-key-notes_delete_btn_"] button {
            white-space: nowrap !important;
            overflow: visible !important;
            text-overflow: clip !important;
            min-width: max-content;
        }
        div[class*="st-key-notes_edit_btn_"] button p,
        div[class*="st-key-notes_delete_btn_"] button p {
            white-space: nowrap !important;
            overflow: visible !important;
            text-overflow: clip !important;
        }
        div[data-testid="column"]:has(div[class*="st-key-notes_edit_save_"]),
        div[data-testid="column"]:has(div[class*="st-key-notes_edit_cancel_"]) {
            flex: 0 0 auto !important;
            width: auto !important;
            min-width: max-content !important;
        }
        div[class*="st-key-notes_edit_save_"] button,
        div[class*="st-key-notes_edit_cancel_"] button {
            width: auto !important;
            min-width: max-content !important;
            white-space: nowrap !important;
            border-radius: 8px !important;
            min-height: 2.35rem;
            padding: 0.35rem 1.15rem !important;
            box-shadow: 0 1px 2px rgba(16, 24, 40, 0.06);
        }
        div[class*="st-key-notes_edit_save_"] button p,
        div[class*="st-key-notes_edit_cancel_"] button p {
            white-space: nowrap !important;
            overflow: visible !important;
            text-overflow: clip !important;
            line-height: 1.2 !important;
        }
        div[class*="st-key-notes_edit_save_"] button {
            background: #1f6b4a !important;
            color: #fff !important;
            border: 1px solid #1f6b4a !important;
        }
        div[class*="st-key-notes_edit_save_"] button p {
            color: #fff !important;
        }
        div[class*="st-key-notes_edit_cancel_"] button {
            background: #fff !important;
            color: #3f4654 !important;
            border: 1px solid #d0d5dd !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    shown: set[str] = set()
    for record in records:
        rid = str(record.get("record_id") or "")
        if not rid or rid in shown:
            continue
        shown.add(rid)
        date = str(record.get("date") or "без даты")
        client = str(record.get("client") or "—")
        when = "–".join(
            part for part in (
                str(record.get("meeting_start") or ""),
                str(record.get("meeting_end") or ""),
            ) if part
        )
        if not when and record.get("meeting_duration"):
            when = str(record.get("meeting_duration"))
        meta = " · ".join(
            part for part in (
                _notes_join(record.get("products")),
                _notes_join(record.get("currencies")),
                when,
            ) if part
        )
        text = str(record.get("raw_text") or "").strip()
        can_edit = _notes_can_edit(record)
        kind = str(record.get("event_kind") or "").strip()
        if kind in ("конференция", "гемба", "gemba"):
            kind = "мероприятие"
        rtype = str(record.get("record_type") or "").strip()
        title = kind or ("мероприятие" if rtype == "мероприятие" else client)
        if rtype == "мероприятие":
            head = f"**{_notes_md_text(date)} · {_notes_md_text(title)}**"
            if client and client != "внутренняя":
                head = f"{head} · {_notes_md_text(client)}"
        else:
            head = f"**{_notes_md_text(date)} · {_notes_md_text(client)}**"
        if meta:
            head = f"{head} · {_notes_md_text(meta)}"
        author = _notes_author_short(record.get("created_by"))
        if author:
            head = f"{head} · {_notes_md_text(author)}"
        lines = [head]
        if text:
            lines.append(_notes_md_text(text).replace("\n", "  \n"))
        with st.container(border=True):
            st.markdown("\n\n".join(lines))
            if can_edit:
                edit_col, delete_col, _pad = st.columns([2.8, 1.6, 3.6])
                if edit_id != rid and edit_col.button("Редактировать", key=f"notes_edit_btn_{rid}"):
                    st.session_state[NOTES_EDIT_ID_KEY] = rid
                    st.session_state.pop(NOTES_DELETE_ID_KEY, None)
                    st.rerun()
                if delete_id != rid and delete_col.button("Удалить", key=f"notes_delete_btn_{rid}"):
                    st.session_state[NOTES_DELETE_ID_KEY] = rid
                    st.session_state.pop(NOTES_EDIT_ID_KEY, None)
                    st.rerun()
            if can_edit and delete_id == rid:
                st.caption(f"Удалить заметку {date} · {client}?")
                yes_col, no_col = st.columns(2)
                if yes_col.button("Да, удалить", key=f"notes_delete_yes_{rid}"):
                    try:
                        _notes_delete(team, rid)
                    except Exception as exc:
                        st.error(str(exc))
                    else:
                        ctx = st.session_state.get(NOTES_LIST_CTX_KEY) or {}
                        kept = [
                            item for item in (ctx.get("records") or [])
                            if str(item.get("record_id") or "") != rid
                        ]
                        st.session_state[NOTES_LIST_CTX_KEY] = {**ctx, "records": kept}
                        st.session_state.pop(NOTES_DELETE_ID_KEY, None)
                        st.session_state[NOTES_EDIT_FLASH_KEY] = "Удалено."
                        st.rerun()
                if no_col.button("Отмена", key=f"notes_delete_no_{rid}"):
                    st.session_state.pop(NOTES_DELETE_ID_KEY, None)
                    st.rerun()
            if can_edit and edit_id == rid:
                _notes_edit_form(record, team)


def _openclaw_friendly_error(exc: BaseException, gateway_url: str) -> str:
    import requests

    base_hint = (
        "\n\n*Проверьте: gateway запущен, в `~/.openclaw/openclaw1.json` включён* "
        "`gateway.http.endpoints.chatCompletions.enabled: true`*, перезапуск* "
        "`openclaw gateway restart`*. Подробнее:* `docs/synaptica-coder/GATEWAY_HTTP_SETUP.md`."
    )
    if isinstance(exc, requests.exceptions.ConnectionError):
        return (
            f"🦞 ⚠️ **Нет соединения** с OpenClaw Gateway (`{gateway_url}`). "
            "Процесс gateway не слушает порт или недоступен с хоста Streamlit."
            + base_hint
        )
    if isinstance(exc, requests.exceptions.Timeout):
        return f"🦞 ⚠️ **Таймаут** запроса к Gateway (`{gateway_url}`)." + base_hint
    if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        snippet = (exc.response.text or "")[:800].strip()
        body = f"\n```\n{snippet}\n```" if snippet else ""
        if status == 401:
            msg = "неверный или пустой `OPENCLAW_API_KEY` (Bearer для gateway)."
        elif status == 404:
            msg = (
                "маршрут `/v1/responses` не найден — убедись что в конфиге gateway включено "
                "`responses: {enabled: true}` в секции `gateway.http.endpoints`."
            )
        elif status == 405:
            msg = "метод не разрешён — эндпоинт выключен или другой путь API."
        else:
            msg = f"HTTP {status}."
        return f"🦞 ⚠️ **OpenClaw Gateway:** {msg}{body}{base_hint}"
    return f"🦞 ⚠️ **Ошибка OpenClaw** (`{gateway_url}`): {str(exc)}" + base_hint


def _openclaw_active_tool_progress_label(
    events: list[dict],
    *,
    sse_tool_calls: dict[str, dict] | None = None,
    sse_active_id: str | None = None,
) -> str | None:
    """Текст спиннера для инструмента, который ещё выполняется."""
    n_calls = sum(1 for ev in events if ev.get("event") == "tool_call")
    n_results = sum(1 for ev in events if ev.get("event") == "tool_result")
    if n_calls > n_results:
        last_call = next(
            (ev for ev in reversed(events) if ev.get("event") == "tool_call"),
            None,
        )
        if last_call:
            name = last_call.get("tool", "")
            detail = last_call.get("detail", "") or ""
            return f"🦞 {_openclaw_tool_status_label(name, detail)}…"
    if sse_active_id and sse_tool_calls and sse_active_id in sse_tool_calls:
        t = sse_tool_calls[sse_active_id]
        if t.get("name"):
            return f"🦞 {_openclaw_tool_status_label(t['name'], t['arguments'])}…"
    return None


def _openclaw_idle_progress_label(agent_id: str, user_input: str) -> str:
    """Пока агент думает и tool-событий ещё нет."""
    snippet = (user_input or "").strip()[:35]
    if agent_id == "pres-gen":
        if snippet:
            return f"🦞 Готовлю презентацию: `{snippet}`"
        return "🦞 Генерирую презентацию"
    if agent_id == "client-helper":
        if snippet:
            return f"🦞 Изучаю запрос: `{snippet}`"
        return "🦞 Изучаю документы"
    return "🦞 Получаю ответ от агента…"


def _call_openclaw_streaming(
    user_input: str,
    gateway_url: str,
    api_key: str,
    agent_id: str = "synaptica-coder",
    previous_response_id: str | None = None,
    show_tool_details: bool = True,
    table_query_preview: str | None = None,
) -> tuple[str, str | None]:
    import json

    import requests

    gateway_base = gateway_url.rstrip("/")

    headers = {"Content-Type": "application/json", "Accept": "text/event-stream"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    headers["x-openclaw-agent-id"] = agent_id

    payload: dict = {
        "model": f"openclaw:{agent_id}",
        "input": user_input,
        "stream": True,
    }
    if previous_response_id:
        payload["previous_response_id"] = previous_response_id

    full_response = ""
    response_id: str | None = None
    tool_calls: dict[str, dict] = {}   # id → {name, arguments}
    current_tool_id: str | None = None

    # ── Фоновый поток: читает /tmp/openclaw1-tools.jsonl пока идёт стрим ──
    import queue as _queue
    import threading, time as _time, json as _json

    _TOOL_EVENTS_FILE = "/tmp/openclaw1-tools.jsonl"
    _log_tool_calls: list[dict] = []   # накапливаем здесь
    _log_stop = threading.Event()
    _req_ts = _time.time()             # события только после этой отметки

    def _tail_tool_log():
        try:
            with open(_TOOL_EVENTS_FILE, "r", encoding="utf-8") as fh:
                fh.seek(0, 2)  # встаём в конец файла
                while not _log_stop.is_set():
                    line = fh.readline()
                    if not line:
                        _time.sleep(0.2)
                        continue
                    try:
                        ev = _json.loads(line)
                    except Exception:
                        continue
                    if ev.get("ts", 0) < _req_ts:
                        continue
                    _ev_agent = ev.get("agent")
                    # Иногда прокси не может извлечь agent_id и ставит "?".
                    # Такие события всё равно относятся к нашему запросу
                    # (отсечены по timestamp) — берём их тоже.
                    if _ev_agent and _ev_agent != "?" and _ev_agent != agent_id:
                        continue
                    _log_tool_calls.append(ev)
        except Exception:
            pass  # файл ещё не существует — ждём

    _log_thread = threading.Thread(target=_tail_tool_log, daemon=True)
    _log_thread.start()

    def _render_log_tools() -> str:
        if not show_tool_details:
            return ""
        # Build ordered list of (call_ev, result_ev|None) pairs
        result_queues: dict[str, list[dict]] = {}
        for ev in _log_tool_calls:
            if ev["event"] == "tool_result":
                result_queues.setdefault(ev.get("tool", "?"), []).append(ev)

        result_cursors: dict[str, int] = {}
        blocks: list[str] = []
        for ev in _log_tool_calls:
            if ev["event"] != "tool_call":
                continue
            t = ev.get("tool", "?")
            idx = result_cursors.get(t, 0)
            results = result_queues.get(t, [])
            result_ev = results[idx] if idx < len(results) else None
            result_cursors[t] = idx + 1

            icon = "✅" if result_ev is not None else "⏳"
            detail = ev.get("detail", "")
            label = _openclaw_tool_label(t, detail)
            block = f"{icon} **`{t}`**{label}"

            # Args JSON block
            if detail:
                try:
                    args_obj = _json.loads(detail)
                    args_str = _json.dumps(args_obj, ensure_ascii=False, indent=2)
                except Exception:
                    args_str = detail
                block += f"\n```json\n{args_str}\n```"

            # Output block
            if result_ev is not None:
                output = (result_ev.get("detail") or "").strip()
                if output:
                    output_short = output[:300] + ("…" if len(output) > 300 else "")
                    # escape blockquote: replace newlines with space
                    output_line = output_short.replace("\n", " ")
                    block += f"\n> `{output_line}`"

            blocks.append(block)

        return "\n\n---\n\n".join(blocks)

    # empty().container() — один слот вместо spinner+answer, иначе ghost прошлого ответа.
    with st.chat_message("assistant"), st.empty().container():
        answer_placeholder = st.empty()
        status = None
        tool_placeholder = None
        spinner_label = None

        if show_tool_details:
            status = st.status(
                "🦞 Анализирую запрос…",
                expanded=True,
            )
            with status:
                tool_placeholder = st.empty()
        else:
            spinner_label = st.empty()
            spinner_label.markdown(
                _render_openclaw_simple_spinner("🦞 Анализирую запрос…"),
                unsafe_allow_html=True,
            )

        def _update_progress(
            label: str,
            *,
            prog_state: str | None = None,
            expanded: bool | None = None,
        ) -> None:
            if show_tool_details and status is not None:
                kwargs = {"label": label}
                if prog_state is not None:
                    kwargs["state"] = prog_state
                if expanded is not None:
                    kwargs["expanded"] = expanded
                try:
                    status.update(**kwargs)
                except Exception:
                    pass
            elif spinner_label is not None and prog_state not in ("complete", "error"):
                try:
                    spinner_label.markdown(
                        _render_openclaw_simple_spinner(label),
                        unsafe_allow_html=True,
                    )
                except Exception:
                    pass

        _first_sse_seen = False
        _stream_label_state = "init"  # init → connected → tool → answering
        _label_lock = threading.Lock()
        _last_seen_count = 0

        def _refresh_from_tool_log(*, force: bool = False) -> None:
            """Подтягивает свежие tool-события из /tmp/openclaw1-tools.jsonl
            и сразу двигает label/плейсхолдер. Безопасно вызывать только из
            основного потока (между SSE-чанками или при poll-таймауте)."""
            nonlocal _last_seen_count, _stream_label_state
            with _label_lock:
                active_label = _openclaw_active_tool_progress_label(
                    _log_tool_calls,
                    sse_tool_calls=tool_calls,
                    sse_active_id=current_tool_id,
                )
                has_new = len(_log_tool_calls) != _last_seen_count
                if not has_new and not force and not active_label:
                    return
                if has_new:
                    _last_seen_count = len(_log_tool_calls)
                    log_md = _render_log_tools()
                    if log_md and show_tool_details and tool_placeholder is not None:
                        try:
                            tool_placeholder.markdown(log_md)
                        except Exception:
                            pass
                if active_label:
                    _update_progress(active_label)
                    _stream_label_state = "tool"
                    return
                if _stream_label_state == "answering":
                    return
                if _stream_label_state in ("connected", "init") and not _log_tool_calls:
                    _update_progress(_openclaw_idle_progress_label(agent_id, user_input))
                    return
                last_call = next(
                    (ev for ev in reversed(_log_tool_calls) if ev.get("event") == "tool_call"),
                    None,
                )
                if not last_call:
                    return
                _name = last_call.get("tool", "")
                _detail = last_call.get("detail", "") or ""
                _update_progress(f"🦞 {_openclaw_tool_status_label(_name, _detail)}…")
                _stream_label_state = "tool"

        def _maybe_show_answering_progress() -> None:
            nonlocal _stream_label_state
            active_label = _openclaw_active_tool_progress_label(
                _log_tool_calls,
                sse_tool_calls=tool_calls,
                sse_active_id=current_tool_id,
            )
            if active_label:
                _update_progress(active_label)
                _stream_label_state = "tool"
            elif _stream_label_state != "answering":
                _update_progress("🦞 Формирую ответ…")
                _stream_label_state = "answering"

        # UI обновляем только из основного потока (между SSE-чанками).
        # Фоновый тикер с st.* из другого потока ломает SessionInfo Streamlit.

        try:
            url = f"{gateway_base}/responses"
            with requests.post(url, json=payload, headers=headers, stream=True, timeout=(30, 600)) as resp:
                resp.raise_for_status()

                buf = ""
                event_type = ""
                stream_done = False
                _sse_q: _queue.Queue = _queue.Queue()
                _reader_exc: list[BaseException | None] = [None]

                def _sse_reader() -> None:
                    try:
                        for chunk in resp.iter_content(chunk_size=4096, decode_unicode=False):
                            if chunk:
                                _sse_q.put(chunk)
                        _sse_q.put(None)
                    except BaseException as exc:
                        _reader_exc[0] = exc
                        _sse_q.put(None)

                threading.Thread(target=_sse_reader, daemon=True).start()

                while not stream_done:
                    try:
                        chunk = _sse_q.get(timeout=0.25)
                    except _queue.Empty:
                        _refresh_from_tool_log(force=True)
                        continue
                    if chunk is None:
                        if _reader_exc[0] is not None:
                            raise _reader_exc[0]
                        break
                    buf += chunk.decode("utf-8", errors="replace") if isinstance(chunk, bytes) else str(chunk)

                    while "\n" in buf and not stream_done:
                        line, buf = buf.split("\n", 1)
                        line = line.rstrip("\r")

                        if not line:
                            event_type = ""
                            continue
                        if line.startswith(":"):
                            continue
                        if line.startswith("event:"):
                            event_type = line[6:].strip()
                            continue
                        if not line.startswith("data:"):
                            continue

                        data_str = line[5:].strip()
                        if data_str == "[DONE]":
                            stream_done = True
                            break
                        try:
                            obj = json.loads(data_str)
                        except json.JSONDecodeError:
                            continue

                        et = event_type or obj.get("type") or ""

                        if not _first_sse_seen:
                            _first_sse_seen = True
                            if _stream_label_state == "init":
                                _update_progress(_openclaw_idle_progress_label(agent_id, user_input))
                                _stream_label_state = "connected"

                        _refresh_from_tool_log()

                        if not et and isinstance(obj.get("choices"), list) and obj["choices"]:
                            _choice = obj["choices"][0] or {}
                            _delta = _choice.get("delta") or {}
                            _tc_delta = _delta.get("tool_calls")
                            if isinstance(_tc_delta, list):
                                for _tc in _tc_delta:
                                    if not isinstance(_tc, dict):
                                        continue
                                    _idx = _tc.get("index", 0)
                                    _tid = _tc.get("id") or f"cc_{_idx}"
                                    _fn = _tc.get("function") or {}
                                    if _tid not in tool_calls:
                                        tool_calls[_tid] = {
                                            "name": _fn.get("name") or "",
                                            "arguments": "",
                                        }
                                    if _fn.get("name"):
                                        tool_calls[_tid]["name"] = _fn["name"]
                                    if _fn.get("arguments") is not None:
                                        tool_calls[_tid]["arguments"] += str(
                                            _fn.get("arguments", "")
                                        )
                                    current_tool_id = _tid
                                    _t = tool_calls[_tid]
                                    _update_progress(
                                        f"🦞 {_openclaw_tool_status_label(_t['name'], _t['arguments'])}…"
                                    )
                                    _stream_label_state = "tool"
                                if show_tool_details and tool_placeholder is not None:
                                    tool_placeholder.markdown(
                                        _render_tool_calls(tool_calls, current_tool_id)
                                    )
                            _content = _delta.get("content")
                            if _content:
                                full_response += _content
                                _maybe_show_answering_progress()
                                answer_placeholder.markdown(full_response + "▌")
                            if _choice.get("finish_reason"):
                                stream_done = True
                                break

                        if et in ("response.completed", "response.done"):
                            response_id = obj.get("response", {}).get("id")
                            stream_done = True
                            break

                        if et == "response.error":
                            err_msg = obj.get("error", {})
                            if isinstance(err_msg, dict):
                                err_msg = err_msg.get("message") or str(err_msg)
                            full_response = f"🦞 ⚠️ **Ошибка агента:** {err_msg}"
                            stream_done = True
                            break

                        if et == "response.output_item.added":
                            item = obj.get("item", {})
                            if item.get("type") == "function_call":
                                tid = item.get("id") or item.get("call_id") or f"tc_{len(tool_calls)}"
                                name = item.get("name", "")
                                tool_calls[tid] = {"name": name, "arguments": ""}
                                current_tool_id = tid
                                _update_progress(f"🦞 {_openclaw_tool_status_label(name, '')}…")
                                _stream_label_state = "tool"
                                if show_tool_details and tool_placeholder is not None:
                                    tool_placeholder.markdown(_render_tool_calls(tool_calls, tid))

                        elif et == "response.function_call_arguments.delta":
                            tid = obj.get("item_id") or current_tool_id
                            if tid and tid in tool_calls:
                                tool_calls[tid]["arguments"] += obj.get("delta", "")
                                t = tool_calls[tid]
                                _update_progress(
                                    f"🦞 {_openclaw_tool_status_label(t['name'], t['arguments'])}…"
                                )
                                _stream_label_state = "tool"
                                if show_tool_details and tool_placeholder is not None:
                                    tool_placeholder.markdown(_render_tool_calls(tool_calls, tid))

                        elif et == "response.output_item.done":
                            item = obj.get("item", {})
                            if item.get("type") == "function_call":
                                current_tool_id = None
                                if show_tool_details and tool_placeholder is not None:
                                    tool_placeholder.markdown(_render_tool_calls(tool_calls, None))

                        elif et in ("response.output_text.delta", "response.text.delta"):
                            delta = obj.get("delta", "")
                            if delta:
                                full_response += delta
                                _maybe_show_answering_progress()
                                answer_placeholder.markdown(full_response + "▌")

            _refresh_from_tool_log(force=True)
            _time.sleep(0.3)
            log_md_final = _render_log_tools()
            n_log = sum(1 for ev in _log_tool_calls if ev["event"] == "tool_call")
            if log_md_final and show_tool_details and tool_placeholder is not None:
                tool_placeholder.markdown(log_md_final)
            if n_log:
                if show_tool_details:
                    word = "инструмент" if n_log == 1 else ("инструмента" if n_log < 5 else "инструментов")
                    _update_progress(
                        f"✅ Выполнено {n_log} {word}",
                        prog_state="complete",
                        expanded=False,
                    )
                else:
                    _update_progress("✅ Готово", prog_state="complete")
            elif tool_calls:
                if show_tool_details:
                    n = len(tool_calls)
                    word = "инструмент" if n == 1 else ("инструмента" if n < 5 else "инструментов")
                    _update_progress(
                        f"✅ Выполнено {n} {word}",
                        prog_state="complete",
                        expanded=False,
                    )
                    if tool_placeholder is not None:
                        tool_placeholder.markdown(_render_tool_calls(tool_calls, None))
                else:
                    _update_progress("✅ Готово", prog_state="complete")
            else:
                _update_progress("✅ Готово", prog_state="complete")

            if spinner_label is not None:
                spinner_label.empty()

            if agent_id == "client-helper":
                full_response = _ensure_fin_report_marker(full_response, _log_tool_calls)

            if full_response:
                _docx_paths = re.findall(r'(/[\w/.\-]+\.docx)', full_response)
                _xlsx_paths = list(dict.fromkeys(re.findall(r'(/[\w/.\-]+\.xlsx)', full_response)))
                _pdf_paths = _collect_pdf_paths(full_response)
                _pptx_paths = list(dict.fromkeys(re.findall(r'(/[\w/.\-]+\.pptx)', full_response)))
                _pptx_urls = list(dict.fromkeys(
                    re.findall(r'(https?://[^\s)\]]+/presentation/download/[^\s)\]]+)', full_response)
                ))
                _display = full_response
                if _docx_paths:
                    _display = re.sub(r'[^\n]*(/[\w/.\-]+\.docx)[^\n]*\n?', '', _display).strip()
                if _xlsx_paths:
                    _display = re.sub(r'[^\n]*(/[\w/.\-]+\.xlsx)[^\n]*\n?', '', _display).strip()
                # Презентация: всегда убираем http-ссылку на скачивание и маркер пути —
                # вместо них показываем нативную кнопку ниже (даже если агент дописал ссылку сам).
                _display = re.sub(r'\[[^\]]*\]\(https?://[^)]*?/presentation/download/[^)]*\)', '', _display)
                _display = re.sub(r'<!--\s*PPTX:[^>]*-->\n?', '', _display)
                if _pptx_paths:
                    _display = re.sub(r'[^\n]*(/[\w/.\-]+\.pptx)[^\n]*\n?', '', _display).strip()
                _display = _strip_notes_list_marker(_display)
                _display = _strip_pdf_from_display(_display)
                _display = re.sub(r'<!--\s*CHART:\s*/[\w/.\-]+\.png\s*-->\n?', '', _display).strip()
                if agent_id == "calls":
                    _calls_dir = _parse_calls_table_directive(full_response)
                    st.session_state["_openclaw_calls_table_directive"] = _calls_dir
                    _logger.info("openclaw calls directive: %s", _calls_dir)
                    _display = _strip_calls_table_marker(_display)
                    if not _display and _calls_dir:
                        _display = "Готово — сводная таблица ниже 👇"
                answer_placeholder.markdown(_display)
                for _doc_path in _docx_paths:
                    _p = Path(_doc_path)
                    if _p.exists():
                        st.download_button(
                            label=f"⬇ Скачать {_p.name}",
                            data=_p.read_bytes(),
                            file_name=_p.name,
                            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                            key=f"cur_docx_{hash(_doc_path)}",
                        )
                _render_pdf_download_buttons(_pdf_paths, "cur")
                _render_pptx_download_buttons(_pptx_paths, _pptx_urls, "cur")
                for _cp in dict.fromkeys(
                    re.findall(r'<!--\s*CHART:\s*(/[\w/.\-]+\.png)\s*-->', full_response)
                ):
                    _chart_p = Path(_cp)
                    if _chart_p.is_file():
                        st.image(str(_chart_p))
            else:
                if show_tool_details and status is not None:
                    with status:
                        st.code("Агент завершил работу, но не вернул текст.")
                    _update_progress(
                        "⚠️ Технические неполадки",
                        prog_state="error",
                        expanded=False,
                    )
                elif spinner_label is not None:
                    spinner_label.empty()
                answer_placeholder.markdown("🛠🦞  Упс, что-то пошло не так — попробуй ещё раз!")
                full_response = ""

            if agent_id == "corp-sales":
                _log_stop.set()
                _log_thread.join(timeout=0.5)
                _store_corp_notes_table_ctx(
                    _log_tool_calls,
                    table_query_preview if table_query_preview is not None else user_input,
                )
            if agent_id == "fi-sales":
                _log_stop.set()
                _log_thread.join(timeout=0.5)
                _store_notes_list_ctx(
                    _log_tool_calls,
                    _active_notes_team(),
                    full_response,
                    table_query_preview or user_input,
                )

        except (requests.exceptions.RequestException, OSError) as exc:
            import traceback as _tb
            err_text = _openclaw_friendly_error(exc, gateway_base)
            if show_tool_details and status is not None:
                with status:
                    st.code(f"{type(exc).__name__}: {exc}\n\n{_tb.format_exc()}")
                _update_progress(
                    f"❌ {type(exc).__name__}",
                    prog_state="error",
                    expanded=False,
                )
            elif spinner_label is not None:
                spinner_label.empty()
            answer_placeholder.markdown("🛠🦞  Упс, что-то пошло не так — попробуй ещё раз!")
            full_response = err_text
        finally:
            _log_stop.set()
            _log_thread.join(timeout=0.2)

    if agent_id == "calls":
        _has_dir = bool(st.session_state.get("_openclaw_calls_table_directive"))
        full_response = _strip_calls_table_marker(full_response)
        if not full_response and _has_dir:
            full_response = "Готово — сводная таблица ниже 👇"

    return full_response, response_id


def _render_tool_calls(tool_calls: dict[str, dict], active_id: str | None) -> str:
    """Рендерит список tool calls с иконками статуса."""
    lines = []
    for tid, t in tool_calls.items():
        name = t.get("name") or "…"
        args = (t.get("arguments") or "").strip()
        label = _openclaw_tool_label(name, args)
        is_active = tid == active_id
        icon = "⏳" if is_active else "✅"
        lines.append(f"{icon} **`{name}`**{label}")
    return "\n\n".join(lines)


def _openclaw_answer_impl(
    original_user_input: str,
    agent_id: str,
    mode_key: str,
    hist_key: str,
    pipeline: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_tool_details: bool = True,
    agent_user_input: str | None = None,
    router_context: str | None = None,
    response_state_key: str | None = None,
) -> None:
    _settings = DefaultSettings()
    hist: list = list(st.session_state.get(hist_key, []))
    rid_key = response_state_key or f"openclaw_{agent_id}_response_id"
    prev_response_id: str | None = st.session_state.get(rid_key)
    request_user_input = agent_user_input if agent_user_input is not None else original_user_input
    # Контекст диалога роутера (вопросы/ответы других агентов) — если задан,
    # подмешиваем перед запросом, чтобы агент понимал, на что ссылается юзер.
    if router_context:
        request_user_input = f"{router_context}\n\n{request_user_input}"
    try:
        full_response, new_response_id = _call_openclaw_streaming(
            user_input=request_user_input,
            gateway_url=_settings.openclaw_gateway_url,
            api_key=_settings.openclaw_api_key,
            agent_id=agent_id,
            previous_response_id=prev_response_id,
            show_tool_details=show_tool_details,
            table_query_preview=original_user_input,
        )
    except Exception as exc:
        from synaptica.utils.helpers import is_streamlit_script_control_exception
        if is_streamlit_script_control_exception(exc):
            raise
        _logger.exception("openclaw streaming failed for agent %s", agent_id)
        full_response = f"🦞 ⚠️ **Ошибка при формировании ответа:** {exc}"
        new_response_id = prev_response_id
    if full_response and agent_id == "client-helper":
        full_response = _ensure_fin_report_marker(full_response)
    if not full_response or not full_response.strip():
        st.warning("Агент не вернул ответ. Повторите вопрос — история не очищена.")
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input=original_user_input,
            chat_answer="🦞 ⚠️ Ответ не получен — попробуйте ещё раз.",
            chat_history_last=str(hist),
            pipeline=pipeline,
        )
        return
    if new_response_id:
        st.session_state[rid_key] = new_response_id
    hist.append({"role": "user", "content": original_user_input})
    hist.append({"role": "assistant", "content": full_response})
    st.session_state[hist_key] = hist
    st.session_state[mode_key] = True
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=original_user_input,
        transformed_user_input=original_user_input,
        chat_answer=full_response,
        chat_history_last=str(hist[:-2] if len(hist) > 2 else []),
        pipeline=pipeline,
    )
    if re.search(r"/[\w/.\-]+\.xlsx", full_response or "") or st.session_state.pop(
        "_notes_cards_pending_rerun", False
    ):
        st.rerun()


def write_openclaw_coder_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    _openclaw_answer_impl(
        original_user_input=original_user_input,
        agent_id="synaptica-coder",
        mode_key="openclaw_coder_mode",
        hist_key="openclaw_coder_chat_history",
        pipeline="openclaw_coder",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
    )


def write_openclaw_helper_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    router_context: str | None = None,
) -> None:
    _openclaw_answer_impl(
        original_user_input=original_user_input,
        agent_id="synaptica-helper",
        mode_key="openclaw_helper_mode",
        hist_key="openclaw_helper_chat_history",
        pipeline="openclaw_helper",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        router_context=router_context,
    )


def write_openclaw_client_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    router_context: str | None = None,
) -> None:
    _openclaw_answer_impl(
        original_user_input=original_user_input,
        agent_id="client-helper",
        mode_key="openclaw_client_mode",
        hist_key="openclaw_client_chat_history",
        pipeline="openclaw_client",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        show_tool_details=not OPENCLAW_CLIENT_SIMPLE_UI,
        router_context=router_context,
    )


FI_NOTES_WELCOME = (
    "📋 🦞 **FI Notes — режим включён.**\n\n"
    "Добавляю заметки и ищу историю по клиентам.\n"
    "- **Потребность** — конкретный интерес/обсуждение продукта (с датой).\n"
    "- **Общая информация** — фон по клиенту (предпочтения, как лучше работать).\n"
    "- **Мероприятие** — внутренние гемба, звонок или встреча, без компании "
    "(длительность или начало–конец)."
)


def write_openclaw_fi_sales_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    command: str = "/fi_notes",
    access_check=is_openclaw_fi_sales_allowed_user,
    show_user_msg: bool = True,
) -> None:
    st.session_state.pop(FI_NOTES_TABLE_CTX_KEY, None)
    _openclaw_start_impl(
        command=command,
        welcome_msg=FI_NOTES_WELCOME,
        mode_key="openclaw_fi_sales_mode",
        hist_key="openclaw_fi_sales_chat_history",
        pipeline="openclaw_fi_sales",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=access_check,
        show_user_msg=show_user_msg,
    )


def write_openclaw_fi_sales_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    router_context: str | None = None,
) -> None:
    _openclaw_answer_impl(
        original_user_input=original_user_input,
        agent_id="fi-sales",
        mode_key="openclaw_fi_sales_mode",
        hist_key="openclaw_fi_sales_chat_history",
        pipeline="openclaw_fi_sales",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        agent_user_input=_with_fi_actor_context(original_user_input),
        router_context=router_context,
        # Как у звонков/rates: крутящийся спиннер, без раскрывающегося st.status.
        show_tool_details=False,
    )


def write_openclaw_corp_sales_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    st.session_state.pop(CORP_NOTES_TABLE_CTX_KEY, None)
    _openclaw_start_impl(
        command="/corp_notes",
        welcome_msg=(
            "📋 🦞 **Corp Notes — режим включён.**\n\n"
            "Добавляю заметки о встречах и ищу историю по клиентам. "
            "Чтобы выйти — любая другая команда."
        ),
        mode_key="openclaw_corp_sales_mode",
        hist_key="openclaw_corp_sales_chat_history",
        pipeline="openclaw_corp_sales",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_openclaw_corp_sales_allowed_user,
    )


def write_openclaw_corp_sales_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    router_context: str | None = None,
) -> None:
    _openclaw_answer_impl(
        original_user_input=original_user_input,
        agent_id="corp-sales",
        mode_key="openclaw_corp_sales_mode",
        hist_key="openclaw_corp_sales_chat_history",
        pipeline="openclaw_corp_sales",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        agent_user_input=_with_corp_actor_context(original_user_input),
        router_context=router_context,
    )


CALLS_OPENCLAW_COMMAND = "/calls_ai"


def write_openclaw_calls_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    _openclaw_start_impl(
        command=CALLS_OPENCLAW_COMMAND,
        welcome_msg=(
            "📞 🦞 **Анализ звонков — режим включён.**\n\n"
            "Считаю статистику, строю сводные разбивки и нахожу нужные звонки. "
            "Чтобы выйти — любая другая команда."
        ),
        mode_key="openclaw_calls_mode",
        hist_key="openclaw_calls_chat_history",
        pipeline="openclaw_calls",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_openclaw_calls_allowed_user,
    )


def write_openclaw_calls_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    router_context: str | None = None,
) -> None:
    # ТБ для дэша берём из доступа юзера (как и user_bank_scope для сервиса).
    st.session_state["calls_bank"] = resolve_calls_bank(_current_synaptica_username()) or "ALL"
    # Сбрасываем таблицу прошлого хода и директиву — дэш покажем только если агент его попросит сейчас.
    st.session_state.pop("_openclaw_calls_table_directive", None)
    st.session_state.pop(CALL_TABLE_INTERACTIVE_CTX_KEY, None)

    _openclaw_answer_impl(
        original_user_input=original_user_input,
        agent_id="calls",
        mode_key="openclaw_calls_mode",
        hist_key="openclaw_calls_chat_history",
        pipeline="openclaw_calls",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        agent_user_input=_with_calls_context(original_user_input),
        router_context=router_context,
    )

    directive = st.session_state.pop("_openclaw_calls_table_directive", None)
    if directive:
        try:
            render_call_sales_table(
                dt_from=str(directive["dt_from"]),
                dt_to=str(directive["dt_to"]),
                period_label=(str(directive.get("period_label") or "").strip() or None),
                operators=(directive.get("operators") or None),
                bank=(str(directive.get("bank") or "").strip() or None),
            )
        except Exception:
            _logger.exception("openclaw calls table render failed")


PRESENTATION_GEN_COMMAND = "/pres"
PRESENTATION_OPENCLAW_COMMAND = "/pres_gen"
COMMENTS_COMMAND = "/comments"
CALL_UPLOAD_COMMAND = "/call_upload"
# Как FI / VND: относительно cwd процесса streamlit (локально и на сервере).
CALLS_DATA_DIR = Path("synaptica/data/calls_tb")
VND_RAG_COMMAND = "/vnd_rag"

CALL_UPLOAD_INSTRUCTION = """
📤 **Загрузка звонков — режим включён**

**Формат Excel:** нужны колонки (любые алиасы):
- **starttime** / start_time — дата и время звонка
- **duration** — длительность в секундах (звонки ≤ 30 сек отбрасываются)
- **operator** — ФИО оператора
- **text** — транскрипт
- **remotephonenumber** / remotepho / dialerphonenumber — телефон клиента

Лишние колонки игнорируются. Дубликаты звонков из базы (CALL_DATA_PATH) отфильтровываются автоматически.

**Пайплайн после загрузки:** prepare → tag (GigaChat) → фильтр (external + internal «Юго-Западный банк») → merge → **`synaptica/data/calls_tb/final_calls.xlsx`**.

Бэкап REF перед merge: **`synaptica/data/calls_tb/final_calls_backup_YYYYMMDD_HHMMSS.xlsx`**.

Прикрепите `.xlsx` или `.csv` ниже. Чтобы выйти — любая другая команда.
""".strip()

# ---------------------------------------------------------------------------
# VND RAG — агент OpenClaw работает с инструментами сервиса на порту 18004
# ---------------------------------------------------------------------------


def write_openclaw_vnd_rag_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    _openclaw_start_impl(
        command=VND_RAG_COMMAND,
        welcome_msg=(
            "📚 🦞 **VND RAG — режим включён.**\n\n"
            "Задавайте вопросы по ВНД (2391, 5106, 5730, 5892) — "
            "агент сам ищет нужные фрагменты, при необходимости запрашивает "
            "полный текст раздела и итерирует поиск, пока не найдёт точный ответ.\n\n"
            "Чтобы выйти — любая другая команда."
        ),
        mode_key="openclaw_vnd_rag_mode",
        hist_key="openclaw_vnd_rag_chat_history",
        pipeline="openclaw_vnd_rag",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_openclaw_vnd_rag_allowed_user,
    )


def write_openclaw_vnd_rag_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
    router_context: str | None = None,
) -> None:
    _openclaw_answer_impl(
        original_user_input=original_user_input,
        agent_id="vnd-rag",
        mode_key="openclaw_vnd_rag_mode",
        hist_key="openclaw_vnd_rag_chat_history",
        pipeline="openclaw_vnd_rag",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        router_context=router_context,
    )


def write_openclaw_pres_gen_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    _openclaw_start_impl(
        command=PRESENTATION_OPENCLAW_COMMAND,
        welcome_msg=(
            "📽 🦞 ✨ **PresGen — режим OpenClaw включён.**\n\n"
            "Опишите задачу по презентации: новую генерацию, правки слайдов, "
            "пересчёт параметров или обновление текста.\n\n"
            "Чтобы выйти — любая другая команда."
        ),
        mode_key="openclaw_pres_gen_mode",
        hist_key="openclaw_pres_gen_chat_history",
        pipeline="openclaw_pres_gen",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_openclaw_pres_gen_allowed_user,
        show_user_msg=show_user_msg,
    )


def write_openclaw_pres_gen_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    router_context: str | None = None,
) -> None:
    _openclaw_answer_impl(
        original_user_input=original_user_input,
        agent_id="pres-gen",
        mode_key="openclaw_pres_gen_mode",
        hist_key="openclaw_pres_gen_chat_history",
        pipeline="openclaw_pres_gen",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        show_tool_details=not OPENCLAW_CLIENT_SIMPLE_UI,
        router_context=router_context,
    )


# ---------------------------------------------------------------------------
# Rates & Pricing (/rates_pricing) — агент OpenClaw работает с rates-сервисом
# (сделки, репрайс-апдейты, рестракт, прайсинг) на порту 18007.
# ---------------------------------------------------------------------------

RATES_PRICING_COMMAND = "/rates_pricing"


def write_openclaw_rates_pricing_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    _openclaw_start_impl(
        command=RATES_PRICING_COMMAND,
        welcome_msg=(
            "📈 🦞 **Rates & Pricing — режим OpenClaw включён.**\n\n"
            "Работа со сделками ставок и прайсингом: сохранить/удалить сделку, "
            "показать её параметры, репрайс-апдейты, рестракт по диапазонам страйков "
            "и барьеров, прайсинг продукта.\n\n"
            "Чтобы выйти — любая другая команда."
        ),
        mode_key="openclaw_rates_pricing_mode",
        hist_key="openclaw_rates_pricing_chat_history",
        pipeline="openclaw_rates_pricing",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_openclaw_rates_pricing_allowed_user,
        show_user_msg=show_user_msg,
    )


def write_openclaw_rates_pricing_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    router_context: str | None = None,
) -> None:
    # Логин пользователя нужен сервису для всех ручек — подмешиваем в контекст запроса,
    # не засоряя видимое пользователю сообщение.
    username = str(st.session_state.get("username", ""))
    user_ctx = f"(контекст: username={username})" if username else None
    if router_context and user_ctx:
        user_ctx = f"{router_context}\n{user_ctx}"
    elif router_context:
        user_ctx = router_context
    _openclaw_answer_impl(
        original_user_input=original_user_input,
        agent_id="rates-pricing",
        mode_key="openclaw_rates_pricing_mode",
        hist_key="openclaw_rates_pricing_chat_history",
        pipeline="openclaw_rates_pricing",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        show_tool_details=False,
        router_context=user_ctx,
    )


# ---------------------------------------------------------------------------
# Products RAG (/openclaw_products) — агент OpenClaw по базе продуктов ДГР
# (тот же индекс, что write_rag / products_rag) на порту 18008.
# ---------------------------------------------------------------------------

PRODUCTS_RAG_COMMAND = "/openclaw_products"


def write_openclaw_products_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    _openclaw_start_impl(
        command=PRODUCTS_RAG_COMMAND,
        welcome_msg=(
            "🍯 🦞 **Products RAG — режим OpenClaw включён.**\n\n"
            "Общие вопросы по продуктам глобальных рынков: что это, как работает, "
            "чем отличается, условия. Агент сам ищет в той же базе, что и обычный RAG.\n\n"
            "Чтобы выйти — любая другая команда."
        ),
        mode_key="openclaw_products_mode",
        hist_key="openclaw_products_chat_history",
        pipeline="openclaw_products",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_openclaw_products_allowed_user,
        show_user_msg=show_user_msg,
    )


def write_openclaw_products_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    router_context: str | None = None,
) -> None:
    _openclaw_answer_impl(
        original_user_input=original_user_input,
        agent_id="products-rag",
        mode_key="openclaw_products_mode",
        hist_key="openclaw_products_chat_history",
        pipeline="openclaw_products",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        show_tool_details=False,
        router_context=router_context,
    )


# ---------------------------------------------------------------------------
# OpenClaw Router (/openclaw1) — единая точка входа: агент-роутер классифицирует
# запрос и Synaptica делегирует его профильному OpenClaw-агенту.
# ---------------------------------------------------------------------------

OPENCLAW_ROUTER_COMMAND = "/openclaw1"
_OPENCLAW_ROUTER_AGENT_ID = OPENCLAW_ROUTER_AGENT_ID

# Кто может принять метку: доступ + подпись по умолчанию (подпись из ROUTE.md важнее).
# Навык для классификатора — только docs/agents/<id>/ROUTE.md, не этот словарь.
_AGENT_ACCESS: dict[str, tuple] = {
    "calls": (is_openclaw_calls_allowed_user, "📞 анализ звонков"),
    "fi-sales": (is_notes_allowed_user, "💼 заметки FI"),
    "corp-sales": (is_openclaw_corp_sales_allowed_user, "🏢 заметки Corp"),
    "pres-gen": (is_openclaw_pres_gen_allowed_user, "📽 презентации"),
    "client-helper": (is_openclaw_client_allowed_user, "📋 фин. отчётность"),
    "rates-pricing": (is_openclaw_rates_pricing_allowed_user, "📈 ставки и прайсинг"),
    "products-rag": (is_openclaw_products_allowed_user, "🍯 продукты"),
    "vnd-rag": (is_openclaw_vnd_rag_allowed_user, "📚 ВНД"),
    "synaptica-helper": (is_openclaw_helper_allowed_user, "📖 навигация по Synaptica"),
}

# Метки «не нашёл агента» — роутер ответит сам в [РЕЖИМ ОТВЕТА].

# «Читающие» агенты: их ответы можно опросить пачкой и свести в один (research).
# Остальные агрегации не подлежат и маршрутизируются поодиночке
# (pres-gen, rates-pricing, products-rag).
_ROUTER_READ_AGENTS: frozenset[str] = frozenset({
    "calls", "fi-sales", "client-helper",
})

# Метка роутера → ключ истории профильного агента в session_state
# (откуда забираем последний ответ агента, чтобы прокинуть его в контекст
# следующего хода — особенно когда дальше отвечает другой агент).
_ROUTER_AGENT_HIST_KEY: dict[str, str] = {
    "calls": "openclaw_calls_chat_history",
    "fi-sales": "openclaw_fi_sales_chat_history",
    "corp-sales": "openclaw_corp_sales_chat_history",
    "pres-gen": "openclaw_pres_gen_chat_history",
    "client-helper": "openclaw_client_chat_history",
    "rates-pricing": "openclaw_rates_pricing_chat_history",
    "products-rag": "openclaw_products_chat_history",
    "vnd-rag": "openclaw_vnd_rag_chat_history",
    "synaptica-helper": "openclaw_helper_chat_history",
}

# Сколько последних ходов держать в session_state и отдавать классификатору.
# Для router_context профильным агентам — урезанная выборка (_ROUTER_CONTEXT_MAX_TURNS).
_ROUTER_HISTORY_MAX_ENTRIES = 20
_ROUTER_CONTEXT_MAX_TURNS = 3
_ROUTER_CONTEXT_ANSWER_CHARS = 1500

_OPENCLAW_ROUTING_KEY = "openclaw_routing"
_ROUTER_RESPONSE_ID_KEY = f"openclaw_{OPENCLAW_ROUTER_AGENT_ID}_response_id"


def _get_openclaw_routing() -> dict:
    raw = st.session_state.get(_OPENCLAW_ROUTING_KEY)
    if isinstance(raw, dict):
        return raw
    return {"active_agent": None, "pending": None}


def _set_openclaw_routing(*, active_agent: str | None, pending: dict | None) -> None:
    st.session_state[_OPENCLAW_ROUTING_KEY] = {
        "active_agent": active_agent,
        "pending": pending,
    }


def _format_routing_block_for_classify() -> str:
    routing = _get_openclaw_routing()
    lines = ["[ROUTING]"]
    active = routing.get("active_agent")
    if active:
        lines.append(f"active_agent: {active}")
    pending = routing.get("pending")
    if isinstance(pending, dict) and pending.get("agent"):
        lines.append(f"pending_agent: {pending['agent']}")
        if pending.get("kind"):
            lines.append(f"pending_kind: {pending['kind']}")
        if pending.get("last_question"):
            lines.append(f"pending_question: {pending['last_question']}")
    if len(lines) == 1:
        return ""
    return "\n".join(lines)


def _extract_last_question(text: str) -> str:
    for line in reversed((text or "").splitlines()):
        line = line.strip()
        if line.endswith("?") or line.endswith("？"):
            return line[:200]
    snippet = (text or "").strip()
    return snippet[:200] if snippet else ""


def _parse_need_params_from_answer(text: str) -> dict | None:
    """need_params из JSON сервиса или явный запрос параметров в тексте агента."""
    raw = text or ""
    if re.search(r'"status"\s*:\s*"need_params"', raw, re.I):
        return {
            "kind": "need_params",
            "missing": [],
            "last_question": _extract_last_question(raw),
        }
    lower = raw.lower()
    if "need_params" in lower:
        return {
            "kind": "need_params",
            "missing": [],
            "last_question": _extract_last_question(raw),
        }
    if "?" not in raw and "？" not in raw:
        return None
    if not any(w in lower for w in (
        "укажите", "нужен", "нужна", "нужно", "не хватает", "уточните",
        "какой ", "какая ", "какие ", "сколько ", "на какой", "укажи",
    )):
        return None
    return {
        "kind": "need_params",
        "missing": [],
        "last_question": _extract_last_question(raw),
    }


def _update_routing_after_agent_turn(agent: str, answer: str) -> None:
    pending_info = _parse_need_params_from_answer(answer)
    if pending_info:
        pending_info["agent"] = agent
        _set_openclaw_routing(active_agent=agent, pending=pending_info)
    else:
        _set_openclaw_routing(active_agent=agent, pending=None)


def _last_agent_answer(target: str) -> str:
    """Последний ответ профильного агента из его истории в session_state."""
    hist = st.session_state.get(_ROUTER_AGENT_HIST_KEY.get(target, ""), []) or []
    for msg in reversed(hist):
        if isinstance(msg, dict) and msg.get("role") == "assistant":
            return str(msg.get("content") or "")
    return ""


def _build_router_context(
    history: list,
    exclude_agent: str | None,
    *,
    max_turns: int | None = _ROUTER_CONTEXT_MAX_TURNS,
) -> str:
    """Выжимка прошлых ходов для промпта.

    `max_turns=None` — вся сохранённая история (для classify).
    `exclude_agent` — пропуск ходов текущего агента (у него свой OpenClaw-тред).
    """
    entries = list(history) if max_turns is None else history[-max_turns:]
    lines: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        agent = entry.get("agent")
        if exclude_agent and agent == exclude_agent:
            continue
        user_q = (entry.get("user") or "").strip()
        answer = (entry.get("assistant") or "").strip()
        if not user_q and not answer:
            continue
        label = _agent_caption(agent)
        block = f"— Пользователь: {user_q}"
        if answer:
            if len(answer) > _ROUTER_CONTEXT_ANSWER_CHARS:
                answer = "…" + answer[-_ROUTER_CONTEXT_ANSWER_CHARS:].lstrip()
            block += f"\n— Ответ ({label}): {answer}"
        lines.append(block)
    if not lines:
        return ""
    return (
        "[Контекст диалога Synaptica — предыдущие вопросы и ответы; "
        "используй их, чтобы понять, на что ссылается пользователь, "
        "но отвечай только на текущий вопрос]\n" + "\n\n".join(lines)
    )


def _extract_responses_text(data: dict) -> str:
    """Финальный текст из ответа gateway /responses (или chat/completions)."""
    if not isinstance(data, dict):
        return ""
    txt = data.get("output_text")
    if isinstance(txt, str) and txt.strip():
        return txt
    parts: list[str] = []
    output = data.get("output")
    if isinstance(output, list):
        for item in output:
            content = item.get("content") if isinstance(item, dict) else None
            if isinstance(content, list):
                for c in content:
                    if isinstance(c, dict) and isinstance(c.get("text"), str):
                        parts.append(c["text"])
    if parts:
        return "\n".join(parts)
    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        msg = (choices[0] or {}).get("message") or {}
        if isinstance(msg.get("content"), str):
            return msg["content"]
    return ""


def _openclaw_router_call(user_input: str) -> str | None:
    """Тихий вызов synaptica-router с тредом (previous_response_id).

    Один тред на весь /openclaw1: classify, synthesize и fallback видят историю роутера.
    """
    text, rid = _call_openclaw_router(
        user_input,
        previous_response_id=st.session_state.get(_ROUTER_RESPONSE_ID_KEY),
        agent_id=OPENCLAW_ROUTER_AGENT_ID,
    )
    if rid:
        st.session_state[_ROUTER_RESPONSE_ID_KEY] = rid
    return text


def _agent_caption(agent_id: str) -> str:
    spec = _AGENT_ACCESS.get(agent_id)
    default = spec[1] if spec else "Synaptica"
    return _agent_catalog_caption(agent_id, fallback=default)


def _router_available(username: str) -> dict[str, tuple]:
    """id → (access_fn, подпись) для агентов с ROUTE.md, доступом и хендлером."""
    catalog_ids = {r.id for r in load_agent_routes()}
    out: dict[str, tuple] = {}
    for agent_id, (check, default_label) in _AGENT_ACCESS.items():
        if agent_id not in catalog_ids:
            continue
        if not check(username):
            continue
        out[agent_id] = (check, _agent_caption(agent_id) or default_label)
    return out


def _parse_router_labels(raw: str, known: set[str] | None = None) -> list[str]:
    """Достаёт известные метки из ответа роутера. Берёт только первую."""
    allowed = known if known is not None else set(_AGENT_ACCESS)
    return parse_router_labels(raw, allowed, max_targets=_ROUTER_MAX_TARGETS)


def _openclaw_route_classify(
    user_input: str,
    history: list | None = None,
    available_ids: set[str] | None = None,
) -> tuple[list[str], bool]:
    """Маршрутизация только через synaptica-router (одна метка или fallback)."""
    labels, is_fallback, rid, _raw = _classify_openclaw(
        user_input,
        available_ids=available_ids,
        routing_block=_format_routing_block_for_classify(),
        history_block=_build_router_context(history or [], exclude_agent=None, max_turns=None),
        previous_response_id=st.session_state.get(_ROUTER_RESPONSE_ID_KEY),
        agent_id=OPENCLAW_ROUTER_AGENT_ID,
    )
    if rid:
        st.session_state[_ROUTER_RESPONSE_ID_KEY] = rid
    return labels, is_fallback


def _router_agent_input(target: str, user_input: str) -> str:
    """Тот же спец-контекст ввода, что и при прямом вызове агента."""
    if target == "calls":
        return _with_calls_context(user_input)
    if target == "fi-sales":
        return _with_fi_actor_context(user_input)
    if target == "corp-sales":
        return _with_corp_actor_context(user_input)
    return user_input


def _openclaw_silent_call(agent_id: str, user_input: str) -> str:
    """Тихий (без рендера) вызов профильного агента с его тредом. '' при ошибке."""
    import requests

    _settings = DefaultSettings()
    gateway_base = _settings.openclaw_gateway_url.rstrip("/")
    headers = {"Content-Type": "application/json", "x-openclaw-agent-id": agent_id}
    if _settings.openclaw_api_key:
        headers["Authorization"] = f"Bearer {_settings.openclaw_api_key}"
    payload: dict = {"model": f"openclaw:{agent_id}", "input": user_input, "stream": False}
    prev = st.session_state.get(f"openclaw_{agent_id}_response_id")
    if prev:
        payload["previous_response_id"] = prev
    try:
        resp = requests.post(f"{gateway_base}/responses", json=payload, headers=headers, timeout=(10, 300))
        resp.raise_for_status()
        data = resp.json()
        rid = data.get("id") if isinstance(data, dict) else None
        if rid:
            st.session_state[f"openclaw_{agent_id}_response_id"] = rid
        return _extract_responses_text(data)
    except Exception:
        _logger.exception("openclaw silent call failed for %s", agent_id)
        return ""


def _openclaw_router_synthesize(question: str, sources: list[tuple[str, str]]) -> str:
    """Просит сам synaptica-router свести ответы источников в единый ответ."""
    parts = [
        "[РЕЖИМ СИНТЕЗА] Не классифицируй. Ты Synaptica: ответь человеку от своего лица, "
        "без слов «роутер», «агент», «маршрут». Ниже — материалы навыков по вопросу. "
        "Возьми факты из источников, которые **релевантны** запросу; остальные упомяни "
        "одной короткой фразой («по звонкам — нет данных»), не разворачивай. "
        "Не задавай уточняющих вопросов, не пиши «Нужно что-то конкретное?», "
        "не передавай просьбы уточнить период/параметры. "
        "Без мета-комментариев и скобок «для системы». Только факты из блоков ниже.",
        f"\nВопрос пользователя: {question}",
    ]
    for label, ans in sources:
        ans = (ans or "").strip() or "(источник не вернул данных)"
        parts.append(f"\n=== Источник: {label} ===\n{ans}")
    return _openclaw_router_call("\n".join(parts)) or ""


def _openclaw_router_direct_answer(question: str, history: list | None = None) -> str:
    """Fallback: Synaptica отвечает сама ([РЕЖИМ ОТВЕТА] — правила в AGENTS.md)."""
    parts = [
        "[РЕЖИМ ОТВЕТА] Ты Synaptica. Ответь человеку от своего лица. "
        "Не называй себя роутером, маршрутизатором или агентом."
    ]
    ctx = _build_router_context(history or [], exclude_agent=None, max_turns=None)
    if ctx:
        parts.append(ctx)
    parts.append(f"Вопрос пользователя: {question}")
    return _openclaw_router_call("\n\n".join(parts)) or ""


def _router_fallback_answer(
    question: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    history: list,
) -> None:
    """Запрос вне профиля роутера — ответ формирует сам synaptica-router."""
    with st.spinner("🧭 Отвечаю…"):
        answer = _openclaw_router_direct_answer(question, history)
    if not answer.strip():
        answer = "🦞 ⚠️ Не удалось получить ответ — повторите вопрос."
    stream_md_message(answer)
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=question,
        transformed_user_input=question,
        chat_answer=answer,
        chat_history_last=str(history),
        pipeline="openclaw_router",
    )
    history.append({"agent": "fallback", "user": question, "assistant": answer})
    st.session_state["openclaw_router_chat_history"] = history[-_ROUTER_HISTORY_MAX_ENTRIES:]


def _new_research_artifacts() -> dict:
    return {"docx": [], "pdf": [], "pptx_paths": [], "pptx_urls": [], "charts": [], "calls_dir": None}


def _extract_research_artifacts(target: str, raw: str, artifacts: dict) -> str:
    """Достаёт из сырого ответа агента файловые маркеры/директивы (детерминированно,
    без доверия синтезатору), копит их в `artifacts` и возвращает чистый текст для синтеза."""
    text = raw or ""
    if target == "client-helper":
        text = _ensure_fin_report_marker(text)

    docx = re.findall(r'(/[\w/.\-]+\.docx)', text)
    pdfs = _collect_pdf_paths(text)
    pptx_paths = list(dict.fromkeys(re.findall(r'(/[\w/.\-]+\.pptx)', text)))
    pptx_urls = list(dict.fromkeys(
        re.findall(r'(https?://[^\s)\]]+/presentation/download/[^\s)\]]+)', text)
    ))
    charts = re.findall(r'<!--\s*CHART:\s*(/[\w/.\-]+\.png)\s*-->', text)

    if docx:
        text = re.sub(r'[^\n]*(/[\w/.\-]+\.docx)[^\n]*\n?', '', text).strip()
    text = re.sub(r'\[[^\]]*\]\(https?://[^)]*?/presentation/download/[^)]*\)', '', text)
    text = re.sub(r'<!--\s*PPTX:[^>]*-->\n?', '', text)
    if pptx_paths:
        text = re.sub(r'[^\n]*(/[\w/.\-]+\.pptx)[^\n]*\n?', '', text).strip()
    text = _strip_pdf_from_display(text)
    text = re.sub(r'<!--\s*CHART:\s*/[\w/.\-]+\.png\s*-->\n?', '', text).strip()
    if target == "calls":
        directive = _parse_calls_table_directive(raw)
        if directive:
            artifacts["calls_dir"] = directive
        text = _strip_calls_table_marker(text)

    artifacts["docx"].extend(docx)
    artifacts["pdf"].extend(pdfs)
    artifacts["pptx_paths"].extend(pptx_paths)
    artifacts["pptx_urls"].extend(pptx_urls)
    artifacts["charts"].extend(charts)
    return text


def _render_research_artifacts(artifacts: dict) -> None:
    """Рисует кнопки/таблицы/картинки, собранные из ответов источников."""
    for doc_path in dict.fromkeys(artifacts["docx"]):
        p = Path(doc_path)
        if p.is_file():
            st.download_button(
                label=f"⬇ Скачать {p.name}",
                data=p.read_bytes(),
                file_name=p.name,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key=f"research_docx_{hash(doc_path)}",
            )
    _render_pdf_download_buttons(list(dict.fromkeys(artifacts["pdf"])), "research")
    _render_pptx_download_buttons(
        list(dict.fromkeys(artifacts["pptx_paths"])),
        list(dict.fromkeys(artifacts["pptx_urls"])),
        "research",
    )
    for chart_path in dict.fromkeys(artifacts["charts"]):
        cp = Path(chart_path)
        if cp.is_file():
            st.image(str(cp))
    directive = artifacts.get("calls_dir")
    if directive:
        try:
            render_call_sales_table(
                dt_from=str(directive["dt_from"]),
                dt_to=str(directive["dt_to"]),
                period_label=(str(directive.get("period_label") or "").strip() or None),
                operators=(directive.get("operators") or None),
                bank=(str(directive.get("bank") or "").strip() or None),
            )
        except Exception:
            _logger.exception("research calls table render failed")


def _router_research(
    read_targets: list[str],
    question: str,
    available: dict,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    history: list,
) -> None:
    """Опрашивает несколько читающих агентов и сводит ответы в один ответ.

    Файловые артефакты (кнопка PPTX, таблица звонков, PDF/графики) извлекаются из
    сырых ответов детерминированно и рисуются нами — синтезатор работает только с текстом.
    """
    st.caption(" · ".join(available[t][1] for t in read_targets))
    artifacts = _new_research_artifacts()
    sources: list[tuple[str, str]] = []
    for target in read_targets:
        if target == "calls":
            st.session_state["calls_bank"] = resolve_calls_bank(_current_synaptica_username()) or "ALL"
        with st.spinner(f"Смотрю: {available[target][1]}…"):
            raw = _openclaw_silent_call(target, _router_agent_input(target, question))
        sources.append((available[target][1], _extract_research_artifacts(target, raw, artifacts)))

    with st.spinner("Свожу ответы воедино…"):
        synthesized = _openclaw_router_synthesize(question, sources)
    if not synthesized.strip():
        synthesized = "🦞 ⚠️ Не удалось собрать ответ — повторите вопрос."

    stream_md_message(synthesized)
    _render_research_artifacts(artifacts)
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=question,
        transformed_user_input=question,
        chat_answer=synthesized,
        chat_history_last=str(history),
        pipeline="openclaw_router",
    )
    history.append({"agent": "research", "user": question, "assistant": synthesized})
    st.session_state["openclaw_router_chat_history"] = history[-_ROUTER_HISTORY_MAX_ENTRIES:]


def write_openclaw_router_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    _openclaw_start_impl(
        command=OPENCLAW_ROUTER_COMMAND,
        welcome_msg=(
            "🦞 **Synaptica.**\n\n"
            "Задайте вопрос — подключу нужный навык "
            "(звонки, заметки, отчётность, презентации, ставки, продукты, ВНД)."
        ),
        mode_key="openclaw_router_mode",
        hist_key="openclaw_router_chat_history",
        pipeline="openclaw_router",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_openclaw_router_allowed_user,
        show_user_msg=show_user_msg,
    )


def _write_openclaw_vnd_rag_from_router(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    router_context: str | None = None,
) -> None:
    write_openclaw_vnd_rag_answer(
        original_user_input=original_user_input,
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        chat_history_last=st.session_state.get("openclaw_vnd_rag_chat_history") or [],
        router_context=router_context,
    )


def _write_legacy_rates_from_router(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    router_context: str | None = None,
) -> None:
    """Роутер выбрал rates — отвечаем старым rates-агентом, не OpenClaw."""
    del router_context
    chat_history_last = st.session_state.get("chat_history_last") or []
    with st.spinner("Пожалуйста, подождите, обрабатываем запрос…"):
        sub = choose_case_rates_agent(original_user_input)
    kwargs = dict(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        transformed_user_input=original_user_input,
        chat_history_last=chat_history_last,
    )
    if sub == "прайсинг" or "rates" not in sub:
        write_rates_trading_bot(
            settings.model, original_user_input, sub, **kwargs
        )
        return
    write_rates_agent(
        settings.model, original_user_input, sub, **kwargs
    )


# Метка роутера → профильная answer-функция (со всей её спец-логикой и UI).
_ROUTER_ANSWER_FUNCS = {
    "calls": write_openclaw_calls_answer,
    "fi-sales": write_openclaw_fi_sales_answer,
    "corp-sales": write_openclaw_corp_sales_answer,
    "pres-gen": write_openclaw_pres_gen_answer,
    "client-helper": write_openclaw_client_answer,
    "rates-pricing": _write_legacy_rates_from_router,
    "products-rag": write_openclaw_products_answer,
    "vnd-rag": _write_openclaw_vnd_rag_from_router,
    "synaptica-helper": write_openclaw_helper_answer,
}


def _router_dispatch(target: str, **kwargs) -> None:
    """Делегирует профильной answer-функции выбранного агента."""
    func = _ROUTER_ANSWER_FUNCS.get(target)
    if func is not None:
        func(**kwargs)


def write_openclaw_router_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    username = str(st.session_state.get("username", ""))
    available = _router_available(username)

    history = list(st.session_state.get("openclaw_router_chat_history") or [])
    if not available:
        _router_fallback_answer(
            question=original_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            history=history,
        )
        for _k in _OPENCLAW_MODE_KEYS:
            st.session_state[_k] = False
        st.session_state["openclaw_router_mode"] = True
        return

    raw_targets, _explicit_fallback = _openclaw_route_classify(
        original_user_input,
        history=history,
        available_ids=set(available),
    )
    targets = [t for t in raw_targets if t in available][:1]

    if not targets:
        _router_fallback_answer(
            question=original_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            history=history,
        )
        for _k in _OPENCLAW_MODE_KEYS:
            st.session_state[_k] = False
        st.session_state["openclaw_router_mode"] = True
        return

    target = targets[0]
    st.caption(available[target][1])
    voice = (
        "[Голос] Отвечай как Synaptica. Не представляйся отдельным агентом "
        "или роутером. Не упоминай маршрутизацию."
    )
    prior = _build_router_context(history, exclude_agent=target)
    _router_dispatch(
        target,
        original_user_input=original_user_input,
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        router_context=f"{voice}\n\n{prior}" if prior else voice,
    )
    history.append({
        "agent": target,
        "user": original_user_input,
        "assistant": _last_agent_answer(target),
    })
    _update_routing_after_agent_turn(target, history[-1]["assistant"])
    st.session_state["openclaw_router_chat_history"] = history[-_ROUTER_HISTORY_MAX_ENTRIES:]

    # Остаёмся в режиме роутера: следующий вопрос снова пройдёт классификацию.
    for _k in _OPENCLAW_MODE_KEYS:
        st.session_state[_k] = False
    st.session_state["openclaw_router_mode"] = True


def _render_file_stub_response(
    file_path: str,
    chat_text: str,
    *,
    download_mime: str,
    key_prefix: str,
) -> None:
    with st.chat_message("assistant"):
        display_text = re.sub(r"<!--\s*PPTX:\s*/[\w/.\-]+\.pptx\s*-->", "", chat_text)
        display_text = re.sub(r"<!--\s*XLSX:\s*/[\w/.\-]+\.xlsx\s*-->", "", display_text)
        st.markdown(display_text.strip())
        
        p = Path(file_path)
        if p.is_file():
            # ===== ГАРАНТИРОВАННО УНИКАЛЬНЫЙ КЛЮЧ =====
            if "download_counter" not in st.session_state:
                st.session_state.download_counter = 0
            st.session_state.download_counter += 1
            
            unique_key = f"{key_prefix}_{st.session_state.download_counter}_{uuid.uuid4().hex[:4]}"
            
            st.download_button(
                label=f"⬇ Скачать {p.name}",
                data=p.read_bytes(),
                file_name=p.name,
                mime=download_mime,
                key=unique_key,
            )


def write_presentation_generate_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    # ===== ИНИЦИАЛИЗИРУЕМ СЕССИЮ =====
    if "presentation_session_id" not in st.session_state:
        st.session_state.presentation_session_id = uuid.uuid4().hex[:12]
    
    _openclaw_start_impl(
        command=PRESENTATION_GEN_COMMAND,
        welcome_msg=(
            "📽 **Генерация презентации — режим включён.**\n\n"
            "Укажите продукт (доступны: Кэп, Коллар, Своп, Флор) и его параметры — и я сгенерирую готовую презентацию для клиента.\n\n"
            "Чтобы выйти — любая другая команда."
        ),
        mode_key="presentation_gen_mode",
        hist_key="presentation_gen_chat_history",
        pipeline="presentation_gen",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_presentation_gen_allowed_user,
        show_user_msg=show_user_msg,
    )

def write_presentation_generate_button(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    write_presentation_generate_start(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
    )


def write_news_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
    command: str = "/news",
) -> None:
    """Единственная проверка доступа к /news: список → режим новостей, остальные → заглушка."""
    if not is_special_news_allowed_user(str(st.session_state.get("username", ""))):
        msg = (
            "📰 **Персональные новости** — в разработке.\n\n"
            "Скоро здесь появится подборка под ваши компании и отрасли. "
            "Следите за обновлениями Synaptica — откроем доступ, как только всё будет готово."
        )
        if show_user_msg:
            st.chat_message("user").write(command)
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=command,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last="",
            pipeline="news",
        )
        return

    _openclaw_start_impl(
        command=command,
        welcome_msg=(
            "📰 **Персональные новости.** "
            "Выберите компании, отрасли и период в панели выше — "
            "дайджест соберётся по кнопке «Вывести новости»."
        ),
        mode_key="special_news_mode",
        hist_key="special_news_chat_history",
        pipeline="special_news",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        show_user_msg=show_user_msg,
    )


def write_special_news_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    write_news_start(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        show_user_msg=show_user_msg,
        command=SPECIAL_NEWS_COMMAND,
    )


def process_special_news_panel_action(
    username: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    """Вызывается из synaptica_app после отрисовки панели."""
    if not is_special_news_allowed_user(username):
        reset_to_zero_special_news_pipeline()
        return

    from synaptica.backend.streamlit_functions.special_news import (
        render_special_news_panel,
        run_special_news_digest,
    )

    result = render_special_news_panel(username)
    if result is None:
        return
    prefs, action = result
    if action != "digest":
        return
    run_special_news_digest(
        prefs,
        username=username,
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        save_fn=save_in_both_memories_and_in_logs,
        stream_fn=stream_md_message,
    )


def write_presentation_generate_answer(
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
) -> None:
    if not is_presentation_gen_allowed_user(str(st.session_state.get("username", ""))):
        msg = "⛔ У вас нет доступа к генерации презентаций."
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="presentation_gen",
        )
        reset_to_zero_presentation_gen_pipeline()
        return

    try:
        from synaptica.backend.streamlit_functions.presentation_gen import (
            PresCancelled,
            finish_pres_job,
            run_presentation_gen,
            start_pres_job,
        )

        st.session_state["_pres_pending_user"] = original_user_input
        job_id = start_pres_job()
        st.session_state["_pres_job_id"] = job_id
        step_ph = st.empty()
        bar_ph = st.empty()
        def _pres_ui_step(text: str, frac: float | None = None, **_kwargs) -> None:
            step_ph.markdown(
                _render_openclaw_simple_spinner(text),
                unsafe_allow_html=True,
            )
            bar_ph.progress(min(1.0, max(0.0, float(frac if frac is not None else 0.0))))
        try:
            if "presentation_session_id" not in st.session_state:
                st.session_state.presentation_session_id = uuid.uuid4().hex[:12]
            session_id = st.session_state.presentation_session_id
            _pres_ui_step("Готовлю презентацию…", frac=0.0)
            file_path, chat_text = run_presentation_gen(
                transformed_user_input,
                retriever=st.session_state.get("retriever"),
                session_id=session_id,
                job_id=job_id,
                on_step=_pres_ui_step,
            )
        except PresCancelled:
            return
        finally:
            step_ph.empty()
            bar_ph.empty()
            finish_pres_job(job_id)
            st.session_state.pop("_pres_job_id", None)
        st.session_state.pop("_pres_pending_user", None)
    except ImportError as exc:
        st.session_state.pop("_pres_pending_user", None)
        msg = f"⚠️ Генерация презентаций недоступна в этой среде: {exc}"
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="presentation_gen",
        )
        return
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        st.session_state.pop("_pres_pending_user", None)
        msg = f"⚠️ Не удалось собрать презентацию: {exc}"
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="presentation_gen",
        )
        return

    _render_file_stub_response(
        file_path,
        chat_text,
        download_mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        key_prefix="presentation_dl",
    )

    hist = list(st.session_state.get("presentation_gen_chat_history", []))
    hist.append({"role": "user", "content": original_user_input})
    hist.append({"role": "assistant", "content": chat_text})
    st.session_state.presentation_gen_chat_history = hist
    st.session_state.presentation_gen_mode = True

    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=original_user_input,
        transformed_user_input=original_user_input,
        chat_answer=chat_text,
        chat_history_last=str(chat_history_last) if chat_history_last else "",
        pipeline="presentation_gen",
    )

def _ensure_comments_session_defaults() -> None:
    """Один раз инициализирует ключи session_state для панели фильтров."""
    if st.session_state.get("comments_initialized"):
        return
    st.session_state.comments_initialized = True
    st.session_state.comments_started = False
    st.session_state.comments_df = None
    st.session_state.comments_stats_df = None
    st.session_state.comments_date_from = None
    st.session_state.comments_date_to = None
    st.session_state.selected_comment_trigger = "Все триггеры"
    st.session_state.comments_trigger_index = 0
    st.session_state.show_comments_analysis = False
    st.session_state.comments_stats_shown = False
    st.session_state.comments_selected_products = []
    st.session_state.comments_selected_trigger = "Все триггеры"
    st.session_state.comments_stat_type = "Общая (по классам)"


def render_comments_analytics_panel_if_active() -> None:
    """Перерисовывает панель фильтров на каждом rerun, пока режим активен."""
    if not st.session_state.get("comments_initialized"):
        return
    memories = st.session_state.get("comments_memories")
    if not memories:
        return
    all_messages_memory, last_messages_memory = memories
    comments_get_list_values_and_chat(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        skip_welcome=True,
    )


def _comments_unique_sorted(series) -> list:
    values = [v for v in series.dropna().unique().tolist() if str(v).strip() and str(v).lower() != "nan"]
    return sorted(values, key=lambda x: str(x).lower())


def _comments_filters_config(date_from, date_to, trigger: str, products: list | None) -> dict:
    filters = [{"column": "date", "operator": "between", "value": [str(date_from), str(date_to)]}]
    if trigger and trigger != "Все триггеры":
        filters.append({"column": "trigger_name", "operator": "eq", "value": trigger})
    if products:
        filters.append({"column": "product_name", "operator": "in", "value": list(products)})
    return {"filters": filters}


def _render_comments_ai_download() -> None:
    """Кнопка скачивания Excel после AI-классификации. Таблицу не дублируем — она уже в чате."""
    if st.session_state.get("comments_stat_type") != "Классы AI":
        return
    ai_file_path = st.session_state.get("comments_ai_file_path")
    if not ai_file_path or not os.path.isfile(ai_file_path):
        return
    date_from = st.session_state.get("comments_date_from")
    with open(ai_file_path, "rb") as f:
        st.download_button(
            label="⬇️ Скачать Excel с AI классификацией",
            data=f.read(),
            file_name=f"ai_classification_{date_from.strftime('%Y%m%d') if date_from else 'comments'}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"download_ai_{Path(ai_file_path).name}",
        )


def comments_get_list_values_and_chat(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    *,
    skip_welcome: bool = False,
) -> None:
    """Инициализация агента комментариев с выбором периода и отображением статистики."""
    _ensure_comments_session_defaults()

    if not skip_welcome and not st.session_state.comments_started:
        user_prompt = COMMENTS_COMMAND
        st.chat_message("user").write(user_prompt)
        chat_answer = (
            "**💬 Анализ комментариев по триггерам**\n\n"
            "Добро пожаловать в режим анализа комментариев!\n\n"
            "**Как работать:**\n\n"
            "1️⃣ **Выберите фильтры** — период, продукты и триггер\n\n"
            "2️⃣ **Посмотрите статистику** — выберите тип:\n\n"
            "   • 📊 **Общая** — распределение по основным классам (информативные/неинформативные/недозвон)\n\n"
            "   • 📈 **Детальная** — по конкретным причинам отказов\n\n"
            "   • 🤖 **Классы AI** — AI найдёт и обновит категории причин отказов в моменте\n\n"
            "3️⃣ **Подробный анализ** — запросите в чате разбор комментариев из текущей статистики\n\n"
            "*(История диалога учитывается. Для выхода — другая команда.)*"
        )
        stream_md_message(chat_answer)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=user_prompt,
            chat_answer=chat_answer,
            transformed_user_input="",
            chat_history_last="",
            pipeline="comments_analytics",
        )
        st.session_state.comments_started = True

    if st.session_state.comments_df is None:
        try:
            with st.spinner("⏳ Загрузка данных..."):
                df_comments = _load_comments_df()
                class_mapping = {
                    0: "Информативный",
                    1: "Неинформативный (отписка)",
                    2: "Не удалось связаться",
                }
                if "class_id" in df_comments.columns:
                    df_comments["class_name"] = df_comments["class_id"].map(class_mapping)
                else:
                    st.error("❌ В данных нет колонки 'class_id'")
                    return

                if "date" in df_comments.columns:
                    df_comments["date"] = pd.to_datetime(df_comments["date"])
                    today = datetime.now().date()
                    df_comments = df_comments[df_comments["date"].dt.date <= today]
                    df_comments = df_comments[df_comments["date"].dt.year >= 2000]
                    if df_comments.empty:
                        min_date = today - timedelta(days=30)
                        max_date = today
                    else:
                        min_date = df_comments["date"].min().date()
                        max_date = df_comments["date"].max().date()
                        if max_date > today:
                            max_date = today
                else:
                    min_date = datetime.now().date() - timedelta(days=30)
                    max_date = datetime.now().date()

                st.session_state.comments_df = df_comments
                st.session_state.comments_min_date = min_date
                st.session_state.comments_max_date = max_date
                if st.session_state.comments_date_from is None:
                    st.session_state.comments_date_from = min_date
                if st.session_state.comments_date_to is None:
                    st.session_state.comments_date_to = max_date
        except Exception as e:
            st.error(f"Ошибка загрузки данных: {e}")
            import traceback
            st.code(traceback.format_exc())
            return

    df_comments = st.session_state.comments_df
    min_date = st.session_state.comments_min_date
    max_date = st.session_state.comments_max_date

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        stats_shown = st.session_state.get("comments_stats_shown", False)
        if not st.session_state.get("show_comments_analysis", False) or stats_shown:
            if st.button(
                "📊 Показать выбор фильтров",
                key="show_comments_analysis_btn",
                use_container_width=True,
                type="primary",
            ):
                st.session_state.show_comments_analysis = True
                st.session_state.comments_stats_shown = False
                st.rerun()
        else:
            if st.button(
                "❌ Скрыть выбор фильтров",
                key="hide_comments_analysis_btn",
                use_container_width=True,
                type="secondary",
            ):
                st.session_state.show_comments_analysis = False
                st.session_state.comments_stats_df = None
                st.session_state.comments_stats_shown = False
                st.rerun()

    if not st.session_state.get("show_comments_analysis", False):
        return

    show_widgets = not st.session_state.get("comments_stats_shown", False)
    if not show_widgets:
        # Таблица уже лежит в истории чата — повторный stream_md_message даёт дубль.
        _render_comments_ai_download()
        return

    st.markdown("### 📅 Выберите период")
    col1, col2 = st.columns(2)
    with col1:
        date_from = st.date_input(
            label="Дата С:",
            value=st.session_state.comments_date_from,
            min_value=min_date,
            max_value=max_date,
            key="comments_date_from_widget",
        )
        st.session_state.comments_date_from = date_from
    with col2:
        date_to = st.date_input(
            label="Дата ПО:",
            value=st.session_state.comments_date_to,
            min_value=min_date,
            max_value=max_date,
            key="comments_date_to_widget",
        )
        st.session_state.comments_date_to = date_to

    if date_from > date_to:
        st.warning("⚠️ Дата 'С' не может быть позже даты 'ПО'")

    if "product_name" in df_comments.columns:
        available_products = _comments_unique_sorted(df_comments["product_name"])
        selected_products = st.multiselect(
            label="📦 Выберите продукты:",
            options=available_products,
            default=st.session_state.get("comments_selected_products", []),
            key="comments_product_select",
            placeholder="Выберите продукты (необязательно)",
        )
        st.session_state.comments_selected_products = selected_products
        if selected_products:
            df_for_triggers = df_comments[df_comments["product_name"].isin(selected_products)]
        else:
            df_for_triggers = df_comments
    else:
        selected_products = []
        df_for_triggers = df_comments
        st.session_state.comments_selected_products = []

    if "trigger_name" in df_for_triggers.columns:
        available_triggers = ["Все триггеры"] + _comments_unique_sorted(df_for_triggers["trigger_name"])
    else:
        available_triggers = ["Все триггеры"]

    if st.session_state.get("comments_selected_trigger") not in available_triggers:
        st.session_state.comments_selected_trigger = "Все триггеры"

    selected_trigger = st.selectbox(
        label="🎯 Выберите триггер:",
        options=available_triggers,
        index=available_triggers.index(st.session_state.comments_selected_trigger),
        key="comments_trigger_select",
    )
    st.session_state.comments_selected_trigger = selected_trigger
    st.session_state.selected_comment_trigger = selected_trigger

    stat_options = ["Общая (по классам)", "Детальная (по причинам)", "Классы AI"]
    if st.session_state.get("comments_stat_type") not in stat_options:
        st.session_state.comments_stat_type = "Общая (по классам)"
    stat_type = st.radio(
        label="📈 Тип статистики:",
        options=stat_options,
        index=stat_options.index(st.session_state.comments_stat_type),
        key="comments_stat_type_radio",
        horizontal=True,
    )
    st.session_state.comments_stat_type = stat_type

    if st.button("📊 Показать статистику", key="show_comments_stats"):
        if date_from > date_to:
            st.error("❌ Дата 'С' не может быть позже даты 'ПО'")
        else:
            with st.spinner("⏳ Загрузка статистики..."):
                _show_comments_dashboard(
                    all_messages_memory,
                    last_messages_memory,
                    date_from,
                    date_to,
                    selected_trigger,
                    selected_products,
                    df_comments,
                    stat_type,
                )


def _show_comments_dashboard(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    date_from: datetime.date,
    date_to: datetime.date,
    trigger: str,
    products: list | None = None,
    df_comments: Optional[pd.DataFrame] = None,
    stat_type: str = "Общая (по классам)",
) -> None:
    """Показывает дашборд с статистикой по комментариям."""
    del last_messages_memory
    products = products or []
    try:
        df_filtered = (df_comments.copy() if df_comments is not None else _load_comments_df().copy())

        if "class_id" in df_filtered.columns:
            df_filtered["class_id"] = pd.to_numeric(df_filtered["class_id"], errors="coerce").fillna(-1).astype(int)

        if "class_id" in df_filtered.columns and "class_name" not in df_filtered.columns:
            class_mapping = {
                0: "Информативный",
                1: "Неинформативный (отписка)",
                2: "Не удалось связаться",
            }
            df_filtered["class_name"] = df_filtered["class_id"].map(class_mapping)

        if "date" in df_filtered.columns:
            df_filtered["date"] = pd.to_datetime(df_filtered["date"])
            df_filtered = df_filtered[
                (df_filtered["date"] >= pd.Timestamp(date_from))
                & (df_filtered["date"] <= pd.Timestamp(date_to) + pd.Timedelta(days=1))
            ]
        else:
            st.warning("⚠️ В данных нет колонки с датами.")
            return

        if trigger != "Все триггеры" and "trigger_name" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["trigger_name"] == trigger]

        if products and "product_name" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["product_name"].isin(products)]

        if df_filtered.empty:
            st.warning("⚠️ По выбранным критериям комментариев не найдено.")
            return

        use_ai = stat_type == "Классы AI"
        use_detailed = stat_type == "Детальная (по причинам)"
        use_general = stat_type == "Общая (по классам)"
        filters_config = _comments_filters_config(date_from, date_to, trigger, products)

        if use_detailed and "class_id" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["class_id"] == 0]

        if use_ai:
            from synaptica.backend.streamlit_functions.comments_analytics import generate_new_classification

            if "class_id" in df_filtered.columns:
                df_informative = df_filtered[df_filtered["class_id"] == 0].copy()
            else:
                df_informative = df_filtered.copy()
            if df_informative.empty:
                st.warning("⚠️ Нет информативных комментариев для AI-классификации.")
                return
            df_informative = df_informative.reset_index(drop=True)
            df_informative["orig_index"] = df_informative.index
            needed_cols = [
                "orig_index", "trigger_name", "comment_fr", "date", "trigger_desc",
                "inn", "cust_name", "segment", "cust_tb", "factory_type", "product_name",
            ]
            available_cols = [c for c in needed_cols if c in df_informative.columns]
            if "comment_fr" not in available_cols:
                st.warning("⚠️ В данных нет колонки comment_fr — AI-классификация недоступна.")
                return
            df_for_class = df_informative[available_cols].copy()

            try:
                progress_bar = st.progress(0, text="🤖 AI классифицирует комментарии...")

                def update_progress(current, total):
                    progress_bar.progress(current / total, text=f"Классификация батча {current}/{total}...")

                ai_report, ai_path, ai_stats_df = generate_new_classification(
                    df_for_class,
                    (
                        "Проанализируй комментарии и выдели основные классы/категории "
                        f"для периода {date_from.strftime('%d.%m.%Y')} - {date_to.strftime('%d.%m.%Y')}"
                    ),
                    filters_config,
                    full_report=False,
                    progress_callback=update_progress,
                )
                progress_bar.progress(1.0, text="✅ Классификация завершена!")
                st.session_state.comments_ai_classification = ai_report
                st.session_state.comments_ai_path = ai_path
                st.session_state.comments_ai_file_path = ai_path
                st.session_state.comments_stats_df = ai_stats_df
                st.session_state.comments_filtered_df = df_filtered
                st.session_state.comments_date_from = date_from
                st.session_state.comments_date_to = date_to
                st.session_state.comments_stat_type = stat_type
                st.session_state.comments_selected_trigger = trigger
                st.session_state.selected_comment_trigger = trigger
                st.session_state.comments_last_filters = filters_config
                st.session_state.comments_stats_shown = True
                st.session_state.pending_feedback_meta = {"pipeline": "comments_analytics"}

                period_str = f"{date_from.strftime('%d.%m.%Y')} - {date_to.strftime('%d.%m.%Y')}"
                chat_text = "## 🤖 Классы AI\n\n"
                chat_text += f"**📅 Период:** {period_str}\n"
                if trigger != "Все триггеры":
                    chat_text += f"**🎯 Триггер:** {trigger}\n\n"
                else:
                    chat_text += "\n"
                if products:
                    chat_text += f"**📦 Продукты:** {', '.join(products)}\n\n"
                chat_text += ai_report

                save_in_all_messages_memory_and_in_logs(
                    memory=all_messages_memory,
                    original_user_input=f"Показать AI классификацию за {period_str} по триггеру {trigger}",
                    chat_answer=chat_text,
                    transformed_user_input="",
                    chat_history_last="",
                    pipeline="comments_analytics",
                )
                st.rerun()
                return
            except Exception as e:
                st.error(f"Ошибка классификации: {e}")
                _logger.error("AI classification error: %s", e, exc_info=True)
                return

        if use_detailed:
            if "spec_name" not in df_filtered.columns:
                classific = _load_classific_df()
                df_filtered = merge_class_names(df_filtered, classific)
            if "spec_name" not in df_filtered.columns and "class_spec_id" in df_filtered.columns:
                classific = _load_classific_df()
                if "class_spec_id" in classific.columns and "class_name" in classific.columns:
                    spec_map = (
                        classific.drop_duplicates("class_spec_id")
                        .set_index("class_spec_id")["class_name"]
                    )
                    df_filtered["spec_name"] = df_filtered["class_spec_id"].map(spec_map)
            if "spec_name" not in df_filtered.columns:
                st.warning("⚠️ В данных нет колонки spec_name. Выберите другой тип статистики.")
                return
            stats = df_filtered.groupby("spec_name", as_index=False).size()
            stats["share"] = (stats["size"] / stats["size"].sum() * 100).round(2)
            stats = stats.rename(
                columns={
                    "spec_name": "Причина отказа",
                    "size": "Количество комментариев",
                    "share": "%",
                }
            )
            stats = stats.sort_values("Количество комментариев", ascending=False)
        elif use_general:
            if "class_name" not in df_filtered.columns:
                st.warning("⚠️ В данных нет колонки с классами.")
                return
            stats = df_filtered.groupby("class_name", as_index=False).size()
            stats["share"] = (stats["size"] / stats["size"].sum() * 100).round(2)
            stats = stats.rename(
                columns={
                    "class_name": "Тип комментария",
                    "size": "Количество комментариев",
                    "share": "%",
                }
            )
            stats = stats.sort_values("Количество комментариев", ascending=False)
        else:
            st.warning(f"⚠️ Неизвестный тип статистики: {stat_type}")
            return

        st.session_state.comments_stats_df = stats
        st.session_state.comments_filtered_df = df_filtered
        st.session_state.comments_date_from = date_from
        st.session_state.comments_date_to = date_to
        st.session_state.comments_stat_type = stat_type
        st.session_state.comments_selected_trigger = trigger
        st.session_state.selected_comment_trigger = trigger
        st.session_state.comments_last_filters = filters_config
        st.session_state.comments_stats_shown = True
        st.session_state.pending_feedback_meta = {"pipeline": "comments_analytics"}

        period_str = f"{date_from.strftime('%d.%m.%Y')} - {date_to.strftime('%d.%m.%Y')}"
        stat_label = "Детальная статистика" if use_detailed else "Общая статистика"
        chat_text = f"## 📊 {stat_label} по комментариям\n\n"
        chat_text += f"**📅 Период:** {period_str}\n"
        if trigger != "Все триггеры":
            chat_text += f"**🎯 Триггер:** {trigger}\n"
        if products:
            chat_text += f"**📦 Продукты:** {', '.join(products)}\n"
        chat_text += "\n"

        if use_detailed:
            chat_text += f"**Всего информативных комментариев:** {len(df_filtered)}\n\n"
            chat_text += "| Причина отказа | Количество | % |\n"
            chat_text += "|---------------|------------|-----|\n"
            for _, row in stats.iterrows():
                chat_text += (
                    f"| {row['Причина отказа']} | {row['Количество комментариев']} | {row['%']:.2f}% |\n"
                )
        else:
            chat_text += f"**Всего комментариев по отказам:** {len(df_filtered)}\n\n"
            chat_text += "| Тип комментария | Количество | % |\n"
            chat_text += "|---------------|------------|-----|\n"
            for _, row in stats.iterrows():
                chat_text += (
                    f"| {row['Тип комментария']} | {row['Количество комментариев']} | {row['%']:.2f}% |\n"
                )

        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=f"Показать статистику за {period_str} по триггеру {trigger} ({stat_label})",
            chat_answer=chat_text,
            transformed_user_input="",
            chat_history_last="",
            pipeline="comments_analytics",
        )
        st.rerun()
    except Exception as e:
        st.error(f"Ошибка при загрузке статистики: {e}")
        import traceback
        st.code(traceback.format_exc())


def reset_to_zero_comments_pipeline() -> None:
    """Сброс состояния агента комментариев при выходе из режима."""
    for key in (
        "comments_initialized",
        "comments_started",
        "comments_df",
        "comments_stats_df",
        "comments_filtered_df",
        "comments_date_from",
        "comments_date_to",
        "comments_min_date",
        "comments_max_date",
        "selected_comment_trigger",
        "comments_trigger_index",
        "comments_memories",
        "comments_selected_trigger",
        "comments_selected_products",
        "comments_stat_type",
        "show_comments_analysis",
        "comments_stats_shown",
        "comments_ai_file_path",
        "comments_ai_path",
        "comments_ai_classification",
        "comments_last_filters",
        "comments_last_query",
    ):
        st.session_state.pop(key, None)


def reset_to_zero_logs_view_pipeline() -> None:
    _reset_to_zero_logs_view_pipeline_orig()
    reset_to_zero_comments_pipeline()


def write_logs_view_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    """Запуск агента комментариев с выбором периода и статистикой."""
    username = str(st.session_state.get("username", ""))
    if not is_logs_view_allowed_user(username):
        msg = f"⛔ У вас нет доступа к режиму {COMMENTS_COMMAND}."
        if show_user_msg:
            st.chat_message("user").write(COMMENTS_COMMAND)
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=COMMENTS_COMMAND,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last="",
            pipeline="comments_analytics",
        )
        return

    reset_to_zero_ALL_pipelines()
    st.session_state.logs_view_mode = True
    st.session_state.logs_view_chat_history = []
    st.session_state.comments_memories = (all_messages_memory, last_messages_memory)
    comments_get_list_values_and_chat(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
    )


def write_logs_view_button(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    write_logs_view_start(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
    )


def write_logs_view_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
) -> None:
    st.session_state.comments_memories = (all_messages_memory, last_messages_memory)
    if not is_logs_view_allowed_user(str(st.session_state.get("username", ""))):
        msg = "⛔ У вас нет доступа к анализу комментариев."
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="comments_analytics",
        )
        reset_to_zero_logs_view_pipeline()
        return

    try:
        with st.spinner("Анализирую комментарии…"):
            enhanced_request = original_user_input
            filter_parts = []

            date_from = st.session_state.get("comments_date_from")
            date_to = st.session_state.get("comments_date_to")
            if date_from and date_to:
                filter_parts.append(
                    f"период с {date_from.strftime('%d.%m.%Y')} по {date_to.strftime('%d.%m.%Y')}"
                )

            trigger = st.session_state.get("comments_selected_trigger", "Все триггеры")
            if trigger and trigger != "Все триггеры":
                filter_parts.append(f"триггер '{trigger}'")

            products = st.session_state.get("comments_selected_products", [])
            if products:
                filter_parts.append(f"продукты: {', '.join(products)}")

            if filter_parts:
                enhanced_request = f"{original_user_input}. Фильтры: {', '.join(filter_parts)}"

            file_path, chat_text, filters_config = run_comments_analytics(
                enhanced_request,
                last_query=st.session_state.get("comments_last_query"),
                last_filters=st.session_state.get("comments_last_filters"),
                neural_file_path=st.session_state.get("comments_ai_file_path"),
                last_stat_type=st.session_state.get("comments_stat_type"),
            )
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        msg = f"⚠️ Не удалось проанализировать комментарии: {exc}"
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="comments_analytics",
        )
        return

    _render_file_stub_response(
        file_path,
        chat_text,
        download_mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key_prefix="comments_dl",
    )

    hist = list(st.session_state.get("logs_view_chat_history", []))
    hist.append({"role": "user", "content": original_user_input})
    hist.append({"role": "assistant", "content": chat_text})
    st.session_state.logs_view_chat_history = hist
    st.session_state.logs_view_mode = True
    st.session_state.comments_last_query = original_user_input
    st.session_state.comments_last_filters = filters_config

    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=original_user_input,
        transformed_user_input=original_user_input,
        chat_answer=chat_text,
        chat_history_last=str(chat_history_last) if chat_history_last else "",
        pipeline="comments_analytics",
    )


def write_call_upload_start(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    username = str(st.session_state.get("username", ""))
    if not is_call_upload_allowed_user(username):
        msg = f"⛔ У вас нет доступа к режиму {CALL_UPLOAD_COMMAND}."
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=CALL_UPLOAD_COMMAND,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last="",
            pipeline="call_upload",
        )
        return

    _openclaw_start_impl(
        command=CALL_UPLOAD_COMMAND,
        welcome_msg=CALL_UPLOAD_INSTRUCTION,
        mode_key="call_upload_mode",
        hist_key="call_upload_chat_history",
        pipeline="call_upload",
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        access_check=is_call_upload_allowed_user,
        show_user_msg=show_user_msg,
    )
    st.session_state.call_upload_uploader_key = f"cu_{uuid.uuid4().hex[:8]}"


def write_call_upload_button(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    write_call_upload_start(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
    )


# ============================================================
# OPTIMIZER — подбор оптимальной структуры хеджирования
# ============================================================
OPTIMIZER_COMMAND = "/optimizer"


def write_optimizer_button(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    """Активирует режим Optimizer.

    Само сообщение в историю не пишем: панель-форма и результат подбора
    рендерятся в ``render_optimizer_panel`` — иначе из истории дублировался бы
    лишний приветственный блок.
    """
    del all_messages_memory, last_messages_memory  # не используются: ничего не сохраняем
    st.session_state.optimizer_mode = True
    st.session_state.pop("optimizer_last_result", None)


_OPTIMIZER_FIELD_LABELS = {
    "Strike": "Страйк",
    "CapStrike": "Страйк кэпа",
    "FloorStrike": "Страйк флора",
    "Barrier": "Барьер",
    "Premium": "Премия",
    "TotalPayoff": "Совокупная выплата",
    "Profit": "Прибыль",
}
_OPTIMIZER_FIELDS_BY_PRODUCT = {
    "vanilla_cap": ("Strike", "Premium", "TotalPayoff"),
    "barrier_cap": ("Strike", "Barrier", "Premium", "TotalPayoff"),
    "vanilla_floor": ("Strike", "Premium", "TotalPayoff"),
    "collar": ("CapStrike", "FloorStrike", "Premium", "TotalPayoff"),
    "barrier_collar": ("CapStrike", "FloorStrike", "Barrier", "Premium", "TotalPayoff"),
}


def _format_optimizer_best(best: dict, product_key: str) -> str:
    fields = _OPTIMIZER_FIELDS_BY_PRODUCT.get(product_key, tuple(_OPTIMIZER_FIELD_LABELS))
    lines = []
    for k in fields:
        if k in best and best[k] is not None:
            label = _OPTIMIZER_FIELD_LABELS.get(k, k)
            lines.append(f"**{label}:** {best[k]}")
    return "  \n".join(lines)


def _optimizer_header_md(result: dict) -> str:
    cond = result.get("market_condition")
    cond_ru = "Бэквордация (ставки снижаются)" if cond == "backwardation" else "Контанго (ставки растут)"
    rates = result.get("rates", {})
    inputs = result.get("inputs", {})
    nominal = inputs.get("nominal", 0)
    return (
        f"**Рыночная ситуация:** {cond_ru}  \n"
        f"ЕТС {rates.get('ets')}% · Своп {rates.get('swap_rate')}% · КС {rates.get('ks')}%  \n"
        f"Номинал {nominal:,} ₽ · Срок {inputs.get('days')} дн. · "
        f"Тип ставки: {'плавающая' if inputs.get('loan_type') == 'floating' else 'фиксированная'}"
    )


def _optimizer_product_md(p: dict) -> str:
    lines = [f"#### {p.get('name', p.get('key'))}"]
    if p.get("error"):
        lines.append(f"Недоступно: {p['error']}")
        return "\n\n".join(lines)
    best = p.get("best") or {}
    summary = _format_optimizer_best(best, p.get("key", ""))
    if summary:
        lines.append(f"**Лучшая структура**  \n{summary}")
    return "\n\n".join(lines)


def _render_optimizer_results(result: dict) -> None:
    st.markdown(_optimizer_header_md(result))
    products = result.get("products", [])
    if not any(p.get("best") and not p.get("error") for p in products):
        st.warning("Не удалось подобрать структуры по заданным параметрам.")
    for p in products:
        st.markdown(_optimizer_product_md(p))


def _optimizer_summary_text(result: dict) -> str:
    cond = result.get("market_condition")
    cond_ru = "бэквордация" if cond == "backwardation" else "контанго"
    parts = [f"Optimizer: рыночная ситуация — {cond_ru}."]
    for p in result.get("products", []):
        if p.get("error") or not p.get("best"):
            continue
        best = p["best"]
        kv = ", ".join(
            f"{_OPTIMIZER_FIELD_LABELS.get(k, k)}={v}"
            for k, v in best.items()
            if k in _OPTIMIZER_FIELD_LABELS
        )
        parts.append(f"{p.get('name')}: {kv}")
    return "\n".join(parts)


def render_optimizer_panel(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    """Форма ввода 4 параметров + запуск подбора со спиннером и выводом результатов."""
    if not getattr(st.session_state, "optimizer_mode", False):
        return

    import requests
    from synaptica.backend.streamlit_functions import optimizer_logic

    with st.chat_message("assistant"):
        st.markdown("### 🧮 Optimizer — параметры кредита клиента")
        with st.form("optimizer_form"):
            col1, col2 = st.columns(2)
            loan_amount = col1.number_input(
                "Сумма кредита, млн ₽", min_value=0.1, value=10.0, step=1.0, format="%.2f"
            )
            loan_term = col2.number_input(
                "Срок кредита, лет", min_value=0.1, max_value=10.0, value=1.0, step=0.5, format="%.2f"
            )
            client_loan_rate = col1.number_input(
                "Ставка клиента, %", min_value=0.0, value=16.5, step=0.25, format="%.2f"
            )
            loan_type_label = col2.selectbox("Тип ставки кредита", ["Плавающая", "Фиксированная"])
            submitted = st.form_submit_button("🚀 Optimize", type="primary")

        if submitted:
            loan_type = "floating" if loan_type_label == "Плавающая" else "fixed"
            status = st.status("Подбираю оптимальную структуру…", expanded=True)
            header_ph = st.empty()
            products_ph = st.empty()
            warning_ph = st.empty()
            live_products: list[dict] = []
            live_meta: dict = {}

            def _progress(text: str) -> None:
                if text:
                    status.update(label=text)

            def _on_meta(data: dict) -> None:
                live_meta.clear()
                live_meta.update(data)
                header_ph.markdown(_optimizer_header_md(data))

            def _on_product(product: dict) -> None:
                live_products.append(product)
                products_ph.markdown("\n\n".join(_optimizer_product_md(p) for p in live_products))

            try:
                result = optimizer_logic.run_optimizer(
                    loan_amount=loan_amount,
                    loan_term=loan_term,
                    client_loan_rate=client_loan_rate,
                    loan_type=loan_type,
                    progress_cb=_progress,
                    meta_cb=_on_meta,
                    product_cb=_on_product,
                )
                status.update(label="Готово", state="complete")
                if not live_meta:
                    header_ph.markdown(_optimizer_header_md(result))
                if len(live_products) < len(result.get("products", [])):
                    products_ph.markdown(
                        "\n\n".join(_optimizer_product_md(p) for p in result.get("products", []))
                    )
                if not any(p.get("best") and not p.get("error") for p in result.get("products", [])):
                    warning_ph.warning("Не удалось подобрать структуры по заданным параметрам.")

                st.session_state.optimizer_last_result = result
                save_in_both_memories_and_in_logs(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                    original_user_input=(
                        f"{OPTIMIZER_COMMAND} {loan_amount} млн, {loan_term} лет, "
                        f"{client_loan_rate}%, {loan_type}"
                    ),
                    transformed_user_input=OPTIMIZER_COMMAND,
                    chat_answer=_optimizer_summary_text(result),
                    chat_history_last="",
                    pipeline="optimizer",
                )
            except optimizer_logic.OptimizerServiceError as exc:
                status.update(label="Ошибка подбора", state="error")
                _logger.exception("optimizer service returned error")
                st.error(f"⚠️ Сервис оптимизатора вернул ошибку: {exc}")
            except requests.exceptions.RequestException:
                status.update(label="Сервис недоступен", state="error")
                _logger.exception("optimizer service unreachable")
                st.error(
                    "⚠️ Не удаётся подключиться к сервису оптимизатора по адресу "
                    f"`{optimizer_logic.OPTIMIZER_SERVICE_URL}`.\n\n"
                    "Запустите `optimizer_service.py` на этом же сервере "
                    "(там, где доступна `pslibforsonya`)."
                )
            except Exception as exc:  # noqa: BLE001
                status.update(label="Ошибка", state="error")
                _logger.exception("optimizer failed")
                st.error(f"⚠️ Не удалось выполнить оптимизацию: {exc}")
        elif st.session_state.get("optimizer_last_result"):
            _render_optimizer_results(st.session_state["optimizer_last_result"])


def _process_call_upload_file(
    uploaded_file,
    *,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last,
) -> None:
    try:
        ref_path = resolve_call_data_path()
    except FileNotFoundError as exc:
        msg = f"⚠️ {exc}"
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=f"📎 {uploaded_file.name}",
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="call_upload",
        )
        return

    CALLS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = uploaded_file.name.replace(" ", "_")
    raw_path = CALLS_DATA_DIR / f"upload_{safe_name}"
    raw_path.write_bytes(uploaded_file.getvalue())

    user_line = f"📎 {uploaded_file.name}"
    st.chat_message("user").write(user_line)

    def _chat_progress(text: str) -> None:
        with st.chat_message("assistant"):
            st.markdown(text)

    try:
        with st.status("Обрабатываю звонки…", expanded=True) as status:
            def _progress(text: str) -> None:
                _chat_progress(text)
                status.write(re.sub(r"\*\*|`|_", "", text))

            _progress("🚀 Запускаю пайплайн: prepare → tag → merge…")
            result = run_call_upload_pipeline(
                raw_path,
                ref_path=ref_path,
                work_dir=CALLS_DATA_DIR,
                progress_cb=_progress,
            )
            status.update(label="Готово", state="complete")

        final_file = Path(result["final_path"])
        ref_backup_file = Path(result["ref_backup_path"])
        merge = result.get("merge_stats") or {}
        msg = (
            f"✅ **Готово.**\n\n"
            f"- Подготовлено к разметке: **{result['prepared_rows']}**\n"
            f"- REF: `{result['ref_path']}` ({merge.get('ref_rows', '?')} строк)\n"
            f"- Добавлено новых: **{merge.get('appended', '?')}**\n"
            f"- Итого в merge: **{merge.get('out_rows', '?')}** строк\n"
            f"- Бэкап REF: `{ref_backup_file.name}`\n"
            f"- Итоговый файл: `{final_file}`"
        )
        stream_md_message(msg)

    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        msg = f"⚠️ Ошибка пайплайна: {exc}"
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=user_line,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="call_upload",
        )
        return

    st.session_state.call_upload_uploader_key = f"cu_{uuid.uuid4().hex[:8]}"
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=user_line,
        transformed_user_input=user_line,
        chat_answer=msg,
        chat_history_last=str(chat_history_last) if chat_history_last else "",
        pipeline="call_upload",
    )


def _process_call_upload_saved_file(
    *,
    original_filename: str,
    saved_path: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last,
) -> None:
    try:
        ref_path = resolve_call_data_path()
    except FileNotFoundError as exc:
        msg = f"⚠️ {exc}"
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=f"📎 {original_filename}",
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="call_upload",
        )
        return

    raw_path = Path(saved_path)
    user_line = f"📎 {original_filename}"
    st.chat_message("user").write(user_line)

    def _chat_progress(text: str) -> None:
        with st.chat_message("assistant"):
            st.markdown(text)

    try:
        with st.status("Обрабатываю звонки…", expanded=True) as status:
            def _progress(text: str) -> None:
                _chat_progress(text)
                status.write(re.sub(r"\*\*|`|_", "", text))

            _progress("🚀 Запускаю пайплайн: prepare → tag → merge…")
            result = run_call_upload_pipeline(
                raw_path,
                ref_path=ref_path,
                work_dir=CALLS_DATA_DIR,
                progress_cb=_progress,
            )
            status.update(label="Готово", state="complete")

        final_file = Path(result["final_path"])
        ref_backup_file = Path(result["ref_backup_path"])
        merge = result.get("merge_stats") or {}
        msg = (
            f"✅ **Готово.**\n\n"
            f"- Подготовлено к разметке: **{result['prepared_rows']}**\n"
            f"- REF: `{result['ref_path']}` ({merge.get('ref_rows', '?')} строк)\n"
            f"- Добавлено новых: **{merge.get('appended', '?')}**\n"
            f"- Итого в merge: **{merge.get('out_rows', '?')}** строк\n"
            f"- Бэкап REF: `{ref_backup_file.name}`\n"
            f"- Итоговый файл: `{final_file}`"
        )
        stream_md_message(msg)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        msg = f"⚠️ Ошибка пайплайна: {exc}"
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=user_line,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="call_upload",
        )
        return

    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=user_line,
        transformed_user_input=user_line,
        chat_answer=msg,
        chat_history_last=str(chat_history_last) if chat_history_last else "",
        pipeline="call_upload",
    )


def render_call_upload_uploader(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    if not getattr(st.session_state, "call_upload_mode", False):
        return
    if not is_call_upload_allowed_user(str(st.session_state.get("username", ""))):
        return

    if "call_upload_uploader_key" not in st.session_state:
        st.session_state.call_upload_uploader_key = "cu_0"

    uploaded = st.file_uploader(
        "Загрузите Excel или CSV со звонками",
        type=["xlsx", "xls", "csv"],
        key=st.session_state.call_upload_uploader_key,
    )
    if uploaded is not None:
        _process_call_upload_file(
            uploaded,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=st.session_state.get("chat_history_last"),
        )


# Старый алиас — ведёт на coder.
def write_openclaw_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    write_openclaw_coder_answer(original_user_input, all_messages_memory, last_messages_memory)


def registration_vidget(
    authenticator: stauth.Authenticate, config: dict, settings: DefaultSettings
) -> None:
    st.info(
        """Обращаем ваше внимание, что имя пользователя и логин должны содержать только латинские \
буквы (без цифр, нижних подчёркиваний, спец. символов (%, &, и т.д.))

При регистрации необходимо указывать корпоративную почту с доменом @sberbank.ru или @sber.ru"""
    )
    try:
        (email_of_registered_user, username_of_registered_user, name_of_registered_user) = (
            authenticator.register_user(pre_authorization=False, domains=["sberbank.ru", "sber.ru"])
        )
        if email_of_registered_user:
            with open(settings.config_path, "w", encoding="utf-8") as file:
                yaml.dump(config, file, default_flow_style=False)
            st.success("User registered successfully")
            st.session_state.exec_register_pipeline = False
    except RegisterError as e:
        _logger.exception("streamlit_function_failed")
        st.error(e)
        st.session_state.exec_register_pipeline = False
        st.error("Пожалуйста, обновите страницу и попробуйте ещё раз")


def _register_feedback_pipeline_for_run(run_id, pipeline: str) -> None:
    """Привязать колонку pipeline в reactions к ответу с данным run_id."""
    if run_id is None:
        return
    st.session_state.setdefault("feedback_meta_by_run_id", {})
    st.session_state["feedback_meta_by_run_id"][str(run_id)] = {"pipeline": pipeline}


def _coerce_chat_input_for_memory(original_user_input) -> str:
    """HumanMessage принимает строку; selectbox персонализации отдаёт (label, index)."""
    if isinstance(original_user_input, tuple) and len(original_user_input) > 0:
        return str(original_user_input[0])
    if isinstance(original_user_input, list) and len(original_user_input) > 0:
        return str(original_user_input[0])
    if original_user_input is None:
        return ""
    return str(original_user_input)


def save_in_all_messages_memory_and_in_logs(
    memory: ConversationBufferMemory,
    original_user_input: str,
    chat_answer: str,
    transformed_user_input: str,
    chat_history_last: str,
    *,
    pipeline: str | None = None,
) -> None:
    original_user_input = _coerce_chat_input_for_memory(original_user_input)
    memory.save_context({"input": original_user_input}, {"output": chat_answer})
    st.session_state.run_id = uuid.uuid4()

    meta = dict(st.session_state.get("pending_feedback_meta") or {})
    if pipeline is not None:
        meta["pipeline"] = pipeline
    elif "pipeline" not in meta:
        meta["pipeline"] = "default"

    st.session_state.setdefault("feedback_meta_by_run_id", {})
    st.session_state["feedback_meta_by_run_id"][str(st.session_state.run_id)] = meta

    st.session_state.pending_feedback_meta = {}

    save_logs(
        full_response=chat_answer,
        original_user_input=original_user_input,
        transformed_user_input=transformed_user_input,
        chat_history_last=chat_history_last,
    )


def save_in_both_memories_and_in_logs(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    original_user_input: str,
    transformed_user_input: str,
    chat_answer: str,
    chat_history_last: str,
    *,
    pipeline: str | None = None,
) -> None:
    original_user_input = _coerce_chat_input_for_memory(original_user_input)
    all_messages_memory.save_context({"input": original_user_input}, {"output": chat_answer})
    last_messages_memory.save_context({"input": original_user_input}, {"output": chat_answer})
    st.session_state.run_id = uuid.uuid4()

    meta = dict(st.session_state.get("pending_feedback_meta") or {})
    if pipeline is not None:
        meta["pipeline"] = pipeline
    elif "pipeline" not in meta:
        meta["pipeline"] = "default"

    st.session_state.setdefault("feedback_meta_by_run_id", {})
    st.session_state["feedback_meta_by_run_id"][str(st.session_state.run_id)] = meta

    st.session_state.pending_feedback_meta = {}

    save_logs(
        full_response=chat_answer,
        original_user_input=original_user_input,
        transformed_user_input=transformed_user_input,
        chat_history_last=chat_history_last,
    )


def stream_md_message(data: str, unsafe_allow_html = False) -> None:
    full_response = ""
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        for chunk in data.split(" "):
            full_response += chunk + " "
            time.sleep(0.02)
            message_placeholder.markdown(full_response + " ", unsafe_allow_html=unsafe_allow_html)
        for doc_path in re.findall(r'(/[\w/.\-]+\.docx)', full_response):
            p = Path(doc_path)
            if p.exists():
                st.download_button(
                    label=f"⬇ Скачать {p.name}",
                    data=p.read_bytes(),
                    file_name=p.name,
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )
        _render_pdf_download_buttons(_collect_pdf_paths(full_response), "stream")


def stream_md_message_with_role(data: str, role: str) -> None:
    full_response = ""
    with st.chat_message(role):
        message_placeholder = st.empty()
        for chunk in data.split(" "):
            full_response += chunk + " "
            time.sleep(0.02)
            message_placeholder.markdown(full_response + " ", unsafe_allow_html=True)


def stream_md_message_with_picture(data: str, image_data_path: str) -> None:
    full_response = ""
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        for chunk in data.split(" "):
            full_response += chunk + " "
            time.sleep(0.02)
            message_placeholder.markdown(full_response + " ")
        st.image(image_data_path)


def save_logs(
    full_response: str,
    original_user_input: str,
    transformed_user_input: str,
    chat_history_last: str,
) -> None:
    columns = [
        "run_id", "timestamp", "user_id", "full_response",
        "input", "transformed_user_input", "chat_history", "pipeline",
    ]
    rid = str(st.session_state.run_id)
    meta = (st.session_state.get("feedback_meta_by_run_id") or {}).get(rid) or {}
    row = {
        "run_id": rid,
        "timestamp": str(datetime.today()),
        "user_id": str(st.session_state["name"]),
        "full_response": str(full_response),
        "input": str(original_user_input),
        "transformed_user_input": str(transformed_user_input),
        "chat_history": str(chat_history_last),
        "pipeline": str(meta.get("pipeline", "") or ""),
    }
    path = settings.all_messages_data_path
    with open(path, "a", encoding="utf-8", newline="") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        writer = csv.DictWriter(f, fieldnames=columns, quoting=csv.QUOTE_ALL)
        if f.tell() == 0:
            writer.writeheader()
        writer.writerow(row)


def save_feedback() -> None:
    feedback_option = "thumbs"
    if not st.session_state.get("run_id"):
        return

    run_id = st.session_state.run_id
    feedback = streamlit_feedback(
        feedback_type=feedback_option,
        optional_text_label="[Optional] Пожалуйста, объясни свой фидбэк",
        key=f"feedback_{run_id}",
    )

    score_mappings = {"thumbs": {"👍": 1, "👎": 0}}
    meta_by_run = st.session_state.get("feedback_meta_by_run_id", {}) or {}
    meta = meta_by_run.get(str(run_id), {}) or {}
    pipeline = str(meta.get("pipeline", "default"))

    if not feedback:
        return

    score = score_mappings[feedback_option].get(feedback["score"])
    if score is None:
        st.warning("Invalid feedback score.")
        return

    # Protect against re-saving on Streamlit reruns (widget returns stored value each rerun)
    saved_ids = st.session_state.setdefault("saved_feedback_run_ids", set())
    if str(run_id) in saved_ids:
        return
    saved_ids.add(str(run_id))

    feedback_type_str = f"{feedback_option} {feedback['score']}"
    columns = ["run_id", "user_id", "feedback_type_str", "score", "comment", "pipeline"]
    row = {
        "run_id": str(run_id),
        "user_id": str(st.session_state["name"]),
        "feedback_type_str": feedback_type_str,
        "score": str(score),
        "comment": str(feedback.get("text")),
        "pipeline": pipeline,
    }
    path = settings.reactions_data_path
    with open(path, "a", encoding="utf-8", newline="") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        writer = csv.DictWriter(f, fieldnames=columns, quoting=csv.QUOTE_ALL)
        if f.tell() == 0:
            writer.writeheader()
        writer.writerow(row)


def start_products_pipeline() -> None:
    reset_to_zero_ALL_pipelines()

    st.session_state.show_selectbox_products = True


def products_get_list_and_chat(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_input_pipeline()
    st.session_state.selectbox_key_products = str(uuid.uuid4())
    st.session_state.selected_product = None
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    user_prompt = "/products"
    st.chat_message("user").write(user_prompt)
    products = list(get_products_with_descriptions_data().keys())
    chat_answer = "Выберите продукт"
    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=user_prompt,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="products",
    )
    st.session_state.products_list = products
    st.session_state.show_selectbox_products = False
    st.session_state.selectbox_key_products = str(uuid.uuid4())


def products_get_product_from_list() -> None:
    reset_to_zero_input_pipeline()
    st.session_state.show_selectbox_products = False
    st.session_state.selected_product = None
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.selected_product = st.selectbox(
        "product",
        [""] + st.session_state.products_list,
        key=st.session_state.selectbox_key_products,
        label_visibility="hidden",
    )


def products_finish(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:

    reset_to_zero_input_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.chat_message("user").write(st.session_state.selected_product)

    products_descriptions = get_products_with_descriptions_data()

    data_products = products_descriptions[st.session_state.selected_product]
    stream_md_message(data_products)

    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=st.session_state.selected_product,
        transformed_user_input="",
        chat_answer=data_products,
        chat_history_last="",
        pipeline="products",
    )

    reset_to_zero_products_pipeline()
    reset_to_zero_input_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_sales_pipeline()


def start_pers_pipeline() -> None:
    reset_to_zero_ALL_pipelines()

    st.session_state.show_selectbox_pers = True


def pers_get_list_values_and_chat(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_potential_pipeline()
    
    st.session_state.selected_option_pers = None
    st.session_state.pers_list = None
    st.session_state.pers_list_period = None
    st.session_state.selected_option_pers_period = None
    st.session_state.pers_list_period = None
    st.session_state.selectbox_key_pers_period = str(uuid.uuid4())
    st.session_state.start_period_pipeline = False
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_sales_pipeline()

    client_list = [
                    "Клиент осуществляет внешнюю экономическую деятельность (ВЭД)", "Клиент несколько раз запрашивал котировки", "Клиент заключил сделку впервые за долгое время",
                    "Прохождение тестов Винограда (Winograd schema challenge)"
                   ]
    
    user_prompt = "/personalization"
    st.chat_message("user").write(user_prompt)
    chat_answer = "**Тестирование персонификации модели в диалогах с клиентами**\n\n*(ВАЖНО: история предыдущего диалога учитываться НЕ будет. После выхода из режима тестирования история диалога удаляется!)*\n\nВыберите сценарий из списка ниже:"
    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=user_prompt,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="personalization",
    )
    st.session_state.pers_list = client_list
    st.session_state.show_selectbox_pers = False
    st.session_state.selectbox_key_pers = str(uuid.uuid4())


def pers_get_pers_from_list() -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    st.session_state.show_selectbox_pers = False
    st.session_state.pers_list_period = None
    st.session_state.selected_option_pers_period = None

    st.session_state.show_selectbox_pers_period = False
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.selected_option_pers = st.selectbox(
        "pers",
        [""] + st.session_state.pers_list,
        key=st.session_state.selectbox_key_pers,
        label_visibility="hidden",
    )
    pers_dd = {
        "": ("", 0),
        "Клиент осуществляет внешнюю экономическую деятельность (ВЭД)": (USER_1, 1),
        "Клиент несколько раз запрашивал котировки": (USER_2, 2),
        "Клиент заключил сделку впервые за долгое время": (USER_3, 3),
        "Прохождение тестов Винограда (Winograd schema challenge)": ("""Ниже будет представлен ряд вопросов на проверку уровня понимания контекста предложения и реальных ответов Синаптики на них.\n\nВопросы составлялись в парадигме <a href = "https://en.wikipedia.org/wiki/Winograd_schema_challenge">теста Винограда</a>. Вы также можете задать похожие вопросы в диалоге ниже!""", 4)
    }
    st.session_state.selected_option_pers = pers_dd[st.session_state.selected_option_pers]
    st.session_state.start_period_pipeline = True


def show_pers_period(all_messages_memory, last_messages_memory) -> None:
    if (st.session_state.selected_option_pers[0] != "") and (st.session_state.selected_option_pers[1] < 4):
        st.session_state.show_pers_period_ = True
    elif (st.session_state.selected_option_pers[0] != ""):
        df = pd.read_excel(Path(__file__).resolve().parent / "схема_винограда_4.xlsx")
        dd = df[["Вопрос", "Ответ", "Ответ Синпатики"]].to_dict("split")["data"]
        dd_good = [(x[0].strip() + "\n\n**Ответ: " + x[1].strip() + "**", x[2]) for x in dd]
        dd_last = []
        for i in range(len(dd_good)):
            dd_last.append({"role": "user", "text_md": dd_good[i][0]})
            dd_last.append({"role": "assistant", "text_md": dd_good[i][1]})

        st.session_state.show_selectbox_pers = False
        st.session_state.pers_list = None
        st.session_state.selectbox_key_pers = str(uuid.uuid4())
        st.session_state.selected_option_pers_period = None
        st.session_state.selectbox_key_pers_period = str(uuid.uuid4())
        
        st.chat_message("user").write(st.session_state.selected_option_pers[0], unsafe_allow_html = True)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=st.session_state.selected_option_pers[0],
            chat_answer="---",
            transformed_user_input="",
            chat_history_last="",
            pipeline="personalization",
        )

        for m in dd_last:
            role = "assistant" if m.get("role") != "user" else "user"
            text = m.get("text_md", "")
            if not text and role == "user":
                text = m.get("text", "")
            elif not text:
                continue

            stream_md_message_with_role(text, role)

            try:
                if role == "user":
                    all_messages_memory.chat_memory.add_user_message(text)
                    last_messages_memory.chat_memory.add_user_message(text)
                else:
                    all_messages_memory.chat_memory.add_ai_message(text)
                    last_messages_memory.chat_memory.add_ai_message(text)
            except Exception:
                pass

        st.session_state.show_selectbox_pers_period = False
        st.session_state.start_period_pipeline = False
        st.session_state.show_pers_period_ = False
        st.session_state.selectbox_key_pers_period = str(uuid.uuid4())

        reset_to_zero_ALL_pipelines()



def exit_button_for_commands(b, c, text1, text2, reset_pipe, all_messages_memory, last_messages_memory) -> None:
    top = st.columns([1 - b - c, b, c])
    top[0].markdown(text1) # f"**Продолжить диалог о звонках**"
    top[1].markdown(f"<span style=\"font-size: 14px; line-height: 0.5; font-style: italic; font-family: var(--font);\">{text2}</span>", unsafe_allow_html=True) # Выйти из разговора:
    if top[2].button("✕", key="call_exit_btn", help="Выход"):
        reset_to_zero_ALL_pipelines()
        for mem in (all_messages_memory, last_messages_memory):
            try:
                mem.clear()
            except Exception:
                try:
                    mem.chat_memory.messages = []
                except Exception:
                    pass

        for k in ("langchain_messages", "messages", "last_messages", "chat_history", "chat_history_last"):
            if k in st.session_state:
                v = st.session_state.get(k)
                if isinstance(v, list):
                    st.session_state[k] = []
                else:
                    st.session_state[k] = None
        
        _maybe_call(reset_pipe) # "reset_to_zero_calls_pipeline"
        return "-"

    _maybe_call("reset_to_zero_input_pipeline")
    return "+"

def pers_start_period_pipeline() -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    st.session_state.show_selectbox_pers = False

    st.session_state.pers_list = None
    st.session_state.selectbox_key_pers = str(uuid.uuid4())
    st.session_state.selected_option_pers_period = None

    st.session_state.selectbox_key_pers_period = str(uuid.uuid4())
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.show_selectbox_pers_period = True
    st.session_state.start_period_pipeline = False
    st.session_state.show_pers_period_ = False


def pers_get_list_period_and_chat(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    st.session_state.show_selectbox_pers = False
    st.session_state.pers_list = None
    st.session_state.selected_option_pers_period = None
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.chat_message("user").write(st.session_state.selected_option_pers[0], unsafe_allow_html = True)

    st.session_state.show_selectbox_pers_period = False
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_option_pers[0],
        chat_answer="---",
        transformed_user_input="",
        chat_history_last="",
        pipeline="personalization",
    )
    st.session_state.pers_list_period = True

    st.session_state.show_selectbox_pers_period = False
    st.session_state.selectbox_key_pers_period = str(uuid.uuid4())

    st.session_state.langchain_messages = st.session_state.langchain_messages[-4:]
    st.session_state.messages = []
    st.session_state.last_messages = []

def _maybe_call(name):
    fn = globals().get(name)
    if callable(fn):
        fn()


def pick_personification_scenario() -> None:
    st.write("Выберите вариант продолжения диалога:")
    cols = st.columns(2)
    companies = ["Создать новый диалог", "Продолжить в диалоге из примера"]
    for i, c in enumerate(companies):
        if cols[i].button(c, use_container_width=True):
            st.session_state.personification_scenario = c
            st.session_state.personification_seeded_scenario = None
            st.rerun()
    # Не используем st.stop(): иначе не доходят до конца скрипта save_feedback и прочий UI.


def seed_personification_dialogue(all_messages_memory, last_messages_memory):
    script = PERSONIFICATION_SCRIPTS.get(st.session_state.selected_option_pers, [])
    if not script:
        return

    for m in script:
        role = "assistant" if m.get("role") != "user" else "user"
        text = m.get("text_md", "")
        if not text and role == "user":
            text = m.get("text", "")
        elif not text:
            continue

        stream_md_message_with_role(text, role)

        try:
            if role == "user":
                all_messages_memory.chat_memory.add_user_message(text)
                last_messages_memory.chat_memory.add_user_message(text)
            else:
                all_messages_memory.chat_memory.add_ai_message(text)
                last_messages_memory.chat_memory.add_ai_message(text)
        except Exception:
            pass

    rid = uuid.uuid4()
    st.session_state.run_id = rid
    _register_feedback_pipeline_for_run(rid, "personalization")


def seed_personification_dialogue_new(all_messages_memory, last_messages_memory):
    script = PERSONIFICATION_SCRIPTS.get(st.session_state.selected_option_pers, [])
    if not script:
        return

    # Блокируем вывод кнопок до конца диалога
    with st.container():
        for m in script:
            role = "assistant" if m.get("role") != "user" else "user"
            text = m.get("text_md", "")
            if not text and role == "user":
                text = m.get("text", "")
            elif not text:
                continue

            stream_md_message_with_role(text, role)

            try:
                if role == "user":
                    all_messages_memory.chat_memory.add_user_message(text)
                    last_messages_memory.chat_memory.add_user_message(text)
                else:
                    all_messages_memory.chat_memory.add_ai_message(text)
                    last_messages_memory.chat_memory.add_ai_message(text)
            except Exception:
                pass
    
    # После вывода всего скрипта помечаем, что диалог засеян
    st.session_state.personification_seeded_scenario = st.session_state.personification_scenario

def ensure_personification_state():
    if "exec_personification_pipeline" not in st.session_state:
        st.session_state.exec_personification_pipeline = False
    if "personification_scenario" not in st.session_state:
        st.session_state.personification_scenario = None
    if "personification_seeded_scenario" not in st.session_state:
        st.session_state.personification_seeded_scenario = None

def write_personification(all_messages_memory, last_messages_memory):
    _maybe_call("reset_to_zero_input_pipeline")
    _maybe_call("reset_to_zero_products_pipeline")
    _maybe_call("reset_to_zero_fx_pipeline")
    _maybe_call("reset_to_zero_cmdt_pipeline")
    _maybe_call("reset_to_zero_indicatives_pipeline")
    _maybe_call("reset_to_zero_potential_pipeline")
    _maybe_call("reset_to_zero_sales_pipeline")
    _maybe_call("reset_to_zero_calls_pipeline")

    ensure_personification_state()

    if st.session_state.personification_scenario is None:
        pick_personification_scenario()
        return

    if isinstance(st.session_state.selected_option_pers, tuple):
        st.session_state.selected_option_pers = st.session_state.selected_option_pers[0]
    
    company = st.session_state.personification_scenario
    st.session_state.DO_PERS = True
    st.session_state.ADDITIONAL_INSTRUCTION = ADDITIONAL_INSTRUCTION_RAW.format(client_info_text = PERSONIFICATION_CLIENT_INFO[st.session_state.selected_option_pers])


    if st.session_state.personification_seeded_scenario != company:
        if company == "Продолжить в диалоге из примера":
            seed_personification_dialogue(all_messages_memory, last_messages_memory)
            st.session_state.personification_seeded_scenario = company
        else:
            script = PERSONIFICATION_SCRIPTS.get(st.session_state.selected_option_pers, [])[:1]
            st.session_state.personification_seeded_scenario = company
            if not script:
                return

            for m in script:
                role = "assistant" if m.get("role") != "user" else "user"
                text = m.get("text_md", "")
                if not text:
                    continue
                
                stream_md_message_with_role(text, role)

                try:
                    if role == "user":
                        all_messages_memory.chat_memory.add_user_message(text)
                        last_messages_memory.chat_memory.add_user_message(text)
                    else:
                        all_messages_memory.chat_memory.add_ai_message(text)
                        last_messages_memory.chat_memory.add_ai_message(text)
                except Exception:
                    pass

            rid = uuid.uuid4()
            st.session_state.run_id = rid
            _register_feedback_pipeline_for_run(rid, "personalization")


    b, c = 0.2, 0.06
    top = st.columns([1 - b - c, b, c])
    top[0].markdown(f"**Продолжить диалог в роли клиента**")
    top[1].markdown(f"<span style=\"font-size: 14px; line-height: 0.5; font-style: italic; font-family: var(--font);\">Нажмите дважды для выхода:</span>", unsafe_allow_html=True)
    if top[2].button("✕", key="personification_exit_btn", help="Выход"):
        st.session_state.DO_PERS = False
        st.session_state.ADDITIONAL_INSTRUCTION = ""
        st.session_state.exec_personification_pipeline = False
        st.session_state.personification_scenario = None
        st.session_state.personification_seeded_scenario = None

        for mem in (all_messages_memory, last_messages_memory):
            try:
                mem.clear()
            except Exception:
                try:
                    mem.chat_memory.messages = []
                except Exception:
                    pass

        for k in ("langchain_messages", "messages", "last_messages", "chat_history", "chat_history_last"):
            if k in st.session_state:
                v = st.session_state.get(k)
                if isinstance(v, list):
                    st.session_state[k] = []
                else:
                    st.session_state[k] = None
        st.session_state.show_selectbox_pers_period = True
        _maybe_call("reset_to_zero_pers_pipeline")

    _maybe_call("reset_to_zero_input_pipeline")


def start_fx_pipeline() -> None:
    reset_to_zero_ALL_pipelines()

    st.session_state.show_selectbox_fx = True


def fx_get_list_values_and_chat(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_potential_pipeline()
    st.session_state.selected_option_fx = None
    st.session_state.fx_list = None
    st.session_state.fx_list_period = None
    st.session_state.selected_option_fx_period = None
    st.session_state.fx_list_period = None
    st.session_state.selectbox_key_fx_period = str(uuid.uuid4())
    st.session_state.start_period_pipeline = False
    st.session_state.show_selectbox_fx_period = False
    st.session_state.show_fx_period = False
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_sales_pipeline()

    fx_list = list(get_fx_dict().keys())
    user_prompt = "/fx"
    st.chat_message("user").write(user_prompt)
    chat_answer = "Выберите валюту"
    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=user_prompt,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="market",
    )
    st.session_state.fx_list = fx_list
    st.session_state.show_selectbox_fx = False
    st.session_state.selectbox_key_fx = str(uuid.uuid4())


def fx_get_fx_from_list() -> None:
    reset_to_zero_pers_pipeline()
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    st.session_state.show_selectbox_fx = False
    st.session_state.fx_list_period = None
    st.session_state.selected_option_fx_period = None

    st.session_state.show_selectbox_fx_period = False
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.selected_option_fx = st.selectbox(
        "fx",
        [""] + st.session_state.fx_list,
        key=st.session_state.selectbox_key_fx,
        label_visibility="hidden",
    )
    st.session_state.start_period_pipeline = True


def show_fx_period() -> None:
    if st.session_state.selected_option_fx:
        st.session_state.start_period_pipeline = False
        st.session_state.show_fx_period = True


def fx_start_period_pipeline() -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_products_pipeline()
    st.session_state.show_selectbox_fx = False

    st.session_state.fx_list = None
    st.session_state.selectbox_key_fx = str(uuid.uuid4())
    st.session_state.selected_option_fx_period = None

    st.session_state.selectbox_key_fx_period = str(uuid.uuid4())
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.show_selectbox_fx_period = True
    st.session_state.start_period_pipeline = False
    st.session_state.show_fx_period = False


def fx_get_list_period_and_chat(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_products_pipeline()
    st.session_state.show_selectbox_fx = False
    st.session_state.fx_list = None
    st.session_state.selected_option_fx_period = None
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()
    if not st.session_state.selected_option_fx:
        return

    st.chat_message("user").write(st.session_state.selected_option_fx)
    chat_answer = "Выберите период"
    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_option_fx,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="market",
    )
    st.session_state.fx_list_period = ["неделя", "месяц", "квартал", "год"]

    st.session_state.show_selectbox_fx_period = False
    st.session_state.selectbox_key_fx_period = str(uuid.uuid4())


def fx_get_period_from_list() -> None:
    reset_to_zero_pers_pipeline()
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    st.session_state.show_selectbox_fx = False
    st.session_state.fx_list = None

    st.session_state.show_selectbox_fx_period = False
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.selected_option_fx_period = st.selectbox(
        "fx period",
        [""] + st.session_state.fx_list_period,
        key=st.session_state.selectbox_key_fx_period,
        label_visibility="hidden",
    )


def fx_finish(
    all_messages_memory: ConversationBufferMemory,
) -> None:

    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.chat_message("user").write(st.session_state.selected_option_fx_period)

    dfs_dict = get_fx_dict()
    full_response = (
        f"{st.session_state.selected_option_fx}, {st.session_state.selected_option_fx_period}"
    )
    stream_md_message(full_response)

    if st.session_state.selected_option_fx_period == "неделя":
        number_of_days = 7

    elif st.session_state.selected_option_fx_period == "месяц":
        number_of_days = 30

    elif st.session_state.selected_option_fx_period == "квартал":
        number_of_days = 90

    elif st.session_state.selected_option_fx_period == "год":
        number_of_days = 365
    df_to_plot = get_data_per_period_for_graph(
        dfs_dict, st.session_state.selected_option_fx, number_of_days
    )
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_option_fx_period,
        chat_answer=full_response,
        transformed_user_input="",
        chat_history_last="",
        pipeline="market",
    )
    fig = px.line(df_to_plot, x="Дата", y="руб.")
    st.plotly_chart(fig, use_container_width=True)
    _logger.info(
        "FX: показан график, инструмент=%s, период=%s",
        st.session_state.selected_option_fx,
        st.session_state.selected_option_fx_period,
    )

    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_sales_pipeline()


def start_cmdt_pipeline() -> None:
    reset_to_zero_ALL_pipelines()

    st.session_state.show_selectbox_cmdt = True


def _market_combined_instrument_keys() -> list[str]:
    """Все валюты из FX + все инструменты commodities (золото, серебро и т.д.), без дубликатов."""
    fx_keys = sorted(get_fx_dict().keys())
    cm_keys = sorted(get_commodities_dict().keys())
    fx_set = set(fx_keys)
    return fx_keys + [k for k in cm_keys if k not in fx_set]


def start_market_pipeline() -> None:
    reset_to_zero_ALL_pipelines()
    st.session_state.show_market_choice = True


def market_get_list_values_and_chat(all_messages_memory: ConversationBufferMemory) -> None:
    """Первый шаг /market: в чате — запрос и общий список (как раньше у /fx), плюс товары из commodities."""
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_potential_pipeline()
    st.session_state.selected_option_fx = None
    st.session_state.fx_list = None
    st.session_state.fx_list_period = None
    st.session_state.selected_option_fx_period = None
    st.session_state.fx_list_period = None
    st.session_state.selectbox_key_fx_period = str(uuid.uuid4())
    st.session_state.start_period_pipeline = False
    st.session_state.show_selectbox_fx_period = False
    st.session_state.show_fx_period = False
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_sales_pipeline()

    combined = _market_combined_instrument_keys()
    user_prompt = "/market"
    st.chat_message("user").write(user_prompt)
    if not combined:
        chat_answer = "Нет доступных инструментов для отображения (данные FX/товаров пусты)."
        stream_md_message(chat_answer)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=user_prompt,
            chat_answer=chat_answer,
            transformed_user_input="",
            chat_history_last="",
            pipeline="market",
        )
        st.session_state.market_combined_list = None
        st.session_state.show_market_choice = False
        return
    chat_answer = "Выберите опцию"
    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=user_prompt,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="market",
    )
    st.session_state.market_combined_list = combined
    st.session_state.show_market_choice = False
    st.session_state.selectbox_key_market = str(uuid.uuid4())


def market_get_instrument_from_list(_all_messages_memory: ConversationBufferMemory) -> None:
    """Второй шаг /market: один selectbox в области чата — как fx_get_fx_from_list / cmdt_get_fx_from_list."""
    reset_to_zero_pers_pipeline()
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    st.session_state.show_selectbox_fx = False
    st.session_state.fx_list_period = None
    st.session_state.selected_option_fx_period = None
    st.session_state.show_selectbox_fx_period = False
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    sel = st.selectbox(
        "market",
        [""] + list(st.session_state.market_combined_list),
        key=st.session_state.selectbox_key_market,
        label_visibility="hidden",
    )
    if not sel:
        return
    if sel in get_fx_dict():
        _market_dispatch_fx(sel)
    elif sel in get_commodities_dict():
        _market_dispatch_cmdt(sel)


def _market_dispatch_fx(instrument: str) -> None:
    st.session_state.market_combined_list = None
    st.session_state.fx_list = None
    st.session_state.selected_option_fx = instrument
    st.session_state.start_period_pipeline = True


def _market_dispatch_cmdt(instrument: str) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    st.session_state.show_selectbox_cmdt = False
    st.session_state.cmdt_list_period = None
    st.session_state.selected_option_cmdt_period = None
    st.session_state.show_selectbox_cmdt_period = False
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()
    st.session_state.market_combined_list = None
    st.session_state.cmdt_list = None
    st.session_state.selected_option_cmdt = instrument
    st.session_state.start_cmdt_period_pipeline = True
    st.session_state.selectbox_key_cmdt = str(uuid.uuid4())


def cmdt_get_list_values_and_chat(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    st.session_state.selected_option_cmdt = None
    st.session_state.cmdt_list = None
    st.session_state.cmdt_list_period = None
    st.session_state.selected_option_cmdt_period = None
    st.session_state.cmdt_list_period = None
    st.session_state.selectbox_key_cmdt_period = str(uuid.uuid4())
    st.session_state.start_cmdt_period_pipeline = False
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    cmdt_list = list(get_commodities_dict().keys())
    user_prompt = "/cmdt"
    st.chat_message("user").write(user_prompt)
    chat_answer = "Выберите биржевой товар"
    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=user_prompt,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="market",
    )

    st.session_state.cmdt_list = cmdt_list
    st.session_state.show_selectbox_cmdt = False
    st.session_state.selectbox_key_cmdt = str(uuid.uuid4())


def cmdt_get_fx_from_list() -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    st.session_state.show_selectbox_cmdt = False
    st.session_state.cmdt_list_period = None
    st.session_state.selected_option_cmdt_period = None

    st.session_state.show_selectbox_cmdt_period = False
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.selected_option_cmdt = st.selectbox(
        "cmdt",
        [""] + st.session_state.cmdt_list,
        key=st.session_state.selectbox_key_cmdt,
        label_visibility="hidden",
    )
    st.session_state.start_cmdt_period_pipeline = True


def show_cmdt_period() -> None:
    if st.session_state.selected_option_cmdt:
        st.session_state.start_cmdt_period_pipeline = False
        st.session_state.show_cmdt_period = True


def cmdt_start_period_pipeline() -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    st.session_state.show_selectbox_cmdt = False

    st.session_state.cmdt_list = None
    st.session_state.selectbox_key_cmdt = str(uuid.uuid4())
    st.session_state.selected_option_cmdt_period = None

    st.session_state.selectbox_key_cmdt_period = str(uuid.uuid4())
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.show_selectbox_cmdt_period = True
    st.session_state.start_cmdt_period_pipeline = False
    st.session_state.show_cmdt_period = False


def cmdt_get_list_period_and_chat(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    st.session_state.show_selectbox_cmdt = False
    st.session_state.cmdt_list = None
    st.session_state.selected_option_cmdt_period = None
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.chat_message("user").write(st.session_state.selected_option_cmdt)
    chat_answer = "Выберите период"
    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_option_cmdt,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="market",
    )

    st.session_state.cmdt_list_period = ["неделя", "месяц", "квартал", "год"]

    st.session_state.show_selectbox_cmdt_period = False
    st.session_state.selectbox_key_cmdt_period = str(uuid.uuid4())


def cmdt_get_period_from_list() -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    st.session_state.show_selectbox_cmdt = False
    st.session_state.cmdt_list = None

    st.session_state.show_selectbox_cmdt_period = False
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.selected_option_cmdt_period = st.selectbox(
        "cmdt period",
        [""] + st.session_state.cmdt_list_period,
        key=st.session_state.selectbox_key_cmdt_period,
        label_visibility="hidden",
    )


def cmdt_finish(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.chat_message("user").write(st.session_state.selected_option_cmdt_period)

    dfs_dict = get_commodities_dict()
    full_response = (
        f"{st.session_state.selected_option_cmdt}, {st.session_state.selected_option_cmdt_period}"
    )
    stream_md_message(full_response)

    if st.session_state.selected_option_cmdt_period == "неделя":
        number_of_days = 7

    elif st.session_state.selected_option_cmdt_period == "месяц":
        number_of_days = 30

    elif st.session_state.selected_option_cmdt_period == "квартал":
        number_of_days = 90

    elif st.session_state.selected_option_cmdt_period == "год":
        number_of_days = 365
    df_to_plot = get_data_per_period_for_graph(
        dfs_dict, st.session_state.selected_option_cmdt, number_of_days
    )

    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_option_cmdt_period,
        chat_answer=full_response,
        transformed_user_input="",
        chat_history_last="",
        pipeline="market",
    )

    fig = px.line(df_to_plot, x="Дата", y="руб./грамм")
    st.plotly_chart(fig, use_container_width=True)

    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()


def write_hello(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    reset_to_zero_ALL_pipelines()
    user_input = "/start"
    st.chat_message("user").write(user_input)
    data = get_hello_data()

    stream_md_message(data)
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=user_input,
        transformed_user_input="",
        chat_answer=data,
        chat_history_last="",
    )

    reset_to_zero_input_pipeline()


def write_rates_help(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    reset_to_zero_ALL_pipelines()
    user_input = "/rates_help"
    st.chat_message("user").write(user_input)
    data = get_rates_help_data()

    stream_md_message(data)
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=user_input,
        transformed_user_input="",
        chat_answer=data,
        chat_history_last="",
        pipeline="rates",
    )

    reset_to_zero_input_pipeline()


def write_eco(
    all_messages_memory: ConversationBufferMemory,
    show_user_msg: bool = True,
) -> None:
    reset_to_zero_ALL_pipelines()

    file_path, text, eco_file_name = get_eco_data()
    with open(file_path, "rb") as pdf_file:
        PDFByte = pdf_file.read()
    user_prompt = "/eco"
    if show_user_msg:
        st.chat_message("user").write(user_prompt)
    chat_answer = text
    stream_md_message(chat_answer)
    st.download_button(
        label=f"📥 {eco_file_name}",
        data=PDFByte,
        file_name=f"{eco_file_name}",
    )

    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=user_prompt,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="eco",
    )

    reset_to_zero_input_pipeline()


def write_news(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    write_news_start(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        show_user_msg=show_user_msg,
        command="/news",
    )


def write_news_from_user_input(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    transformed_user_input: str,
    chat_history_last: str,
) -> None:
    del original_user_input, transformed_user_input, chat_history_last
    write_news_start(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        show_user_msg=False,
        command="/news",
    )


def write_faq(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    reset_to_zero_ALL_pipelines()

    user_prompt = "/faq"
    if show_user_msg:
        st.chat_message("user").write(user_prompt)
    data = get_faq_data()

    stream_md_message(data)

    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=user_prompt,
        transformed_user_input="",
        chat_answer=data,
        chat_history_last="",
        pipeline="faq",
    )

    reset_to_zero_input_pipeline()


def write_notes_button(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    show_user_msg: bool = True,
) -> None:
    """FI Notes (/notes): OpenClaw-агент fi-sales."""
    write_openclaw_fi_sales_start(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        command="/notes",
        access_check=is_notes_allowed_user,
        show_user_msg=show_user_msg,
    )


def write_broker_button(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    """Брокерка (/brokerka): answer_generation prompts + products RAG ДГР."""
    username = str(st.session_state.get("username", ""))
    if not is_broker_allowed_user(username):
        msg = "⛔ У вас нет доступа к режиму /brokerka."
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=BROKER_COMMAND,
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last="",
            pipeline="brokerka",
        )
        return
    _broker_start(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        save_fn=save_in_both_memories_and_in_logs,
        stream_fn=stream_md_message,
        reset_all_fn=reset_to_zero_ALL_pipelines,
    )


def ensure_calls_retriever():
    if st.session_state.calls_retriever is not None:
        return True

    st.session_state.calls_retriever = retriever
    st.session_state.calls_data_loaded = True
    return True


def write_calls(all_messages_memory: ConversationBufferMemory, last_messages_memory: ConversationBufferWindowMemory, show_user_msg: bool = True) -> None:
    username = str(st.session_state.get("username", ""))
    if not is_calls_allowed_user(username):
        msg = "⛔ У вас нет доступа к режиму анализа звонков."
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input="/calls",
            transformed_user_input="",
            chat_answer=msg,
            chat_history_last="",
            pipeline="calls",
        )
        return

    reset_to_zero_ALL_pipelines()

    st.session_state["calls_bank"] = resolve_calls_bank(username)
    st.session_state.calls_mode = True
    st.session_state.calls_chat_history = []

    user_prompt = "/calls"
    if show_user_msg:
        st.chat_message("user").write(user_prompt)

    chat_answer = "**Режим анализа звонков включен.**"
    stream_md_message(chat_answer)

    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=user_prompt,
        transformed_user_input="",
        chat_answer=chat_answer,
        chat_history_last="",
        pipeline="calls",
    )

    reset_to_zero_input_pipeline()


def _call_label_date_time(st_str):
    """Дата и время без года: 2025-12-08 14:30:00 -> 08.12 14:30."""
    if not st_str:
        return ""
    s = str(st_str).strip()
    try:
        date_part = s.split(" ")[0] if " " in s else s
        time_part = s.split(" ")[1] if " " in s and len(s.split(" ")) > 1 else ""
        parts = date_part.replace("-", " ").replace(".", " ").split()
        if len(parts) >= 3:
            m, d = parts[1], parts[2]
            dm = f"{int(d):02d}.{int(m):02d}"
        else:
            dm = date_part[:5] if len(date_part) >= 5 else date_part
        if time_part:
            t = time_part.split(":")
            if len(t) >= 2:
                dm += f" {int(t[0]):02d}:{int(t[1]):02d}"
        return dm.strip()
    except Exception:
        pass
    return s[:16] if len(s) >= 16 else s


def _operator_surname_initials(name):
    """ФИО -> Фамилия И.О. (первое слово = фамилия, остальные — инициалы)."""
    if not name:
        return ""
    parts = str(name).strip().split()
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    surname = parts[0]
    initials = ".".join((p[0].upper() for p in parts[1:] if p)) + "." if parts[1:] else ""
    return f"{surname} {initials}".strip()


def write_calls_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
    transformed_user_input: str
) -> None:
    reset_to_zero_input_pipeline()

    st.session_state.setdefault("calls_chat_history", [])
    st.session_state.calls_chat_history.append({"role": "user", "content": original_user_input})

    records = []
    chat_answer = ""
    with st.chat_message("assistant"):
        spinner_label = st.empty()
        message_placeholder = st.empty()
        stream_started = False

        def _show_progress(label: str) -> None:
            if stream_started:
                return
            spinner_label.markdown(
                _render_openclaw_simple_spinner(label),
                unsafe_allow_html=True,
            )

        def _on_stream(partial: str) -> None:
            nonlocal stream_started
            if not partial:
                return
            if not stream_started:
                stream_started = True
                spinner_label.empty()
            message_placeholder.markdown(partial + "▌")

        _show_progress("Готовлю запрос…")

        if st.session_state.pop("_calls_paraphrase_in_answer", False):
            _show_progress("Уточняю контекст вопроса…")
            transformed_user_input = paraphrase_bot_message(
                original_user_input,
                chat_history_last,
                in_call_mode=True,
            )
        elif not transformed_user_input:
            transformed_user_input = original_user_input

        _show_progress("Подключаюсь к базе звонков…")
        svc = ensure_call_intelligence_service()

        st.session_state[CALLS_RENDER_EMBED_KEY] = True
        try:
            if hasattr(svc, "answer_with_records"):
                chat_answer, records = svc.answer_with_records(
                    original_user_input,
                    transformed_user_input=transformed_user_input,
                    bank=st.session_state.get("calls_bank", "ALL"),
                    progress_callback=_show_progress,
                    stream_callback=_on_stream,
                )
                if not isinstance(records, list):
                    records = []
            else:
                chat_answer = svc.answer(
                    original_user_input,
                    transformed_user_input=transformed_user_input,
                    bank=st.session_state.get("calls_bank", "ALL"),
                    progress_callback=_show_progress,
                    stream_callback=_on_stream,
                )
        except Exception:
            chat_answer = "Ой! Я перезагрузился, задайте, пожалуйста, вопрос снова!"
        finally:
            st.session_state.pop(CALLS_RENDER_EMBED_KEY, None)

        _operator_summary_ui = getattr(svc, "skip_next_md_stream", False)
        if _operator_summary_ui:
            svc.skip_next_md_stream = False
            spinner_label.empty()
            if chat_answer and "**Может так:**" in chat_answer:
                sug = chat_answer[chat_answer.rfind("**Может так:**"):]
                message_placeholder.markdown(sug)
        else:
            spinner_label.empty()
            message_placeholder.markdown(chat_answer)
            for doc_path in re.findall(r'(/[\w/.\-]+\.docx)', chat_answer):
                p = Path(doc_path)
                if p.exists():
                    st.download_button(
                        label=f"⬇ Скачать {p.name}",
                        data=p.read_bytes(),
                        file_name=p.name,
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    )
            _render_pdf_download_buttons(_collect_pdf_paths(chat_answer), "stream")
            st.session_state.pop(CALL_TABLE_INTERACTIVE_CTX_KEY, None)
            st.session_state.pop(CALL_TABLE_PENDING_DETAIL_KEY, None)
            st.session_state.pop(CALL_TABLE_DF_SELECTION_WIDGET_KEY, None)

    if records and len(records) <= 15:
        fe = getattr(svc, "filter_engine", None)
        get_summary = getattr(fe, "get_summary_by_call_id", None) if fe else None
        cols_per_row = 3
        n_records = len(records)
        with st.container():
            st.markdown(
                """
                <style>
                [data-testid="stExpander"] {
                    border: none !important;
                    box-shadow: none !important;
                    outline: none !important;
                }
                [data-testid="stExpander"] details {
                    border: none !important;
                    box-shadow: none !important;
                }
                [data-testid="stExpander"] details > div {
                    border: none !important;
                    box-shadow: none !important;
                }
                /* Обнуляем зазор между колонками: без него сдвиг раскрытого саммари
                   на ширину колонки совпадает точно, и левая/центральная/правая
                   кнопки раскрываются в одной и той же точке (от левого края ряда). */
                [data-testid="stHorizontalBlock"] {
                    gap: 0 !important;
                }
                [data-testid="stExpander"] details > div {
                    width: 300% !important;
                    max-width: 300% !important;
                    margin-left: 0 !important;
                    position: relative;
                    box-sizing: border-box;
                }
                /* В Streamlit 1.39 у колонки data-testid="stColumn" (в других версиях — "column").
                   Указываем оба, иначе сдвиг по колонкам не применяется и кнопки уезжают. */
                [data-testid="stColumn"]:nth-child(3n+1) [data-testid="stExpander"] details > div,
                [data-testid="column"]:nth-child(3n+1) [data-testid="stExpander"] details > div { margin-left: 0 !important; }
                [data-testid="stColumn"]:nth-child(3n+2) [data-testid="stExpander"] details > div,
                [data-testid="column"]:nth-child(3n+2) [data-testid="stExpander"] details > div { margin-left: -100% !important; }
                [data-testid="stColumn"]:nth-child(3n) [data-testid="stExpander"] details > div,
                [data-testid="column"]:nth-child(3n) [data-testid="stExpander"] details > div { margin-left: -200% !important; }
                </style>
                """,
                unsafe_allow_html=True,
            )
            for row_start in range(0, n_records, cols_per_row):
                chunk = records[row_start : row_start + cols_per_row]
                columns = st.columns(cols_per_row)
                for col_idx, r in enumerate(chunk):
                    with columns[col_idx]:
                        call_id = r.get("call_id", "")
                        start_time = r.get("start_time", "")
                        operator = r.get("operator", "")
                        date_short = _call_label_date_time(start_time)
                        op_short = _operator_surname_initials(operator)
                        label_text = " ".join(x for x in [date_short, op_short] if x).strip() or "Звонок"
                        summary = (r.get("summary") or "").strip()
                        if not summary and callable(get_summary) and call_id:
                            summary = (get_summary(call_id) or "").strip()
                        with st.expander(f"📞 {label_text}"):
                            if summary:
                                st.markdown(summary)
                            else:
                                st.caption("Саммари в Excel не заполнено.")

    st.session_state.calls_chat_history.append({"role": "assistant", "content": chat_answer})

    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=original_user_input,
        transformed_user_input="",
        chat_answer=chat_answer,
        chat_history_last=chat_history_last,
        pipeline="calls",
    )

    # Детализация по сейлзу (flush) — в synaptica_app сразу после интерактивной сводки по всем, чтобы порядок был: общая таблица → детализация.

    reset_to_zero_input_pipeline()


def write_call_table_answer(
    original_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
) -> None:
    reset_to_zero_input_pipeline()
    st.session_state.setdefault("calls_chat_history", [])
    st.session_state.calls_chat_history.append({"role": "user", "content": original_user_input})

    with st.chat_message("assistant"):
        spinner_label = st.empty()
        spinner_label.markdown(
            _render_openclaw_simple_spinner("Собираю статистику по звонкам…"),
            unsafe_allow_html=True,
        )
        st.session_state[CALLS_RENDER_EMBED_KEY] = True
        try:
            chat_answer = render_call_sales_table()
        finally:
            st.session_state.pop(CALLS_RENDER_EMBED_KEY, None)
            spinner_label.empty()

    st.session_state.calls_chat_history.append({"role": "assistant", "content": chat_answer})
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=original_user_input,
        transformed_user_input="",
        chat_answer=chat_answer,
        chat_history_last=str(chat_history_last) if chat_history_last else "",
        pipeline="calls",
    )
    reset_to_zero_input_pipeline()


def write_call_table_sidebar_button(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last,
) -> None:
    """Запуск сводки по звонкам из бокового меню (эквивалент команды /calls_tb в чате)."""
    st.session_state.pop(CALL_TABLE_PENDING_DETAIL_KEY, None)
    st.session_state.pop(CALL_TABLE_DF_SELECTION_WIDGET_KEY, None)
    reset_to_zero_calls_pipeline()

    st.chat_message("user").write(CALL_TABLE_COMMAND)

    _hist = chat_history_last if chat_history_last is not None else st.session_state.get("chat_history_last")
    _hist_s = str(_hist) if _hist else ""

    _ct_user = str(st.session_state.get("username", ""))
    if not is_calls_allowed_user(_ct_user):
        _ct_denied = "⛔ У вас нет доступа к сводке по звонкам."
        stream_md_message(_ct_denied)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=CALL_TABLE_COMMAND,
            transformed_user_input="",
            chat_answer=_ct_denied,
            chat_history_last=_hist_s,
            pipeline="calls",
        )
        reset_to_zero_input_pipeline()
        return

    st.session_state["calls_bank"] = resolve_calls_bank(_ct_user)
    write_call_table_answer(
        original_user_input=CALL_TABLE_COMMAND,
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        chat_history_last=_hist,
    )


def _call_detail_filter_widget_prefix(operator: str, dt_from: str, dt_to: str) -> str:
    frag = re.sub(r"[^0-9a-zA-Z_-]", "_", str(operator))[:32].strip("_") or "op"
    tfrag = re.sub(r"[^0-9a-zA-Z_-]", "_", f"{dt_from}_{dt_to}")[:48].strip("_")
    return f"ctd_{frag}_{tfrag}"


def _call_operator_detail_column_config(columns: pd.Index) -> dict:
    """Все колонки детализации — одинаковая начальная ширина small."""
    return {
        str(c): st.column_config.TextColumn(str(c), width="small") for c in columns
    }


def _render_call_operator_detail_table_with_filters(display_df: pd.DataFrame, *, widget_prefix: str) -> None:
    """
    Streamlit 1.37+: у st.dataframe в UI уже есть сортировка и поиск в тулбаре таблицы.
    Здесь дополнительно: multiselect по типу, статусу, этапу, клиенту, продукту и телефону.
    """
    if display_df is None or display_df.empty:
        st.dataframe(
            display_df if display_df is not None else pd.DataFrame(),
            use_container_width=True,
            hide_index=True,
        )
        return

    df_src = display_df.copy()
    n_src = len(df_src)

    with st.expander("Фильтры таблицы", expanded=False):
        ms_call_type: list = []
        if "Тип звонка" in df_src.columns:
            opts_ct = sorted({str(x) for x in df_src["Тип звонка"].unique()})
            ms_call_type = st.multiselect(
                "Тип звонка",
                options=opts_ct,
                default=[],
                key=f"{widget_prefix}_call_type",
            )
        c1, c2 = st.columns(2)
        ms_deal: list = []
        ms_stage: list = []
        if "Статус звонка" in df_src.columns:
            opts_d = sorted({str(x) for x in df_src["Статус звонка"].unique()})
            with c1:
                ms_deal = st.multiselect(
                    "Статус звонка",
                    options=opts_d,
                    default=[],
                    key=f"{widget_prefix}_deal",
                )
        if "Этап продажи" in df_src.columns:
            opts_s = sorted({str(x) for x in df_src["Этап продажи"].unique()})
            with c2:
                ms_stage = st.multiselect(
                    "Этап продажи",
                    options=opts_s,
                    default=[],
                    key=f"{widget_prefix}_stage",
                )
        c3, c4 = st.columns(2)
        ms_client: list = []
        ms_prod_group: list = []
        ms_prod_name: list = []
        if "Клиент" in df_src.columns:
            opts_c = sorted({str(x) for x in df_src["Клиент"].unique()})
            with c3:
                ms_client = st.multiselect(
                    "Клиент",
                    options=opts_c,
                    default=[],
                    key=f"{widget_prefix}_client",
                )
        if "Группа продукта" in df_src.columns:
            opts_g = sorted({str(x) for x in df_src["Группа продукта"].unique()})
            with c4:
                ms_prod_group = st.multiselect(
                    "Группа продукта",
                    options=opts_g,
                    default=[],
                    key=f"{widget_prefix}_prod_group",
                )
        c5, c6 = st.columns(2)
        ms_phone: list = []
        if "Название продукта" in df_src.columns:
            opts_n = sorted({str(x) for x in df_src["Название продукта"].unique()})
            with c5:
                ms_prod_name = st.multiselect(
                    "Название продукта",
                    options=opts_n,
                    default=[],
                    key=f"{widget_prefix}_prod_name",
                )
        if "Телефон" in df_src.columns:
            opts_p = sorted({str(x) for x in df_src["Телефон"].unique()})
            with c6:
                ms_phone = st.multiselect(
                    "Телефон",
                    options=opts_p,
                    default=[],
                    key=f"{widget_prefix}_phone",
                )

    out = df_src
    if ms_call_type:
        out = out[out["Тип звонка"].astype(str).isin(ms_call_type)]
    if ms_deal:
        out = out[out["Статус звонка"].astype(str).isin(ms_deal)]
    if ms_stage:
        out = out[out["Этап продажи"].astype(str).isin(ms_stage)]
    if ms_client:
        out = out[out["Клиент"].astype(str).isin(ms_client)]
    if ms_prod_group:
        out = out[out["Группа продукта"].astype(str).isin(ms_prod_group)]
    if ms_prod_name:
        out = out[out["Название продукта"].astype(str).isin(ms_prod_name)]
    if ms_phone:
        out = out[out["Телефон"].astype(str).isin(ms_phone)]

    st.caption(f"Показано строк: {len(out)} из {n_src}")
    st.caption(
        "Чтобы развернуть таблицу, нажмите символ ⛶ в правом верхнем углу таблицы ниже."
    )
    st.caption(
        "Чтобы просмотреть полный текст звонка или саммари — дважды нажмите по нужной ячейке."
    )
    # Телефон используется только для фильтра, в самой таблице не показываем.
    visible = out.drop(columns=["Телефон"], errors="ignore")
    _cc = _call_operator_detail_column_config(visible.columns)
    st.dataframe(
        visible,
        use_container_width=True,
        hide_index=True,
        column_config=_cc,
    )


def flush_call_table_detail_if_pending(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
) -> None:
    """После выбора сейлза в сводке или NL с одним оператором: синтетическая строка пользователя «Детализация /calls_tb …» и таблица детализации."""
    pending = st.session_state.pop(CALL_TABLE_PENDING_DETAIL_KEY, None)
    resolved = resolve_call_table_pending_window(pending or {})
    if not resolved:
        return
    op, dt_from, dt_to, plabel = resolved
    detail = build_call_table_operator_detail(str(op), dt_from, dt_to, plabel or None)
    user_line = format_call_table_detail_user_message(str(op))
    if not detail:
        with st.chat_message("user"):
            st.markdown(user_line)
        with st.chat_message("assistant"):
            st.warning(f"Не удалось построить детализацию для «{op}» (нет звонков в выборке).")
        reset_to_zero_input_pipeline()
        return
    chat_answer = detail["markdown_for_memory"]
    with st.chat_message("user"):
        st.markdown(user_line)
    with st.chat_message("assistant"):
        st.markdown(detail["header"], unsafe_allow_html=True)
        wprefix = _call_detail_filter_widget_prefix(str(op), dt_from, dt_to)
        _render_call_operator_detail_table_with_filters(detail["display_df"], widget_prefix=wprefix)
    st.session_state.setdefault("calls_chat_history", []).append(
        {"role": "user", "content": user_line}
    )
    st.session_state.calls_chat_history.append({"role": "assistant", "content": chat_answer})
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=user_line,
        transformed_user_input="",
        chat_answer=chat_answer,
        chat_history_last=chat_history_last,
        pipeline="calls",
    )
    # Не сбрасываем CALL_TABLE_INTERACTIVE_CTX_KEY: иначе пропадает сводка внизу и нельзя выбрать другого сейлза / пользоваться фильтрами той же сессии сводки.
    reset_to_zero_input_pipeline()


def write_rag(
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
) -> None:
    reset_to_zero_ALL_pipelines()

    original_user_input = _coerce_chat_input_for_memory(original_user_input)

    chain = create_multivector_chain_with_chat_history(
        st.session_state["retriever"],
        settings.model,
    )


    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""


        input_dict = {
            "input": original_user_input,
            "transformed_user_input": transformed_user_input,
            "chat_history_last": chat_history_last,
        }
        seed_all(42)
        with collect_runs() as cb:
            for chunk in chain.stream(input_dict, config={"tags": ["Streamlit Chat"]}):
                if "answer" in chunk.keys():
                    full_response += chunk["answer"]
                    message_placeholder.markdown(full_response + "▌")
            all_messages_memory.save_context(
                {"input": original_user_input}, {"output": full_response}
            )
            last_messages_memory.save_context(
                {"input": original_user_input}, {"output": full_response}
            )
            st.session_state.run_id = cb.traced_runs[0].id
            _register_feedback_pipeline_for_run(st.session_state.run_id, "products_rag")

    save_logs(
        full_response=full_response,
        original_user_input=original_user_input,
        transformed_user_input=transformed_user_input,
        chat_history_last=str(chat_history_last),
    )
    message_placeholder.markdown(full_response)

    reset_to_zero_input_pipeline()


def PERS_write_rag(
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
    ADDITIONAL_INSTRUCTION: str,
) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()
    reset_to_zero_calls_pipeline()

    original_user_input = _coerce_chat_input_for_memory(original_user_input)

    chain = PERS_create_multivector_chain_with_chat_history(
        st.session_state["retriever"],
        settings.model,
        ADDITIONAL_PROMPT=SESSION_SYSTEM_PROMPT
    )


    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""


        input_dict = {
            "input": original_user_input + f"\n\n<10OW_a24>{ADDITIONAL_INSTRUCTION}</10OW_a24>",
            "transformed_user_input": transformed_user_input,
            "chat_history_last": chat_history_last,
        }
        seed_all(42)
        with collect_runs() as cb:
            for chunk in chain.stream(input_dict, config={"tags": ["Streamlit Chat"]}):
                if "answer" in chunk.keys():
                    full_response += chunk["answer"]
                    message_placeholder.markdown(full_response + "▌")
            all_messages_memory.save_context(
                {"input": original_user_input}, {"output": full_response}
            )
            last_messages_memory.save_context(
                {"input": original_user_input}, {"output": full_response}
            )
            st.session_state.run_id = cb.traced_runs[0].id
            _register_feedback_pipeline_for_run(st.session_state.run_id, "personalization")

    save_logs(
        full_response=full_response,
        original_user_input=original_user_input,
        transformed_user_input=transformed_user_input,
        chat_history_last=str(chat_history_last),
    )
    message_placeholder.markdown(full_response)

    reset_to_zero_input_pipeline()


def write_analytics_rag(
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()
    reset_to_zero_pers_pipeline()

    original_user_input = _coerce_chat_input_for_memory(original_user_input)

    dash_retriever = st.session_state["dash_retriever"]

    qp = st.session_state["analytics_rag_processor"]

    def update_status(message, excel_out=None):
        if 'status_container' in st.session_state:
            st.session_state.status_container.markdown(f"{message}")
            if excel_out is not None:
                st.download_button(
                    label="📥 Download as CSV",
                    data=excel_out,
                    file_name="result.csv",
                    mime="text/csv",
                    key="Pre-result",
                )

    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""
        st.session_state.status_container = message_placeholder
        message_placeholder.markdown("⏳ Ищем данные...")

        input_dict = {
            "input": original_user_input,
            "transformed_user_input": transformed_user_input,
            "chat_history_last": chat_history_last,
        }
        seed_all(42)
        with collect_runs() as cb:

            # full_response_data = ("not_found", "", "", "")
            try:
                full_response_data = qp.process_query(transformed_user_input, status_updater=update_status)
                dashboard_response = get_dashborard_rag_response(
                    transformed_user_input,
                    dash_retriever,
                    st.session_state["giga_client"],
                )

                
                if full_response_data[0] != "not_found":
                    full_response_text = full_response_data[0] + "\n\n\n" + dashboard_response
                else:
                    full_response_text = dashboard_response

                full_response = str(full_response_text)
                sql_script = str(full_response_data[1])
                sql_explanation = str(full_response_data[2])
                excel_out = str(full_response_data[3])
            except Exception as e:
                full_response = "Error in AI Analytics"
                sql_script = ""
                sql_explanation = ""
                excel_out = None


            all_messages_memory.save_context(
                {"input": original_user_input}, {"output": full_response}
            )
            
            last_messages_memory.save_context(
                {"input": original_user_input}, {"output": full_response}
            )
            
            st.session_state.run_id = str(uuid.uuid4())
            _register_feedback_pipeline_for_run(st.session_state.run_id, "analytics_rag")


    save_logs(
        full_response=full_response + "\n" + sql_script,
        original_user_input=original_user_input,
        transformed_user_input=transformed_user_input,
        chat_history_last=str(chat_history_last),
    )

    message_placeholder.markdown(full_response)


    if sql_script is not None and sql_script != "":
        with st.expander("ℹ️ SQL-запрос"):
            st.write(sql_script + "\n\n" + full_response_data[2])

    reset_to_zero_input_pipeline()



def get_or_create_rates_chat():
    import requests
    """Создаёт новый чат с торговым ботом, если его ещё нет.
    Возвращает True, если чат готов к использованию."""
    BASE_URL = "http://10.10.206.123:8090"

    if st.session_state.get("rates_trading_agent_chat_id") is None:
        with st.spinner("Создаём новый чат..."):
            try:
                response = requests.post(f"{BASE_URL}/api/new_chat")
                if response.status_code == 200 and response.json().get("success"):
                    st.session_state.rates_trading_agent_chat_id = response.json()["chat_id"]
                    return True
                else:
                    st.error(f"Не удалось создать чат: {response.text}")
                    return False
            except Exception as e:
                st.error(f"Ошибка соединения: {e}")
                return False
    return True

def _clip_only_uploader_css(
    scope_prefix: str,
    icon: str = "📎",
    box_size: str = "2.75rem",
    icon_size: str = "1.35rem",
) -> str:
    """Скрывает Drag-and-drop / Limit / Browse; видна только иконка, клик открывает файл."""
    safe_icon = icon.replace("\\", "\\\\").replace("'", "\\'")
    root = f"{scope_prefix} [data-testid='stFileUploader']" if scope_prefix else "[data-testid='stFileUploader']"
    dz = f"{root} [data-testid='stFileUploaderDropzone']"
    instr = f"{root} [data-testid='stFileUploaderDropzoneInstructions']"

    parts: list[str] = []

    parts.append(
        f"{root}{{margin:0!important;max-width:{box_size}!important;min-width:{box_size}!important;"
        f"overflow:hidden!important;}}"
    )
    parts.append(
        f"{root} section{{position:relative!important;overflow:hidden!important;"
        f"min-height:{box_size}!important;max-height:{box_size}!important;height:{box_size}!important;"
        f"width:{box_size}!important;padding:0!important;margin:0!important;border:none!important;"
        "background:transparent!important;box-shadow:none!important;cursor:pointer!important;}}"
    )
    parts.append(
        f"{dz}{{position:relative!important;overflow:hidden!important;"
        f"min-height:{box_size}!important;max-height:{box_size}!important;height:{box_size}!important;"
        f"width:{box_size}!important;padding:0!important;margin:0!important;border:none!important;"
        "background:transparent!important;font-size:0!important;color:transparent!important;}}"
    )
    parts.append(
        f"{root} label,{instr},{instr} *,{root} small,"
        f"{dz} svg,"
        f"{dz} span,{dz} small,{dz} p,{dz} label,"
        f"{dz} [data-testid='stMarkdownContainer'],"
        f"{dz} div:not(:has(> button)){{display:none!important;height:0!important;max-height:0!important;"
        "overflow:hidden!important;visibility:hidden!important;}}"
    )
    parts.append(
        f"{dz} *:not(button){{visibility:hidden!important;height:0!important;max-height:0!important;"
        "overflow:hidden!important;margin:0!important;padding:0!important;font-size:0!important;"
        "line-height:0!important;border:none!important;box-shadow:none!important;}}"
    )
    parts.append(
        f"{root} button,{dz} button{{position:absolute!important;inset:0!important;z-index:3!important;"
        "opacity:0!important;width:100%!important;height:100%!important;min-height:100%!important;"
        "margin:0!important;padding:0!important;border:none!important;background:transparent!important;"
        "cursor:pointer!important;display:block!important;visibility:visible!important;}}"
    )
    parts.append(
        f"{root} section::after{{content:'{safe_icon}'!important;display:flex!important;"
        "align-items:center!important;justify-content:center!important;"
        f"font-size:{icon_size}!important;line-height:1!important;position:absolute!important;"
        "inset:0!important;z-index:2!important;pointer-events:none!important;}}"
    )

    return "".join(parts)


def compact_file_uploader_styles(hint: str | None = None) -> str:
    """Компактный file_uploader клиентского чата: только 📎, клик = выбор файла.

    CSS заскоуплен на uploader с ключом cfu_*, чтобы не задевать другие аплоадеры
    (например, внутри окошка 📎 у chat_input).
    """
    icon = hint if hint else "📎"
    body = _clip_only_uploader_css(
        "div[class*='st-key-cfu_']",
        icon=icon,
        box_size="2.4rem",
        icon_size="1.25rem" if hint else "1.35rem",
    )
    return f"<style>{body}</style>"


def _rates_ps_file_attached() -> bool:
    """Файл уже загружен (rates / общая скрепка Синаптики)."""
    if st.session_state.get("rates_ps_file_attached"):
        return True
    if st.session_state.get("synaptica_attach"):
        return True
    return bool(st.session_state.get("rates_last_upload_sig"))


def _rates_attach_popover_styles(file_attached: bool = False) -> str:
    """Кнопка 📎 слева от chat_input; красная точка — файл уже загружен."""
    scope = "div.st-key-rates_ps_attach_pop"
    btn = f"{scope} [data-testid='stPopover'] button"
    parts = [
        f"{btn}{{position:relative!important;width:2.75rem!important;height:2.75rem!important;"
        "min-height:2.75rem!important;padding:0!important;border:none!important;"
        "background:transparent!important;font-size:1.35rem!important;line-height:1!important;}",
        f"{btn}:hover{{background:rgba(0,0,0,0.06)!important;border-radius:50%!important;}}",
        # прячем стрелку-шеврон у кнопки popover
        f"{btn} svg{{display:none!important;}}",
        "[data-testid='stPopoverBody']{min-width:21rem;}",
        "[data-testid='stBottom'] [data-testid='stHorizontalBlock']{align-items:flex-end!important;}",
        "[data-testid='stBottom'] [data-testid='column']:first-child{"
        "flex:0 0 2.85rem!important;min-width:2.85rem!important;max-width:2.85rem!important;"
        "margin-left:-0.45rem!important;padding-left:0!important;}",
    ]
    if file_attached:
        parts.append(
            f"{btn}::after{{content:'';position:absolute;top:3px;right:3px;width:9px;height:9px;"
            "border-radius:50%;background:#e53935;}"
        )
    return f"<style>{''.join(parts)}</style>"


def _streamlit_bottom_container():
    for name in ("bottom", "_bottom"):
        cm = getattr(st, name, None)
        if cm is not None:
            return cm
    return None


def _rates_remove_current_file(chat_id: str) -> None:
    """Удаляет текущий файл в чате PS Chat, если есть."""
    import requests

    BASE_URL = "http://10.10.206.123:8090"
    try:
        resp = requests.get(f"{BASE_URL}/api/attached_file/{chat_id}", timeout=30)
        if resp.status_code != 200 or not resp.json().get("success"):
            return
        old_name = resp.json().get("filename")
        if not old_name:
            return
        rm = requests.post(
            f"{BASE_URL}/api/remove_file",
            json={"chat_id": chat_id, "filename": old_name},
            timeout=60,
        )
        _logger.info(
            "rates_ps_remove_file chat_id=%s filename=%s status=%s body=%s",
            chat_id,
            old_name,
            rm.status_code,
            (rm.text or "")[:300],
        )
    except Exception:
        _logger.exception("rates_ps_remove_file failed chat_id=%s", chat_id)


def _process_rates_file_upload(uploaded) -> None:
    """Загрузка Excel в PS Chat; новый файл заменяет предыдущий."""
    import requests

    BASE_URL = "http://10.10.206.123:8090"
    upload_sig = f"{uploaded.name}:{len(uploaded.getvalue())}"
    if st.session_state.get("rates_last_upload_sig") == upload_sig:
        return

    if not get_or_create_rates_chat():
        _logger.warning("rates_ps_upload skipped: chat not created")
        return
    chat_id = st.session_state.get("rates_trading_agent_chat_id")
    if not chat_id:
        _logger.warning("rates_ps_upload skipped: no chat_id")
        return

    file_size = len(uploaded.getvalue())
    _logger.info(
        "rates_ps_upload start chat_id=%s file=%s size_bytes=%s",
        chat_id,
        uploaded.name,
        file_size,
    )

    with st.spinner("Загружаю файл…"):
        _rates_remove_current_file(chat_id)
        try:
            resp = requests.post(
                f"{BASE_URL}/api/upload_file",
                files={
                    "file": (
                        uploaded.name,
                        uploaded.getvalue(),
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                },
                data={"chat_id": chat_id},
                timeout=120,
            )
            body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            if resp.status_code == 200 and body.get("success"):
                _logger.info(
                    "rates_ps_upload ok chat_id=%s file=%s size_bytes=%s filename_on_server=%s",
                    chat_id,
                    uploaded.name,
                    file_size,
                    body.get("filename") or uploaded.name,
                )
                st.session_state.rates_last_upload_sig = upload_sig
                st.session_state.rates_ps_file_attached = True
                st.session_state.rates_attached_filename = uploaded.name
                st.session_state.rates_file_uploader_key = f"rates_ps_attach_{uuid.uuid4().hex[:8]}"
                st.session_state.rates_upload_toast_pending = True
                st.rerun()
            else:
                _logger.warning(
                    "rates_ps_upload failed chat_id=%s file=%s status=%s body=%s",
                    chat_id,
                    uploaded.name,
                    resp.status_code,
                    (resp.text or "")[:500],
                )
                st.error(f"Ошибка загрузки: {resp.text}")
        except Exception as exc:
            _logger.exception(
                "rates_ps_upload error chat_id=%s file=%s",
                chat_id,
                uploaded.name,
            )
            st.error(f"Ошибка соединения: {exc}")


def _process_openclaw_client_file_upload(uploaded) -> None:
    """Загрузка файла из общей скрепки для режима fin_reporting (OpenClaw client)."""
    upload_sig = f"{uploaded.name}:{len(uploaded.getvalue())}"
    if st.session_state.get("openclaw_client_last_upload_sig") == upload_sig:
        return

    upload_dir = Path(__file__).resolve().parent / "uploads"
    upload_dir.mkdir(exist_ok=True)
    safe_name = uploaded.name.replace(" ", "_")
    dest_path = str(upload_dir / f"{uuid.uuid4().hex}_{safe_name}")
    with open(dest_path, "wb") as fh:
        fh.write(uploaded.getvalue())

    st.session_state.openclaw_client_last_upload_sig = upload_sig
    st.session_state.openclaw_client_pending_upload = {
        "name": uploaded.name,
        "path": dest_path,
    }
    st.session_state.rates_ps_file_attached = True
    st.session_state.rates_attached_filename = uploaded.name
    st.session_state.rates_file_uploader_key = f"rates_ps_attach_{uuid.uuid4().hex[:8]}"
    st.session_state.rates_upload_toast_pending = True
    st.rerun()


def _process_call_upload_file_upload(uploaded) -> None:
    """Загрузка файла из общей скрепки для режима call_upload."""
    upload_sig = f"{uploaded.name}:{len(uploaded.getvalue())}"
    if st.session_state.get("call_upload_last_upload_sig") == upload_sig:
        return

    CALLS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = uploaded.name.replace(" ", "_")
    raw_path = CALLS_DATA_DIR / f"clip_upload_{uuid.uuid4().hex[:8]}_{safe_name}"
    raw_path.write_bytes(uploaded.getvalue())

    st.session_state.call_upload_last_upload_sig = upload_sig
    st.session_state.call_upload_pending_upload = {
        "name": uploaded.name,
        "path": str(raw_path),
    }
    st.session_state.rates_ps_file_attached = True
    st.session_state.rates_attached_filename = uploaded.name
    st.session_state.rates_file_uploader_key = f"rates_ps_attach_{uuid.uuid4().hex[:8]}"
    st.session_state.rates_upload_toast_pending = True
    st.rerun()


def _giga_files():
    """Модуль вложений. None, если файла нет на сервере — чат не роняем."""
    try:
        from synaptica.backend.streamlit_functions import giga_files as mod
        return mod
    except ImportError:
        _logger.warning("giga_files missing — attach helpers disabled")
        return None


def _should_use_rates_upload(uploaded) -> bool:
    """Старый путь Excel → PS Chat, если это не похоже на фин. отчётность."""
    name = str(getattr(uploaded, "name", "") or "")
    ext = Path(name).suffix.lower()
    if ext not in {".xlsx", ".xls"}:
        return False
    giga = _giga_files()
    if giga is None:
        return True
    return not giga.looks_finrag(name)


def _process_synaptica_file_upload(uploaded) -> None:
    """Общая скрепка основного чата: локально + GigaChat file_id."""
    upload_sig = f"{uploaded.name}:{len(uploaded.getvalue())}"
    if st.session_state.get("synaptica_last_upload_sig") == upload_sig:
        return

    upload_dir = Path(__file__).resolve().parent / "uploads"
    upload_dir.mkdir(exist_ok=True)
    safe_name = uploaded.name.replace(" ", "_")
    dest_path = str(upload_dir / f"{uuid.uuid4().hex}_{safe_name}")
    with open(dest_path, "wb") as fh:
        fh.write(uploaded.getvalue())

    st.session_state.synaptica_last_upload_sig = upload_sig
    st.session_state.synaptica_pending_upload = {
        "name": uploaded.name,
        "path": dest_path,
    }
    st.session_state.rates_ps_file_attached = True
    st.session_state.rates_attached_filename = uploaded.name
    st.session_state.rates_file_uploader_key = f"rates_ps_attach_{uuid.uuid4().hex[:8]}"
    st.session_state.rates_upload_toast_pending = True
    st.rerun()


def _rates_detach_current_file() -> None:
    """Удаляет файл из чата PS Chat и сбрасывает локальное состояние."""
    is_openclaw_client_mode = bool(st.session_state.get("openclaw_client_mode", False))
    is_call_upload_mode = bool(st.session_state.get("call_upload_mode", False))
    if not is_openclaw_client_mode:
        chat_id = st.session_state.get("rates_trading_agent_chat_id")
        if chat_id:
            _rates_remove_current_file(chat_id)
    if is_call_upload_mode:
        st.session_state.pop("call_upload_pending_upload", None)
    st.session_state.pop("synaptica_pending_upload", None)
    st.session_state.pop("synaptica_attach", None)
    st.session_state.synaptica_last_upload_sig = None
    st.session_state.rates_attached_filename = None
    st.session_state.rates_ps_file_attached = False
    st.session_state.rates_last_upload_sig = None
    st.session_state.openclaw_client_last_upload_sig = None
    st.session_state.call_upload_last_upload_sig = None
    st.session_state.pop("openclaw_client_pending_upload", None)
    st.session_state.rates_remove_toast_pending = True


def _render_rates_attach_popover_body() -> None:
    """Содержимое окошка: текущий файл + загрузка нового."""
    is_openclaw_client_mode = bool(st.session_state.get("openclaw_client_mode", False))
    is_call_upload_mode = bool(st.session_state.get("call_upload_mode", False))
    attached_name = st.session_state.get("rates_attached_filename")
    if attached_name:
        name_col, remove_col = st.columns([5, 1])
        name_col.markdown(f"📄 {attached_name}")
        if remove_col.button("✕", key="rates_ps_remove_btn", help="Удалить файл"):
            _rates_detach_current_file()
            st.rerun()
        st.caption("Новый файл заменит текущий.")
    elif _rates_ps_file_attached():
        st.caption("Файл загружен. Новый файл заменит текущий.")
    else:
        st.caption("Файл ещё не загружен.")

    if is_openclaw_client_mode:
        upload_types = ["pdf", "xlsx", "xls", "csv", "txt"]
    elif is_call_upload_mode:
        upload_types = ["xlsx", "xls", "csv"]
    else:
        upload_types = ["xlsx", "xls", "csv", "pdf", "png", "jpg", "jpeg", "webp", "gif", "txt"]
    uploaded = st.file_uploader(
        "Добавить файл",
        type=upload_types,
        key=st.session_state.rates_file_uploader_key,
    )
    if uploaded is not None:
        if is_openclaw_client_mode:
            _process_openclaw_client_file_upload(uploaded)
        elif is_call_upload_mode:
            _process_call_upload_file_upload(uploaded)
        elif _should_use_rates_upload(uploaded):
            _process_rates_file_upload(uploaded)
        else:
            _process_synaptica_file_upload(uploaded)


def process_openclaw_client_pending_upload(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    """Отправляет в fin_reporting событие о файле, загруженном через общую скрепку."""
    pending = st.session_state.pop("openclaw_client_pending_upload", None)
    if not pending:
        return
    if not bool(st.session_state.get("openclaw_client_mode", False)):
        return

    file_name = str(pending.get("name") or "").strip()
    file_path = str(pending.get("path") or "").strip()
    if not file_name or not file_path:
        return

    st.chat_message("user").write(f"📎 {file_name}")
    auto_msg = (
        f"Пользователь загрузил файл `{file_name}`.\n"
        f"Файл сохранён на сервере: `{file_path}`"
    )
    write_openclaw_client_answer(
        original_user_input=auto_msg,
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
    )


def process_call_upload_pending_upload(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    """Обрабатывает файл для call_upload, загруженный через общую скрепку."""
    pending = st.session_state.pop("call_upload_pending_upload", None)
    if not pending:
        return
    if not bool(st.session_state.get("call_upload_mode", False)):
        return

    file_name = str(pending.get("name") or "").strip()
    file_path = str(pending.get("path") or "").strip()
    if not file_name or not file_path:
        return

    _process_call_upload_saved_file(
        original_filename=file_name,
        saved_path=file_path,
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        chat_history_last=st.session_state.get("chat_history_last"),
    )


_VL_SUMMARY_PROMPT = (
    "Кратко опиши этот файл: что это, о чём, ключевые числа и даты если есть. "
    "5–8 коротких пунктов, без воды."
)


def process_synaptica_pending_attach(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    """После скрепки в основном чате: GigaChat files + короткий VL, при фин.отчёте — вопрос про базу."""
    pending = st.session_state.pop("synaptica_pending_upload", None)
    if not pending:
        return
    giga = _giga_files()
    file_name = str(pending.get("name") or "").strip()
    file_path = str(pending.get("path") or "").strip()
    if not file_name or not file_path:
        return

    st.chat_message("user").write(f"📎 {file_name}")
    summary = ""
    file_id = None
    if giga is not None:
        with st.spinner("Смотрю файл…"):
            file_id, up_raw = giga.upload_file(file_path)
            if file_id:
                summary, _ = giga.chat_with_file(file_id, _VL_SUMMARY_PROMPT)
            else:
                _logger.warning("synaptica attach: giga upload failed file=%s raw=%s", file_name, up_raw[:400])
    awaiting = bool(giga and giga.looks_finrag(file_name, summary))
    st.session_state.synaptica_attach = {
        "name": file_name,
        "path": file_path,
        "file_id": file_id,
        "summary": summary,
        "awaiting_finrag": awaiting,
        "in_finrag": False,
    }

    parts = [f"Файл **{file_name}** принят."]
    if summary:
        parts.append(summary)
    elif file_id:
        parts.append("Файл в GigaChat загружен, но описание не пришло.")
    else:
        parts.append("Залить в GigaChat не вышло — путь сохранён, обычный чат не сломан.")
    if awaiting:
        parts.append("Похоже на финансовую отчётность. Залить в базу finRAG? Напишите **да** или **нет**.")
    answer = "\n\n".join(parts)
    stream_md_message(answer)
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=f"📎 {file_name}",
        transformed_user_input=file_path,
        chat_answer=answer,
        chat_history_last=str(st.session_state.get("chat_history_last") or ""),
        pipeline="synaptica_attach",
    )


def try_handle_synaptica_attach_reply(
    prompt: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> bool:
    """да/нет про finRAG или вопрос про приложенный файл. True = обычный пайплайн не трогаем."""
    attach = st.session_state.get("synaptica_attach")
    if not isinstance(attach, dict):
        return False
    text = (prompt or "").strip()
    if not text:
        return False
    giga = _giga_files()
    if giga is None:
        return False

    kind = giga.classify_user_turn(
        text,
        awaiting_finrag=bool(attach.get("awaiting_finrag")),
        filename=str(attach.get("name") or ""),
    )

    if kind == "yes":
        st.chat_message("user").write(text)
        ok, raw = giga.upload_finrag(str(attach.get("path") or ""), str(attach.get("name") or "file"))
        attach["awaiting_finrag"] = False
        attach["in_finrag"] = bool(ok)
        st.session_state.synaptica_attach = attach
        answer = (
            "Залил в базу finRAG. Можно спрашивать по документу."
            if ok
            else f"В базу не ушло ({raw[:300]}). Файл в чате остаётся, можно спрашивать по нему."
        )
    elif kind == "no":
        st.chat_message("user").write(text)
        attach["awaiting_finrag"] = False
        st.session_state.synaptica_attach = attach
        answer = "Ок, в базу не кладу. Можно спрашивать по файлу — отвечу по вложению."
    elif kind == "about_file" and attach.get("file_id"):
        st.chat_message("user").write(text)
        with st.spinner("Смотрю вложение…"):
            answer, raw = giga.chat_with_file(str(attach["file_id"]), text)
        if not answer:
            answer = (
                "По вложению ответа нет. Можно переформулировать или спросить без файла.\n"
                f"({raw[:400]})"
            )
    else:
        return False

    stream_md_message(answer)
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=text,
        transformed_user_input=text,
        chat_answer=answer,
        chat_history_last=str(st.session_state.get("chat_history_last") or ""),
        pipeline="synaptica_attach",
    )
    return True


def _render_rates_attach_uploader() -> None:
    """📎 слева от ввода: клик открывает окошко с файлами."""
    if st.session_state.pop("rates_upload_toast_pending", False):
        st.toast("Файл загружен (предыдущий заменён)", icon="✅")
    if st.session_state.pop("rates_remove_toast_pending", False):
        st.toast("Файл удалён", icon="🗑️")
    if "rates_file_uploader_key" not in st.session_state:
        st.session_state.rates_file_uploader_key = "rates_ps_attach_html_0"

    st.markdown(_rates_attach_popover_styles(_rates_ps_file_attached()), unsafe_allow_html=True)
    with st.container(key="rates_ps_attach_pop"):
        with st.popover("📎", help="Файл для анализа"):
            _render_rates_attach_popover_body()


_OPENCLAW_MODE_KEYS = (
    "openclaw_coder_mode",
    "openclaw_helper_mode",
    "openclaw_client_mode",
    "openclaw_fi_sales_mode",
    "openclaw_corp_sales_mode",
    "openclaw_calls_mode",
    "openclaw_vnd_rag_mode",
    "openclaw_pres_gen_mode",
    "openclaw_rates_pricing_mode",
    "openclaw_products_mode",
    "openclaw_router_mode",
)


def _openclaw_mode_active() -> bool:
    """True, если нужен ✕ (выход из OpenClaw или стоп генерации /pres)."""
    return any(bool(st.session_state.get(k, False)) for k in _OPENCLAW_MODE_KEYS) or bool(
        st.session_state.get("presentation_gen_mode", False)
    )


def _exit_openclaw_mode() -> None:
    """Выход из любого режима OpenClaw: сброс всех openclaw-пайплайнов."""
    reset_to_zero_openclaw_pipeline()
    reset_to_zero_openclaw_vnd_rag_pipeline()
    reset_to_zero_openclaw_router_pipeline()


def _openclaw_exit_btn_styles() -> str:
    """Кнопка ✕ без рамки/фона (как иконка 📎); круглая подсветка при наведении."""
    scope = "div.st-key-openclaw_exit_pop button"
    parts = [
        f"{scope}{{width:2.75rem!important;height:2.75rem!important;min-height:2.75rem!important;"
        "padding:0!important;border:none!important;background:transparent!important;"
        "box-shadow:none!important;color:inherit!important;font-size:1.35rem!important;line-height:1!important;}",
        f"{scope}:hover{{background:rgba(0,0,0,0.06)!important;border-radius:50%!important;}}",
    ]
    return f"<style>{''.join(parts)}</style>"


def _render_rates_chat_bar(placeholder: str) -> str | None:
    show_exit = _openclaw_mode_active()
    ratios = [0.45, 0.45, 11.1] if show_exit else [0.45, 11.55]
    try:
        cols = st.columns(ratios, gap="small", vertical_alignment="bottom")
    except TypeError:
        cols = st.columns(ratios, gap="small")
    if show_exit:
        # ✕ слева от скрепки — выход из активного режима OpenClaw.
        with cols[0]:
            st.markdown(_openclaw_exit_btn_styles(), unsafe_allow_html=True)
            with st.container(key="openclaw_exit_pop"):
                _in_pres = bool(st.session_state.get("presentation_gen_mode", False))
                if st.button(
                    "✕",
                    key="openclaw_exit_btn",
                    help="Остановить генерацию" if _in_pres else "Выйти из режима",
                ):
                    if not _in_pres:
                        _exit_openclaw_mode()
                        st.rerun()
                    try:
                        from synaptica.backend.streamlit_functions.presentation_gen import stop_pres_job
                        stop_pres_job(st.session_state.get("_pres_job_id"))
                    except Exception:
                        pass
                    pending = st.session_state.pop("_pres_pending_user", None)
                    if pending:
                        from langchain_core.messages import HumanMessage
                        msg = HumanMessage(content=pending)
                        st.session_state.langchain_messages = list(
                            st.session_state.get("langchain_messages") or []
                        ) + [msg]
                        st.session_state.last_messages = list(
                            st.session_state.get("last_messages") or []
                        ) + [msg]
                        hist = list(st.session_state.get("presentation_gen_chat_history") or [])
                        hist.append({"role": "user", "content": pending})
                        st.session_state.presentation_gen_chat_history = hist
                    st.rerun()
        with cols[1]:
            _render_rates_attach_uploader()
        with cols[2]:
            return st.chat_input(placeholder=placeholder)
    with cols[0]:
        _render_rates_attach_uploader()
    with cols[1]:
        return st.chat_input(placeholder=placeholder)


def render_chat_input_with_rates_attach(placeholder: str) -> str | None:
    """Нижняя панель: 📎 слева, chat_input справа; клик по 📎 = Browse files."""
    render_comments_analytics_panel_if_active()
    bottom_cm = _streamlit_bottom_container()
    if bottom_cm is None:
        return _render_rates_chat_bar(placeholder)

    with bottom_cm:
        return _render_rates_chat_bar(placeholder)


def write_rates_trading_bot(
    giga,
    original_user_input: str,
    choosed_case,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    transformed_user_input,
    chat_history_last,
):
    reset_to_zero_ALL_pipelines()

    # Разметка пайплайна до любого save_logs (включая ранний выход по ошибке),
    # иначе прайсинговые запросы не попадают в дашборд rates.
    st.session_state.run_id = str(uuid.uuid4())
    _register_feedback_pipeline_for_run(st.session_state.run_id, "rates")

    BASE_URL = "http://10.10.206.123:8090"
    import requests
    import re
    import io

    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        updates_df = None

        st.session_state.status_container = message_placeholder

        if getattr(st.session_state, "rates_trading_agent_chat_id", None) is None:
            message_placeholder.markdown("[PS Chat] ⏳ Creating new chat with trading bot ...")
            response = requests.post(f"{BASE_URL}/api/new_chat")
            if response.status_code == 200:
                data = response.json()
                if data["success"]:
                    print(st.session_state)
                    st.session_state.rates_trading_agent_chat_id = data["chat_id"]
                    print("Created chat ", data["chat_id"])
                else:
                    full_response_for_display = "[PS Chat] Error while creating chat..."
                    message_placeholder.markdown(full_response_for_display)
                    save_logs(
                        full_response=full_response_for_display,
                        original_user_input=original_user_input,
                        transformed_user_input=original_user_input,
                        chat_history_last=str(""),
                    )
                    return
            else:
                return
        message_placeholder.markdown("[PS Chat] ⏳ Waiting for PS Chat ...")
        chat_id = getattr(st.session_state, "rates_trading_agent_chat_id", None)
        print("Sending request. chat id =", chat_id, "message", original_user_input)
        response = requests.post(
            f"{BASE_URL}/api/send_message",
            json={
                "message": original_user_input,
                "chat_id": chat_id
            }
        )

        if response.status_code == 200:
            if response.json()['success']:
                print("Response OK", response.json())
                full_response_for_display = response.json()["response"]
            else:
                full_response_for_display = f"[PS Chat] Internal error in chat... Chat id = `{chat_id}`"
        else:
            full_response_for_display = "[PS Chat] Internal error in chat..."

        # --- Handle downloadable file ---
        # Look for a link to /api/download_table/...
        link_match = re.search(r'(https?://[^\s]+/api/download_table/[^\s]+\.xlsx)', full_response_for_display)
        file_content = None
        file_name = None
        if link_match:
            original_url = link_match.group(1)
            try:
                file_resp = requests.get(original_url, timeout=30)
                if file_resp.status_code == 200:
                    file_content = io.BytesIO(file_resp.content)
                    file_name = original_url.split('/')[-1]
                    # Remove the link from the message
                    full_response_for_display = full_response_for_display.replace(original_url, "")
                    print(f"Downloaded file: {original_url}")
                else:
                    print(f"Failed to download file: status {file_resp.status_code}")
            except Exception as e:
                print(f"Error downloading file: {e}")

        # Display the message (without the link)
        message_placeholder.markdown(full_response_for_display)

        # If we have a file, show a download button
        if file_content is not None and file_name is not None:
            st.download_button(
                label="📥 Скачать файл",
                data=file_content,
                file_name=file_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        with collect_runs() as cb:
            all_messages_memory.save_context(
                {"input": original_user_input}, {"output": str(full_response_for_display)}
            )
            
            last_messages_memory.save_context(
                {"input": original_user_input}, {"output": str(full_response_for_display)}
            )

    save_logs(
        full_response=full_response_for_display,
        original_user_input=original_user_input,
        transformed_user_input=transformed_user_input,
        chat_history_last=str(chat_history_last),
    )
    reset_to_zero_input_pipeline()

def write_rates_agent(
    giga,
    original_user_input: str,
    choosed_case,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    transformed_user_input,
    chat_history_last,
) -> None:
    reset_to_zero_ALL_pipelines()

    original_user_input = _coerce_chat_input_for_memory(original_user_input)

    def update_status(message):
        if 'status_container' in st.session_state:
            st.session_state.status_container.markdown(f"{message}")

    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        updates_df = None

        st.session_state.status_container = message_placeholder
        message_placeholder.markdown("⏳ Starting process...")

        if choosed_case == "сохранить rates сделку":
            full_response = process_save_rates_deal_request(giga, transformed_user_input)
        if choosed_case == "удалить rates сделку":
            full_response = process_delete_rates_deal_request(giga, original_user_input)
        elif choosed_case == "rates апдейты":
            full_response, updates_df = process_rates_updates(giga, original_user_input)
        if choosed_case == "rates рестракты":
            full_response, updates_df = process_rates_restruct(giga, transformed_user_input, status_updater=update_status)
        if choosed_case == "rates параметры сделки":
            full_response = process_get_rates_deal_params(giga, original_user_input)
        if choosed_case == "изменить параметры rates сделки":
            full_response = process_change_rates_params(giga, transformed_user_input)
        st.session_state.status_container = message_placeholder

        seed_all(42)
        with collect_runs() as cb:
            st.session_state.run_id = str(uuid.uuid4())
            _register_feedback_pipeline_for_run(st.session_state.run_id, "rates")

            all_messages_memory.save_context(
                {"input": original_user_input}, {"output": str(full_response)}
            )

            last_messages_memory.save_context(
                {"input": original_user_input}, {"output": str(full_response)}
            )
        
        full_response_for_display = full_response
        if "**Restruct hashes**" in full_response:
            full_response_for_display = full_response[:full_response.find("**Restruct hashes**")]

        message_placeholder.markdown(full_response_for_display, unsafe_allow_html=True)
        if updates_df is not None:
            st.table(updates_df)
            

    save_logs(
        full_response=full_response_for_display,
        original_user_input=original_user_input,
        transformed_user_input=transformed_user_input,
        chat_history_last=str(chat_history_last),
    )
    reset_to_zero_input_pipeline()

def write_pers(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    reset_to_zero_input_pipeline()
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    user_prompt = "/personalization"
    st.chat_message("user").write(user_prompt)
    chat_answer = "Погоди"

    stream_md_message(chat_answer)

    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=user_prompt,
        transformed_user_input="",
        chat_answer=chat_answer,
        chat_history_last="",
        pipeline="personalization",
    )

    reset_to_zero_input_pipeline()



def write_giga_model_as_is_answer(
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
) -> None:
    reset_to_zero_ALL_pipelines()

    base_prompt = ChatPromptTemplate.from_messages(
        [
            ("system", GENERAL_SYNAPTICA_SYSTEM_PROMPT_TEMPLATE),
            MessagesPlaceholder(variable_name="chat_history_last"),
            ("user", "{content}"),
        ]
    )
    chain = base_prompt | settings.model  # | StrOutputParser()

    with st.chat_message("assistant"):
        message_placeholder = st.empty()

        full_response = ""
        input_dict = {
            "content": original_user_input,
            "chat_history_last": chat_history_last,
        }

        seed_all(42)
        for chunk in chain.stream(input_dict):  # type: ignore
            full_response += chunk.content  # type: ignore
            message_placeholder.markdown(full_response + "▌")
        message_placeholder.markdown(full_response)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            chat_answer=full_response,
            chat_history_last=str(chat_history_last),
            pipeline="general_llm",
        )



    reset_to_zero_input_pipeline()


def show_register_form() -> None:
    st.session_state.exec_register_pipeline = True


def start_indicaties_pipeline(
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: str,
) -> None:
    reset_to_zero_ALL_pipelines()

    st.session_state.products_list_ind = get_products_from_user_input(transformed_user_input)

    full_response = ""

    if st.session_state.products_list_ind == []:
        chat_answer = full_response + "По данному продукту информация пока что не добавлена"
        stream_md_message(chat_answer)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            chat_answer=chat_answer,
            chat_history_last=chat_history_last,
            pipeline="indicatives",
        )
        reset_to_zero_indicatives_pipeline()
        reset_to_zero_input_pipeline()

    else:
        chat_answer = full_response + "Выберите продукт"
        stream_md_message(chat_answer)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=original_user_input,
            chat_answer=chat_answer,
            transformed_user_input=transformed_user_input,
            chat_history_last=chat_history_last,
            pipeline="indicatives",
        )
        last_messages_memory.chat_memory.add_user_message(original_user_input)

        st.session_state.button_key_ind_products = str(uuid.uuid4())


def get_indicative_from_list() -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.ind_product = sac.buttons(
        items=st.session_state.products_list_ind,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_ind_products,
    )
    if st.session_state.ind_product:
        st.session_state.start_ind_pipe_product = True
        st.session_state.products_list_ind = None


def start_indicatives_pipeline_with_product() -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    if st.session_state.ind_product:
        st.session_state.start_ind_pipe_product = False
        st.session_state.show_indicatives_button_1 = True


def indicatives_1(
    product: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_sales_pipeline()


    st.chat_message("user").write(product)

    if product == "Валютные форварда и опционы":
        st.session_state.indicatives_df_fx_options = pd.read_excel(
            settings.external_data_path / "indicatives/FXOPTIONS_Ind.xlsx"
        )
        modification_time = os.path.getmtime(
            settings.external_data_path / "indicatives/FXOPTIONS_Ind.xlsx"
        )
        st.session_state.indicatives_file_modification_date = datetime.fromtimestamp(
            modification_time
        )

        st.session_state.indicatives_df_fx_options["STRIKE"] = (
            st.session_state.indicatives_df_fx_options["STRIKE"].apply(lambda x: round(x, 4))
        )
        chat_answer = "Вам нужен опцион CALL или PUT?"
        stream_md_message(chat_answer)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=product,
            chat_answer=chat_answer,
            transformed_user_input="",
            chat_history_last="",
            pipeline="indicatives",
        )
        st.session_state.button_values_ind_type = ["CALL", "PUT"]
        st.session_state.button_key_indicatives_type = str(uuid.uuid4())
    else:
        st.session_state.ind_full_response_d = "К сожалению, индикативные котировки по данному \
продукту в процессе подключения. Я могу вам помочь чем-либо ещё?"
        stream_md_message(st.session_state.ind_full_response_d)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=product,
            chat_answer=st.session_state.ind_full_response_d,
            transformed_user_input="",
            chat_history_last="",
            pipeline="indicatives",
        )
        last_messages_memory.chat_memory.add_ai_message(st.session_state.ind_full_response_d)

        reset_to_zero_input_pipeline()
        reset_to_zero_products_pipeline()
        reset_to_zero_fx_pipeline()
        reset_to_zero_pers_pipeline()
        reset_to_zero_cmdt_pipeline()
        reset_to_zero_indicatives_pipeline()
        reset_to_zero_potential_pipeline()
        reset_to_zero_sales_pipeline()
    st.session_state.products_list_ind = None
    st.session_state.show_indicatives_button_1 = False


def indicatives_2() -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.selected_btn_ind_type = sac.buttons(
        items=st.session_state.button_values_ind_type,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_indicatives_type,
    )
    if st.session_state.selected_btn_ind_type:

        st.session_state.start_indicatives_tenor = True
        st.session_state.button_key_indicatives_tenor = str(uuid.uuid4())
        st.session_state.button_values_ind_type = None


def indicatives_3(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.chat_message("user").write(st.session_state.selected_btn_ind_type)
    chat_answer = "Какой тенор Вам нужен?"
    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_btn_ind_type,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="indicatives",
    )

    st.session_state.button_values_ind_tenor = (
        st.session_state.indicatives_df_fx_options[
            st.session_state.indicatives_df_fx_options["Type"]
            == st.session_state.selected_btn_ind_type
        ]["TENOR"]
        .unique()
        .tolist()
    )
    st.session_state.start_indicatives_tenor = False


def indicatives_4() -> None:
    st.session_state.selected_btn_ind_tenor = sac.buttons(
        items=st.session_state.button_values_ind_tenor,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_indicatives_tenor,
    )

    if st.session_state.selected_btn_ind_tenor:

        st.session_state.button_key_indicatives_strike = str(uuid.uuid4())
        st.session_state.button_values_ind_tenor = None
        st.session_state.start_indicatives_strike = True


def indicatives_5(all_messages_memory: ConversationBufferMemory) -> None:

    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.chat_message("user").write(st.session_state.selected_btn_ind_tenor)
    chat_answer = "Какой страйк Вас интересует?"

    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_btn_ind_tenor,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="indicatives",
    )

    st.session_state.button_values_ind_strike = (
        st.session_state.indicatives_df_fx_options[
            (
                st.session_state.indicatives_df_fx_options["Type"]
                == st.session_state.selected_btn_ind_type
            )
            & (
                st.session_state.indicatives_df_fx_options["TENOR"]
                == st.session_state.selected_btn_ind_tenor
            )
        ]["STRIKE"]
        .astype(str)
        .unique()
        .tolist()
    )
    st.session_state.start_indicatives_strike = False


def indicatives_6() -> None:
    st.session_state.btn_ind_strike = sac.buttons(
        items=st.session_state.button_values_ind_strike,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_indicatives_strike,
    )
    if st.session_state.btn_ind_strike:

        st.session_state.indicatives_finish = True
        st.session_state.button_values_ind_strike = None


def indicatives_7() -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.session_state.indicatives_finish = False

    ind_bid = str(
        round(
            st.session_state.indicatives_df_fx_options["PREMIUM BID, RUB"][
                (
                    st.session_state.indicatives_df_fx_options["Type"]
                    == st.session_state.selected_btn_ind_type
                )
                & (
                    st.session_state.indicatives_df_fx_options["TENOR"]
                    == st.session_state.selected_btn_ind_tenor
                )
                & (
                    st.session_state.indicatives_df_fx_options["STRIKE"]
                    == float(st.session_state.btn_ind_strike)
                )
            ].item(),
            5,
        )
    )
    ind_offer = str(
        round(
            st.session_state.indicatives_df_fx_options["PREMIUM OFFER, RUB"][
                (
                    st.session_state.indicatives_df_fx_options["Type"]
                    == st.session_state.selected_btn_ind_type
                )
                & (
                    st.session_state.indicatives_df_fx_options["TENOR"]
                    == st.session_state.selected_btn_ind_tenor
                )
                & (
                    st.session_state.indicatives_df_fx_options["STRIKE"]
                    == float(st.session_state.btn_ind_strike)
                )
            ].item(),
            5,
        )
    )
    year = str(st.session_state.indicatives_file_modification_date.year)[2:]
    month = str(st.session_state.indicatives_file_modification_date.month)
    day = str(st.session_state.indicatives_file_modification_date.day)
    ind_full_response = f"""По опциону \
{st.session_state.selected_btn_ind_type} с тенором \
{st.session_state.selected_btn_ind_tenor} \
и страйком {st.session_state.btn_ind_strike}:
- PREMIUM BID, RUB = {ind_bid}
- PREMIUM OFFER, RUB = {ind_offer}.

*Дата обновления – {day}.{month}.’{year}. Обращаем внимание, что котировки индикативные*"""
    st.session_state.ind_full_response_d = ind_full_response


def indicatives_finish(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_potential_pipeline()
    reset_to_zero_sales_pipeline()

    st.chat_message("user").write(st.session_state.btn_ind_strike)
    stream_md_message(st.session_state.ind_full_response_d)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.btn_ind_strike,
        chat_answer=st.session_state.ind_full_response_d,
        transformed_user_input="",
        chat_history_last="",
        pipeline="indicatives",
    )
    last_messages_memory.chat_memory.add_ai_message(st.session_state.ind_full_response_d)
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_input_pipeline()


def potenial_answer_with_no_data(
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: str,
) -> None:
    full_response = "Совпадений по наименованию компании не найдено"
    stream_md_message(full_response)
    save_in_both_memories_and_in_logs(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        original_user_input=original_user_input,
        chat_answer=full_response,
        transformed_user_input=transformed_user_input,
        chat_history_last=chat_history_last,
        pipeline="potential",
    )

    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_input_pipeline()
    reset_to_zero_potential_pipeline()


def start_potential_pipeline(
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: str,
) -> None:
    reset_to_zero_ALL_pipelines()

    st.session_state.selected_company = select_company(transformed_user_input)
    holdings_list, stand_alone_list, companies_in_holdings_list = get_potential_data(settings)

    st.session_state.most_similar_holdings = do_fuzzy_search(
        st.session_state.selected_company, holdings_list
    )
    st.session_state.most_similar_stand_alones = do_fuzzy_search(
        st.session_state.selected_company, stand_alone_list
    )

    most_similar_companies_in_holdings = do_fuzzy_search(
        st.session_state.selected_company, companies_in_holdings_list
    )

    if (
        st.session_state.most_similar_holdings == []
        and st.session_state.most_similar_stand_alones == []
        and most_similar_companies_in_holdings == []
    ):
        potenial_answer_with_no_data(
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
        )
    else:
        stat_potential_df = pd.read_feather(settings.external_data_path / "stat_potential.feather")
        dyn_potential_df = pd.read_feather(settings.external_data_path / "dyn_potential.feather")
        st.session_state.potential_df = pd.concat([stat_potential_df, dyn_potential_df])
        st.session_state.holdings_with_similar_companies = (
            st.session_state.potential_df[
                st.session_state.potential_df["short_nm"].isin(most_similar_companies_in_holdings)
            ]["holding_name"]
            .dropna()
            .unique()
        ).tolist()

        full_response = ""

        chat_answer = full_response + "Выберите холдинг или компанию"
        stream_md_message(chat_answer)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=original_user_input,
            chat_answer=chat_answer,
            transformed_user_input=transformed_user_input,
            chat_history_last=chat_history_last,
            pipeline="potential",
        )
        last_messages_memory.chat_memory.add_user_message(transformed_user_input)
        st.session_state.selectbox_key_potential_holdings = str(uuid.uuid4())
        st.session_state.selectbox_key_potential_holdings_with_similar_companies = str(uuid.uuid4())
        st.session_state.selectbox_key_potential_stand_alone_companies = str(uuid.uuid4())
        st.session_state.show_first_selectboxes = True


def select_company_from_first_lists() -> None:

    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()

    col1, col2, col3 = st.columns(3)

    st.session_state.holding = col1.selectbox(
        "Совпадения по названиям холдингов",
        [""] + st.session_state.most_similar_holdings,
        key=st.session_state.selectbox_key_potential_holdings,
    )

    st.session_state.holding_with_similar_companies = col2.selectbox(
        "Совпадения по названиям юр. лиц внутри холдинга",
        [""] + st.session_state.holdings_with_similar_companies,
        key=st.session_state.selectbox_key_potential_holdings_with_similar_companies,
    )

    st.session_state.stand_alone_company = col3.selectbox(
        "Совпадения по названиям юр. лиц вне холдингов",
        [""] + st.session_state.most_similar_stand_alones,
        key=st.session_state.selectbox_key_potential_stand_alone_companies,
    )

    if st.session_state.holding:
        st.session_state.show_first_selectboxes = False
        st.session_state.show_potential_df_holding = True

    if st.session_state.stand_alone_company:
        st.session_state.show_first_selectboxes = False
        st.session_state.show_potential_df_stand_alone = True

    if st.session_state.holding_with_similar_companies:
        st.session_state.show_first_selectboxes = False
        show_companies_in_holding_selectbox()


def potential_stand_alone(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:

    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()

    st.session_state.potential_table = st.session_state.potential_df.query(
        "short_nm == @st.session_state.stand_alone_company and type == 'Автономная'"
    )[["Наименование продукта", "Потенциальный доход, тыс. руб."]].reset_index(drop=True)


    tmp_full_response = f"Потенциал {st.session_state.stand_alone_company}"
    stream_md_message(tmp_full_response)
    st.chat_message("assistant").write(st.session_state.potential_table)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.stand_alone_company,
        chat_answer=tmp_full_response,
        transformed_user_input="",
        chat_history_last="",
        pipeline="potential",
    )
    last_messages_memory.chat_memory.add_ai_message(tmp_full_response)

    start_sales_pipeline(all_messages_memory)
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()


def potential_holding(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:

    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    st.session_state.potential_table = st.session_state.potential_df.query(
        "short_nm == @st.session_state.holding and type == 'Холдинг'"
    )[["Наименование продукта", "Потенциальный доход, тыс. руб."]].reset_index(drop=True)

    tmp_full_response = f"Потенциал {st.session_state.holding}"
    stream_md_message(tmp_full_response)
    st.chat_message("assistant").write(st.session_state.potential_table)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.holding,
        chat_answer=tmp_full_response,
        transformed_user_input="",
        chat_history_last="",
        pipeline="potential",
    )
    last_messages_memory.chat_memory.add_ai_message(tmp_full_response)

    start_sales_pipeline(all_messages_memory)
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_potential_pipeline()


def show_companies_in_holding_selectbox() -> None:
    st.session_state.companies_in_holdings_list = (
        st.session_state.potential_df[
            st.session_state.potential_df["holding_name"]
            == st.session_state.holding_with_similar_companies
        ]["short_nm"]
        .dropna()
        .unique()
    ).tolist()

    st.session_state.selectbox_key_potential_companies_from_holding = str(uuid.uuid4())


def select_company_from_second_list() -> None:
    st.session_state.option_potential_companies_from_holding = st.selectbox(
        "Выберите компанию из холдинга",
        [""] + st.session_state.companies_in_holdings_list,
        key=st.session_state.selectbox_key_potential_companies_from_holding,
    )

    if st.session_state.option_potential_companies_from_holding:

        st.session_state.companies_in_holdings_list = None
        st.session_state.show_potential_df_company_from_holding = True


def potential_company_from_holding(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:

    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()

    st.session_state.potential_table = st.session_state.potential_df.query(
        "short_nm == @st.session_state.option_potential_companies_from_holding and \
type == 'Компания в холдинге'"
    )[["Наименование продукта", "Потенциальный доход, тыс. руб."]].reset_index(drop=True)


    tmp_full_response = f"Потенциал {st.session_state.option_potential_companies_from_holding}"
    stream_md_message(tmp_full_response)
    st.chat_message("assistant").write(st.session_state.potential_table)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.option_potential_companies_from_holding,
        chat_answer=tmp_full_response,
        transformed_user_input="",
        chat_history_last="",
        pipeline="potential",
    )
    last_messages_memory.chat_memory.add_ai_message(tmp_full_response)

    start_sales_pipeline(all_messages_memory)
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()


def write_default_answer_if_any_error(
    original_user_input: str, all_messages_memory: ConversationBufferMemory
) -> None:
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
    full_response = "Данный вопрос вызвал временные сложности - Вы можете мне помочь исправить \
ситуацию, оставив отзыв с описанием проблемы по адресу <SYNAPTICA_SUPPORT@sberbank.ru>"
    message_placeholder.markdown(full_response)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=original_user_input,
        chat_answer=full_response,
        transformed_user_input="",
        chat_history_last="",
        pipeline="error",
    )


def _write_choose_case_answer(
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
) -> None:
    """Старый оркестратор GigaChat (choose_case), когда OPENCLAW_CLASSIFIER выключен."""
    pc = getattr(st.session_state, "precomputed_choosed_case", None)
    if pc is not None:
        choosed_case = pc
        st.session_state.precomputed_choosed_case = None
    else:
        with st.spinner("Пожалуйста, подождите, обрабатываем запрос…"):
            choosed_case = choose_case(transformed_user_input, original_user_input)

    if (
        choosed_case == "вопрос по звонкам продаж"
        and not is_calls_allowed_user(str(st.session_state.get("username", "")))
    ):
        msg = "⛔ У вас нет доступа к аналитике звонков."
        stream_md_message(msg)
        save_in_both_memories_and_in_logs(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            chat_answer=msg,
            chat_history_last=str(chat_history_last) if chat_history_last else "",
            pipeline="calls",
        )
        reset_to_zero_ALL_pipelines()
        return

    if choosed_case == "rates agent":
        with st.spinner("Пожалуйста, подождите, обрабатываем запрос…"):
            choosed_case = choose_case_rates_agent(transformed_user_input)

    if choosed_case == "другое":
        write_giga_model_as_is_answer(
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
        )
        reset_to_zero_ALL_pipelines()

    elif choosed_case == "вопрос по продуктам":
        write_rag(
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
        )
    elif choosed_case == "вывести аналитику данных":
        write_analytics_rag(
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
        )
    elif choosed_case == "прайсинг":
        write_rates_trading_bot(
            settings.model,
            original_user_input,
            choosed_case,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            transformed_user_input=transformed_user_input,
            chat_history_last=chat_history_last,
        )
    elif "rates" in choosed_case:
        write_rates_agent(
            settings.model,
            original_user_input,
            choosed_case,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            transformed_user_input=transformed_user_input,
            chat_history_last=chat_history_last,
        )
    elif choosed_case == "запрос во внутренние документы (RAG)":
        start_vnd_pipeline_2(
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=str(chat_history_last),
        )
    elif choosed_case == "показать потенциал компании":
        start_potential_pipeline(
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=str(chat_history_last),
        )
    elif choosed_case == "запрос индикативных котировок":
        start_indicaties_pipeline(
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=str(chat_history_last),
        )
    elif choosed_case == "показать новости рынка":
        write_news_from_user_input(
            original_user_input=original_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            transformed_user_input=transformed_user_input,
            chat_history_last=str(chat_history_last),
        )
    elif choosed_case == "вопрос по звонкам продаж":
        st.session_state.pending_feedback_meta = {"pipeline": "calls"}
        _cu = str(st.session_state.get("username", ""))
        st.session_state.calls_mode = True
        st.session_state["calls_bank"] = resolve_calls_bank(_cu)
        if "calls_chat_history" not in st.session_state:
            st.session_state.calls_chat_history = []
        write_calls_answer(
            original_user_input=original_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
            transformed_user_input=transformed_user_input,
        )


def write_llm_answer_from_text(  # noqa: C901
    original_user_input: str,
    transformed_user_input: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    chat_history_last: list,
    personalization_flag: bool,
    ADDITIONAL_INSTRUCTION: str,
) -> None:

    if getattr(st.session_state, "vnd_mode", False) and original_user_input.startswith("/") and original_user_input != "/vnd":
        reset_to_zero_vnd_pipeline()

    if getattr(st.session_state, "vnd_mode", False) and not original_user_input.startswith("/"):
        write_vnd_answer(
            original_user_input=original_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
        )
        return

    if (
        getattr(st.session_state, "presentation_gen_mode", False)
        and original_user_input.startswith("/")
        and original_user_input != PRESENTATION_GEN_COMMAND
    ):
        reset_to_zero_presentation_gen_pipeline()

    if getattr(st.session_state, "presentation_gen_mode", False) and not original_user_input.startswith("/"):
        write_presentation_generate_answer(
            original_user_input=original_user_input,
            transformed_user_input=transformed_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
        )
        return

    if (
        getattr(st.session_state, "special_news_mode", False)
        and original_user_input.startswith("/")
        and original_user_input != SPECIAL_NEWS_COMMAND
    ):
        reset_to_zero_special_news_pipeline()

    if getattr(st.session_state, "special_news_mode", False) and not original_user_input.startswith("/"):
        stream_md_message(
            "Настройка подписки — через панель над чатом "
            "(компании → отрасли → «Вывести новости» для проверки подборки)."
        )
        return

    if (
        getattr(st.session_state, "logs_view_mode", False)
        and original_user_input.startswith("/")
        and original_user_input != COMMENTS_COMMAND
    ):
        reset_to_zero_logs_view_pipeline()

    if getattr(st.session_state, "logs_view_mode", False) and not original_user_input.startswith("/"):
        write_logs_view_answer(
            original_user_input=original_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
        )
        return

    if (
        getattr(st.session_state, "call_upload_mode", False)
        and original_user_input.startswith("/")
        and original_user_input != CALL_UPLOAD_COMMAND
    ):
        reset_to_zero_call_upload_pipeline()

    if (
        getattr(st.session_state, "openclaw_vnd_rag_mode", False)
        and original_user_input.startswith("/")
        and original_user_input != VND_RAG_COMMAND
    ):
        reset_to_zero_openclaw_vnd_rag_pipeline()

    if getattr(st.session_state, "openclaw_vnd_rag_mode", False) and not original_user_input.startswith("/"):
        write_openclaw_vnd_rag_answer(
            original_user_input=original_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
        )
        return

    _OPENCLAW_COMMANDS = {
        "/openclaw1_coder",
        "/openclaw1_helper",
        "/openclaw",
        "/openclaw1",
        "/fi_notes",
        "/notes",
        "/corp_notes",
        CALLS_OPENCLAW_COMMAND,
        VND_RAG_COMMAND,
        PRESENTATION_OPENCLAW_COMMAND,
        RATES_PRICING_COMMAND,
        PRODUCTS_RAG_COMMAND,
    }
    for _oc_mode, _oc_answer_fn, _oc_cmd in [
        ("openclaw_coder_mode", write_openclaw_coder_answer, "/openclaw1_coder"),
        ("openclaw_helper_mode", write_openclaw_helper_answer, "/openclaw1_helper"),
        ("openclaw_client_mode", write_openclaw_client_answer, "/openclaw"),
        ("openclaw_fi_sales_mode", write_openclaw_fi_sales_answer, "/notes"),
        ("openclaw_corp_sales_mode", write_openclaw_corp_sales_answer, "/corp_notes"),
        ("openclaw_calls_mode", write_openclaw_calls_answer, CALLS_OPENCLAW_COMMAND),
        ("openclaw_pres_gen_mode", write_openclaw_pres_gen_answer, PRESENTATION_OPENCLAW_COMMAND),
        ("openclaw_rates_pricing_mode", write_openclaw_rates_pricing_answer, RATES_PRICING_COMMAND),
        ("openclaw_products_mode", write_openclaw_products_answer, PRODUCTS_RAG_COMMAND),
    ]:
        if getattr(st.session_state, _oc_mode, False) and original_user_input.startswith("/") and original_user_input not in _OPENCLAW_COMMANDS:
            reset_to_zero_openclaw_pipeline()
        if getattr(st.session_state, _oc_mode, False) and not original_user_input.startswith("/"):
            _oc_answer_fn(
                original_user_input=original_user_input,
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
            return

    if (
        getattr(st.session_state, "openclaw_router_mode", False)
        and original_user_input.startswith("/")
        and original_user_input not in _OPENCLAW_COMMANDS
    ):
        reset_to_zero_openclaw_router_pipeline()

    if getattr(st.session_state, "openclaw_router_mode", False) and not original_user_input.startswith("/"):
        if settings.openclaw_classifier:
            write_openclaw_router_answer(
                original_user_input=original_user_input,
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
            return
        reset_to_zero_openclaw_router_pipeline()

    _fi_first = (
        (original_user_input or "").strip().split(maxsplit=1)[0]
        if (original_user_input or "").strip()
        else ""
    )
    if (
        getattr(st.session_state, "notes_mode", False)
        and original_user_input.startswith("/")
        and _fi_first != "/notes"
    ):
        reset_to_zero_notes_pipeline()

    if getattr(st.session_state, "notes_mode", False) and not original_user_input.startswith("/"):
        _notes_answer(
            original_user_input=original_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
            save_fn=save_in_both_memories_and_in_logs,
            stream_fn=stream_md_message,
        )
        return

    if (
        getattr(st.session_state, "broker_mode", False)
        and original_user_input.startswith("/")
        and _fi_first != BROKER_COMMAND
    ):
        reset_to_zero_broker_pipeline()

    if getattr(st.session_state, "broker_mode", False) and not original_user_input.startswith("/"):
        _broker_answer(
            original_user_input=original_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
            save_fn=save_in_both_memories_and_in_logs,
            stream_fn=stream_md_message,
        )
        return

    _first_slash_token = (
        (original_user_input or "").strip().split(maxsplit=1)[0]
        if (original_user_input or "").strip()
        else ""
    )
    # В режиме звонков любая slash-команда сбрасывает режим, кроме /calls_tb (таблица статистики).
    if (
        getattr(st.session_state, "calls_mode", False)
        and _first_slash_token.startswith("/")
        and _first_slash_token != CALL_TABLE_COMMAND
    ):
        st.session_state.calls_mode = False
    if getattr(st.session_state, "calls_mode", False) and _first_slash_token == CALL_TABLE_COMMAND:
        write_call_table_answer(
            original_user_input=original_user_input.strip(),
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
        )
        return
    if getattr(st.session_state, "calls_mode", False) and not original_user_input.startswith("/"):
        write_calls_answer(
            original_user_input=original_user_input,
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
            chat_history_last=chat_history_last,
            transformed_user_input=transformed_user_input
        )
        return
        
    else:
        reset_to_zero_calls_pipeline()
        reset_to_zero_notes_pipeline()
        reset_to_zero_broker_pipeline()

        if (
            "".join(filter(str.isalpha, original_user_input.lower()))
            in [
                "привет",
                "здравствуй",
                "здравствуйте",
                "доброе утро",
                "добрый день",
                "добрый вечер",
            ]
            or original_user_input == "/start"
        ):
            write_hello(all_messages_memory, last_messages_memory)
        elif original_user_input == "/vnd":
            start_vnd_pipeline(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif original_user_input == "/products":
            start_products_pipeline()
        elif (original_user_input or "").strip().split(maxsplit=1)[0] == "/market":
            start_market_pipeline()
        elif original_user_input == "/fx":
            start_fx_pipeline()
        elif original_user_input == "/cmdt":
            start_cmdt_pipeline()
        elif original_user_input == "/personalization":
            start_pers_pipeline()
        elif original_user_input == "/eco":
            write_eco(all_messages_memory=all_messages_memory, show_user_msg=False)
        elif original_user_input == "/news":
            write_news(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif original_user_input == "/calls":
            write_calls(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif _first_slash_token == CALL_TABLE_COMMAND:
            _ct_user = str(st.session_state.get("username", ""))
            if not is_calls_allowed_user(_ct_user):
                _ct_denied = "⛔ У вас нет доступа к сводке по звонкам."
                stream_md_message(_ct_denied)
                save_in_both_memories_and_in_logs(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                    original_user_input=original_user_input.strip(),
                    transformed_user_input=transformed_user_input,
                    chat_answer=_ct_denied,
                    chat_history_last=str(chat_history_last) if chat_history_last else "",
                    pipeline="calls",
                )
            else:
                st.session_state["calls_bank"] = resolve_calls_bank(_ct_user)
                write_call_table_answer(
                    original_user_input=original_user_input.strip(),
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                    chat_history_last=chat_history_last,
                )
        elif (original_user_input or "").strip().split(maxsplit=1)[0] == "/notes":
            write_notes_button(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif (original_user_input or "").strip().split(maxsplit=1)[0] == BROKER_COMMAND:
            write_broker_button(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        elif original_user_input == "/faq":
            write_faq(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )

        elif original_user_input == OPENCLAW_ROUTER_COMMAND:
            write_openclaw_router_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )

        elif original_user_input == "/openclaw1_coder":
            write_openclaw_coder_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )

        elif original_user_input == "/openclaw1_helper":
            write_openclaw_helper_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )

        elif original_user_input == "/openclaw":
            write_openclaw_client_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif original_user_input in (PRESENTATION_GEN_COMMAND, "/gen_presentation"):
            write_presentation_generate_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif original_user_input == PRESENTATION_OPENCLAW_COMMAND:
            write_openclaw_pres_gen_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif original_user_input == RATES_PRICING_COMMAND:
            write_openclaw_rates_pricing_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif original_user_input == PRODUCTS_RAG_COMMAND:
            write_openclaw_products_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif original_user_input in (COMMENTS_COMMAND, "/view_logs"):
            write_logs_view_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif original_user_input == CALL_UPLOAD_COMMAND:
            write_call_upload_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )
        elif original_user_input == SPECIAL_NEWS_COMMAND:
            write_special_news_start(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                show_user_msg=False,
            )

        # elif original_user_input == "/alerts":
        #     on_tosts_button_clicked()

        elif personalization_flag:
            PERS_write_rag(
                original_user_input=original_user_input,
                transformed_user_input=transformed_user_input,
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                chat_history_last=chat_history_last,
                ADDITIONAL_INSTRUCTION = ADDITIONAL_INSTRUCTION,
            )
        else:

            reset_to_zero_pers_pipeline()
            if settings.openclaw_classifier:
                write_openclaw_router_answer(
                    original_user_input=original_user_input,
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
            else:
                _write_choose_case_answer(
                    original_user_input=original_user_input,
                    transformed_user_input=transformed_user_input,
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                    chat_history_last=chat_history_last,
                )


def not_interested_pipeline(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    original_user_input: str,
) -> None:
    full_response = "Пожалуйста, задайте мне еще вопрос, и я постараюсь Вам помочь"
    stream_md_message(full_response)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=original_user_input,
        chat_answer=full_response,
        transformed_user_input="",
        chat_history_last="",
    )
    last_messages_memory.chat_memory.add_ai_message(full_response)
    reset_to_zero_ALL_pipelines()


def answer_if_error_pipeline(
    all_messages_memory: ConversationBufferMemory, original_user_input: str
) -> None:
    original_user_input = _coerce_chat_input_for_memory(original_user_input)

    full_response = "Данный вопрос вызвал временные сложности — Вы можете мне помочь исправить \
ситуацию, оставив отзыв с описанием проблемы по адресу <SYNAPTICA_SUPPORT@sberbank.ru>."
    stream_md_message(full_response)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=original_user_input,
        chat_answer=full_response,
        transformed_user_input="",
        chat_history_last="",
        pipeline="error",
    )
    reset_to_zero_ALL_pipelines()


def answer_if_error_pipeline_saving_only_llm_answer(
    all_messages_memory: ConversationBufferMemory,
) -> None:

    full_response = "Данный вопрос вызвал временные сложности — Вы можете мне помочь исправить \
ситуацию, оставив отзыв с описанием проблемы по адресу <SYNAPTICA_SUPPORT@sberbank.ru>."
    stream_md_message(full_response)
    all_messages_memory.chat_memory.add_ai_message(full_response)
    reset_to_zero_ALL_pipelines()


def saled_function(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
    original_user_input: str,
) -> None:
    full_response = "Спасибо за ваши вопросы, на этом этапе при общении \
с Клиентом я направлю инструкцию для заключения сделки в удаленном канале либо подключу сейлза"
    stream_md_message(full_response)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=original_user_input,
        chat_answer=full_response,
        transformed_user_input="",
        chat_history_last="",
    )
    last_messages_memory.chat_memory.add_ai_message(full_response)
    reset_to_zero_ALL_pipelines()


def sales_indicatives_1(
    product: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()


    st.chat_message("user").write(product)

    if product == "Валютные форварда и опционы":
        st.session_state.indicatives_df_fx_options = pd.read_excel(
            settings.external_data_path / "indicatives/FXOPTIONS_Ind.xlsx"
        )
        modification_time = os.path.getmtime(
            settings.external_data_path / "indicatives/FXOPTIONS_Ind.xlsx"
        )
        st.session_state.indicatives_file_modification_date = datetime.fromtimestamp(
            modification_time
        )
        st.session_state.indicatives_df_fx_options["STRIKE"] = (
            st.session_state.indicatives_df_fx_options["STRIKE"].apply(lambda x: round(x, 4))
        )
        chat_answer = "Вам нужен опцион CALL или PUT?"
        stream_md_message(chat_answer)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=product,
            chat_answer=chat_answer,
            transformed_user_input="",
            chat_history_last="",
            pipeline="sales",
        )
        st.session_state.sales_button_values_ind_type = ["CALL", "PUT"]
        st.session_state.button_key_indicatives_type = str(uuid.uuid4())
    else:
        st.session_state.sales_ind_full_response_d = "К сожалению, индикативные котировки по \
данному продукту в процессе подключения. Я могу вам помочь чем-либо ещё?"
        stream_md_message(st.session_state.sales_ind_full_response_d)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=product,
            chat_answer=st.session_state.sales_ind_full_response_d,
            transformed_user_input="",
            chat_history_last="",
            pipeline="sales",
        )
        last_messages_memory.chat_memory.add_ai_message(st.session_state.sales_ind_full_response_d)

        reset_to_zero_input_pipeline()
        reset_to_zero_products_pipeline()
        reset_to_zero_fx_pipeline()
        reset_to_zero_pers_pipeline()
        reset_to_zero_cmdt_pipeline()
        reset_to_zero_indicatives_pipeline()
        reset_to_zero_potential_pipeline()
        reset_to_zero_sales_pipeline()
    st.session_state.start_sales_indicatives = False


def sales_indicatives_2() -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()

    st.session_state.selected_btn_ind_type = sac.buttons(
        items=st.session_state.sales_button_values_ind_type,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_indicatives_type,
    )
    if st.session_state.selected_btn_ind_type:

        st.session_state.sales_start_indicatives_tenor = True
        st.session_state.button_key_indicatives_tenor = str(uuid.uuid4())
        st.session_state.sales_button_values_ind_type = None


def sales_indicatives_3(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()

    st.chat_message("user").write(st.session_state.selected_btn_ind_type)
    chat_answer = "Какой тенор Вам нужен?"
    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_btn_ind_type,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="sales",
    )

    st.session_state.sales_button_values_ind_tenor = (
        st.session_state.indicatives_df_fx_options[
            st.session_state.indicatives_df_fx_options["Type"]
            == st.session_state.selected_btn_ind_type
        ]["TENOR"]
        .unique()
        .tolist()
    )
    st.session_state.sales_start_indicatives_tenor = False


def sales_indicatives_4() -> None:
    st.session_state.selected_btn_ind_tenor = sac.buttons(
        items=st.session_state.sales_button_values_ind_tenor,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_indicatives_tenor,
    )

    if st.session_state.selected_btn_ind_tenor:

        st.session_state.button_key_indicatives_strike = str(uuid.uuid4())
        st.session_state.sales_button_values_ind_tenor = None
        st.session_state.sales_start_indicatives_strike = True


def sales_indicatives_5(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()

    st.chat_message("user").write(st.session_state.selected_btn_ind_tenor)
    chat_answer = "Какой страйк Вас интересует?"

    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_btn_ind_tenor,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="sales",
    )

    st.session_state.sales_button_values_ind_strike = (
        st.session_state.indicatives_df_fx_options[
            (
                st.session_state.indicatives_df_fx_options["Type"]
                == st.session_state.selected_btn_ind_type
            )
            & (
                st.session_state.indicatives_df_fx_options["TENOR"]
                == st.session_state.selected_btn_ind_tenor
            )
        ]["STRIKE"]
        .astype(str)
        .unique()
        .tolist()
    )
    st.session_state.sales_start_indicatives_strike = False


def sales_indicatives_6() -> None:
    st.session_state.btn_ind_strike = sac.buttons(
        items=st.session_state.sales_button_values_ind_strike,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_indicatives_strike,
    )
    if st.session_state.btn_ind_strike:

        st.session_state.sales_indicatives_finish = True
        st.session_state.sales_button_values_ind_strike = None


def sales_indicatives_7() -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()

    st.session_state.sales_indicatives_finish = False

    ind_bid = str(
        round(
            st.session_state.indicatives_df_fx_options["PREMIUM BID, RUB"][
                (
                    st.session_state.indicatives_df_fx_options["Type"]
                    == st.session_state.selected_btn_ind_type
                )
                & (
                    st.session_state.indicatives_df_fx_options["TENOR"]
                    == st.session_state.selected_btn_ind_tenor
                )
                & (
                    st.session_state.indicatives_df_fx_options["STRIKE"]
                    == float(st.session_state.btn_ind_strike)
                )
            ].item(),
            5,
        )
    )
    ind_offer = str(
        round(
            st.session_state.indicatives_df_fx_options["PREMIUM OFFER, RUB"][
                (
                    st.session_state.indicatives_df_fx_options["Type"]
                    == st.session_state.selected_btn_ind_type
                )
                & (
                    st.session_state.indicatives_df_fx_options["TENOR"]
                    == st.session_state.selected_btn_ind_tenor
                )
                & (
                    st.session_state.indicatives_df_fx_options["STRIKE"]
                    == float(st.session_state.btn_ind_strike)
                )
            ].item(),
            5,
        )
    )
    year = str(st.session_state.indicatives_file_modification_date.year)[2:]
    month = str(st.session_state.indicatives_file_modification_date.month)
    day = str(st.session_state.indicatives_file_modification_date.day)
    ind_full_response = f"""По опциону \
{st.session_state.selected_btn_ind_type} с тенором \
{st.session_state.selected_btn_ind_tenor} \
и страйком {st.session_state.btn_ind_strike}:
- PREMIUM BID, RUB = {ind_bid}
- PREMIUM OFFER, RUB = {ind_offer}

*Дата обновления – {day}.{month}.’{year}. Обращаем внимание, что котировки индикативные*"""
    st.session_state.sales_ind_full_response_d = ind_full_response


def sales_indicatives_finish(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()

    st.chat_message("user").write(st.session_state.btn_ind_strike)
    stream_md_message(st.session_state.sales_ind_full_response_d)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.btn_ind_strike,
        chat_answer=st.session_state.sales_ind_full_response_d,
        transformed_user_input="",
        chat_history_last="",
        pipeline="sales",
    )

    reset_to_zero_indicatives_pipeline()
    reset_to_zero_input_pipeline()

    chat_answer = "Заинтересовало?"
    stream_md_message(chat_answer)
    all_messages_memory.chat_memory.add_ai_message(chat_answer)

    st.session_state.sales_ind_full_response_d = None
    st.session_state.binary_button_values_3 = ["да", "нет"]
    st.session_state.button_key_sales_binary_3 = str(uuid.uuid4())


def sales_5() -> None:
    st.session_state.selected_sales_binary_answer_3 = sac.buttons(
        items=st.session_state.binary_button_values_3,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_sales_binary_3,
    )
    if st.session_state.selected_sales_binary_answer_3:
        st.session_state.binary_button_values_3 = None
        if st.session_state.selected_sales_binary_answer_3 == "да":
            st.chat_message("user").write(st.session_state.selected_sales_binary_answer_3)
            st.session_state.saled_user_input = "да"
        if st.session_state.selected_sales_binary_answer_3 == "нет":
            st.session_state.start_alternative_pipeline = True


def start_sales_pipeline(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_ALL_pipelines()
    
    st.session_state.show_potential_df_holding = False
    st.session_state.show_potential_df_stand_alone = False
    st.session_state.show_potential_df_company_from_holding = False

    fx_option_list = [
        "Валютные форварда и опционы на продажу",
        "Валютные форварда и опционы на покупку",
    ]

    button_values = st.session_state.potential_table["Наименование продукта"].unique().tolist() + [
        "не подходит"
    ]
    st.session_state.button_products_sales = [
        "Валютные форварда и опционы" if x in fx_option_list else x for x in button_values
    ]
    chat_answer = "Выбрать продукт:"
    stream_md_message(chat_answer)
    all_messages_memory.chat_memory.add_ai_message(chat_answer)
    st.session_state.button_key_sales_products = str(uuid.uuid4())


def sales_1() -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()
    reset_to_zero_indicatives_pipeline()
    st.session_state.show_potential_df_holding = False
    st.session_state.show_potential_df_stand_alone = False
    st.session_state.show_potential_df_company_from_holding = False

    st.session_state.selected_sales_product = sac.buttons(
        items=st.session_state.button_products_sales,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_sales_products,
    )

    if st.session_state.selected_sales_product:

        st.session_state.button_products_sales = None
        if st.session_state.selected_sales_product == "не подходит":
            st.chat_message("user").write(st.session_state.selected_sales_product)
            st.session_state.not_interested_user_input = st.session_state.selected_sales_product
        else:
            example_products_descriptions_dict = get_products_with_descriptions_data()
            st.session_state.product_sales_description = example_products_descriptions_dict[
                st.session_state.selected_sales_product
            ]


def sales_2(all_messages_memory: ConversationBufferMemory) -> None:
    st.chat_message("user").write(st.session_state.selected_sales_product)
    stream_md_message(st.session_state.product_sales_description)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_sales_product,
        chat_answer=st.session_state.product_sales_description,
        transformed_user_input="",
        chat_history_last="",
        pipeline="sales",
    )

    chat_answer = "Заинтересовало?"
    stream_md_message(chat_answer)
    all_messages_memory.chat_memory.add_ai_message(chat_answer)

    st.session_state.binary_button_values_1 = ["да", "нет"]
    st.session_state.button_key_sales_binary_1 = str(uuid.uuid4())
    st.session_state.product_sales_description = None


def sales_3(all_messages_memory: ConversationBufferMemory) -> None:
    st.session_state.sales_binary_btn_1 = sac.buttons(
        items=st.session_state.binary_button_values_1,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_sales_binary_1,
    )
    if st.session_state.sales_binary_btn_1:

        st.session_state.binary_button_values_1 = None
        if st.session_state.sales_binary_btn_1 == "да":
            user_answer = "да"
            st.chat_message("user").write(user_answer)
            chat_answer = "Показать индикативные котировки?"
            st.session_state.binary_button_values_2 = ["да", "нет"]
            st.session_state.button_key_sales_binary_2 = str(uuid.uuid4())
            stream_md_message(chat_answer)
            save_in_all_messages_memory_and_in_logs(
                memory=all_messages_memory,
                original_user_input=user_answer,
                chat_answer=chat_answer,
                transformed_user_input="",
                chat_history_last="",
                pipeline="sales",
            )
        if st.session_state.sales_binary_btn_1 == "нет":
            st.session_state.start_alternative_pipeline = True


def sales_4() -> None:
    st.session_state.sales_binary_btn_2 = sac.buttons(
        items=st.session_state.binary_button_values_2,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_sales_binary_2,
    )
    if st.session_state.sales_binary_btn_2:
        st.session_state.binary_button_values_2 = None
        if st.session_state.sales_binary_btn_2 == "нет":
            st.chat_message("user").write(st.session_state.sales_binary_btn_2)
            st.session_state.not_interested_user_input = "нет"
        if st.session_state.sales_binary_btn_2 == "да":
            st.session_state.start_sales_indicatives = True


def sales_alternative_start(all_messages_memory: ConversationBufferMemory) -> None:
    alternative_data = get_alternative_products_data()
    alternative_products_list = alternative_data[st.session_state.selected_sales_product]

    if alternative_products_list == []:
        st.chat_message("user").write("нет")
        st.session_state.not_interested_user_input = "нет"
    else:
        user_answer = "нет"
        st.chat_message("user").write(user_answer)
        chat_answer = "Альтернативные продукты:"
        st.session_state.button_values_alt = alternative_products_list + ["не подходит"]
        st.session_state.start_alternative_pipeline = False
        st.session_state.button_key_sales_products_alt = str(uuid.uuid4())
        stream_md_message(chat_answer)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=user_answer,
            chat_answer=chat_answer,
            transformed_user_input="",
            chat_history_last="",
            pipeline="sales",
        )


def sales_alternative_1() -> None:

    st.session_state.btn_alt = sac.buttons(
        items=st.session_state.button_values_alt,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_sales_products_alt,
    )

    if st.session_state.btn_alt:
        st.session_state.button_values_alt = None
        if st.session_state.btn_alt == "не подходит":
            st.chat_message("user").write(st.session_state.btn_alt)
            st.session_state.not_interested_user_input = "не подходит"
        else:
            example_products_descriptions_dict = get_products_with_descriptions_data()
            st.session_state.sales_alt_description = example_products_descriptions_dict[
                st.session_state.btn_alt
            ]


def sales_alternative_2(all_messages_memory: ConversationBufferMemory) -> None:
    st.chat_message("user").write(st.session_state.btn_alt)
    stream_md_message(st.session_state.sales_alt_description)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.btn_alt,
        chat_answer=st.session_state.sales_alt_description,
        transformed_user_input="",
        chat_history_last="",
        pipeline="sales",
    )

    chat_answer = "Заинтересовало?"

    st.session_state.binary_button_values_1_alt = ["да", "нет"]
    st.session_state.button_key_sales_binary_1_alt = str(uuid.uuid4())
    st.session_state.sales_alt_description = None
    stream_md_message(chat_answer)
    all_messages_memory.chat_memory.add_ai_message(chat_answer)


def sales_alternative_3(all_messages_memory: ConversationBufferMemory) -> None:
    st.session_state.binary_btn_1_alt = sac.buttons(
        items=st.session_state.binary_button_values_1_alt,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_sales_binary_1_alt,
    )
    if st.session_state.binary_btn_1_alt:
        st.session_state.binary_button_values_1_alt = None
        if st.session_state.binary_btn_1_alt == "нет":
            st.chat_message("user").write(st.session_state.binary_btn_1_alt)
            st.session_state.not_interested_user_input = "нет"

        elif st.session_state.binary_btn_1_alt == "да":
            user_answer = "да"
            st.chat_message("user").write(user_answer)
            chat_answer = "Показать индикативные котировки?"

            st.session_state.binary_button_values_2_alt = ["да", "нет"]
            st.session_state.button_key_sales_binary_2_alt = str(uuid.uuid4())
            stream_md_message(chat_answer)
            save_in_all_messages_memory_and_in_logs(
                memory=all_messages_memory,
                original_user_input=user_answer,
                chat_answer=chat_answer,
                transformed_user_input="",
                chat_history_last="",
                pipeline="sales",
            )


def sales_alternative_4() -> None:
    st.session_state.binary_btn_2_alt = sac.buttons(
        items=st.session_state.binary_button_values_2_alt,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_sales_binary_2_alt,
    )

    if st.session_state.binary_btn_2_alt:
        st.session_state.binary_button_values_2_alt = None
        if st.session_state.binary_btn_2_alt == "нет":
            st.chat_message("user").write(st.session_state.binary_btn_2_alt)
            st.session_state.not_interested_user_input = "нет"
        elif st.session_state.binary_btn_2_alt == "да":
            st.session_state.indicatives_pipeline_alt = True


def sales_indicatives_1_alt(
    product: str,
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()


    st.chat_message("user").write(product)

    if product == "Валютные форварда и опционы":
        st.session_state.indicatives_df_fx_options = pd.read_excel(
            settings.external_data_path / "indicatives/FXOPTIONS_Ind.xlsx"
        )
        modification_time = os.path.getmtime(
            settings.external_data_path / "indicatives/FXOPTIONS_Ind.xlsx"
        )
        st.session_state.indicatives_file_modification_date = datetime.fromtimestamp(
            modification_time
        )
        st.session_state.indicatives_df_fx_options["STRIKE"] = (
            st.session_state.indicatives_df_fx_options["STRIKE"].apply(lambda x: round(x, 4))
        )
        chat_answer = "Вам нужен опцион CALL или PUT?"

        st.session_state.sales_alt_button_values_ind_type = ["CALL", "PUT"]
        st.session_state.button_key_indicatives_type = str(uuid.uuid4())
        stream_md_message(chat_answer)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=product,
            chat_answer=chat_answer,
            transformed_user_input="",
            chat_history_last="",
            pipeline="sales",
        )
    else:
        st.session_state.sales_alt_ind_full_response_d = "К сожалению, индикативные котировки по \
данному продукту в процессе подключения. Я могу вам помочь чем-либо ещё?"
        stream_md_message(st.session_state.sales_alt_ind_full_response_d)
        save_in_all_messages_memory_and_in_logs(
            memory=all_messages_memory,
            original_user_input=product,
            chat_answer=st.session_state.sales_alt_ind_full_response_d,
            transformed_user_input="",
            chat_history_last="",
            pipeline="sales",
        )
        last_messages_memory.chat_memory.add_ai_message(
            st.session_state.sales_alt_ind_full_response_d
        )

        reset_to_zero_input_pipeline()
        reset_to_zero_products_pipeline()
        reset_to_zero_fx_pipeline()
        reset_to_zero_pers_pipeline()
        reset_to_zero_cmdt_pipeline()
        reset_to_zero_indicatives_pipeline()
        reset_to_zero_potential_pipeline()
        reset_to_zero_sales_pipeline()
    st.session_state.indicatives_pipeline_alt = False


def sales_indicatives_2_alt() -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()

    st.session_state.selected_btn_ind_type = sac.buttons(
        items=st.session_state.sales_alt_button_values_ind_type,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_indicatives_type,
    )
    if st.session_state.selected_btn_ind_type:

        st.session_state.sales_alt_start_indicatives_tenor = True
        st.session_state.button_key_indicatives_tenor = str(uuid.uuid4())
        st.session_state.sales_alt_button_values_ind_type = None


def sales_indicatives_3_alt(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()

    st.chat_message("user").write(st.session_state.selected_btn_ind_type)
    chat_answer = "Какой тенор Вам нужен?"
    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_btn_ind_type,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="sales",
    )

    st.session_state.sales_alt_button_values_ind_tenor = (
        st.session_state.indicatives_df_fx_options[
            st.session_state.indicatives_df_fx_options["Type"]
            == st.session_state.selected_btn_ind_type
        ]["TENOR"]
        .unique()
        .tolist()
    )
    st.session_state.sales_alt_start_indicatives_tenor = False


def sales_indicatives_4_alt() -> None:
    st.session_state.selected_btn_ind_tenor = sac.buttons(
        items=st.session_state.sales_alt_button_values_ind_tenor,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_indicatives_tenor,
    )

    if st.session_state.selected_btn_ind_tenor:

        st.session_state.button_key_indicatives_strike = str(uuid.uuid4())
        st.session_state.sales_alt_button_values_ind_tenor = None
        st.session_state.sales_alt_start_indicatives_strike = True


def sales_indicatives_5_alt(all_messages_memory: ConversationBufferMemory) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()

    st.chat_message("user").write(st.session_state.selected_btn_ind_tenor)
    chat_answer = "Какой страйк Вас интересует?"

    stream_md_message(chat_answer)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.selected_btn_ind_tenor,
        chat_answer=chat_answer,
        transformed_user_input="",
        chat_history_last="",
        pipeline="sales",
    )

    st.session_state.sales_alt_button_values_ind_strike = (
        st.session_state.indicatives_df_fx_options[
            (
                st.session_state.indicatives_df_fx_options["Type"]
                == st.session_state.selected_btn_ind_type
            )
            & (
                st.session_state.indicatives_df_fx_options["TENOR"]
                == st.session_state.selected_btn_ind_tenor
            )
        ]["STRIKE"]
        .astype(str)
        .unique()
        .tolist()
    )
    st.session_state.sales_alt_start_indicatives_strike = False


def sales_indicatives_6_alt() -> None:
    st.session_state.btn_ind_strike = sac.buttons(
        items=st.session_state.sales_alt_button_values_ind_strike,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_indicatives_strike,
    )
    if st.session_state.btn_ind_strike:

        st.session_state.sales_alt_indicatives_finish = True
        st.session_state.sales_alt_button_values_ind_strike = None


def sales_indicatives_7_alt() -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()

    st.session_state.sales_alt_indicatives_finish = False

    ind_bid = str(
        round(
            st.session_state.indicatives_df_fx_options["PREMIUM BID, RUB"][
                (
                    st.session_state.indicatives_df_fx_options["Type"]
                    == st.session_state.selected_btn_ind_type
                )
                & (
                    st.session_state.indicatives_df_fx_options["TENOR"]
                    == st.session_state.selected_btn_ind_tenor
                )
                & (
                    st.session_state.indicatives_df_fx_options["STRIKE"]
                    == float(st.session_state.btn_ind_strike)
                )
            ].item(),
            5,
        )
    )
    ind_offer = str(
        round(
            st.session_state.indicatives_df_fx_options["PREMIUM OFFER, RUB"][
                (
                    st.session_state.indicatives_df_fx_options["Type"]
                    == st.session_state.selected_btn_ind_type
                )
                & (
                    st.session_state.indicatives_df_fx_options["TENOR"]
                    == st.session_state.selected_btn_ind_tenor
                )
                & (
                    st.session_state.indicatives_df_fx_options["STRIKE"]
                    == float(st.session_state.btn_ind_strike)
                )
            ].item(),
            5,
        )
    )
    year = str(st.session_state.indicatives_file_modification_date.year)[2:]
    month = str(st.session_state.indicatives_file_modification_date.month)
    day = str(st.session_state.indicatives_file_modification_date.day)
    ind_full_response = f"""По опциону \
{st.session_state.selected_btn_ind_type} с тенором \
{st.session_state.selected_btn_ind_tenor} \
и страйком {st.session_state.btn_ind_strike}:
- PREMIUM BID, RUB = {ind_bid}
- PREMIUM OFFER, RUB = {ind_offer}

*Дата обновления – {day}.{month}.’{year}. Обращаем внимание, что котировки индикативные*"""
    st.session_state.sales_alt_ind_full_response_d = ind_full_response


def sales_indicatives_8_alt(
    all_messages_memory: ConversationBufferMemory,
    last_messages_memory: ConversationBufferWindowMemory,
) -> None:
    reset_to_zero_products_pipeline()
    reset_to_zero_fx_pipeline()
    reset_to_zero_pers_pipeline()
    reset_to_zero_cmdt_pipeline()

    st.chat_message("user").write(st.session_state.btn_ind_strike)
    stream_md_message(st.session_state.sales_alt_ind_full_response_d)
    save_in_all_messages_memory_and_in_logs(
        memory=all_messages_memory,
        original_user_input=st.session_state.btn_ind_strike,
        chat_answer=st.session_state.sales_alt_ind_full_response_d,
        transformed_user_input="",
        chat_history_last="",
        pipeline="sales",
    )
    last_messages_memory.chat_memory.add_ai_message(st.session_state.sales_alt_ind_full_response_d)
    reset_to_zero_indicatives_pipeline()
    reset_to_zero_input_pipeline()

    chat_answer = "Заинтересовало?"
    stream_md_message(chat_answer)
    all_messages_memory.chat_memory.add_ai_message(chat_answer)

    st.session_state.sales_alt_ind_full_response_d = None
    st.session_state.binary_button_values_3_alt = ["да", "нет"]
    st.session_state.button_key_sales_binary_3 = str(uuid.uuid4())


def sales_indicatives_finish_alt() -> None:
    st.session_state.selected_sales_binary_answer_3 = sac.buttons(
        items=st.session_state.binary_button_values_3_alt,
        index=None,
        align="center",
        direction="horizontal",
        radius="lg",
        return_index=False,
        key=st.session_state.button_key_sales_binary_3,
    )
    if st.session_state.selected_sales_binary_answer_3:
        st.session_state.binary_button_values_3_alt = None
        if st.session_state.selected_sales_binary_answer_3 == "да":
            st.chat_message("user").write(st.session_state.selected_sales_binary_answer_3)
            st.session_state.saled_user_input = "да"
        if st.session_state.selected_sales_binary_answer_3 == "нет":
            st.chat_message("user").write(st.session_state.selected_sales_binary_answer_3)
            st.session_state.not_interested_user_input = "нет"
