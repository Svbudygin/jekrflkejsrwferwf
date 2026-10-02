import hashlib
import json
import logging
import os
import uuid
import requests
import streamlit as st

st.set_page_config(
    page_title="Synaptica",
    page_icon="🦜️️🛠️",
)

import streamlit_authenticator as stauth
import yaml
from langchain.memory import ConversationBufferMemory, ConversationBufferWindowMemory
from langchain_community.chat_message_histories.streamlit import StreamlitChatMessageHistory
from langchain.retrievers import MultiVectorRetriever
from streamlit_authenticator.utilities.exceptions import LoginError
from streamlit_authenticator.utilities.hasher import Hasher
from yaml.loader import SafeLoader


from synaptica.backend.streamlit_functions.st_functions import (
    answer_if_error_pipeline,
    answer_if_error_pipeline_saving_only_llm_answer,
    cmdt_finish,
    cmdt_get_fx_from_list,
    cmdt_get_list_period_and_chat,
    cmdt_get_list_values_and_chat,
    cmdt_get_period_from_list,
    cmdt_start_period_pipeline,
    fx_finish,
    fx_get_fx_from_list,
    fx_get_list_period_and_chat,
    fx_get_list_values_and_chat,
    fx_get_period_from_list,
    fx_start_period_pipeline,
    get_indicative_from_list,
    indicatives_1,
    indicatives_2,
    indicatives_3,
    indicatives_4,
    indicatives_5,
    indicatives_6,
    indicatives_7,
    indicatives_finish,
    not_interested_pipeline,
    potential_company_from_holding,
    potential_holding,
    potential_stand_alone,
    products_finish,
    products_get_list_and_chat,
    products_get_product_from_list,
    saled_function,
    sales_1,
    sales_2,
    sales_3,
    sales_4,
    sales_5,
    sales_alternative_1,
    sales_alternative_2,
    sales_alternative_3,
    sales_alternative_4,
    sales_alternative_start,
    sales_indicatives_1,
    sales_indicatives_1_alt,
    sales_indicatives_2,
    sales_indicatives_2_alt,
    sales_indicatives_3,
    sales_indicatives_3_alt,
    sales_indicatives_4,
    sales_indicatives_4_alt,
    sales_indicatives_5,
    sales_indicatives_5_alt,
    sales_indicatives_6,
    sales_indicatives_6_alt,
    sales_indicatives_7,
    sales_indicatives_7_alt,
    sales_indicatives_8_alt,
    sales_indicatives_finish,
    sales_indicatives_finish_alt,
    save_feedback,
    select_company_from_first_lists,
    select_company_from_second_list,
    show_cmdt_period,
    show_fx_period,
    start_market_pipeline,
    market_get_list_values_and_chat,
    market_get_instrument_from_list,
    start_indicatives_pipeline_with_product,
    start_products_pipeline,
    write_eco,
    write_faq,
    render_chat_input_with_rates_attach,
    write_hello,
    write_rates_help,
    write_llm_answer_from_text,
    write_news,
    write_calls,
    write_pers,
    start_pers_pipeline,
    pers_get_list_values_and_chat,
    pers_get_pers_from_list,
    show_pers_period,
    pers_start_period_pipeline,
    pers_get_list_period_and_chat,
    # pers_get_period_from_list,
    # pers_finish,
    write_personification,
    start_vnd_pipeline,
    exit_button_for_commands,
    write_notes_button,
    write_broker_button,
    write_call_table_sidebar_button,
    flush_call_table_detail_if_pending,
    write_openclaw_coder_start,
    write_openclaw_helper_start,
    write_openclaw_corp_sales_start,
    write_openclaw_corp_sales_answer,
    write_openclaw_calls_start,
    write_openclaw_vnd_rag_start,
    write_openclaw_pres_gen_start,
    write_openclaw_rates_pricing_start,
    write_openclaw_products_start,
    write_presentation_generate_button,
    write_logs_view_button,
    write_call_upload_button,
    write_optimizer_button,
    render_optimizer_panel,
    process_special_news_panel_action,
    process_openclaw_client_pending_upload,
    process_call_upload_pending_upload,
    process_synaptica_pending_attach,
    try_handle_synaptica_attach_reply,
    render_corp_notes_interactive_dataframe,
    render_notes_records_panel,
    notes_list_anchor_message_idx,
)
from synaptica.backend.streamlit_functions.call_classes import (
    CALL_TABLE_DETAIL_USER_PREFIX,
    CALL_TABLE_DF_SELECTION_WIDGET_KEY,
    CALL_TABLE_INTERACTIVE_CTX_KEY,
    CALL_TABLE_PENDING_DETAIL_KEY,
    render_call_table_interactive_dataframe,
)
from synaptica.backend.streamlit_functions.st_initializing_functions import (
    initialize_cmdt_pipeline,
    initialize_market_pipeline,
    initialize_fx_pipeline,
    initialize_indicatives_pipeline,
    initialize_input_pipeline,
    initialize_potential_pipeline,
    initialize_products_pipeline,
    initialize_sales_pipeline,
    initialize_pers_pipeline,
    initialize_vnd_pipeline,
    initialize_openclaw_pipeline,
    initialize_presentation_gen_pipeline,
    initialize_logs_view_pipeline,
    initialize_call_upload_pipeline,
    initialize_special_news_pipeline,
)
from synaptica.backend.streamlit_functions.st_reseting_functions import (
    reset_to_zero_cmdt_pipeline,
    reset_to_zero_fx_pipeline,
    reset_to_zero_indicatives_pipeline,
    reset_to_zero_input_pipeline,
    reset_to_zero_potential_pipeline,
    reset_to_zero_products_pipeline,
    reset_to_zero_sales_pipeline,
)
from synaptica.backend.analytics_rag import (
    AnalyticsRagQueryProcessor,
    DatabaseConnector,
)
from synaptica.backend.streamlit_functions.calls_access import (
    is_calls_allowed_user,
    is_openclaw_allowed_user,
    is_openclaw_client_allowed_user,
    is_openclaw_coder_allowed_user,
    is_openclaw_helper_allowed_user,
    is_notes_allowed_user,
    is_broker_allowed_user,
    is_openclaw_corp_sales_allowed_user,
    is_openclaw_vnd_rag_allowed_user,
    is_openclaw_pres_gen_allowed_user,
    is_openclaw_rates_pricing_allowed_user,
    is_openclaw_products_allowed_user,
    is_openclaw_calls_allowed_user,
    is_presentation_gen_allowed_user,
    is_logs_view_allowed_user,
    is_call_upload_allowed_user,
    is_special_news_allowed_user,
    is_optimizer_allowed_user,
    resolve_calls_bank,
)
from synaptica.backend.utility_functions_giga import (
    choose_case_resolving_context,
    paraphrase_bot_message,
)
from synaptica.config.settings import DefaultSettings
from synaptica.dashboard.functions import build_data_traffic_light_df, report_stale_sources
from synaptica.data.get_data import (
    load_data,
    prepare_documents,
    prepare_multivector_retriever,
    preprocess_data,
    save_documents,
    is_rag_rebuild,
)
from synaptica.data.get_analytics_rag_data import (
    prepare_analytics_multivector_retriever,
    prepare_dashboard_rag_multivector_retriever,
)
from synaptica.utils.helpers import (
    add_custom_css,
    is_streamlit_script_control_exception,
    seed_all,
)
from synaptica.utils.templates import DOCUMENT_TEMPLATE


LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
logger = logging.getLogger("synaptica_app")

# Глушим шумные сторонние логгеры: GigaChat печатает целиком request/response
# на уровне WARNING, плюс "unknown kwargs" и HTTP-запросы httpx.
for _noisy in ("langchain_community.chat_models.gigachat", "gigachat"):
    logging.getLogger(_noisy).setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)

seed_all(42)

settings = DefaultSettings()


@st.cache_data(ttl=3600, show_spinner=False)
def _check_data_traffic_light(external_root: str) -> None:
    """Светофор данных: при «красном» пишет pending-событие в лог (письмо шлёт поллер).

    Кэш на 10 минут — чтобы не дёргать файлы на каждый rerun; дедуп «раз в сутки
    на источник» внутри report_stale_sources не даёт задвоить письма (даже если
    сигнал придёт и отсюда, и с дашборда).
    """
    try:
        report_stale_sources(build_data_traffic_light_df(external_root))
    except Exception:
        pass


# RAG_REBUILD=1 — пересоздать индексы products/analytics/dashboard (медленно, embeddings).
RAG_REBUILD = is_rag_rebuild()

@st.cache_resource
def load_analytics_rag_retriever(rebuild: bool = False) -> MultiVectorRetriever:
    retriever = prepare_analytics_multivector_retriever(
        model=settings.model, settings=settings, rebuild=rebuild
    )
    return retriever


@st.cache_resource
def load_dash_rag_retriever(rebuild: bool = False) -> MultiVectorRetriever:
    retriever = prepare_dashboard_rag_multivector_retriever(
        model=settings.model, settings=settings, rebuild=rebuild
    )
    return retriever

@st.cache_resource
def load_retriever(rebuild: bool = False) -> MultiVectorRetriever:
    logger.info("retriever_load_start rebuild=%s", rebuild)
    documents_df = None
    if rebuild:
        raw_df = load_data(settings)
        df = preprocess_data(raw_df)
        documents_df = prepare_documents(df, DOCUMENT_TEMPLATE)
        save_documents(documents_df, settings)
    return prepare_multivector_retriever(
        documents_df,
        model=settings.model,
        settings=settings,
        rebuild=rebuild,
        document_template=DOCUMENT_TEMPLATE,
    )

@st.cache_resource
def load_database_and_rag(rebuild: bool = False) -> DatabaseConnector:
    retriever, retriever_client = prepare_analytics_multivector_retriever(
        model=settings.model, settings=settings, rebuild=rebuild
    )

    db = DatabaseConnector()
    qp = AnalyticsRagQueryProcessor(
        giga_client=settings.model,
        retriever=retriever_client,
        database_connector=db,
    )

    return retriever, retriever_client, db, qp

@st.cache_resource
def load_dash_rag(rebuild: bool = False) -> DatabaseConnector:
    dash_retriever, dash_retriever_client = prepare_dashboard_rag_multivector_retriever(
        model=settings.model, settings=settings, rebuild=rebuild
    )
    return dash_retriever, dash_retriever_client

# Локально без серт./OpenSearch: SYNAPTICA_SKIP_RAG=1 (см. run_local.sh)
_SKIP_RAG = os.environ.get("SYNAPTICA_SKIP_RAG", "").strip() in {"1", "true", "yes"}

if "analytics_retriever" not in st.session_state:
    if _SKIP_RAG:
        st.session_state["analytics_retriever"] = None
        st.session_state["analytics_retriever_client"] = None
        st.session_state["analytics_db_connector"] = None
        st.session_state["analytics_rag_processor"] = None
    else:
        (
            st.session_state["analytics_retriever"],
            st.session_state["analytics_retriever_client"],
            st.session_state["analytics_db_connector"],
            st.session_state["analytics_rag_processor"],
        ) = load_database_and_rag(RAG_REBUILD)

if "dash_retriever" not in st.session_state:
    st.session_state["giga_client"] = settings.model
    if _SKIP_RAG:
        st.session_state["dash_retriever"] = None
        st.session_state["dash_retriever_client"] = None
    else:
        st.session_state["dash_retriever"], st.session_state["dash_retriever_client"] = load_dash_rag(
            RAG_REBUILD
        )

if "retriever" not in st.session_state:
    st.session_state["retriever"] = None if _SKIP_RAG else load_retriever(RAG_REBUILD)


import threading
import time
import random
from datetime import datetime, timezone, timedelta
from queue import Queue
message_queue = Queue()

# --- Сброс пароля: событийный лог JSONL (только email, id, code_hash, expires_at, status) ---
RESET_LOG_PATH = os.path.join(os.path.dirname(settings.config_path), "reset_pas.jsonl")
_RESET_LOG_DIR = os.path.dirname(RESET_LOG_PATH)
RESET_RATE_LIMIT_COUNT = 3
RESET_RATE_LIMIT_MINUTES = 10
RESET_CODE_VALID_MINUTES = 15


def _reset_iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _reset_append_event(obj: dict) -> None:
    os.makedirs(_RESET_LOG_DIR, exist_ok=True)
    with open(RESET_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _reset_code_hash(code: str) -> str:
    return hashlib.sha256(code.strip().encode("utf-8")).hexdigest()


def write_reset_request(email: str) -> tuple[bool, str]:
    """
    Создаёт заявку на сброс: дописывает событие {id, email, created_at, status: "pending"}.
    Rate limit по email. Возвращает (успех, сообщение).
    """
    email = (email or "").strip().lower()
    if "@" not in email:
        return False, "Введите корректный email."
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=RESET_RATE_LIMIT_MINUTES)
    recent_count = 0
    try:
        if os.path.exists(RESET_LOG_PATH):
            with open(RESET_LOG_PATH, "r", encoding="utf-8") as f:
                lines = f.readlines()
            for line in lines[-500:]:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if rec.get("status") != "pending" or "email" not in rec:
                        continue
                    ts_str = rec.get("created_at") or ""
                    try:
                        rec_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    except Exception:
                        continue
                    if rec_ts < cutoff:
                        continue
                    if (rec.get("email") or "").strip().lower() == email:
                        recent_count += 1
                except Exception:
                    continue
    except Exception:
        pass
    if recent_count >= RESET_RATE_LIMIT_COUNT:
        return False, "Слишком много запросов. Попробуйте позже (не более 3 раз в 10 минут)."
    rid = str(uuid.uuid4())
    try:
        _reset_append_event({
            "id": rid,
            "email": email,
            "created_at": _reset_iso_now(),
            "status": "pending",
        })
    except Exception:
        return False, "Временная ошибка. Попробуйте позже."
    return True, "Если такой пользователь существует, инструкция будет отправлена на указанную почту."


def verify_reset_code(email: str, code: str) -> tuple[bool, str]:
    """
    Проверяет введённый код: ищет по email последнюю заявку со status=code_set,
    сравнивает hash(code) с code_hash, проверяет expires_at. При успехе дописывает status=verified.
    Возвращает (успех, сообщение).
    """
    email = (email or "").strip().lower()
    code = (code or "").strip()
    if not email or "@" not in email:
        return False, "Введите корректный email."
    if not code:
        return False, "Введите код из письма."
    if not os.path.exists(RESET_LOG_PATH):
        return False, "Заявка не найдена или код ещё не выдан."
    # Собираем события по id: pending (email, created_at), code_set (code_hash, expires_at), verified
    pending_by_id = {}
    code_set_by_id = {}
    verified_ids = set()
    try:
        with open(RESET_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    rid = rec.get("id")
                    if not rid:
                        continue
                    status = rec.get("status")
                    if status == "pending" and "email" in rec:
                        pending_by_id[rid] = {"email": (rec.get("email") or "").strip().lower(), "created_at": rec.get("created_at")}
                    elif status == "code_set":
                        code_set_by_id[rid] = {"code_hash": rec.get("code_hash"), "expires_at": rec.get("expires_at")}
                    elif status == "verified":
                        verified_ids.add(rid)
                except Exception:
                    continue
    except Exception:
        return False, "Временная ошибка. Попробуйте позже."
    # Найти id для этого email: последний с code_set и без verified
    candidate_ids = [rid for rid, p in pending_by_id.items() if p["email"] == email]
    best_rid = None
    best_expires = None
    best_hash = None
    for rid in candidate_ids:
        if rid in verified_ids:
            continue
        cs = code_set_by_id.get(rid)
        if not cs or not cs.get("code_hash") or not cs.get("expires_at"):
            continue
        best_rid = rid
        best_expires = cs["expires_at"]
        best_hash = cs["code_hash"]
    if not best_rid:
        return False, "Заявка не найдена или код ещё не выдан. Дождитесь письма с кодом."
    try:
        expires_dt = datetime.fromisoformat((best_expires or "").replace("Z", "+00:00"))
    except Exception:
        return False, "Ошибка срока действия кода."
    if datetime.now(timezone.utc) > expires_dt:
        return False, "Срок действия кода истёк. Запросите сброс повторно."
    if _reset_code_hash(code) != best_hash:
        return False, "Неверный код. Проверьте и введите снова."
    try:
        _reset_append_event({
            "id": best_rid,
            "verified_at": _reset_iso_now(),
            "status": "verified",
        })
    except Exception:
        return False, "Временная ошибка. Попробуйте позже."
    return True, "Код подтверждён. Обратитесь к администратору для смены пароля или используйте восстановление доступа."

if "messages" not in st.session_state:
    st.session_state.messages = []
if "thread_started" not in st.session_state:
    st.session_state.thread_started = False

if "out" not in st.session_state:
    st.session_state.out = None

with open(settings.config_path, "r", encoding="utf-8") as file:
    config = yaml.load(file, Loader=SafeLoader)


def _persist_credentials_if_passwords_hashed(cfg: dict, path: str) -> None:
    creds = cfg.get("credentials")
    if not isinstance(creds, dict):
        return
    usernames = creds.get("usernames")
    if not isinstance(usernames, dict) or not usernames:
        return
    before = {u: usernames[u].get("password") for u in usernames}
    Hasher.hash_passwords(creds)
    after = {u: usernames[u].get("password") for u in usernames}
    if before != after:
        with open(path, "w", encoding="utf-8") as file:
            yaml.dump(cfg, file, default_flow_style=False)


_persist_credentials_if_passwords_hashed(config, settings.config_path)

# Creating the authenticator object
authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
    auto_hash=False,
    # config["pre-authorized"],
)

# Плашка поддержки на экранах логина, регистрации и смены пароля
def render_auth_support_notice():
    st.markdown("---")
    st.info("💬 **При затруднениях** просьба обратиться в поддержку SYNAPTICA_SUPPORT@sberbank.ru")

# Экраны входа: login | register | forgot
if "auth_screen" not in st.session_state:
    st.session_state.auth_screen = "login"

name, authentication_status, username = None, False, None

# Если уже авторизован — сбрасываем экран входа и не показываем формы логина/регистрации/сброса
if st.session_state.get("authentication_status"):
    st.session_state.auth_screen = "login"
else:
    # Показываем только один экран входа
    if st.session_state.auth_screen == "login":
        try:
            with st.spinner("Подождите, выполняем вход…"):
                authenticator.login()
        except LoginError as e:
            logger.exception("streamlit_step_failed")

        authentication_status = st.session_state.get("authentication_status")
        name = st.session_state.get("name")
        username = st.session_state.get("username")

        if authentication_status is False:
            st.error("Не удалось войти: проверьте **логин** и **пароль** и попробуйте ещё раз. Если вы вводите почту, то попробуйте без @sberbank.ru")

        if not st.session_state.get("authentication_status"):
            st.markdown("---")
            col1, col2, col3 = st.columns([1,1,1])
            with col1:
                if st.button("Регистрация", key="btn_register"):
                    st.session_state.auth_screen = "register"
                    st.rerun()
            with col3:
                if st.button("Забыли логин или пароль?", key="btn_forgot"):
                    st.session_state.auth_screen = "forgot"
                    st.rerun()
            render_auth_support_notice()

    elif st.session_state.auth_screen == "register":
        # Простая регистрация: почта + два раза пароль, логин = часть до @
        st.markdown("### Регистрация")
        if st.button("← Назад к входу", key="register_back"):
            st.session_state.auth_screen = "login"
            st.rerun()

        reg_email = st.text_input("Почта", key="reg_email", placeholder="user@sberbank.ru")
        reg_pasword = st.text_input("Пароль", key="reg_pasword", type="password", placeholder="Пароль")
        reg_pasword_confirm = st.text_input("Повторите пароль", key="reg_pasword_confirm", type="password", placeholder="Повторите пароль")

        if st.button("Зарегистрироваться", key="reg_submit", type="primary"):
            email = (reg_email or "").strip().lower()
            if not email or "@" not in email:
                st.error("Введите корректную почту.")
            elif email.split("@")[-1] not in ("sberbank.ru", "sber.ru"):
                st.error("Используйте корпоративную почту @sberbank.ru или @sber.ru")
            elif not reg_pasword or not reg_pasword_confirm:
                st.error("Заполните оба поля пароля.")
            elif reg_pasword != reg_pasword_confirm:
                st.error("Пароли не совпадают.")
            elif len(reg_pasword) < 6:
                st.error("Пароль должен быть не менее 6 символов.")
            else:
                login_username = email.split("@")[0]
                if "credentials" not in config or "usernames" not in config["credentials"]:
                    config.setdefault("credentials", {})["usernames"] = {}
                if login_username in config["credentials"]["usernames"]:
                    st.error("Пользователь с такой почтой уже зарегистрирован. Используйте «Забыли пароль?» или другой email.")
                else:
                    try:
                        hashed = Hasher.hash(reg_pasword)
                        config["credentials"]["usernames"][login_username] = {
                            "email": email,
                            "name": login_username,
                            "password": hashed,
                        }
                        with open(settings.config_path, "w", encoding="utf-8") as file:
                            yaml.dump(config, file, default_flow_style=False)
                        
                        # Автоматическая авторизация после регистрации
                        st.session_state["authentication_status"] = True
                        st.session_state["name"] = login_username
                        st.session_state["username"] = login_username
                        st.session_state.auth_screen = "login"
                        
                        # Cookie для сохранения авторизации
                        try:
                            with open(settings.config_path, "r", encoding="utf-8") as file:
                                config_updated = yaml.load(file, Loader=SafeLoader)
                            authenticator_updated = stauth.Authenticate(
                                config_updated["credentials"],
                                config_updated["cookie"]["name"],
                                config_updated["cookie"]["key"],
                                config_updated["cookie"]["expiry_days"],
                                auto_hash=False,
                            )
                            if hasattr(authenticator_updated, "cookie_manager") and authenticator_updated.cookie_manager:
                                import hmac
                                import hashlib
                                from datetime import datetime, timedelta
                                cookie_key = config_updated["cookie"]["key"]
                                token_data = f"{login_username}:{login_username}"
                                token = hmac.new(cookie_key.encode(), token_data.encode(), hashlib.sha256).hexdigest()
                                expiry = datetime.now() + timedelta(days=config_updated["cookie"]["expiry_days"])
                                authenticator_updated.cookie_manager.set(
                                    authenticator_updated.cookie_name,
                                    token,
                                    expires_at=expiry,
                                )
                        except Exception:
                            pass
                        
                        # Сразу перезапуск — пользователь попадёт в приложение без повторного ввода пароля
                        st.session_state["just_registered"] = True
                        st.rerun()
                    except Exception as e:
                        st.error(f"Ошибка: {e}")
        render_auth_support_notice()

    elif st.session_state.auth_screen == "forgot":
        if st.button("← Назад к входу", key="forgot_back"):
            st.session_state.auth_screen = "login"
            st.session_state.reset_step = 1
            st.session_state.reset_email = ""
            st.rerun()

        # Форма «Забыли пароль?» — единая форма с шагами
        st.markdown("---")
        
        # Инициализация состояния формы сброса
        if "reset_step" not in st.session_state:
            st.session_state.reset_step = 1  # 1=запрос кода, 2=ввод кода, 3=смена пароля
        if "reset_email" not in st.session_state:
            st.session_state.reset_email = ""
        
        st.markdown("### 🔑 Забыли логин или пароль?")
        
        # Индикатор шагов
        step_names = ["1️⃣ Запрос кода", "2️⃣ Ввод кода", "3️⃣ Новый пароль"]
        cols = st.columns(3)
        for i, (col, step_name) in enumerate(zip(cols, step_names), 1):
            with col:
                if i == st.session_state.reset_step:
                    st.markdown(f"**{step_name}**")
                elif i < st.session_state.reset_step:
                    st.markdown(f"✅ {step_name}")
                else:
                    st.markdown(f"⏳ {step_name}")
        st.markdown("---")
        
        # Шаг 1: Запрос кода
        if st.session_state.reset_step == 1:
            with st.container():
                st.caption("Введите вашу корпоративную почту от Сигмы. На неё придёт код от администратора.")
                reset_email = st.text_input("Email (Сигма)", key="reset_pasword_email", placeholder="user@sberbank.ru", value=st.session_state.reset_email)
                col1, col2 = st.columns([1, 4])
                with col1:
                    if st.button("Отправить код", key="reset_pasword_btn", type="primary"):
                        if not (reset_email and reset_email.strip()):
                            st.error("Введите email.")
                        else:
                            ok, msg = write_reset_request(reset_email.strip().lower())
                            if ok:
                                st.session_state.reset_email = reset_email.strip().lower()
                                st.session_state.reset_step = 2
                                st.success(msg)
                                st.rerun()
                            else:
                                st.error(msg)
        
        # Шаг 2: Ввод кода
        elif st.session_state.reset_step == 2:
            with st.container():
                st.caption(f"Код отправлен на {st.session_state.reset_email}. Введите код из письма.")
                verify_code = st.text_input("Код из письма", key="reset_verify_code", type="password", placeholder="Введите код")
                col1, col2, col3 = st.columns([1, 1, 3])
                with col1:
                    if st.button("Подтвердить код", key="reset_verify_btn", type="primary"):
                        if not verify_code:
                            st.error("Введите код.")
                        else:
                            ok, msg = verify_reset_code(st.session_state.reset_email, verify_code)
                            if ok:
                                st.session_state.reset_step = 3
                                st.success("Код подтверждён!")
                                st.rerun()
                            else:
                                st.error(msg)
                with col2:
                    if st.button("Назад", key="reset_back_to_step1"):
                        st.session_state.reset_step = 1
                        st.rerun()
        
        # Шаг 3: Смена пароля
        elif st.session_state.reset_step == 3:
            with st.container():
                st.markdown(f"### Создайте новый пароль для пользователя {st.session_state.reset_email}")
                st.caption("Введите новый пароль дважды для подтверждения.")
                
                # Найти username по email из config
                username_for_reset = None
                if "credentials" in config:
                    for uname, creds in config["credentials"].get("usernames", {}).items():
                        if isinstance(creds, dict) and creds.get("email", "").strip().lower() == st.session_state.reset_email:
                            username_for_reset = uname
                            break
                
                if not username_for_reset:
                    st.error(f"Пользователь с email {st.session_state.reset_email} не найден в системе.")
                    if st.button("Начать заново", key="reset_start_over"):
                        st.session_state.reset_step = 1
                        st.session_state.reset_email = ""
                        st.rerun()
                else:
                    new_pasword = st.text_input("Новый пароль", key="reset_new_pasword", type="password", placeholder="Введите новый пароль")
                    new_pasword_confirm = st.text_input("Подтвердите пароль", key="reset_new_pasword_confirm", type="password", placeholder="Повторите пароль")
                    
                    col1, col2, col3 = st.columns([1, 1, 3])
                    with col1:
                        if st.button("Сохранить пароль", key="reset_save_pasword", type="primary"):
                            if not new_pasword or not new_pasword_confirm:
                                st.error("Заполните оба поля.")
                            elif new_pasword != new_pasword_confirm:
                                st.error("Пароли не совпадают.")
                            elif len(new_pasword) < 6:
                                st.error("Пароль должен быть не менее 6 символов.")
                            else:
                                try:
                                    # Хеширование нового пароля
                                    hashed_pasword = Hasher.hash(new_pasword)
                                    # Обновление пароля в config
                                    if "credentials" in config and "usernames" in config["credentials"]:
                                        if username_for_reset in config["credentials"]["usernames"]:
                                            config["credentials"]["usernames"][username_for_reset]["password"] = hashed_pasword
                                            # Сохранение config
                                            with open(settings.config_path, "w", encoding="utf-8") as file:
                                                yaml.dump(config, file, default_flow_style=False)
                                            
                                            # Автоматическая авторизация после смены пароля
                                            user_info = config["credentials"]["usernames"][username_for_reset]
                                            
                                            # Устанавливаем session_state для авторизации
                                            st.session_state["authentication_status"] = True
                                            st.session_state["name"] = user_info.get("name", username_for_reset)
                                            st.session_state["username"] = username_for_reset
                                            
                                            # Создаём cookie для сохранения авторизации через authenticator
                                            try:
                                                # Перезагружаем config для создания нового authenticator
                                                with open(settings.config_path, "r", encoding="utf-8") as file:
                                                    config_updated = yaml.load(file, Loader=SafeLoader)
                                                authenticator_updated = stauth.Authenticate(
                                                    config_updated["credentials"],
                                                    config_updated["cookie"]["name"],
                                                    config_updated["cookie"]["key"],
                                                    config_updated["cookie"]["expiry_days"],
                                                    auto_hash=False,
                                                )
                                                # Устанавливаем временный флаг
                                                st.session_state["_temp_auth_username"] = username_for_reset
                                                st.session_state["_temp_auth_name"] = user_info.get("name", username_for_reset)
                                                
                                                if hasattr(authenticator_updated, 'cookie_manager') and authenticator_updated.cookie_manager:
                                                    import hmac
                                                    import hashlib
                                                    from datetime import datetime, timedelta
                                                    cookie_key = config_updated["cookie"]["key"]
                                                    token_data = f"{username_for_reset}:{user_info.get('name', username_for_reset)}"
                                                    token = hmac.new(cookie_key.encode(), token_data.encode(), hashlib.sha256).hexdigest()
                                                    expiry = datetime.now() + timedelta(days=config_updated["cookie"]["expiry_days"])
                                                    authenticator_updated.cookie_manager.set(
                                                        authenticator_updated.cookie_name,
                                                        token,
                                                        expires_at=expiry
                                                    )
                                            except Exception:
                                                pass
                                            
                                            st.success("✅ Пароль успешно изменён! Вы автоматически авторизованы.")
                                            st.balloons()
                                            st.session_state.reset_step = 1
                                            st.session_state.reset_email = ""
                                            st.session_state.auth_screen = "login"
                                            st.rerun()
                                        else:
                                            st.error("Ошибка: пользователь не найден в конфигурации.")
                                    else:
                                        st.error("Ошибка: неверная структура конфигурации.")
                                except Exception as e:
                                    st.error(f"Ошибка при смене пароля: {e}")
                    with col2:
                        if st.button("Назад", key="reset_back_to_step2"):
                            st.session_state.reset_step = 2
                            st.rerun()
        render_auth_support_notice()


all_messages_memory = ConversationBufferMemory(
    chat_memory=StreamlitChatMessageHistory(key="langchain_messages"),
    return_messages=True,
    memory_key="chat_history",
)

last_messages_memory = ConversationBufferWindowMemory(
    chat_memory=StreamlitChatMessageHistory(key="last_messages"),
    k=4,
    return_messages=True,
    memory_key="last",
)

if "chat_history_last" not in st.session_state:
    st.session_state.chat_history_last = None


if st.session_state["authentication_status"]:  # noqa: C901

    # from synaptica.backend.streamlit_functions.presentation_gen import maybe_run_daily_artifact_cleanup

    # maybe_run_daily_artifact_cleanup()
    _check_data_traffic_light(str(settings.external_data_path))

    authenticator.logout()
    # Приветствие после первой регистрации (без повторного ввода пароля)
    if st.session_state.pop("just_registered", False):
        st.success("Регистрация прошла успешно. Добро пожаловать!")
        st.balloons()
    st.write(f'Добро пожаловать, *{st.session_state["name"]}*')
    st.subheader("Синаптика")

    st.session_state.exec_register_pipeline = False

    feedback_option = "thumbs"

    from streamlit.runtime.scriptrunner import get_script_run_ctx

    def get_streamlit_port():
        try:
            # Get the server address and extract port
            server_address = st.get_option('server.address')
            server_port = st.get_option('server.port')
            
            return server_port
        except Exception as e:
            st.error(f"Error getting port: {e}")
            return None

    if get_streamlit_port() != 8051:
        st.sidebar.error("🚨😲 **ВАЖНО - это тестовая версия Синаптики, возможна нестабильная работа сервиса**. Для полной версии сервиса, используйте окно на портале GM-Pro")



    st.sidebar.info("""# 🚀 **Synaptica**""")
    st.sidebar.write("""### Полезные команды:""")

    # Добавление CSS-стилей в приложение
    add_custom_css()

    _notes_anchor_idx = notes_list_anchor_message_idx(st.session_state.langchain_messages)
    for _msg_idx, msg in enumerate(st.session_state.langchain_messages):
        with st.chat_message(msg.type):
            import re as _re
            from pathlib import Path as _Path
            _content = msg.content
            _docx_paths = _re.findall(r'(/[\w/.\-]+\.docx)', _content)
            _pdf_paths = list(dict.fromkeys(
                _re.findall(r'<!--\s*PDF:\s*(/[\w/.\-]+\.pdf)\s*-->', _content)
                + _re.findall(r'(/[\w/.\-]+\.pdf)', _content)
            ))
            _pptx_paths = list(dict.fromkeys(
                _re.findall(r'<!--\s*PPTX:\s*(/[\w/.\-]+\.pptx)\s*-->', _content)
                + _re.findall(r'(/[\w/.\-]+\.pptx)', _content)
            ))
            _xlsx_paths = list(dict.fromkeys(
                _re.findall(r'<!--\s*XLSX:\s*(/[\w/.\-]+\.xlsx)\s*-->', _content)
                + _re.findall(r'(/[\w/.\-]+\.xlsx)', _content)
            ))
            _chart_paths = _re.findall(r'<!--\s*CHART:\s*(/[\w/.\-]+\.png)\s*-->', _content)
            if _docx_paths:
                _content = _re.sub(r'[^\n]*(/[\w/.\-]+\.docx)[^\n]*\n?', '', _content).strip()
            _content = _re.sub(r'<!--\s*PDF:\s*/[\w/.\-]+\.pdf\s*-->\n?', '', _content).strip()
            if _pdf_paths:
                _content = _re.sub(r'[^\n]*(/[\w/.\-]+\.pdf)[^\n]*\n?', '', _content).strip()
            _content = _re.sub(r'<!--\s*PPTX:\s*/[\w/.\-]+\.pptx\s*-->\n?', '', _content).strip()
            if _pptx_paths:
                _content = _re.sub(r'[^\n]*(/[\w/.\-]+\.pptx)[^\n]*\n?', '', _content).strip()
            _content = _re.sub(
                r"<!--\s*NOTES_LIST(?:\s+ids=[0-9A-Za-z_,.\-\s]*?)?\s*-->",
                "",
                _content,
                flags=_re.IGNORECASE,
            ).strip()
            _content = _re.sub(r"<!--\s*NOTES_LIST[\s\S]*$", "", _content, flags=_re.IGNORECASE).strip()
            _content = _re.sub(r'<!--\s*XLSX:\s*/[\w/.\-]+\.xlsx\s*-->\n?', '', _content).strip()
            if _xlsx_paths:
                _content = _re.sub(r'[^\n]*(/[\w/.\-]+\.xlsx)[^\n]*\n?', '', _content).strip()
            _xlsx_ready = []
            for _xp in _xlsx_paths:
                _p = _Path(_xp)
                if _p.is_file():
                    _xlsx_ready.append(_p)
            if _chart_paths:
                _content = _re.sub(r'<!--\s*CHART:\s*/[\w/.\-]+\.png\s*-->\n?', '', _content).strip()
            _content = _re.sub(r'<!--\s*SCENARIO_CHART:\s*[a-zA-Z0-9_-]+\s*-->\n?', '', _content).strip()
            st.markdown(_content, unsafe_allow_html=True)
            for _dp in _docx_paths:
                _p = _Path(_dp)
                if _p.is_file():
                    st.download_button(
                        label=f"⬇ Скачать {_p.name}",
                        data=_p.read_bytes(),
                        file_name=_p.name,
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        key=f"hist_docx_{_msg_idx}_{hash(_dp)}",
                    )
            for _pdf_path in _pdf_paths:
                _p = _Path(_pdf_path)
                if _p.is_file():
                    st.download_button(
                        label=f"⬇ Скачать {_p.name}",
                        data=_p.read_bytes(),
                        file_name=_p.name,
                        mime="application/pdf",
                        key=f"hist_pdf_{_msg_idx}_{hash(_pdf_path)}",
                    )
            for _pp_path in _pptx_paths:
                _p = _Path(_pp_path)
                if _p.is_file():
                    st.download_button(
                        label=f"⬇ Скачать {_p.name}",
                        data=_p.read_bytes(),
                        file_name=_p.name,
                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"hist_pptx_{_msg_idx}_{hash(_pp_path)}",
                    )
            for _cp in _chart_paths:
                _pp = _Path(_cp)
                if _pp.exists():
                    st.image(str(_pp))
            if _msg_idx == _notes_anchor_idx:
                render_notes_records_panel()
            # Стабилизирует слот сообщения: без этого Streamlit рисует «белый» дубль ответа.
            st.empty()
        for _p in _xlsx_ready:
            st.download_button(
                label=f"⬇ Скачать {_p.name}",
                data=_p.read_bytes(),
                file_name=_p.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"hist_xlsx_{_msg_idx}_{_p.name}",
            )

    # Инициализация
    # products
    initialize_products_pipeline()
    # fx
    initialize_fx_pipeline()
    # cmdt
    initialize_cmdt_pipeline()
    initialize_market_pipeline()
    # indicatives
    initialize_indicatives_pipeline()
    # potential
    initialize_potential_pipeline()
    # sales
    initialize_sales_pipeline()
    # pers
    initialize_pers_pipeline()
    # vnd
    initialize_vnd_pipeline()
    # openclaw
    initialize_openclaw_pipeline()
    initialize_presentation_gen_pipeline()
    initialize_logs_view_pipeline()
    initialize_call_upload_pipeline()
    initialize_special_news_pipeline()

    if "chat_history_last" not in st.session_state:
        st.session_state.chat_history_last = None

    try:
        prompt = render_chat_input_with_rates_attach(placeholder="Спроси у меня что-нибудь!")
    except Exception:
        logger.exception("streamlit_step_failed")
        prompt = st.chat_input(placeholder="Спроси у меня что-нибудь!")

    if prompt:
        _attach_handled = False
        try:
            _attach_handled = try_handle_synaptica_attach_reply(
                prompt,
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception:
            logger.exception("streamlit_step_failed")
            _attach_handled = False
        if _attach_handled:
            prompt = None

    if prompt:
        # Любой новый ввод, кроме синтетической строки детализации по клику — убираем интерактивную сводку внизу.
        _pstrip = prompt.strip()
        if _pstrip and not _pstrip.startswith(CALL_TABLE_DETAIL_USER_PREFIX):
            st.session_state.pop(CALL_TABLE_INTERACTIVE_CTX_KEY, None)
            st.session_state.pop(CALL_TABLE_PENDING_DETAIL_KEY, None)
            st.session_state.pop(CALL_TABLE_DF_SELECTION_WIDGET_KEY, None)
        # обнулить все переменные, которые здесь не используются
        reset_to_zero_input_pipeline()
        reset_to_zero_products_pipeline()
        reset_to_zero_fx_pipeline()
        reset_to_zero_cmdt_pipeline()
        reset_to_zero_indicatives_pipeline()
        reset_to_zero_potential_pipeline()
        reset_to_zero_sales_pipeline()
        if "calls_mode" not in st.session_state:
            st.session_state.calls_mode = False
        if "calls_data_loaded" not in st.session_state:
            st.session_state.calls_data_loaded = False
        if "calls_retriever" not in st.session_state:
            st.session_state.calls_retriever = None
        if "calls_chat_history" not in st.session_state:
            st.session_state.calls_chat_history = []
        if "notes_mode" not in st.session_state:
            st.session_state.notes_mode = False
        if "notes_chat_history" not in st.session_state:
            st.session_state.notes_chat_history = []
        if "broker_mode" not in st.session_state:
            st.session_state.broker_mode = False
        if "broker_chat_history" not in st.session_state:
            st.session_state.broker_chat_history = []
        if "openclaw_fi_sales_mode" not in st.session_state:
            st.session_state.openclaw_fi_sales_mode = False
        if "openclaw_fi_sales_chat_history" not in st.session_state:
            st.session_state.openclaw_fi_sales_chat_history = []
        if "openclaw_corp_sales_mode" not in st.session_state:
            st.session_state.openclaw_corp_sales_mode = False
        if "openclaw_corp_sales_chat_history" not in st.session_state:
            st.session_state.openclaw_corp_sales_chat_history = []
        if "openclaw_calls_mode" not in st.session_state:
            st.session_state.openclaw_calls_mode = False
        if "openclaw_calls_chat_history" not in st.session_state:
            st.session_state.openclaw_calls_chat_history = []
        if "openclaw_vnd_rag_mode" not in st.session_state:
            st.session_state.openclaw_vnd_rag_mode = False
        if "openclaw_vnd_rag_chat_history" not in st.session_state:
            st.session_state.openclaw_vnd_rag_chat_history = []
        if "openclaw_pres_gen_mode" not in st.session_state:
            st.session_state.openclaw_pres_gen_mode = False
        if "openclaw_pres_gen_chat_history" not in st.session_state:
            st.session_state.openclaw_pres_gen_chat_history = []
        if "openclaw_rates_pricing_mode" not in st.session_state:
            st.session_state.openclaw_rates_pricing_mode = False
        if "openclaw_rates_pricing_chat_history" not in st.session_state:
            st.session_state.openclaw_rates_pricing_chat_history = []
        if "openclaw_products_mode" not in st.session_state:
            st.session_state.openclaw_products_mode = False
        if "openclaw_products_chat_history" not in st.session_state:
            st.session_state.openclaw_products_chat_history = []
        if "openclaw_router_mode" not in st.session_state:
            st.session_state.openclaw_router_mode = False
        if "openclaw_router_chat_history" not in st.session_state:
            st.session_state.openclaw_router_chat_history = []
        if "optimizer_mode" not in st.session_state:
            st.session_state.optimizer_mode = False

        initialize_input_pipeline()

        if st.session_state.out == "-":
            pass

        st.session_state.user_input = prompt
        st.chat_message("user").write(st.session_state.user_input)

        st.session_state.chat_history_last = st.session_state.last_messages[-8:]

        st.session_state.precomputed_choosed_case = None
        _do_pers = st.session_state.DO_PERS if ("DO_PERS" in st.session_state) else False
        _uname = str(st.session_state.get("username", ""))
        _openclaw_chat = (
            getattr(st.session_state, "openclaw_coder_mode", False)
            or getattr(st.session_state, "openclaw_helper_mode", False)
            or getattr(st.session_state, "openclaw_client_mode", False)
            or getattr(st.session_state, "openclaw_fi_sales_mode", False)
            or getattr(st.session_state, "openclaw_corp_sales_mode", False)
            or getattr(st.session_state, "openclaw_calls_mode", False)
            or getattr(st.session_state, "openclaw_vnd_rag_mode", False)
            or getattr(st.session_state, "openclaw_pres_gen_mode", False)
            or getattr(st.session_state, "openclaw_rates_pricing_mode", False)
            or getattr(st.session_state, "openclaw_products_mode", False)
            or getattr(st.session_state, "openclaw_router_mode", False)
        ) and not prompt.strip().startswith("/")
        _resolved_prompt = None
        _did_context_pass = False
        _locked_specialist = (
            getattr(st.session_state, "vnd_mode", False)
            or getattr(st.session_state, "notes_mode", False)
            or getattr(st.session_state, "broker_mode", False)
            or getattr(st.session_state, "calls_mode", False)
            or getattr(st.session_state, "logs_view_mode", False)
            or getattr(st.session_state, "special_news_mode", False)
            or _do_pers
            or _openclaw_chat
        )
        _default_openclaw = (
            bool(settings.openclaw_classifier)
            and (not prompt.strip().startswith("/"))
            and not _locked_specialist
        )
        if (
            not settings.openclaw_classifier
            and is_calls_allowed_user(_uname)
            and not getattr(st.session_state, "vnd_mode", False)
            and not getattr(st.session_state, "notes_mode", False)
            and not getattr(st.session_state, "scenario_mode", False)
            and not getattr(st.session_state, "broker_mode", False)
            and not _do_pers
            and not prompt.strip().startswith("/")
            and not _openclaw_chat
            and not getattr(st.session_state, "calls_mode", False)
        ):
            _cc, _resolved_prompt, _did_context_pass = choose_case_resolving_context(
                prompt,
                st.session_state.chat_history_last,
                paraphrase_fn=paraphrase_bot_message,
            )
            if _cc == "вопрос по звонкам продаж":
                st.session_state.calls_mode = True
                st.session_state.pending_feedback_meta = {"pipeline": "calls"}
                st.session_state["calls_bank"] = resolve_calls_bank(_uname)
                if "calls_chat_history" not in st.session_state:
                    st.session_state.calls_chat_history = []
                st.session_state.paraphased_user_question = prompt
                st.session_state["_calls_paraphrase_in_answer"] = True
            else:
                st.session_state.calls_mode = False
                st.session_state.precomputed_choosed_case = _cc
                st.session_state.pop(CALL_TABLE_INTERACTIVE_CTX_KEY, None)
                st.session_state.pop(CALL_TABLE_PENDING_DETAIL_KEY, None)
                st.session_state.pop(CALL_TABLE_DF_SELECTION_WIDGET_KEY, None)

        try:
            if _openclaw_chat or _default_openclaw:
                st.session_state.paraphased_user_question = prompt
            elif getattr(st.session_state, "logs_view_mode", False):
                st.session_state.paraphased_user_question = prompt
            elif getattr(st.session_state, "special_news_mode", False):
                st.session_state.paraphased_user_question = prompt
            elif prompt.strip().startswith("/"):
                # Slash-команда (например, /calls_tb) — это фиксированная команда, а не follow-up
                # вопрос, поэтому пропускаем лишний LLM-вызов перефразирования.
                st.session_state.paraphased_user_question = prompt
            elif getattr(st.session_state, "calls_mode", False):
                st.session_state.paraphased_user_question = prompt
                st.session_state["_calls_paraphrase_in_answer"] = True
            elif _did_context_pass and _resolved_prompt:
                # Уже перефразировали на second-pass роутинга — не дергаем paraphrase второй раз.
                st.session_state.paraphased_user_question = _resolved_prompt
            else:
                st.session_state.paraphased_user_question = paraphrase_bot_message(
                    prompt, memory=st.session_state.chat_history_last, in_call_mode=st.session_state.calls_mode
                )
        except Exception as e:
            if is_streamlit_script_control_exception(e):
                raise
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

        try:
            write_llm_answer_from_text(
                original_user_input=st.session_state.user_input,
                transformed_user_input=st.session_state.paraphased_user_question,
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                chat_history_last=st.session_state.chat_history_last,
                personalization_flag = st.session_state.DO_PERS if ("DO_PERS" in st.session_state) else False,
                ADDITIONAL_INSTRUCTION = st.session_state.ADDITIONAL_INSTRUCTION if ("ADDITIONAL_INSTRUCTION" in st.session_state) else ""
            )

        except Exception as e:
            if is_streamlit_script_control_exception(e):
                raise
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    st.session_state.out = None

    if st.sidebar.button("* 📖 **/start**: Давайте знакомиться!"):
        try:
            write_hello(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/start"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    # Боковое меню с кнопкой
    with st.sidebar:
        if st.button("* 🍯 **/products**: Расскажу, что знаю о продуктах глобальных рынков"):
            try:
                start_products_pipeline()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    # Отображение selectbox в чате при нажатии на кнопку в боковом меню
    if st.session_state.show_selectbox_products:
        try:
            products_get_list_and_chat(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/products"
            st.chat_message("user").write(st.session_state.user_input)
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.products_list:
        try:
            products_get_product_from_list()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/products"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.selected_product:
        try:
            products_finish(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = st.session_state.selected_product
            st.chat_message("user").write(st.session_state.selected_product)
            answer_if_error_pipeline(all_messages_memory, st.session_state.selected_product)

    if st.sidebar.button("* 💹 **/rates**: Помогу с анализом действующего портфеля"):
        try:
            write_rates_help(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/rates_help"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    with st.sidebar:
        if st.button(
            "* 💱 **/market**: Актуальные курсы FX и биржевых товаров"
        ):
            try:
                start_market_pipeline()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.show_market_choice:
        try:
            market_get_list_values_and_chat(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/market"
            st.chat_message("user").write(st.session_state.user_input)
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.market_combined_list:
        try:
            market_get_instrument_from_list(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/market"
            st.chat_message("user").write(st.session_state.user_input)
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.show_selectbox_fx:
        try:
            fx_get_list_values_and_chat(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/fx"
            st.chat_message("user").write(st.session_state.user_input)
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.fx_list:
        try:
            fx_get_fx_from_list()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/fx"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.start_period_pipeline and st.session_state.get("selected_option_fx"):
        try:
            show_fx_period()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = st.session_state.selected_option_fx or "/fx"
            st.chat_message("user").write(st.session_state.selected_option_fx or "/fx")
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.show_fx_period:
        try:
            fx_start_period_pipeline()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/fx"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.show_selectbox_fx_period:
        try:
            fx_get_list_period_and_chat(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/fx"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.fx_list_period:
        try:
            fx_get_period_from_list()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/fx"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.selected_option_fx_period:
        try:
            fx_finish(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = st.session_state.selected_option_fx_period
            st.chat_message("user").write(st.session_state.selected_option_fx_period)
            answer_if_error_pipeline(
                all_messages_memory, st.session_state.selected_option_fx_period
            )

    if st.session_state.show_selectbox_cmdt:
        try:
            cmdt_get_list_values_and_chat(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/cmdt"
            st.chat_message("user").write(st.session_state.user_input)
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.cmdt_list:
        try:
            cmdt_get_fx_from_list()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/cmdt"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.start_cmdt_period_pipeline:
        try:
            show_cmdt_period()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = st.session_state.selected_option_cmdt
            st.chat_message("user").write(st.session_state.selected_option_cmdt)
            answer_if_error_pipeline(all_messages_memory, st.session_state.selected_option_cmdt)

    if st.session_state.show_cmdt_period:
        try:
            cmdt_start_period_pipeline()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/cmdt"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.show_selectbox_cmdt_period:
        try:
            cmdt_get_list_period_and_chat(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/cmdt"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.cmdt_list_period:
        try:
            cmdt_get_period_from_list()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/cmdt"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.selected_option_cmdt_period:
        try:
            cmdt_finish(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = st.session_state.selected_option_cmdt_period
            st.chat_message("user").write(st.session_state.selected_option_cmdt_period)
            answer_if_error_pipeline(
                all_messages_memory, st.session_state.selected_option_cmdt_period
            )

    if st.sidebar.button("* 📊 **/eco**: Самый свежий обзор макро для Вас"):
        try:
            write_eco(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/eco"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    # /news: allowlist → write_news; иначе заглушка (write_news не вызываем).
    _news_user = str(st.session_state.get("username", ""))
    if is_special_news_allowed_user(_news_user):
        if st.sidebar.button("* 📢 **/news**: Внимание, новости! Доступна персональная подборка"):
            try:
                write_news(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                st.session_state.user_input = "/news"
                answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)
    else:
        if st.sidebar.button("* 📢 **/news**: Внимание, новости! Доступна персональная подборка"):
            try:
                news_stub = (
                    "📰 **Персональные новости** — в разработке.\n\n"
                    "Скоро здесь появится подборка под ваши компании и отрасли. "
                    "Следите за обновлениями Synaptica — откроем доступ, как только всё будет готово."
                )
                st.session_state.special_news_mode = False
                st.chat_message("user").write("/news")
                st.chat_message("assistant").markdown(news_stub)
                all_messages_memory.chat_memory.add_user_message("/news")
                all_messages_memory.chat_memory.add_ai_message(news_stub)
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)


    with st.sidebar:
        if st.sidebar.button("* 🎨 **/personalization**: Почувствуй себя в роли Клиента!"):
            try:
                start_pers_pipeline()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_calls_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button(
            "* 📞 **/calls_tb**: Кому звонили сейлз ТБ?",
            key="sidebar_calls_tb",
        ):
            try:
                write_call_table_sidebar_button(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                    chat_history_last=st.session_state.chat_history_last,
                )
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.show_selectbox_pers:
        try:
            pers_get_list_values_and_chat(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/personalization"
            st.chat_message("user").write(st.session_state.user_input)
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.pers_list:
        try:
            pers_get_pers_from_list()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/personalization"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if is_notes_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 💼 🦞 **/notes**: заметки FI"):
            try:
                write_notes_button(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_broker_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 🏦 **/brokerka**: брокерка"):
            try:
                write_broker_button(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_openclaw_corp_sales_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 🏢 🦞 **/corp_notes**: заметки Corp"):
            try:
                write_openclaw_corp_sales_start(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_openclaw_coder_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 💻 🦞 **/openclaw1_coder**: Пишу код Synaptica"):
            try:
                write_openclaw_coder_start(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_openclaw_helper_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 📖 🦞 **/openclaw1_helper**: Навигация по Synaptica"):
            try:
                write_openclaw_helper_start(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_openclaw_client_allowed_user(str(st.session_state.get("username", ""))):
        st.sidebar.link_button(
            "* 📋 🦞 **/openclaw**: Внешний помощник клиента",
            "https://synaptica.delta.sbrf.ru:4444",
        )

    if is_openclaw_vnd_rag_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 📚 🦞 **/vnd_rag**: Вопросы по ВНД"):
            try:
                write_openclaw_vnd_rag_start(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_openclaw_pres_gen_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 📽 🦞  **/pres_gen**: генерация презентаций"):
            try:
                write_openclaw_pres_gen_start(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_openclaw_rates_pricing_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 📈 🦞  **/rates_pricing**: сделки и прайсинг"):
            try:
                write_openclaw_rates_pricing_start(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_openclaw_products_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 🍯 🦞 **/openclaw_products**: вопросы по продуктам"):
            try:
                write_openclaw_products_start(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_presentation_gen_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 📽 **/pres**: Готовая презентация для вашего клиента"):
            try:
                write_presentation_generate_button(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_call_upload_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 📤 **/call_upload**: Загрузить Excel звонков"):
            try:
                write_call_upload_button(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if is_logs_view_allowed_user(str(st.session_state.get("username", ""))):
        if st.sidebar.button("* 💬 **/comments**: Проанализирую комментарии по триггерам"):
            try:
                write_logs_view_button(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    _optimizer_allowed = is_optimizer_allowed_user(str(st.session_state.get("username", "")))
    if _optimizer_allowed:
        if st.sidebar.button("* 🧮 **/optimizer**: Подбор оптимальной структуры хеджирования"):
            try:
                write_optimizer_button(
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
                st.rerun()
            except Exception as e:
                logger.exception("streamlit_step_failed")
                answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if _optimizer_allowed and getattr(st.session_state, "optimizer_mode", False):
        try:
            render_optimizer_panel(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if getattr(st.session_state, "openclaw_client_mode", False):
        try:
            process_openclaw_client_pending_upload(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if getattr(st.session_state, "call_upload_mode", False):
        try:
            process_call_upload_pending_upload(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    try:
        process_synaptica_pending_attach(
            all_messages_memory=all_messages_memory,
            last_messages_memory=last_messages_memory,
        )
    except Exception:
        logger.exception("streamlit_step_failed")

    if getattr(st.session_state, "special_news_mode", False):
        if is_special_news_allowed_user(str(st.session_state.get("username", ""))):
            try:
                process_special_news_panel_action(
                    str(st.session_state.get("username", "")),
                    all_messages_memory=all_messages_memory,
                    last_messages_memory=last_messages_memory,
                )
            except Exception:
                logger.exception("streamlit_step_failed")

    if st.sidebar.button("* ❓ **/faq**: Ответы на часто задаваемые вопросы"):
        try:
            write_faq(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/faq"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.start_period_pipeline and st.session_state.get("selected_option_pers"):
        try:
            show_pers_period(all_messages_memory, last_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            _pers = st.session_state.selected_option_pers
            _user_line = (
                _pers[0]
                if isinstance(_pers, (tuple, list)) and len(_pers) > 0
                else _pers
            )
            _user_line = _user_line if _user_line else "/personalization"
            st.session_state.user_input = _user_line
            st.chat_message("user").write(_user_line)
            answer_if_error_pipeline(all_messages_memory, _user_line)

    if st.session_state.show_pers_period_:
        try:
            pers_start_period_pipeline()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/personalization"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.show_selectbox_pers_period:
        
        try:
            pers_get_list_period_and_chat(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = "/personalization"
            answer_if_error_pipeline(all_messages_memory, st.session_state.user_input)

    if st.session_state.pers_list_period:
        try:
            write_personification(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            if is_streamlit_script_control_exception(e):
                raise
            st.exception(e)
            fn = globals().get("answer_if_error_pipeline")
            if callable(fn):
                fn(all_messages_memory, "/personalization")



    if st.session_state.products_list_ind != [] and st.session_state.products_list_ind is not None:
        try:
            get_indicative_from_list()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.start_ind_pipe_product:
        try:
            start_indicatives_pipeline_with_product()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.show_indicatives_button_1:
        try:
            indicatives_1(
                product=st.session_state.ind_product,
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            st.session_state.user_input = st.session_state.ind_product
            st.chat_message("user").write(st.session_state.ind_product)
            answer_if_error_pipeline(all_messages_memory, st.session_state.ind_product)

    if st.session_state.button_values_ind_type:
        try:
            indicatives_2()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.start_indicatives_tenor:
        try:
            indicatives_3(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.button_values_ind_tenor:
        try:
            indicatives_4()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.start_indicatives_strike:
        try:
            indicatives_5(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.button_values_ind_strike:
        try:
            indicatives_6()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.indicatives_finish:
        try:
            indicatives_7()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.ind_full_response_d:
        try:
            indicatives_finish(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.show_first_selectboxes:
        try:
            select_company_from_first_lists()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    # potential
    if st.session_state.show_potential_df_stand_alone:
        try:
            potential_stand_alone(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.show_potential_df_holding:
        try:
            potential_holding(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.companies_in_holdings_list:
        try:
            select_company_from_second_list()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.show_potential_df_company_from_holding:
        try:
            potential_company_from_holding(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.button_products_sales:
        try:
            sales_1()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.product_sales_description:
        try:
            sales_2(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.binary_button_values_1:
        try:
            sales_3(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.binary_button_values_2:
        try:
            sales_4()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.start_sales_indicatives:
        try:
            sales_indicatives_1(
                product=st.session_state.selected_sales_product,
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_button_values_ind_type:
        try:
            sales_indicatives_2()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_start_indicatives_tenor:
        try:
            sales_indicatives_3(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_button_values_ind_tenor:
        try:
            sales_indicatives_4()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_start_indicatives_strike:
        try:
            sales_indicatives_5(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_button_values_ind_strike:
        try:
            sales_indicatives_6()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_indicatives_finish:
        try:
            sales_indicatives_7()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_ind_full_response_d:
        try:
            sales_indicatives_finish(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.binary_button_values_3:
        try:
            sales_5()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.start_alternative_pipeline:
        try:
            sales_alternative_start(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.button_values_alt:
        try:
            sales_alternative_1()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_alt_description:
        try:
            sales_alternative_2(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.binary_button_values_1_alt:
        try:
            sales_alternative_3(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.binary_button_values_2_alt:
        try:
            sales_alternative_4()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.not_interested_user_input:
        try:
            not_interested_pipeline(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                original_user_input=st.session_state.not_interested_user_input,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.saled_user_input:
        try:
            saled_function(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
                original_user_input=st.session_state.saled_user_input,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.indicatives_pipeline_alt:
        try:
            sales_indicatives_1_alt(
                product=st.session_state.btn_alt,
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_alt_button_values_ind_type:
        try:
            sales_indicatives_2_alt()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_alt_start_indicatives_tenor:
        try:
            sales_indicatives_3_alt(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_alt_button_values_ind_tenor:
        try:
            sales_indicatives_4_alt()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_alt_start_indicatives_strike:
        try:
            sales_indicatives_5_alt(all_messages_memory)
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_alt_button_values_ind_strike:
        try:
            sales_indicatives_6_alt()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_alt_indicatives_finish:
        try:
            sales_indicatives_7_alt()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.sales_alt_ind_full_response_d:
        try:
            sales_indicatives_8_alt(
                all_messages_memory=all_messages_memory,
                last_messages_memory=last_messages_memory,
            )
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    if st.session_state.binary_button_values_3_alt:
        try:
            sales_indicatives_finish_alt()
        except Exception as e:
            logger.exception("streamlit_step_failed")
            answer_if_error_pipeline_saving_only_llm_answer(all_messages_memory)

    save_feedback()

    # Сводка /calls_tb: после всего UI; выбор строки обрабатывается внутри render_call_table_interactive_dataframe.
    render_call_table_interactive_dataframe()
    render_corp_notes_interactive_dataframe()
    flush_call_table_detail_if_pending(
        all_messages_memory=all_messages_memory,
        last_messages_memory=last_messages_memory,
        chat_history_last=st.session_state.chat_history_last,
    )
