# этот файл лежит на компе винде и запускается с него подклбчатеся по ssh -
"""
Фоновый скрипт для обработки заявок на сброс пароля.
Опрашивает лог по SSH, генерирует коды, пишет code_set на сервер, отправляет письма.

Запуск: python reset_pas_admin.py
"""

import atexit
import hashlib
import html
import json
import os
import random
import re
import shlex
import signal
import string
import sys
import time
import warnings
import zipfile
import shutil
import subprocess
from xml.sax.saxutils import escape as _xml_escape
from datetime import date, datetime, timezone, timedelta

# Файлы для управления процессом (остановка без Ctrl+C)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PID_FILE = os.path.join(SCRIPT_DIR, "reset_pas_admin.pid")
STOP_FILE = os.path.join(SCRIPT_DIR, "reset_pas_admin.stop")

import paramiko
import win32com.client  # только чтение Incoming для news_*.csv
from exchangelib import (
    NTLM,
    Account,
    Build,
    Configuration,
    Credentials,
    DELEGATE,
    FileAttachment,
    HTMLBody,
    Mailbox,
    Message,
    Version,
)
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter

warnings.filterwarnings("ignore")
BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapter

# ============== НАСТРОЙКИ — заполни переменные ==============
SSH_SECRET = "123QWEasd"  # Пароль для SSH подключения

# ============================
# НАСТРОЙКИ ПОЧТЫ
# ============================
server = "outlook.sberbank.ru"
email = "svibudygin@sberbank.ru"
# лучше не хранить пароль в коде, но если надо быстро - вставь сюда
password = "9273FvdjFhd!"

creds = Credentials(r"SIGMA\23571662", password)
config = Configuration(
    credentials=creds,
    server=server,
    auth_type=NTLM,  # иначе на Windows часто падает SpnegoError
    version=Version(build=Build(15, 0, 0, 0)),  # Exchange 2013+: сразу send, без черновика
)
account = Account(
    primary_smtp_address=email,
    autodiscover=False,
    config=config,
    access_type=DELEGATE,
)


def send_email(account, subject, body, recipients, attachments=None):
    if not password:
        raise RuntimeError("Заполни password в НАСТРОЙКАХ ПОЧТЫ (как в polyan_sender.py)")

    to_recipients = []
    for recipient in recipients:
        to_recipients.append(Mailbox(email_address=recipient))

    m = Message(
        account=account,
        subject=subject,
        body=body,
        to_recipients=to_recipients,
    )

    for attachment_name, attachment_content in attachments or []:
        file = FileAttachment(
            name=attachment_name,
            content=attachment_content,
        )
        m.attach(file)

    # SEND_ONLY — сразу отправка, без промежуточного save() в «Черновики»
    m.send(save_copy=False)
# ============================================================================

# Остальное настраивается автоматически
RESET_SSH = "synaptica@10.56.25.104"     # SSH: user@host
RESET_LOG_PATH = "/data/s3/projects/synaptica/reset_pas.jsonl"
RESET_CODE_MINS = 15
RESET_POLL_INTERVAL = 10   # опрос лога каждые 10 с
# Уже отправленные id (локально на винде). Лог на S3 часто не видит code_set — без этого файл
# шлёт новый код на тот же pending каждые 10–20 мин.
RESET_SENT_FILE = os.path.join(SCRIPT_DIR, "reset_codes_sent.txt")
RESET_PENDING_MAX_AGE_HOURS = 24  # старше не обрабатываем: код всё равно протух

# --- Светофор данных: алерты об устаревших источниках ---
# Сервер пишет в этот лог события {id, source, since, status: "pending"}.
# Поллер читает их так же, как заявки на сброс, и шлёт письмо через Exchange.
ALERTS_LOG_PATH = "/data/s3/projects/synaptica/data_alerts.jsonl"
ALERT_EMAIL_TO = "SYNAPTICA_SUPPORT@sberbank.ru"   # общая почта поддержки Синаптики — сюда уходят алерты светофора
ALERT_SUBJECT_TMPL = "{sources} НЕ обновились с {since}"
# Не слать письма по этим источникам (News отключён по запросу).
ALERT_DISABLED_SOURCES = frozenset({"News"})

# --- Еженедельный бэкап FI-звонков на почту ---
# Раз в неделю: дамп OpenSearch → JSONL, из него собираем Excel, шлём оба.
# Старый fi_calls.xlsx не прикладываем.
FI_WEEKLY_ENABLED = True
FI_WEEKLY_EMAIL_TO = "SYNAPTICA_SUPPORT@sberbank.ru"   # кому слать (через ; можно несколько адресов)
# Команда на сервере, которая ПЕРЕД отправкой обновляет JSONL-бэкап из OpenSearch (чтобы был актуальным).
# Вызываем python прямо из venv (надёжнее, чем 'source .../activate' в неинтерактивном SSH).
# Пусто ("") = не обновлять (только скачать уже лежащий файл).
FI_DUMP_REMOTE_CMD = (
    "cd /home/synaptica/synaptica && "
    "/home/synaptica/synaptica/.venv/bin/python "
    "synaptica/scripts/dump_fi_index_jsonl.py /data/s3/projects/synaptica/fi_backup.jsonl"
)
FI_BACKUP_REMOTE_PATH = "/data/s3/projects/synaptica/fi_backup.jsonl"  # путь к JSONL-бэкапу на сервере
FI_WEEKLY_INTERVAL_DAYS = 7
FI_WEEKLY_STATE_FILE = os.path.join(SCRIPT_DIR, "fi_weekly_last_sent.txt")  # когда последний раз отправляли
FI_WEEKLY_SUBJECT_TMPL = "FI — еженедельный бэкап базы ({date})"

# --- Еженедельный отчёт по заметкам FI / корпы / Comdty ---
# Раз в 7 дней: сводная таблица по командам на почту (копируется в PowerPoint).
# Разбивка по сейлзам — дашборд (dashboard/dashboard_notes_weekly.py).
NOTES_WEEKLY_ENABLED = True
NOTES_WEEKLY_EMAIL_TO = "svibudygin@sberbank.ru"
NOTES_WEEKLY_DASH_URL = "https://synaptica.delta.sbrf.ru:3333"
NOTES_WEEKLY_INTERVAL_DAYS = 7
NOTES_WEEKLY_STATE_FILE = os.path.join(SCRIPT_DIR, "notes_weekly_last_sent.txt")
NOTES_REPORT_REMOTE_CMD = (
    "cd /home/synaptica/synaptica && "
    "/home/synaptica/synaptica/.venv/bin/python "
    "synaptica/scripts/notes_weekly_report.py"
)
FI_XLSX_COLUMNS = (
    "record_id",
    "record_type",
    "interaction_date",
    "client_name_norm",
    "products",
    "currencies",
    "short_summary",
    "raw_text",
    "next_step",
    "status",
    "source_sheet",
    "created_by",
    "created_at",
)

# --- Ежедневный CSV с новостями по клиентам → на сервер + запуск парсинга ---
# Каждое утро приходит письмо с вложением news_*.csv. Скрипт достаёт вложение из
# Outlook, заливает по SFTP в папку новостей на сервере и запускает парсинг.
NEWS_ENABLED = True
NEWS_SUBJECT_MATCH = "Новости по клиентам на основе ML-модели"  # подстрока темы письма
NEWS_ATTACH_PREFIX = "news_"                                # префикс имени вложения .csv
NEWS_REMOTE_DIR = "/data/s3/projects/synaptica/news"  # общая папка, читает news_clean.py
# Как часто писать строку «scan: …», если новых CSV нет (1 = каждый опрос ≈10 с, 6 ≈ раз в минуту).
NEWS_LOG_EVERY_N_SCANS = 1
# Команда парсинга после заливки ({filename} = имя загруженного csv). Пусто = не запускать.
# Пока не чистим: только кладём CSV как пришёл.
NEWS_PARSE_REMOTE_CMD_TMPL = ""
# clean в фоне (nohup): демон не ждёт парсинг; отбивка на почту после успеха.
NEWS_NOTIFY = True
NEWS_NOTIFY_EMAILS = [
    "svibudygin@sberbank.ru",
    "OVaPonomarev@sberbank.ru",
]
NEWS_JOB_LOCAL = os.path.join(SCRIPT_DIR, "news_clean_job.json")
NEWS_RUN_PID = NEWS_REMOTE_DIR.rstrip("/") + "/news_clean_run.pid"
NEWS_RUN_EXIT = NEWS_REMOTE_DIR.rstrip("/") + "/news_clean_run.exit"
NEWS_RUN_LOG = NEWS_REMOTE_DIR.rstrip("/") + "/news_clean_run.log"
NEWS_RUN_META = NEWS_REMOTE_DIR.rstrip("/") + "/news_clean_run.json"
NEWS_SCAN_LIMIT = 50                                        # сколько последних писем просматривать
NEWS_STATE_FILE = os.path.join(SCRIPT_DIR, "news_processed.txt")  # уже обработанные вложения

# --- Weekly pulse (оперативка) из Outlook → папка eco на сервере ---
# Письмо с темой «weekly pulse» (в т.ч. FW:) и вложением *Pulse*.pptx.
# Кладём рядом с Operativka.pdf: .../data/external/eco/
PULSE_ENABLED = True
PULSE_SUBJECT_MATCH = "weekly pulse"  # подстрока темы (без учёта регистра)
PULSE_ATTACH_SUBSTRING = "pulse"      # в имени вложения .pptx должна быть эта подстрока
PULSE_REMOTE_PATH = "/data/s3/projects/synaptica/data/external/eco/Operativka.pptx"
PULSE_SCAN_LIMIT = 50
PULSE_STATE_FILE = os.path.join(SCRIPT_DIR, "pulse_processed.txt")  # уже залитые вложения
PULSE_LOG_EVERY_N_SCANS = 6  # ~ раз в минуту при опросе 10 с

# --- Ежедневная загрузка звонков из Outlook → /call_upload на сервере ---
# Письмо на svibudygin с подстрокой «calls upload» в теме (в т.ч. FW:) и вложением .xlsx/.csv.
# Заливаем в calls_tb и запускаем prepare → tag → merge (как кнопка /call_upload).
CALLS_UPLOAD_ENABLED = True
CALLS_UPLOAD_SUBJECT_MATCH = "calls upload"  # подстрока темы (без учёта регистра)
CALLS_UPLOAD_REMOTE_DIR = "/home/synaptica/synaptica/synaptica/data/calls_tb"
CALLS_UPLOAD_REF = "/home/synaptica/synaptica/synaptica/data/calls_tb/final_calls.xlsx"
CALLS_UPLOAD_SCAN_LIMIT = 50
CALLS_UPLOAD_STATE_FILE = os.path.join(SCRIPT_DIR, "calls_upload_processed.txt")
CALLS_UPLOAD_LOG_EVERY_N_SCANS = 6  # ~ раз в минуту при опросе 10 с
# Пайплайн на сервере в фоне (nohup): демон не ждёт разметку, опрос паролей идёт дальше.
CALLS_UPLOAD_NOTIFY = True  # письмо на ящик скрипта после успешной загрузки
CALLS_UPLOAD_JOB_LOCAL = os.path.join(SCRIPT_DIR, "calls_upload_job.json")
CALLS_UPLOAD_RUN_PID = CALLS_UPLOAD_REMOTE_DIR.rstrip("/") + "/calls_upload_run.pid"
CALLS_UPLOAD_RUN_EXIT = CALLS_UPLOAD_REMOTE_DIR.rstrip("/") + "/calls_upload_run.exit"
CALLS_UPLOAD_RUN_LOG = CALLS_UPLOAD_REMOTE_DIR.rstrip("/") + "/calls_upload_run.log"
CALLS_UPLOAD_RUN_META = CALLS_UPLOAD_REMOTE_DIR.rstrip("/") + "/calls_upload_run.json"
CALLS_UPLOAD_REMOTE_CMD_TMPL = (
    "cd /home/synaptica/synaptica && "
    "PYTHONPATH=/home/synaptica/synaptica "
    "/home/synaptica/synaptica/.venv/bin/python -m "
    "synaptica.backend.streamlit_functions.calls_tag_with_gigachat "
    "--file '{filepath}' "
    "--ref '" + CALLS_UPLOAD_REF + "' "
    "--work-dir '" + CALLS_UPLOAD_REMOTE_DIR + "'"
)

# --- Разметка комментариев из Outlook (от лица Синаптики) ---
# Тема «Обновление комментариев» + pkl/rar/zip/xlsx → uploads/comments → пайплайн в venv.
COMMENTS_UPDATE_ENABLED = True
COMMENTS_SUBJECT_MATCH = "Обновление комментариев"
COMMENTS_ROOT = "/home/synaptica/synaptica"
COMMENTS_REMOTE_DIR = COMMENTS_ROOT + "/synaptica/uploads/comments"
COMMENTS_VORONKI_DIR = COMMENTS_REMOTE_DIR.rstrip("/") + "/voronki"
COMMENTS_PYTHON = COMMENTS_ROOT + "/.venv/bin/python"
COMMENTS_SCRIPT = COMMENTS_REMOTE_DIR.rstrip("/") + "/update_comments_from_funnel.py"
COMMENTS_SCAN_LIMIT = 50
COMMENTS_STATE_FILE = os.path.join(SCRIPT_DIR, "comments_update_processed.txt")
COMMENTS_LOG_EVERY_N_SCANS = 6
COMMENTS_NOTIFY = True
COMMENTS_NOTIFY_EMAILS = [
    "svibudygin@sberbank.ru",
    "avitalantonova@sberbank.ru",
]
COMMENTS_JOB_LOCAL = os.path.join(SCRIPT_DIR, "comments_update_job.json")
COMMENTS_RUN_PID = COMMENTS_REMOTE_DIR.rstrip("/") + "/comments_update_run.pid"
COMMENTS_RUN_EXIT = COMMENTS_REMOTE_DIR.rstrip("/") + "/comments_update_run.exit"
COMMENTS_RUN_LOG = COMMENTS_REMOTE_DIR.rstrip("/") + "/comments_update_run.log"
COMMENTS_RUN_META = COMMENTS_REMOTE_DIR.rstrip("/") + "/comments_update_run.json"
COMMENTS_REMOTE_CMD_TMPL = (
    f"cd {COMMENTS_ROOT} && "
    f"PYTHONPATH={COMMENTS_ROOT} "
    f"{COMMENTS_PYTHON} {COMMENTS_SCRIPT} "
    "--input '{filepath}' "
    f"--comments-dir '{COMMENTS_REMOTE_DIR}'"
)

# --- Рассылка подписок /special_news ---
# Prefs на сервере: /data/s3/projects/synaptica/news/news_prefs/{login}.json
NEWS_PREFS_REMOTE_DIR = "/data/s3/projects/synaptica/news/news_prefs"
NEWS_DIGEST_ENABLED = False
NEWS_DIGEST_EMAIL_DOMAIN = "@sberbank.ru"                   # fallback, если в config.yaml нет email
NEWS_DIGEST_SKIP_EMPTY = True                               # не слать, если подборка пустая
NEWS_DIGEST_SEND_HOUR_FROM = 7                              # окно отправки (локальное время Windows)
NEWS_DIGEST_SEND_HOUR_TO = 23                                 # [FROM, TO) — например 7:00–12:59
NEWS_DIGEST_SSH_TIMEOUT = 90
NEWS_DIGEST_SSH_TIMEOUT = 120  # не блокировать опрос паролей/Ctrl+C на час
NEWS_DIGEST_REMOTE_CMD = (
    "cd /home/synaptica/synaptica && "
    "PYTHONPATH=/home/synaptica/synaptica "
    "/home/synaptica/synaptica/.venv/bin/python "
    "/home/synaptica/synaptica/synaptica/scripts/special_news_digests.py "
    + NEWS_DIGEST_EMAIL_DOMAIN + " --as-of {as_of}"
)

# Тема и тело письма
EMAIL_SUBJECT = "Код для сброса пароля Synaptica"
EMAIL_BODY = """
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333333;
            background-color: #f5f7fa;
            margin: 0;
            padding: 0;
        }}
        .container {{
            background-color: #f5f7fa;
            padding: 40px 20px;
            max-width: 600px;
            margin: 0 auto;
        }}
        .content {{
            background-color: #ffffff;
            border-radius: 10px;
            padding: 40px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
        }}
        .header h1 {{
            color: #2c3e50;
            margin: 0;
            font-size: 28px;
        }}
        .greeting {{
            font-size: 16px;
            color: #555555;
            margin-bottom: 20px;
        }}
        .code-box {{
            background-color: #667eea;
            color: #ffffff;
            font-size: 36px;
            font-weight: bold;
            text-align: center;
            padding: 30px;
            border-radius: 8px;
            letter-spacing: 10px;
            margin: 30px 0;
            font-family: 'Courier New', monospace;
            border: 2px solid #5568d3;
        }}
        .info {{
            background-color: #f8f9fa;
            border-left: 4px solid #667eea;
            padding: 15px;
            margin: 20px 0;
            border-radius: 4px;
        }}
        .footer {{
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #eeeeee;
            text-align: center;
            color: #888888;
            font-size: 14px;
        }}
        .highlight {{
            color: #667eea;
            font-weight: bold;
        }}
        p {{
            margin: 10px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="content">
            <div class="header">
                <h1>🛠️ Synaptica</h1>
            </div>
            
            <div class="greeting">
                <p>Здравствуйте!</p>
                <p>Вас приветствует поддержка <span class="highlight">Синаптики</span> 👋</p>
            </div>
            
            <p>Вы запросили сброс пароля для вашего аккаунта.</p>
            
            <p>Ваш одноразовый код для сброса пароля:</p>
            
            <div class="code-box" style="background-color: #667eea; color: #ffffff; font-size: 36px; font-weight: bold; text-align: center; padding: 30px; border-radius: 8px; letter-spacing: 10px; margin: 30px 0; font-family: 'Courier New', monospace; border: 2px solid #5568d3;">
                {code}
            </div>
            
            <div class="info">
                <p><strong>⚠️ Важно:</strong></p>
                <ul>
                    <li>Код действителен в течение <strong>15 минут</strong></li>
                    <li>Никому не сообщайте этот код</li>
                    <li>Если вы не запрашивали сброс пароля, проигнорируйте это письмо</li>
                </ul>
            </div>
            
            <p>Введите этот код на странице входа в Синаптику для создания нового пароля.</p>
            
            <div class="footer">
                <p>С уважением,<br>Команда поддержки Синаптики</p>
                <p style="font-size: 12px; color: #aaaaaa;">Это автоматическое письмо, пожалуйста, не отвечайте на него.</p>
            </div>
        </div>
    </div>
</body>
</html>
"""


def iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stderr(msg: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}", file=sys.stderr, flush=True)


def _stderr(msg: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}", file=sys.stderr, flush=True)


def code_hash(code: str) -> str:
    return hashlib.sha256(code.strip().encode("utf-8")).hexdigest()


def ssh_cat(ssh: str, path: str) -> str:
    """Читает файл на сервере по SSH (paramiko + SSH_SECRET)."""
    ok, out, err, code = ssh_exec(
        ssh, f"cat {shlex.quote(path)} 2>/dev/null || true", timeout=30
    )
    if code == -1 and not (out or "").strip():
        sys.exit(f"Ошибка SSH: {err or 'нет связи'}")
    return out


def ssh_append(ssh: str, path: str, line: str) -> bool:
    """Дописывает одну строку в файл на сервере по SFTP (без echo — надёжнее на S3)."""
    try:
        user_host = ssh.split("@")
        if len(user_host) != 2:
            print(f"[ssh_append] Неверный формат SSH: {ssh}", file=sys.stderr, flush=True)
            return False
        user, host = user_host
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, username=user, password=SSH_SECRET, timeout=15)
        try:
            sftp = client.open_sftp()
            try:
                with sftp.file(path, "a") as f:
                    f.write(line.rstrip("\n") + "\n")
            finally:
                sftp.close()
        finally:
            client.close()
        return True
    except Exception as e:
        print(f"[ssh_append] Ошибка SSH: {e}", file=sys.stderr, flush=True)
        return False


def parse_log(content: str):
    """Парсит JSONL, возвращает списки событий по типам и счётчики статусов."""
    pending_by_id = {}
    code_set_ids = set()
    verified_ids = set()
    stats = {
        "total_nonempty": 0,
        "invalid_json": 0,
        "without_id": 0,
        "pending": 0,
        "code_set": 0,
        "verified": 0,
        "other_status": 0,
    }
    for line in content.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        stats["total_nonempty"] += 1
        try:
            rec = json.loads(line)
            rid = rec.get("id")
            if not rid:
                stats["without_id"] += 1
                continue
            status = rec.get("status")
            if status == "pending" and "email" in rec:
                pending_by_id[rid] = {
                    "email": (rec.get("email") or "").strip().lower(),
                    "created_at": rec.get("created_at"),
                }
                stats["pending"] += 1
            elif status == "code_set":
                code_set_ids.add(rid)
                stats["code_set"] += 1
            elif status == "verified":
                verified_ids.add(rid)
                stats["verified"] += 1
            else:
                stats["other_status"] += 1
        except json.JSONDecodeError:
            stats["invalid_json"] += 1
            continue
    return pending_by_id, code_set_ids, verified_ids, stats


def get_pending_without_code(pending_by_id, code_set_ids, verified_ids):
    """Заявки pending, для которых ещё не выдан код и не верифицированы."""
    return [
        (rid, data)
        for rid, data in pending_by_id.items()
        if rid not in code_set_ids and rid not in verified_ids
    ]


def _reset_sent_set() -> set:
    try:
        with open(RESET_SENT_FILE, "r", encoding="utf-8") as f:
            return {ln.strip() for ln in f if ln.strip()}
    except FileNotFoundError:
        return set()


def _reset_mark_sent(rid: str) -> None:
    try:
        with open(RESET_SENT_FILE, "a", encoding="utf-8") as f:
            f.write(rid + "\n")
    except Exception as e:
        print(f"[reset_pas_admin] Не удалось записать sent id={rid[:8]}...: {e}", file=sys.stderr, flush=True)


def _pending_created_at(data: dict):
    ts = (data or {}).get("created_at") or ""
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _pending_too_old(data: dict) -> bool:
    created = _pending_created_at(data)
    if created is None:
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - created > timedelta(hours=RESET_PENDING_MAX_AGE_HOURS)


def _newest_pending_per_email(pending_list: list) -> list:
    """Один код на email за цикл — иначе пачка старых pending улетает пачкой писем."""
    best: dict[str, tuple] = {}
    for rid, data in pending_list:
        email = (data.get("email") or "").strip().lower()
        if not email:
            continue
        prev = best.get(email)
        if prev is None:
            best[email] = (rid, data)
            continue
        cur_ts = _pending_created_at(data) or datetime.min.replace(tzinfo=timezone.utc)
        prev_ts = _pending_created_at(prev[1]) or datetime.min.replace(tzinfo=timezone.utc)
        if cur_ts >= prev_ts:
            best[email] = (rid, data)
    return list(best.values())


def _short_id(rid: str, n: int = 8) -> str:
    if not rid:
        return "-"
    if len(rid) <= n:
        return rid
    return f"{rid[:n]}..."


def generate_code(length: int = 6) -> str:
    """Случайный код из 6 цифр."""
    return "".join(random.choices(string.digits, k=length))


def process_one_pending(ssh: str, log_path: str, rid: str, to_email: str, code_valid_minutes: int) -> bool:
    """Генерирует код, пишет code_set на сервер, отправляет письмо."""
    code = generate_code()
    now = datetime.now(timezone.utc)
    expires = now + timedelta(minutes=code_valid_minutes)
    event = {
        "id": rid,
        "set_code_at": iso_now(),
        "code_hash": code_hash(code),
        "expires_at": expires.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "status": "code_set",
    }
    line = json.dumps(event, ensure_ascii=False)
    if not ssh_append(ssh, log_path, line):
        print(f"[reset_pas_admin] Ошибка записи code_set на сервер id={rid[:8]}...", file=sys.stderr)
        return False
    print(f"[reset_pas_admin] code_set записан id={rid[:8]}... email={to_email}", file=sys.stderr)
    # Помечаем локально ДО письма: если S3 не покажет code_set, повторной отправки не будет.
    _reset_mark_sent(rid)

    subject = EMAIL_SUBJECT.format(email=to_email, code=code)
    body = HTMLBody(EMAIL_BODY.format(email=to_email, code=code))
    try:
        send_email(
            account=account,
            subject=subject,
            body=body,
            recipients=[to_email],
        )
        print(f"[reset_pas_admin] Письмо отправлено для {to_email}", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"[reset_pas_admin] Не удалось отправить на {to_email}: {e}. Код вручную: {code}", file=sys.stderr, flush=True)
    return True


def _build_alert_html(sources_rows: list[dict]) -> str:
    """HTML-шаблон письма об устаревших источниках (стиль как у письма сброса пароля)."""
    rows_html = "".join(
        f"""
        <tr>
          <td style="padding:10px 16px;border-bottom:1px solid #eeeeee;font-weight:bold;color:#2c3e50;">{r['source']}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #eeeeee;color:#555555;">{r['since'] or 'нет данных'}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #eeeeee;text-align:center;font-size:20px;">🔴</td>
        </tr>"""
        for r in sources_rows
    )
    return f"""
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background-color:#f5f7fa;font-family:'Segoe UI',Tahoma,Geneva,Verdana,sans-serif;color:#333333;">
  <div style="max-width:600px;margin:40px auto;padding:0 20px;">
    <div style="background:#ffffff;border-radius:10px;padding:40px;box-shadow:0 2px 8px rgba(0,0,0,0.1);">

      <div style="text-align:center;margin-bottom:30px;">
        <h1 style="color:#2c3e50;margin:0;font-size:26px;">🛠️ Synaptica</h1>
        <p style="color:#888888;font-size:14px;margin:6px 0 0;">Светофор данных</p>
      </div>

      <p style="font-size:16px;color:#555555;">Здравствуйте!</p>
      <p style="font-size:15px;color:#333333;">
        Обнаружены <strong>устаревшие источники данных</strong>. Требуется обновление:
      </p>

      <table style="width:100%;border-collapse:collapse;margin:20px 0;font-size:14px;">
        <thead>
          <tr style="background-color:#f8f9fa;">
            <th style="padding:10px 16px;text-align:left;color:#555555;border-bottom:2px solid #dddddd;">Источник</th>
            <th style="padding:10px 16px;text-align:left;color:#555555;border-bottom:2px solid #dddddd;">Последнее обновление</th>
            <th style="padding:10px 16px;text-align:center;color:#555555;border-bottom:2px solid #dddddd;">Статус</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>

      <div style="background:#fff3cd;border-left:4px solid #e67e22;padding:15px;border-radius:4px;margin:20px 0;">
        <p style="margin:0;font-size:14px;color:#856404;">
          ⚠️ Пожалуйста, проверьте источники и загрузите актуальные данные.
        </p>
      </div>

      <div style="margin-top:30px;padding-top:20px;border-top:1px solid #eeeeee;text-align:center;color:#888888;font-size:13px;">
        <p>С уважением,<br>Команда Синаптики</p>
        <p style="font-size:11px;color:#aaaaaa;">Это автоматическое письмо, пожалуйста, не отвечайте на него.</p>
      </div>

    </div>
  </div>
</body>
</html>"""


def parse_alerts_log(content: str):
    """Парсит лог алертов. Возвращает (pending_by_id, sent_ids).

    pending: {id, source, since, status: "pending"}; обработанный помечается status="sent".
    """
    pending_by_id = {}
    sent_ids = set()
    for line in content.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        rid = rec.get("id")
        if not rid:
            continue
        status = rec.get("status")
        if status == "pending" and rec.get("source"):
            pending_by_id[rid] = {"source": rec.get("source"), "since": rec.get("since") or ""}
        elif status == "sent":
            sent_ids.add(rid)
    return pending_by_id, sent_ids


def process_alerts(ssh: str, alerts_path: str, _sent_in_session: set | None = None) -> None:
    """Читает лог алертов; по новым pending шлёт одно письмо и пишет sent обратно.

    _sent_in_session — in-memory кэш уже отправленных ID за текущий сеанс поллера.
    Защищает от повтора, если ssh_append не смог записать "sent" обратно на сервер.
    """
    content = ssh_cat(ssh, alerts_path)
    if not content.strip():
        return
    pending_by_id, sent_ids = parse_alerts_log(content)
    # Объединяем sent из лога и sent из памяти текущего сеанса
    if _sent_in_session is not None:
        sent_ids |= _sent_in_session
    to_send = {rid: data for rid, data in pending_by_id.items() if rid not in sent_ids}
    # Отключённые источники: помечаем sent без письма (иначе pending будет висеть вечно).
    skipped = {
        rid: data
        for rid, data in to_send.items()
        if data.get("source") in ALERT_DISABLED_SOURCES
    }
    for rid in skipped:
        ok = ssh_append(
            ssh,
            alerts_path,
            json.dumps({"id": rid, "status": "sent", "sent_at": iso_now()}, ensure_ascii=False),
        )
        if _sent_in_session is not None:
            _sent_in_session.add(rid)
        if not ok:
            print(f"[alerts] News/disabled: не удалось пометить sent для {rid[:8]}...", file=sys.stderr, flush=True)
    to_send = {rid: data for rid, data in to_send.items() if rid not in skipped}
    if not to_send:
        return
    sources = ", ".join(sorted({data["source"] for data in to_send.values()}))
    since = min(
        (data["since"].split(" ")[0] for data in to_send.values() if data["since"]),
        default="",
    )
    subject = ALERT_SUBJECT_TMPL.format(sources=sources, since=since)
    sources_rows = [
        {"source": data["source"], "since": data["since"]}
        for data in to_send.values()
    ]
    try:
        send_email(
            account=account,
            subject=subject,
            body=HTMLBody(_build_alert_html(sources_rows)),
            recipients=[ALERT_EMAIL_TO],
        )
    except Exception as e:
        print(f"[alerts] Ошибка отправки на {ALERT_EMAIL_TO}: {e}", file=sys.stderr, flush=True)
        return
    # Сразу помечаем как отправленные в памяти — даже если SSH упадёт,
    # в рамках текущего сеанса повтора не будет.
    if _sent_in_session is not None:
        _sent_in_session.update(to_send)
    for rid in to_send:
        ok = ssh_append(
            ssh,
            alerts_path,
            json.dumps({"id": rid, "status": "sent", "sent_at": iso_now()}, ensure_ascii=False),
        )
        if not ok:
            print(f"[alerts] Предупреждение: не удалось записать sent для {rid[:8]}... — повтора не будет (in-memory кэш).", file=sys.stderr, flush=True)
    print(f"[alerts] Письмо отправлено на {ALERT_EMAIL_TO}: {subject}", file=sys.stderr, flush=True)


def ssh_download_binary(ssh: str, remote_path: str, local_path: str) -> bool:
    """Скачивает бинарный файл с сервера по SFTP (paramiko)."""
    user_host = ssh.split("@")
    if len(user_host) != 2:
        print(f"[fi_weekly] Неверный формат SSH: {ssh}", file=sys.stderr, flush=True)
        return False
    user, host = user_host
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, username=user, password=SSH_SECRET, timeout=30)
        try:
            sftp = client.open_sftp()
            try:
                sftp.get(remote_path, local_path)
            finally:
                sftp.close()
        finally:
            client.close()
        return True
    except Exception as e:
        print(f"[fi_weekly] Ошибка скачивания {remote_path}: {e}", file=sys.stderr, flush=True)
        return False


def ssh_upload_binary(ssh: str, local_path: str, remote_path: str) -> bool:
    """Заливает локальный файл на сервер по SFTP (paramiko), создаёт папку при необходимости."""
    user_host = ssh.split("@")
    if len(user_host) != 2:
        print(f"[news] Неверный формат SSH: {ssh}", file=sys.stderr, flush=True)
        return False
    user, host = user_host
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, username=user, password=SSH_SECRET, timeout=30)
        try:
            sftp = client.open_sftp()
            try:
                remote_dir = os.path.dirname(remote_path)
                try:
                    sftp.stat(remote_dir)
                except IOError:
                    client.exec_command(f"mkdir -p '{remote_dir}'")[1].channel.recv_exit_status()
                sftp.put(local_path, remote_path)
            finally:
                sftp.close()
        finally:
            client.close()
        return True
    except Exception as e:
        print(f"[news] Ошибка заливки {remote_path}: {e}", file=sys.stderr, flush=True)
        return False


def ssh_exec(ssh: str, command: str, timeout: int = 600) -> tuple[bool, str, str, int]:
    """Выполняет команду на сервере по SSH. Возвращает (ok, stdout, stderr, exit_code).

    Читает канал короткими тиками, чтобы Ctrl+C и таймаут не зависали на stdout.read().
    """
    user_host = ssh.split("@")
    if len(user_host) != 2:
        return False, "", f"неверный формат SSH: {ssh}", -1
    user, host = user_host
    client = None
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=host,
            username=user,
            password=SSH_SECRET,
            timeout=30,
            banner_timeout=30,
            auth_timeout=30,
        )
        _stdin, stdout, stderr = client.exec_command(command)
        chan = stdout.channel
        chan.settimeout(1.0)
        deadline = time.time() + max(1, int(timeout))
        out_b = bytearray()
        err_b = bytearray()
        while True:
            if time.time() >= deadline:
                try:
                    chan.close()
                except Exception:
                    pass
                return False, out_b.decode("utf-8", "replace"), "ssh timeout", -1
            if chan.recv_ready():
                chunk = chan.recv(65536)
                if chunk:
                    out_b.extend(chunk)
            if chan.recv_stderr_ready():
                chunk = chan.recv_stderr(65536)
                if chunk:
                    err_b.extend(chunk)
            if chan.exit_status_ready() and not chan.recv_ready() and not chan.recv_stderr_ready():
                break
            time.sleep(0.15)
        while chan.recv_ready():
            chunk = chan.recv(65536)
            if not chunk:
                break
            out_b.extend(chunk)
        while chan.recv_stderr_ready():
            chunk = chan.recv_stderr(65536)
            if not chunk:
                break
            err_b.extend(chunk)
        code = chan.recv_exit_status()
        return code == 0, out_b.decode("utf-8", "replace"), err_b.decode("utf-8", "replace"), code
    except KeyboardInterrupt:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        raise
    except Exception as e:
        return False, "", str(e), -1
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def _remote_bg_status(ssh: str, pid_path: str, exit_path: str) -> tuple[str, str]:
    """Статус фонового процесса: none | running | done | dead | unknown + pid/exit/ошибка."""
    cmd = (
        f"exitf={shlex.quote(exit_path)}; "
        f"pidf={shlex.quote(pid_path)}; "
        f"if [ -f \"$exitf\" ]; then echo DONE; tr -d '[:space:]' < \"$exitf\"; exit 0; fi; "
        "if [ -f \"$pidf\" ]; then "
        "pid=$(tr -d '[:space:]' < \"$pidf\"); "
        "if kill -0 \"$pid\" 2>/dev/null; then echo RUNNING; echo \"$pid\"; exit 0; fi; "
        "echo DEAD; echo \"$pid\"; exit 0; fi; "
        "echo NONE"
    )
    ok, out, err, _ = ssh_exec(ssh, cmd, timeout=30)
    text = (out or "").strip()
    if not ok and not text:
        return "unknown", (err or "").strip()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return "none", ""
    kind = lines[0].upper()
    extra = lines[1] if len(lines) > 1 else ""
    if kind == "RUNNING":
        return "running", extra
    if kind == "DONE":
        return "done", extra
    if kind == "DEAD":
        return "dead", extra
    return "none", ""


def _remote_bg_tail(ssh: str, log_path: str, n: int = 40) -> str:
    ok, out, err, _ = ssh_exec(
        ssh, f"tail -n {int(n)} {shlex.quote(log_path)} 2>/dev/null", timeout=30
    )
    return ((out or err or "") if ok else (err or out or "")).strip()


def _remote_bg_cleanup(ssh: str, *paths: str) -> None:
    quoted = " ".join(shlex.quote(p) for p in paths if p)
    if quoted:
        ssh_exec(ssh, f"rm -f {quoted}", timeout=30)


def _remote_bg_start(
    ssh: str, pipeline: str, log_path: str, pid_path: str, exit_path: str
) -> tuple[str, str]:
    """nohup пайплайна на сервере. Возвращает (pid, error)."""
    inner = f"{pipeline}; echo $? > {shlex.quote(exit_path)}"
    start_cmd = (
        f"rm -f {shlex.quote(exit_path)} {shlex.quote(pid_path)}; "
        f"nohup bash -c {shlex.quote(inner)} "
        f"> {shlex.quote(log_path)} 2>&1 </dev/null & "
        f"echo $! > {shlex.quote(pid_path)}; "
        "disown || true; "
        f"sleep 0.2; cat {shlex.quote(pid_path)}"
    )
    ok, out, err, code = ssh_exec(ssh, start_cmd, timeout=30)
    pid = (out or "").strip().splitlines()[-1].strip() if (out or "").strip() else ""
    if not ok or not pid.isdigit():
        return "", f"exit={code} pid={pid!r} {(err or out or '').strip()}"
    return pid, ""


def _admin_notify(subject: str, body: str, *, enabled: bool) -> None:
    if not enabled:
        return
    try:
        send_email(account=account, subject=subject, body=body, recipients=[email])
    except Exception as e:
        print(f"[notify] не удалось отправить «{subject}»: {e}", file=sys.stderr, flush=True)


def _count_jsonl_lines(path: str) -> int:
    """Число непустых строк JSONL (= число документов в бэкапе) или -1 при ошибке."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for ln in f if ln.strip())
    except Exception:
        return -1


def _xlsx_cell_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        value = ", ".join(str(v) for v in value if v not in (None, ""))
    elif isinstance(value, dict):
        value = json.dumps(value, ensure_ascii=False)
    text = str(value).replace("\x00", "")
    if len(text) > 32000:
        text = text[:32000]
    return text


def _xlsx_col_name(idx: int) -> str:
    name = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        name = chr(65 + rem) + name
    return name


def _jsonl_to_xlsx(jsonl_path: str, xlsx_path: str) -> bool:
    """Собирает Excel из JSONL-дампа OpenSearch. Без openpyxl (stdlib zip+xml)."""
    rows = [list(FI_XLSX_COLUMNS)]
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if not isinstance(rec, dict):
                    continue
                rows.append([_xlsx_cell_text(rec.get(col)) for col in FI_XLSX_COLUMNS])
    except Exception as e:
        print(f"[fi_weekly] JSONL→Excel чтение не удалось: {e}", file=sys.stderr, flush=True)
        return False

    sheet_rows = []
    for r_i, row in enumerate(rows, start=1):
        cells = []
        for c_i, val in enumerate(row, start=1):
            ref = f"{_xlsx_col_name(c_i)}{r_i}"
            cells.append(
                f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">'
                f"{_xml_escape(val)}</t></is></c>"
            )
        sheet_rows.append(f'<row r="{r_i}">{"".join(cells)}</row>')
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(sheet_rows)}</sheetData></worksheet>'
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="FI" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/></Relationships>'
    )
    wb_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/></Relationships>'
    )
    ctypes_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        "</Types>"
    )
    try:
        with zipfile.ZipFile(xlsx_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", ctypes_xml)
            zf.writestr("_rels/.rels", rels_xml)
            zf.writestr("xl/workbook.xml", workbook_xml)
            zf.writestr("xl/_rels/workbook.xml.rels", wb_rels_xml)
            zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        return True
    except Exception as e:
        print(f"[fi_weekly] JSONL→Excel запись не удалась: {e}", file=sys.stderr, flush=True)
        return False


def _fi_weekly_due(now: datetime) -> bool:
    """Пора ли слать: нет файла состояния или прошло >= FI_WEEKLY_INTERVAL_DAYS дней."""
    try:
        with open(FI_WEEKLY_STATE_FILE, "r", encoding="utf-8") as f:
            last = datetime.fromisoformat(f.read().strip())
    except (FileNotFoundError, ValueError):
        return True
    return (now - last) >= timedelta(days=FI_WEEKLY_INTERVAL_DAYS)


def _fi_weekly_mark_sent(now: datetime) -> None:
    try:
        with open(FI_WEEKLY_STATE_FILE, "w", encoding="utf-8") as f:
            f.write(now.isoformat())
    except Exception as e:
        print(f"[fi_weekly] Не удалось записать состояние: {e}", file=sys.stderr, flush=True)


def _fi_weekly_html(date_str: str, n_records, xlsx_attached: bool) -> str:
    """HTML-тело письма с еженедельным бэкапом FI."""
    xlsx_li = (
        "<li><b>fi_backup.xlsx</b> — тот же дамп в Excel</li>"
        if xlsx_attached else ""
    )
    return (
        "<html><body style=\"font-family:'Segoe UI',Tahoma,sans-serif;color:#333;line-height:1.6;\">"
        "<p>Здравствуйте!</p>"
        f"<p>Во вложении — еженедельный бэкап базы FI на <b>{date_str}</b>.</p>"
        "<ul><li><b>fi_backup.jsonl</b> — полный дамп из OpenSearch (источник правды; "
        "по нему восстанавливается база)</li>"
        f"{xlsx_li}</ul>"
        f"<p>Всего записей в JSONL: <b>{n_records}</b>.</p>"
        "<p style=\"color:#888;font-size:12px;\">Автоматическое письмо Synaptica.</p>"
        "</body></html>"
    )


def process_fi_weekly(ssh: str) -> None:
    """Раз в неделю: дамп OpenSearch → JSONL, из JSONL собираем Excel, шлём оба."""
    if not FI_WEEKLY_ENABLED:
        return
    now = datetime.now(timezone.utc)
    if not _fi_weekly_due(now):
        return

    import tempfile

    tmp_dir = tempfile.mkdtemp(prefix="fi_weekly_")
    jsonl_local = os.path.join(tmp_dir, "fi_backup.jsonl")
    xlsx_local = os.path.join(tmp_dir, "fi_backup.xlsx")
    try:
        # 1) Свежий дамп из OpenSearch. Без успешного dump старый JSONL не шлём.
        if FI_DUMP_REMOTE_CMD.strip():
            ok, out, err, _ = ssh_exec(ssh, FI_DUMP_REMOTE_CMD)
            if not ok:
                print(
                    f"[fi_weekly] dump не удался — письмо не шлём: {(err or out).strip()}",
                    file=sys.stderr,
                    flush=True,
                )
                return
            print(f"[fi_weekly] dump на сервере: {out.strip() or 'ok'}", file=sys.stderr, flush=True)

        # 2) Главное вложение — JSONL-бэкап. Без него не шлём и не помечаем sent.
        if not ssh_download_binary(ssh, FI_BACKUP_REMOTE_PATH, jsonl_local):
            print("[fi_weekly] JSONL-бэкап не скачан — повтор при следующем опросе.", file=sys.stderr, flush=True)
            return

        # 3) Excel из свежего JSONL, не старый fi_calls.xlsx.
        xlsx_attached = _jsonl_to_xlsx(jsonl_local, xlsx_local)

        attachments = []
        with open(jsonl_local, "rb") as f:
            attachments.append(("fi_backup.jsonl", f.read()))
        if xlsx_attached:
            with open(xlsx_local, "rb") as f:
                attachments.append(("fi_backup.xlsx", f.read()))

        n = _count_jsonl_lines(jsonl_local)
        date_str = now.astimezone().strftime("%d.%m.%Y")
        subject = FI_WEEKLY_SUBJECT_TMPL.format(date=date_str)
        body = HTMLBody(_fi_weekly_html(date_str, n if n >= 0 else "—", xlsx_attached))
        recipients = [x.strip() for x in FI_WEEKLY_EMAIL_TO.replace(";", ",").split(",") if x.strip()]

        try:
            send_email(
                account=account,
                subject=subject,
                body=body,
                recipients=recipients,
                attachments=attachments,
            )
            _fi_weekly_mark_sent(now)
            print(f"[fi_weekly] Бэкап отправлен на {FI_WEEKLY_EMAIL_TO} (записей: {n}).", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"[fi_weekly] Не удалось отправить: {e}", file=sys.stderr, flush=True)
    finally:
        for p in (jsonl_local, xlsx_local):
            try:
                if os.path.isfile(p):
                    os.remove(p)
            except Exception:
                pass
        try:
            os.rmdir(tmp_dir)
        except Exception:
            pass


def _notes_weekly_due(now: datetime) -> bool:
    try:
        with open(NOTES_WEEKLY_STATE_FILE, "r", encoding="utf-8") as f:
            last = datetime.fromisoformat(f.read().strip())
    except (FileNotFoundError, ValueError):
        return True
    return (now - last) >= timedelta(days=NOTES_WEEKLY_INTERVAL_DAYS)


def _notes_weekly_mark_sent(now: datetime) -> None:
    try:
        with open(NOTES_WEEKLY_STATE_FILE, "w", encoding="utf-8") as f:
            f.write(now.isoformat())
    except Exception as e:
        print(f"[notes_weekly] Не удалось записать состояние: {e}", file=sys.stderr, flush=True)


def _fmt_minutes(value) -> str:
    if value in (None, 0, 0.0):
        return "0 мин"
    total = int(round(float(value)))
    hours, minutes = divmod(total, 60)
    if hours and minutes:
        return f"{hours} ч {minutes} мин"
    if hours:
        return f"{hours} ч"
    return f"{minutes} мин"


def _fmt_avg(value) -> str:
    if value in (None, 0, 0.0):
        return "0"
    number = float(value)
    if abs(number - round(number)) < 1e-9:
        return str(int(round(number)))
    return f"{number:.2f}".replace(".", ",")


def _notes_weekly_html(report: dict) -> str:
    """Одна таблица под копирование в PowerPoint: выделить → копировать → вставить."""
    headers = (
        "Команда",
        "Клиентские встречи",
        "Встречи со смежниками",
        "Среднее количество встреч на сотрудника",
        "Среднее количество встреч со смежниками на сотрудника",
        "Всего времени на внешних встречах",
        "Отклонения по посещаемости",
    )
    th = (
        "padding:8px 10px;border:1px solid #1B2838;background:#1B2838;color:#fff;"
        "font-family:Calibri,Arial,sans-serif;font-size:12pt;text-align:center;"
    )
    td = (
        "padding:8px 10px;border:1px solid #1B2838;"
        "font-family:Calibri,Arial,sans-serif;font-size:12pt;text-align:center;"
    )
    head = "<tr>" + "".join(f"<th style=\"{th}\">{html.escape(h)}</th>" for h in headers) + "</tr>"
    body = []
    for row in report.get("summary") or []:
        bold = "font-weight:bold;" if str(row.get("label") or "") == "Итого" else ""
        cells = (
            (str(row.get("label") or ""), td + "text-align:left;" + bold),
            (str(row.get("client") or 0), td + bold),
            (str(row.get("adjacent") or 0), td + bold),
            (_fmt_avg(row.get("avg_client")), td + bold),
            (_fmt_avg(row.get("avg_adjacent")), td + bold),
            (_fmt_minutes(row.get("external_minutes")), td + bold),
            ("—", td + bold),
        )
        body.append(
            "<tr>" + "".join(
                f"<td style=\"{style}\">{html.escape(text)}</td>" for text, style in cells
            ) + "</tr>"
        )
    table = (
        "<table border=\"1\" cellspacing=\"0\" cellpadding=\"8\" "
        "style=\"border-collapse:collapse;\">"
        + head + "".join(body) + "</table>"
    )
    return (
        "<html><body style=\"font-family:Calibri,Arial,sans-serif;color:#111;\">"
        f"<p>Сводка встреч за <b>{html.escape(report.get('from', ''))}</b> — "
        f"<b>{html.escape(report.get('to', ''))}</b>.</p>"
        "<p>Таблицу можно выделить целиком, скопировать и вставить в PowerPoint.</p>"
        f"<p>Разбивка по сотрудникам: "
        f"<a href=\"{html.escape(NOTES_WEEKLY_DASH_URL)}\">{html.escape(NOTES_WEEKLY_DASH_URL)}</a></p>"
        + table
        + "</body></html>"
    )


def _parse_notes_report(stdout: str):
    for line in reversed((stdout or "").splitlines()):
        if line.startswith("NOTES_REPORT_JSON "):
            return json.loads(line[len("NOTES_REPORT_JSON "):])
    return None


def process_notes_weekly(ssh: str) -> None:
    """Раз в неделю: сводная таблица встреч на почту. По сейлзам — дашборд :3333."""
    if not NOTES_WEEKLY_ENABLED:
        return
    now = datetime.now(timezone.utc)
    if not _notes_weekly_due(now):
        return
    ok, out, err, _ = ssh_exec(ssh, NOTES_REPORT_REMOTE_CMD)
    report = _parse_notes_report(out) if ok else None
    if not report:
        print(
            f"[notes_weekly] отчёт не собран — письмо не шлём: {(err or out).strip()}",
            file=sys.stderr,
            flush=True,
        )
        return
    date_to = str(report.get("to") or now.astimezone().strftime("%Y-%m-%d"))
    try:
        shown = datetime.strptime(date_to, "%Y-%m-%d").strftime("%d.%m.%Y")
    except ValueError:
        shown = date_to
    try:
        send_email(
            account=account,
            subject=f"Заметки команд за неделю ({shown})",
            body=HTMLBody(_notes_weekly_html(report)),
            recipients=[NOTES_WEEKLY_EMAIL_TO],
        )
        _notes_weekly_mark_sent(now)
        print(f"[notes_weekly] отчёт отправлен на {NOTES_WEEKLY_EMAIL_TO}.", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"[notes_weekly] Не удалось отправить: {e}", file=sys.stderr, flush=True)


def _news_log(msg: str) -> None:
    _stderr(f"[news] {msg}")


def _news_notify(subject: str, body: str) -> None:
    if not NEWS_NOTIFY:
        return
    recipients = []
    for addr in list(NEWS_NOTIFY_EMAILS or []) + [email]:
        addr = str(addr or "").strip()
        if addr and addr not in recipients:
            recipients.append(addr)
    html_body = HTMLBody("<pre>" + html.escape(body) + "</pre>")
    for addr in recipients:
        try:
            send_email(account=account, subject=subject, body=html_body, recipients=[addr])
            _news_log(f"отбивка ушла на {addr}")
        except Exception as e:
            _news_log(f"не удалось отправить отбивку на {addr}: {e}")


def _news_processed_set() -> set:
    """Имена уже обработанных вложений (по одному на строку)."""
    try:
        with open(NEWS_STATE_FILE, "r", encoding="utf-8") as f:
            return {ln.strip() for ln in f if ln.strip()}
    except FileNotFoundError:
        return set()


def _news_mark_processed(filename: str) -> None:
    try:
        with open(NEWS_STATE_FILE, "a", encoding="utf-8") as f:
            f.write(filename + "\n")
    except Exception as e:
        print(f"[news] Не удалось записать состояние: {e}", file=sys.stderr, flush=True)


def _news_find_csv(item):
    """Первое вложение .csv с нужным префиксом. Возвращает (attachment, filename) или (None, '')."""
    try:
        count = item.Attachments.Count
    except Exception:
        return None, ""
    for i in range(1, count + 1):
        att = item.Attachments.Item(i)
        try:
            fname = str(att.FileName or "")
        except Exception:
            continue
        low = fname.lower()
        if low.endswith(".csv") and (not NEWS_ATTACH_PREFIX or low.startswith(NEWS_ATTACH_PREFIX.lower())):
            return att, fname
    return None, ""


def _markdown_digest_to_html(md: str) -> str:
    """Простой markdown → HTML для дайджеста special_news."""
    parts: list[str] = []
    for line in md.splitlines():
        raw = line.rstrip()
        if not raw:
            parts.append("<br>")
            continue
        esc = html.escape(raw)
        esc = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", esc)
        esc = re.sub(r"\[(.+?)\]\((.+?)\)", r'<a href="\2">\1</a>', esc)
        if esc.startswith("### "):
            parts.append(f"<h3>{esc[4:]}</h3>")
        elif esc.startswith("## "):
            parts.append(f"<h2>{esc[3:]}</h2>")
        elif esc.startswith("_") and esc.endswith("_") and len(esc) > 2:
            parts.append(f"<p><em>{esc.strip('_')}</em></p>")
        else:
            parts.append(f"<p>{esc}</p>")
    body = "\n".join(parts)
    return (
        '<html><head><meta charset="UTF-8"></head>'
        '<body style="font-family:\'Segoe UI\',Tahoma,sans-serif;color:#333;line-height:1.5;'
        'max-width:720px;margin:0 auto;padding:16px;">'
        f"{body}"
        '<p style="color:#888;font-size:12px;margin-top:24px;">'
        "Автоматическая рассылка Synaptica /special_news</p></body></html>"
    )


def _digest_sent_set() -> set[tuple[str, str, str]]:
    try:
        with open(NEWS_DIGEST_STATE_FILE, "r", encoding="utf-8") as f:
            out: set[tuple[str, str, str]] = set()
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                parts = ln.split("\t")
                if len(parts) >= 3:
                    out.add((parts[0], parts[1], parts[2]))
            return out
    except FileNotFoundError:
        return set()


def _digest_mark_sent(period: str, period_key: str, username: str) -> None:
    try:
        with open(NEWS_DIGEST_STATE_FILE, "a", encoding="utf-8") as f:
            f.write(f"{period}\t{period_key}\t{username}\n")
    except Exception as e:
        _news_log(f"не удалось записать digest state: {e}")


def _in_digest_send_window(now: datetime | None = None) -> bool:
    now = now or datetime.now()
    return NEWS_DIGEST_SEND_HOUR_FROM <= now.hour < NEWS_DIGEST_SEND_HOUR_TO


def _parse_digest_json(raw: str) -> list:
    """stdout SSH: чистый JSON или мусор+log_progress + JSON в конце."""
    text = (raw or "").strip()
    if not text:
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("[")
        end = text.rfind("]")
        if start < 0 or end <= start:
            raise
        data = json.loads(text[start : end + 1])
    if not isinstance(data, list):
        raise json.JSONDecodeError("ожидался JSON-массив", text, 0)
    return data


def process_scheduled_news_digests(ssh: str) -> None:
    """Рассылка по расписанию: день=утро за вчера, неделя=пт, месяц=1-е за прошлый месяц."""
    if not NEWS_DIGEST_ENABLED:
        return
    if not _in_digest_send_window():
        return

    as_of = date.today().isoformat()
    cmd_tpl = (NEWS_DIGEST_REMOTE_CMD or "").strip()
    if not cmd_tpl:
        return

    _news_log(
        f"рассылка: окно {NEWS_DIGEST_SEND_HOUR_FROM}:00–{NEWS_DIGEST_SEND_HOUR_TO}:00, "
        f"as_of={as_of}, жду SSH ≤{NEWS_DIGEST_SSH_TIMEOUT}с"
    )
    ok, out, err, exit_code = ssh_exec(
        ssh, cmd_tpl.format(as_of=as_of), timeout=NEWS_DIGEST_SSH_TIMEOUT
    )
    if not ok:
        tail = (err or out or "").strip() or "нет вывода"
        if "timeout" in tail.lower() or exit_code == -1:
            _news_log(f"дайджесты: SSH не дождался за {NEWS_DIGEST_SSH_TIMEOUT}с — иду дальше по опросу")
            return
        _news_log(f"дайджесты не собраны (exit={exit_code}): {tail[:800]}")
        return

    try:
        items = _parse_digest_json(out)
    except json.JSONDecodeError as e:
        _news_log(f"некорректный JSON дайджестов: {e}")
        if err.strip():
            _news_log(f"stderr: {err.strip()[:600]}")
        if out.strip():
            _news_log(f"stdout: {out.strip()[:600]}")
        return
    if not items:
        _news_log("рассылка: подписчиков с готовым дайджестом=0 (пустой JSON — проверьте sn_summary и news_prefs)")
        return

    sent_set = _digest_sent_set()
    sent_n = skipped_n = fail_n = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        username = str(item.get("username") or "").strip()
        email = str(item.get("email") or "").strip()
        subject = str(item.get("subject") or "Synaptica — новости").strip()
        html_body = str(item.get("html") or "").strip()
        markdown = str(item.get("markdown") or "")
        count = int(item.get("count") or 0)
        period = str(item.get("summary_period") or "day")
        period_key = str(item.get("period_key") or "")
        if not username or not email or not period_key:
            continue
        dedup = (period, period_key, username)
        if dedup in sent_set:
            skipped_n += 1
            continue
        if NEWS_DIGEST_SKIP_EMPTY and count <= 0:
            _news_log(f"пропуск {email}: пусто ({period_key})")
            skipped_n += 1
            continue
        if not html_body:
            html_body = _markdown_digest_to_html(markdown)
        try:
            send_email(
                account=account,
                subject=subject,
                body=HTMLBody(html_body),
                recipients=[email],
            )
            _digest_mark_sent(period, period_key, username)
            sent_set.add(dedup)
            sent_n += 1
            _news_log(f"дайджест → {email}: {count} нов. ({period}, {period_key})")
        except Exception as e:
            fail_n += 1
            _news_log(f"ошибка отправки на {email}: {e}")

    if sent_n or fail_n:
        _news_log(f"рассылка итог: отправлено={sent_n}, пропущено={skipped_n}, ошибок={fail_n}")


def _news_job_read() -> dict:
    try:
        with open(NEWS_JOB_LOCAL, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        _news_log(f"не прочитал локальный job: {e}")
        return {}


def _news_job_write(meta: dict) -> None:
    try:
        with open(NEWS_JOB_LOCAL, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except Exception as e:
        _news_log(f"не записал локальный job: {e}")


def _news_job_clear() -> None:
    try:
        if os.path.isfile(NEWS_JOB_LOCAL):
            os.remove(NEWS_JOB_LOCAL)
    except Exception:
        pass


def _news_load_meta(ssh: str) -> dict:
    meta = _news_job_read()
    if meta.get("fname"):
        return meta
    ok, out, _, _ = ssh_exec(ssh, f"cat {shlex.quote(NEWS_RUN_META)} 2>/dev/null", timeout=30)
    if not ok or not (out or "").strip():
        return meta
    try:
        data = json.loads(out)
        return data if isinstance(data, dict) else meta
    except Exception:
        return meta


def _news_finish_job(ssh: str, status: str, extra: str) -> None:
    meta = _news_load_meta(ssh)
    log_tail = _remote_bg_tail(ssh, NEWS_RUN_LOG, 40)
    fname = str(meta.get("fname") or "news.csv")
    subject = str(meta.get("subject") or "")
    remote_path = str(meta.get("remote_path") or "")
    size = meta.get("size") or 0

    if status == "dead":
        _news_log(f"парсинг умер без кода (pid={extra or '?'}) — повтор на следующем цикле")
        if log_tail:
            for ln in log_tail.splitlines()[-15:]:
                _news_log(f"  [clean] {ln}")
        _remote_bg_cleanup(ssh, NEWS_RUN_PID, NEWS_RUN_EXIT, NEWS_RUN_META)
        _news_job_clear()
        return

    try:
        exit_code = int(str(extra).strip() or "1")
    except ValueError:
        exit_code = 1

    if log_tail:
        for ln in log_tail.splitlines()[-40:]:
            _news_log(f"  [clean] {ln}")

    if exit_code == 0:
        _news_mark_processed(fname)
        _news_log(f"готово: {fname} → {remote_path}")
        _news_notify(
            f"Synaptica: новости загружены ({fname})",
            (
                f"Письмо: {subject}\n"
                f"Вложение: {fname} ({size:,} байт)\n"
                f"На сервере: {remote_path}\n\n"
                f"{log_tail or 'парсинг завершился без вывода'}"
            ),
        )
    else:
        _news_log(
            f"парсинг завершился с ошибкой (exit={exit_code}) — повтор на следующем цикле"
        )
    _remote_bg_cleanup(ssh, NEWS_RUN_PID, NEWS_RUN_EXIT, NEWS_RUN_META)
    _news_job_clear()


def process_news_csv(ssh: str) -> None:
    """Письмо → SFTP → news_clean в фоне. Демон не ждёт парсинг."""
    if not NEWS_ENABLED:
        return
    import tempfile

    if not hasattr(process_news_csv, "_scan_no"):
        process_news_csv._scan_no = 0  # type: ignore[attr-defined]
    process_news_csv._scan_no += 1  # type: ignore[attr-defined]
    scan_no: int = process_news_csv._scan_no  # type: ignore[attr-defined]
    verbose = (NEWS_LOG_EVERY_N_SCANS <= 1) or (scan_no % NEWS_LOG_EVERY_N_SCANS == 1)

    parse_cmd = (NEWS_PARSE_REMOTE_CMD_TMPL or "").strip()
    if parse_cmd:
        status, extra = _remote_bg_status(ssh, NEWS_RUN_PID, NEWS_RUN_EXIT)
        if status == "unknown":
            _news_log(f"статус на сервере недоступен: {extra}")
            return
        if status == "running":
            if verbose:
                _news_log(f"парсинг ещё идёт (pid={extra or '?'})")
                tail = _remote_bg_tail(ssh, NEWS_RUN_LOG, 5)
                for ln in (tail.splitlines() if tail else []):
                    _news_log(f"  [clean] {ln}")
            return
        if status in ("done", "dead"):
            _news_finish_job(ssh, status, extra)
            return

    outlook = win32com.client.Dispatch("Outlook.Application")
    namespace = outlook.GetNamespace("MAPI")
    inbox = namespace.GetDefaultFolder(6)  # 6 = Входящие
    items = inbox.Items
    items.Sort("[ReceivedTime]", True)

    processed = _news_processed_set()
    subj_match = (NEWS_SUBJECT_MATCH or "").lower()

    stats = {
        "scanned": 0,
        "subject_ok": 0,
        "csv_ok": 0,
        "already_done": 0,
    }

    scanned = 0
    for item in items:
        if scanned >= NEWS_SCAN_LIMIT:
            break
        try:
            if item.Class != 43:  # olMail
                continue
            scanned += 1
            stats["scanned"] += 1
            subject = str(getattr(item, "Subject", "") or "")
            if subj_match and subj_match not in subject.lower():
                continue
            stats["subject_ok"] += 1
            att, fname = _news_find_csv(item)
            if not att:
                continue
            stats["csv_ok"] += 1
            if fname in processed:
                stats["already_done"] += 1
                continue
            tmp_dir = tempfile.mkdtemp(prefix="news_csv_")
            local_path = os.path.join(tmp_dir, fname)
            try:
                _news_log(f"найдено письмо: «{subject}», вложение {fname}")
                att.SaveAsFile(local_path)
                local_size = os.path.getsize(local_path) if os.path.isfile(local_path) else 0
                _news_log(f"вложение сохранено локально: {local_path} ({local_size:,} байт)")
                remote_path = f"{NEWS_REMOTE_DIR.rstrip('/')}/{fname}"
                if not ssh_upload_binary(ssh, local_path, remote_path):
                    _news_log(f"заливка НЕ удалась — повтор при следующем опросе: {remote_path}")
                    return
                ok_stat, out_stat, err_stat, _ = ssh_exec(
                    ssh, f"ls -la '{remote_path}' 2>&1", timeout=30
                )
                if ok_stat:
                    _news_log(f"CSV на сервере: {out_stat.strip()}")
                else:
                    _news_log(f"заливка прошла, но ls на сервере не ок: {(err_stat or out_stat).strip()}")
                cmd = (NEWS_PARSE_REMOTE_CMD_TMPL or "").strip()
                if not cmd:
                    _news_mark_processed(fname)
                    _news_log(f"готово (без парсинга): {fname} → {remote_path}")
                    _news_notify(
                        f"Synaptica: новости залиты ({fname})",
                        (
                            f"Письмо: {subject}\n"
                            f"Вложение: {fname} ({local_size:,} байт)\n"
                            f"На сервере: {remote_path}\n"
                            "Парсинг выключен, файл как есть."
                        ),
                    )
                    return
                meta = {
                    "fname": fname,
                    "subject": subject,
                    "remote_path": remote_path,
                    "size": local_size,
                    "started": datetime.now().isoformat(timespec="seconds"),
                }
                _news_job_write(meta)
                meta_tmp = os.path.join(tmp_dir, "news_clean_run.json")
                with open(meta_tmp, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False)
                ssh_upload_binary(ssh, meta_tmp, NEWS_RUN_META)
                pipeline = cmd.format(filename=shlex.quote(fname))
                pid, err = _remote_bg_start(
                    ssh, pipeline, NEWS_RUN_LOG, NEWS_RUN_PID, NEWS_RUN_EXIT
                )
                if not pid:
                    _news_log(f"не удалось запустить фон: {err}")
                    _news_job_clear()
                    return
                _news_log(
                    f"парсинг запущен в фоне pid={pid}, демон не ждёт (лог {NEWS_RUN_LOG})"
                )
            finally:
                try:
                    if os.path.isfile(local_path):
                        os.remove(local_path)
                    meta_tmp = os.path.join(tmp_dir, "news_clean_run.json")
                    if os.path.isfile(meta_tmp):
                        os.remove(meta_tmp)
                    os.rmdir(tmp_dir)
                except Exception:
                    pass
            return
        except Exception as e:
            _news_log(f"ошибка на письме: {e}")

    if verbose:
        _news_log(
            "scan: "
            f"просмотрено={stats['scanned']}, "
            f"тема={stats['subject_ok']}, "
            f"csv={stats['csv_ok']}, "
            f"уже_обработано={stats['already_done']}, "
            f"новых=0, "
            f"processed={len(processed)} шт."
        )
        if stats["csv_ok"] and stats["already_done"] == stats["csv_ok"]:
            _news_log("все найденные CSV уже в news_processed.txt — жду новое письмо")
        elif stats["subject_ok"] and not stats["csv_ok"]:
            _news_log("письма с нужной темой есть, но без вложения news_*.csv")
        elif not stats["subject_ok"]:
            _news_log(f"во «Входящих» (последние {NEWS_SCAN_LIMIT}) нет писем с темой «{NEWS_SUBJECT_MATCH}»")


def _pulse_log(msg: str) -> None:
    _stderr(f"[pulse] {msg}")


def _pulse_processed_set() -> set:
    try:
        with open(PULSE_STATE_FILE, "r", encoding="utf-8") as f:
            return {ln.strip() for ln in f if ln.strip()}
    except FileNotFoundError:
        return set()


def _pulse_mark_processed(filename: str) -> None:
    try:
        with open(PULSE_STATE_FILE, "a", encoding="utf-8") as f:
            f.write(filename + "\n")
    except Exception as e:
        _pulse_log(f"не удалось записать состояние: {e}")


def _pulse_find_pptx(item):
    """Первое вложение .pptx с подстрокой Pulse в имени. Возвращает (att, fname) или (None, '')."""
    try:
        count = item.Attachments.Count
    except Exception:
        return None, ""
    needle = (PULSE_ATTACH_SUBSTRING or "").lower()
    for i in range(1, count + 1):
        att = item.Attachments.Item(i)
        try:
            fname = str(att.FileName or "")
        except Exception:
            continue
        low = fname.lower()
        if low.endswith(".pptx") and (not needle or needle in low):
            return att, fname
    return None, ""


def process_pulse_pptx(ssh: str) -> None:
    """Достаёт из Outlook Pulse.pptx (weekly pulse) и заливает в eco рядом с Operativka.pdf."""
    if not PULSE_ENABLED:
        return
    import tempfile

    if not hasattr(process_pulse_pptx, "_scan_no"):
        process_pulse_pptx._scan_no = 0  # type: ignore[attr-defined]
    process_pulse_pptx._scan_no += 1  # type: ignore[attr-defined]
    scan_no: int = process_pulse_pptx._scan_no  # type: ignore[attr-defined]
    verbose = (PULSE_LOG_EVERY_N_SCANS <= 1) or (scan_no % PULSE_LOG_EVERY_N_SCANS == 1)

    outlook = win32com.client.Dispatch("Outlook.Application")
    namespace = outlook.GetNamespace("MAPI")
    inbox = namespace.GetDefaultFolder(6)  # 6 = Входящие
    items = inbox.Items
    items.Sort("[ReceivedTime]", True)

    processed = _pulse_processed_set()
    subj_match = (PULSE_SUBJECT_MATCH or "").lower()
    stats = {"scanned": 0, "subject_ok": 0, "pptx_ok": 0, "already_done": 0}

    scanned = 0
    for item in items:
        if scanned >= PULSE_SCAN_LIMIT:
            break
        try:
            if item.Class != 43:  # olMail
                continue
            scanned += 1
            stats["scanned"] += 1
            subject = str(getattr(item, "Subject", "") or "")
            if subj_match and subj_match not in subject.lower():
                continue
            stats["subject_ok"] += 1
            att, fname = _pulse_find_pptx(item)
            if not att:
                continue
            stats["pptx_ok"] += 1
            if fname in processed:
                stats["already_done"] += 1
                continue

            tmp_dir = tempfile.mkdtemp(prefix="pulse_pptx_")
            local_path = os.path.join(tmp_dir, fname)
            try:
                _pulse_log(f"найдено письмо: «{subject}», вложение {fname}")
                att.SaveAsFile(local_path)
                local_size = os.path.getsize(local_path) if os.path.isfile(local_path) else 0
                _pulse_log(f"вложение сохранено локально: {local_path} ({local_size:,} байт)")
                if not ssh_upload_binary(ssh, local_path, PULSE_REMOTE_PATH):
                    _pulse_log(f"заливка НЕ удалась — повтор при следующем опросе: {PULSE_REMOTE_PATH}")
                    return
                ok_stat, out_stat, err_stat, _ = ssh_exec(
                    ssh, f"ls -la '{PULSE_REMOTE_PATH}' 2>&1", timeout=30
                )
                if ok_stat:
                    _pulse_log(f"файл на сервере: {out_stat.strip()}")
                else:
                    _pulse_log(f"заливка прошла, но ls не ок: {(err_stat or out_stat).strip()}")
                _pulse_mark_processed(fname)
                _pulse_log(f"готово: {fname} → {PULSE_REMOTE_PATH}")
            finally:
                try:
                    if os.path.isfile(local_path):
                        os.remove(local_path)
                    os.rmdir(tmp_dir)
                except Exception:
                    pass
            return
        except Exception as e:
            _pulse_log(f"ошибка на письме: {e}")

    if verbose:
        _pulse_log(
            "scan: "
            f"просмотрено={stats['scanned']}, "
            f"тема={stats['subject_ok']}, "
            f"pptx={stats['pptx_ok']}, "
            f"уже_обработано={stats['already_done']}, "
            f"новых=0, "
            f"processed={len(processed)} шт."
        )


def _calls_upload_log(msg: str) -> None:
    _stderr(f"[calls_upload] {msg}")


def _calls_upload_processed_set() -> set:
    try:
        with open(CALLS_UPLOAD_STATE_FILE, "r", encoding="utf-8") as f:
            return {ln.strip() for ln in f if ln.strip()}
    except FileNotFoundError:
        return set()


def _calls_upload_mark_processed(key: str) -> None:
    try:
        with open(CALLS_UPLOAD_STATE_FILE, "a", encoding="utf-8") as f:
            f.write(key + "\n")
    except Exception as e:
        _calls_upload_log(f"не удалось записать состояние: {e}")


def _calls_upload_item_key(item, fname: str) -> str:
    entry_id = str(getattr(item, "EntryID", "") or "").strip()
    received = str(getattr(item, "ReceivedTime", "") or "").strip()
    if entry_id:
        return f"{entry_id}\t{fname}"
    return f"{received}\t{fname}"


def _calls_upload_find_table(item):
    """Первое вложение .xlsx / .xls / .csv. Предпочитает .xlsx. (att, fname) или (None, '')."""
    try:
        count = item.Attachments.Count
    except Exception:
        return None, ""
    found = []
    for i in range(1, count + 1):
        att = item.Attachments.Item(i)
        try:
            fname = str(att.FileName or "")
        except Exception:
            continue
        low = fname.lower()
        if low.endswith((".xlsx", ".xls", ".csv")):
            found.append((att, fname, low))
    if not found:
        return None, ""
    for att, fname, low in found:
        if low.endswith(".xlsx"):
            return att, fname
    return found[0][0], found[0][1]


def _calls_upload_safe_name(fname: str) -> str:
    base = os.path.basename(fname).replace(" ", "_")
    return "".join(c if (c.isalnum() or c in "._-") else "_" for c in base) or "calls.xlsx"


def _calls_upload_notify(subject: str, body: str) -> None:
    _admin_notify(subject, body, enabled=CALLS_UPLOAD_NOTIFY)


def _calls_upload_job_read() -> dict:
    try:
        with open(CALLS_UPLOAD_JOB_LOCAL, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        _calls_upload_log(f"не прочитал локальный job: {e}")
        return {}


def _calls_upload_job_write(meta: dict) -> None:
    try:
        with open(CALLS_UPLOAD_JOB_LOCAL, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except Exception as e:
        _calls_upload_log(f"не записал локальный job: {e}")


def _calls_upload_job_clear() -> None:
    try:
        if os.path.isfile(CALLS_UPLOAD_JOB_LOCAL):
            os.remove(CALLS_UPLOAD_JOB_LOCAL)
    except Exception:
        pass


def _calls_upload_remote_status(ssh: str) -> tuple[str, str]:
    """Статус фонового пайплайна на сервере: none | running | done | dead + pid/exit."""
    status, extra = _remote_bg_status(ssh, CALLS_UPLOAD_RUN_PID, CALLS_UPLOAD_RUN_EXIT)
    if status == "unknown":
        _calls_upload_log(f"статус на сервере недоступен: {extra}")
        return "unknown", ""
    return status, extra


def _calls_upload_tail_log(ssh: str, n: int = 40) -> str:
    return _remote_bg_tail(ssh, CALLS_UPLOAD_RUN_LOG, n)


def _calls_upload_cleanup_remote(ssh: str) -> None:
    _remote_bg_cleanup(
        ssh, CALLS_UPLOAD_RUN_PID, CALLS_UPLOAD_RUN_EXIT, CALLS_UPLOAD_RUN_META
    )


def _calls_upload_load_meta(ssh: str) -> dict:
    meta = _calls_upload_job_read()
    if meta.get("item_key"):
        return meta
    ok, out, _, _ = ssh_exec(ssh, f"cat {shlex.quote(CALLS_UPLOAD_RUN_META)} 2>/dev/null", timeout=30)
    if not ok or not (out or "").strip():
        return meta
    try:
        data = json.loads(out)
        return data if isinstance(data, dict) else meta
    except Exception:
        return meta


def _calls_upload_start_remote(ssh: str, remote_path: str) -> str:
    """Запускает пайплайн через nohup и сразу возвращает pid (или '')."""
    pipeline = CALLS_UPLOAD_REMOTE_CMD_TMPL.format(filepath=remote_path)
    pid, err = _remote_bg_start(
        ssh, pipeline, CALLS_UPLOAD_RUN_LOG, CALLS_UPLOAD_RUN_PID, CALLS_UPLOAD_RUN_EXIT
    )
    if not pid:
        _calls_upload_log(f"не удалось запустить фон: {err}")
        return ""
    return pid


def _calls_upload_finish_job(ssh: str, status: str, extra: str) -> None:
    meta = _calls_upload_load_meta(ssh)
    log_tail = _calls_upload_tail_log(ssh, 40)
    fname = str(meta.get("fname") or "calls.xlsx")
    subject = str(meta.get("subject") or "")
    remote_path = str(meta.get("remote_path") or "")
    item_key = str(meta.get("item_key") or "")
    size = meta.get("size") or 0

    if status == "dead":
        _calls_upload_log(
            f"пайплайн умер без кода (pid={extra or '?'}) — повтор на следующем цикле"
        )
        if log_tail:
            for ln in log_tail.splitlines()[-15:]:
                _calls_upload_log(f"  [pipeline] {ln}")
        _calls_upload_cleanup_remote(ssh)
        _calls_upload_job_clear()
        return

    try:
        exit_code = int(str(extra).strip() or "1")
    except ValueError:
        exit_code = 1

    if log_tail:
        for ln in log_tail.splitlines()[-40:]:
            _calls_upload_log(f"  [pipeline] {ln}")

    if exit_code == 0:
        if item_key:
            _calls_upload_mark_processed(item_key)
        _calls_upload_log(f"готово: {fname} → {remote_path}")
        _calls_upload_notify(
            subject=f"Synaptica: звонки загружены ({fname})",
            body=(
                f"Письмо: {subject}\n"
                f"Вложение: {fname} ({size:,} байт)\n"
                f"На сервере: {remote_path}\n\n"
                f"{log_tail or 'пайплайн завершился без вывода'}"
            ),
        )
    else:
        _calls_upload_log(
            f"пайплайн завершился с ошибкой (exit={exit_code}) — повтор на следующем цикле"
        )
    _calls_upload_cleanup_remote(ssh)
    _calls_upload_job_clear()


def process_calls_upload(ssh: str) -> None:
    """Письмо → SFTP → пайплайн в фоне на сервере. Демон не ждёт разметку."""
    if not CALLS_UPLOAD_ENABLED:
        return
    import tempfile

    if not hasattr(process_calls_upload, "_scan_no"):
        process_calls_upload._scan_no = 0  # type: ignore[attr-defined]
    process_calls_upload._scan_no += 1  # type: ignore[attr-defined]
    scan_no: int = process_calls_upload._scan_no  # type: ignore[attr-defined]
    verbose = (CALLS_UPLOAD_LOG_EVERY_N_SCANS <= 1) or (
        scan_no % CALLS_UPLOAD_LOG_EVERY_N_SCANS == 1
    )

    status, extra = _calls_upload_remote_status(ssh)
    if status == "running":
        if verbose:
            _calls_upload_log(f"пайплайн ещё идёт (pid={extra or '?'})")
            tail = _calls_upload_tail_log(ssh, 5)
            for ln in (tail.splitlines() if tail else []):
                _calls_upload_log(f"  [pipeline] {ln}")
        return
    if status in ("done", "dead"):
        _calls_upload_finish_job(ssh, status, extra)
        return
    if status == "unknown":
        return

    outlook = win32com.client.Dispatch("Outlook.Application")
    namespace = outlook.GetNamespace("MAPI")
    inbox = namespace.GetDefaultFolder(6)  # 6 = Входящие
    items = inbox.Items
    items.Sort("[ReceivedTime]", True)

    processed = _calls_upload_processed_set()
    subj_match = (CALLS_UPLOAD_SUBJECT_MATCH or "").lower()
    stats = {"scanned": 0, "subject_ok": 0, "file_ok": 0, "already_done": 0}

    scanned = 0
    for item in items:
        if scanned >= CALLS_UPLOAD_SCAN_LIMIT:
            break
        try:
            if item.Class != 43:  # olMail
                continue
            scanned += 1
            stats["scanned"] += 1
            subject = str(getattr(item, "Subject", "") or "")
            if subj_match and subj_match not in subject.lower():
                continue
            stats["subject_ok"] += 1
            att, fname = _calls_upload_find_table(item)
            if not att:
                continue
            stats["file_ok"] += 1
            item_key = _calls_upload_item_key(item, fname)
            if item_key in processed:
                stats["already_done"] += 1
                continue

            tmp_dir = tempfile.mkdtemp(prefix="calls_upload_")
            local_path = os.path.join(tmp_dir, _calls_upload_safe_name(fname))
            try:
                _calls_upload_log(f"найдено письмо: «{subject}», вложение {fname}")
                att.SaveAsFile(local_path)
                local_size = os.path.getsize(local_path) if os.path.isfile(local_path) else 0
                _calls_upload_log(f"вложение сохранено локально: {local_path} ({local_size:,} байт)")
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                remote_name = f"mail_{stamp}_{_calls_upload_safe_name(fname)}"
                remote_path = f"{CALLS_UPLOAD_REMOTE_DIR.rstrip('/')}/{remote_name}"
                if not ssh_upload_binary(ssh, local_path, remote_path):
                    _calls_upload_log(f"заливка НЕ удалась — повтор при следующем опросе: {remote_path}")
                    return
                ok_stat, out_stat, err_stat, _ = ssh_exec(
                    ssh, f"ls -la '{remote_path}' 2>&1", timeout=30
                )
                if ok_stat:
                    _calls_upload_log(f"файл на сервере: {out_stat.strip()}")
                else:
                    _calls_upload_log(
                        f"заливка прошла, но ls на сервере не ок: {(err_stat or out_stat).strip()}"
                    )
                meta = {
                    "item_key": item_key,
                    "subject": subject,
                    "fname": fname,
                    "remote_path": remote_path,
                    "size": local_size,
                    "started": datetime.now().isoformat(timespec="seconds"),
                }
                _calls_upload_job_write(meta)
                meta_tmp = os.path.join(tmp_dir, "calls_upload_run.json")
                with open(meta_tmp, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False)
                ssh_upload_binary(ssh, meta_tmp, CALLS_UPLOAD_RUN_META)
                pid = _calls_upload_start_remote(ssh, remote_path)
                if not pid:
                    _calls_upload_job_clear()
                    return
                _calls_upload_log(
                    f"пайплайн запущен в фоне pid={pid}, демон не ждёт "
                    f"(лог {CALLS_UPLOAD_RUN_LOG})"
                )
            finally:
                try:
                    if os.path.isfile(local_path):
                        os.remove(local_path)
                    meta_tmp = os.path.join(tmp_dir, "calls_upload_run.json")
                    if os.path.isfile(meta_tmp):
                        os.remove(meta_tmp)
                    os.rmdir(tmp_dir)
                except Exception:
                    pass
            return
        except Exception as e:
            _calls_upload_log(f"ошибка на письме: {e}")

    if verbose:
        _calls_upload_log(
            "scan: "
            f"просмотрено={stats['scanned']}, "
            f"тема={stats['subject_ok']}, "
            f"файл={stats['file_ok']}, "
            f"уже_обработано={stats['already_done']}, "
            f"новых=0, "
            f"processed={len(processed)} шт."
        )
        if stats["file_ok"] and stats["already_done"] == stats["file_ok"]:
            _calls_upload_log("все найденные вложения уже в calls_upload_processed.txt — жду новое письмо")
        elif stats["subject_ok"] and not stats["file_ok"]:
            _calls_upload_log(
                f"письма с темой «{CALLS_UPLOAD_SUBJECT_MATCH}» есть, но без вложения .xlsx/.csv"
            )
        elif not stats["subject_ok"]:
            _calls_upload_log(
                f"во «Входящих» (последние {CALLS_UPLOAD_SCAN_LIMIT}) нет писем "
                f"с темой «{CALLS_UPLOAD_SUBJECT_MATCH}»"
            )


def _comments_log(msg: str) -> None:
    _stderr(f"[comments_update] {msg}")


def _comments_processed_set() -> set:
    try:
        with open(COMMENTS_STATE_FILE, "r", encoding="utf-8") as f:
            return {ln.strip() for ln in f if ln.strip()}
    except FileNotFoundError:
        return set()


def _comments_mark_processed(key: str) -> None:
    try:
        with open(COMMENTS_STATE_FILE, "a", encoding="utf-8") as f:
            f.write(key + "\n")
    except Exception as e:
        _comments_log(f"не удалось записать состояние: {e}")


def _comments_item_key(item, fname: str) -> str:
    entry_id = str(getattr(item, "EntryID", "") or "").strip()
    received = str(getattr(item, "ReceivedTime", "") or "").strip()
    return f"{entry_id}\t{fname}" if entry_id else f"{received}\t{fname}"


def _comments_safe_name(fname: str) -> str:
    base = os.path.basename(fname).replace(" ", "_")
    return "".join(c if (c.isalnum() or c in "._-") else "_" for c in base) or "funnel.pkl"


def _comments_find_funnel_in_dir(root: str) -> str:
    files: list[str] = []
    for dirpath, _, names in os.walk(root):
        for name in names:
            files.append(os.path.join(dirpath, name))

    def pick(suffixes: tuple[str, ...], prefix: str = "") -> str:
        hits = [
            p for p in files
            if p.lower().endswith(suffixes)
            and (not prefix or os.path.basename(p).lower().startswith(prefix))
        ]
        return max(hits) if hits else ""

    return (
        pick((".pkl",), "fin_funnel_")
        or pick((".xlsx", ".xls"), "fin_funnel_")
        or pick((".pkl",))
        or pick((".xlsx", ".xls"))
    )


def _comments_win_extract_tools() -> list[str]:
    cands = [
        os.path.expandvars(r"%ProgramFiles%\7-Zip\7z.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\7-Zip\7z.exe"),
        os.path.expandvars(r"%ProgramFiles%\WinRAR\UnRAR.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\WinRAR\UnRAR.exe"),
        os.path.expandvars(r"%ProgramFiles%\WinRAR\WinRAR.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\WinRAR\WinRAR.exe"),
        shutil.which("7z") or "",
        shutil.which("unrar") or "",
    ]
    out = []
    for p in cands:
        if p and os.path.isfile(p) and p not in out:
            out.append(p)
    return out


def _comments_unpack_local(archive: str, dest: str) -> None:
    os.makedirs(dest, exist_ok=True)
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive, "r") as zf:
            zf.extractall(dest)
        return
    last = ""
    for tool in _comments_win_extract_tools():
        low = tool.lower()
        if low.endswith("unrar.exe") or os.path.basename(low) == "unrar":
            cmd = [tool, "x", "-o+", archive, dest + os.sep]
        elif low.endswith("winrar.exe"):
            cmd = [tool, "x", "-ibck", "-o+", archive, dest + os.sep]
        else:
            cmd = [tool, "x", f"-o{dest}", "-y", archive]
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if proc.returncode == 0:
            _comments_log(f"rar распакован через {tool}")
            return
        last = (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()
        _comments_log(f"{tool} не смог: {last}")
    raise RuntimeError(
        f"не удалось распаковать rar на Windows ({last or 'нет 7-Zip/WinRAR'}). "
        "Поставь 7-Zip — с почты приходит rar, на сервере unrar нет."
    )


def _comments_prepare_upload(local_path: str, tmp_dir: str) -> str:
    """С почты приходит rar: распаковать на винде, на сервер уходит pkl/xlsx."""
    low = local_path.lower()
    if not low.endswith((".rar", ".zip", ".7z")):
        return local_path
    dest = os.path.join(tmp_dir, "extracted")
    _comments_unpack_local(local_path, dest)
    found = _comments_find_funnel_in_dir(dest)
    if not found:
        raise RuntimeError(f"в архиве нет pkl/xlsx: {os.path.basename(local_path)}")
    _comments_log(f"из rar взят {os.path.basename(found)}")
    return found


def _comments_find_attach(item):
    try:
        count = item.Attachments.Count
    except Exception:
        return None, ""
    found = []
    prefer = (".pkl", ".rar", ".zip", ".7z", ".xlsx")
    for i in range(1, count + 1):
        att = item.Attachments.Item(i)
        try:
            fname = str(att.FileName or "")
        except Exception:
            continue
        low = fname.lower()
        if low.endswith(prefer):
            found.append((att, fname, low))
    if not found:
        return None, ""
    for ext in prefer:
        for att, fname, low in found:
            if low.endswith(ext):
                return att, fname
    return found[0][0], found[0][1]


def _comments_job_read() -> dict:
    try:
        with open(COMMENTS_JOB_LOCAL, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _comments_job_write(meta: dict) -> None:
    try:
        with open(COMMENTS_JOB_LOCAL, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except Exception as e:
        _comments_log(f"не записал локальный job: {e}")


def _comments_job_clear() -> None:
    try:
        if os.path.isfile(COMMENTS_JOB_LOCAL):
            os.remove(COMMENTS_JOB_LOCAL)
    except Exception:
        pass


_COMMENTS_FAIL_MAILED: set[str] = set()


def _comments_mail_html(text: str) -> HTMLBody:
    parts = []
    for raw in (text or "").splitlines():
        line = html.escape(raw)
        if not raw.strip():
            parts.append("<div style='height:8px'></div>")
            continue
        weight = "700" if raw.startswith(("Результат:", "===", "1.", "2.", "3.")) else "400"
        parts.append(
            f"<div style='font-weight:{weight};margin:0 0 2px 0'>{line}</div>"
        )
    inner = "".join(parts)
    return HTMLBody(
        "<html><head><meta charset='UTF-8'></head>"
        "<body style=\"font-family:Calibri,'Segoe UI',Arial,sans-serif;"
        "font-size:14pt;line-height:1.4;color:#222222;\">"
        f"{inner}</body></html>"
    )


def _comments_log_for_mail(log_tail: str) -> str:
    out = []
    skip = 0
    for ln in (log_tail or "").splitlines():
        if skip:
            skip -= 1
            continue
        if "FutureWarning" in ln:
            skip = 2
            continue
        if "exclude the relevant" in ln or "result = pd.concat" in ln:
            continue
        out.append(ln)
    return "\n".join(out)


def _comments_notify(subject: str, body: str) -> None:
    if not COMMENTS_NOTIFY:
        return
    recipients = []
    for addr in list(COMMENTS_NOTIFY_EMAILS or []) + [email]:
        addr = str(addr or "").strip()
        if addr and addr not in recipients:
            recipients.append(addr)
    if not recipients:
        return
    html_body = _comments_mail_html(body)
    for addr in recipients:
        try:
            send_email(account=account, subject=subject, body=html_body, recipients=[addr])
            _comments_log(f"отбивка ушла на {addr}")
        except Exception as e:
            _comments_log(f"не удалось отправить отбивку на {addr}: {e}")


def _comments_notify_fail_once(key: str, subject: str, body: str) -> None:
    token = key or subject
    if token in _COMMENTS_FAIL_MAILED:
        return
    _COMMENTS_FAIL_MAILED.add(token)
    _comments_notify(subject, body)


def _comments_result_body(*, ok: bool, fname: str, subject: str, size, remote_path: str, extra: str = "", log_tail: str = "") -> str:
    status = "успешно" if ok else "ошибка"
    try:
        size_s = f"{int(size):,}"
    except (TypeError, ValueError):
        size_s = str(size or 0)
    return (
        f"Результат: {status}\n"
        f"Письмо: {subject}\n"
        f"Вложение: {fname} ({size_s} байт)\n"
        f"На сервере: {remote_path}\n"
        f"Каталог: {COMMENTS_REMOTE_DIR}\n"
        f"База: {COMMENTS_REMOTE_DIR}/class_comment_on.xlsx\n"
        f"{extra}\n"
        f"Лог:\n{_comments_log_for_mail(log_tail) or 'без вывода'}"
    )


def _comments_finish_job(ssh: str, status: str, extra: str) -> None:
    meta = _comments_job_read()
    log_tail = _remote_bg_tail(ssh, COMMENTS_RUN_LOG, 80)
    fname = str(meta.get("fname") or "funnel.pkl")
    subject = str(meta.get("subject") or "")
    remote_path = str(meta.get("remote_path") or "")
    item_key = str(meta.get("item_key") or "")
    size = meta.get("size") or 0
    if status == "dead":
        _comments_log(f"пайплайн умер (pid={extra or '?'})")
        _comments_notify_fail_once(
            item_key or fname,
            f"Synaptica: комментарии — ошибка ({fname})",
            _comments_result_body(
                ok=False,
                fname=fname,
                subject=subject,
                size=size,
                remote_path=remote_path,
                extra=f"пайплайн умер pid={extra or '?'}",
                log_tail=log_tail,
            ),
        )
        _remote_bg_cleanup(ssh, COMMENTS_RUN_PID, COMMENTS_RUN_EXIT, COMMENTS_RUN_META)
        _comments_job_clear()
        return
    try:
        exit_code = int(str(extra).strip() or "1")
    except ValueError:
        exit_code = 1
    if log_tail:
        for ln in log_tail.splitlines()[-40:]:
            _comments_log(f"  [pipeline] {ln}")
    if exit_code == 0:
        if item_key:
            _comments_mark_processed(item_key)
        _comments_log(f"готово: {fname} → {remote_path}")
        _comments_notify(
            f"Synaptica: комментарии обновлены ({fname})",
            _comments_result_body(
                ok=True,
                fname=fname,
                subject=subject,
                size=size,
                remote_path=remote_path,
                log_tail=log_tail,
            ),
        )
    else:
        _comments_log(f"пайплайн упал (exit={exit_code}) — повтор на следующем цикле")
        _comments_notify_fail_once(
            item_key or fname,
            f"Synaptica: комментарии — ошибка ({fname})",
            _comments_result_body(
                ok=False,
                fname=fname,
                subject=subject,
                size=size,
                remote_path=remote_path,
                extra=f"exit={exit_code}",
                log_tail=log_tail,
            ),
        )
    _remote_bg_cleanup(ssh, COMMENTS_RUN_PID, COMMENTS_RUN_EXIT, COMMENTS_RUN_META)
    _comments_job_clear()


def process_comments_update(ssh: str) -> None:
    """Письмо на ящик Синаптики → воронка на сервер → разметка в фоне."""
    if not COMMENTS_UPDATE_ENABLED:
        return
    import tempfile

    if not hasattr(process_comments_update, "_scan_no"):
        process_comments_update._scan_no = 0  # type: ignore[attr-defined]
    process_comments_update._scan_no += 1  # type: ignore[attr-defined]
    scan_no: int = process_comments_update._scan_no  # type: ignore[attr-defined]
    verbose = (COMMENTS_LOG_EVERY_N_SCANS <= 1) or (scan_no % COMMENTS_LOG_EVERY_N_SCANS == 1)

    status, extra = _remote_bg_status(ssh, COMMENTS_RUN_PID, COMMENTS_RUN_EXIT)
    if status == "unknown":
        _comments_log(f"статус SSH недоступен: {extra}")
        return
    if status == "running":
        if verbose:
            _comments_log(f"разметка ещё идёт (pid={extra or '?'})")
        return
    if status in ("done", "dead"):
        _comments_finish_job(ssh, status, extra)
        return

    outlook = win32com.client.Dispatch("Outlook.Application")
    namespace = outlook.GetNamespace("MAPI")
    inbox = namespace.GetDefaultFolder(6)
    items = inbox.Items
    items.Sort("[ReceivedTime]", True)

    processed = _comments_processed_set()
    needle = COMMENTS_SUBJECT_MATCH.lower()
    stats = {"scanned": 0, "subject_ok": 0, "file_ok": 0, "already_done": 0}
    scanned = 0
    for item in items:
        if scanned >= COMMENTS_SCAN_LIMIT:
            break
        try:
            if item.Class != 43:
                continue
            scanned += 1
            stats["scanned"] += 1
            subject = str(getattr(item, "Subject", "") or "")
            if needle not in subject.lower():
                continue
            stats["subject_ok"] += 1
            att, fname = _comments_find_attach(item)
            if not att:
                continue
            stats["file_ok"] += 1
            item_key = _comments_item_key(item, fname)
            if item_key in processed:
                stats["already_done"] += 1
                continue

            tmp_dir = tempfile.mkdtemp(prefix="comments_update_")
            local_path = os.path.join(tmp_dir, _comments_safe_name(fname))
            try:
                _comments_log(f"найдено письмо: «{subject}», вложение {fname}")
                att.SaveAsFile(local_path)
                upload_local = _comments_prepare_upload(local_path, tmp_dir)
                size = os.path.getsize(upload_local) if os.path.isfile(upload_local) else 0
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                remote_name = _comments_safe_name(os.path.basename(upload_local))
                remote_path = f"{COMMENTS_VORONKI_DIR}/mail_{stamp}_{remote_name}"
                if not ssh_upload_binary(ssh, upload_local, remote_path):
                    _comments_log("заливка не удалась — повтор позже")
                    _comments_notify_fail_once(
                        item_key,
                        f"Synaptica: комментарии — ошибка заливки ({fname})",
                        _comments_result_body(
                            ok=False,
                            fname=fname,
                            subject=subject,
                            size=size,
                            remote_path=remote_path,
                            extra="SFTP заливка не удалась",
                        ),
                    )
                    return
                meta = {
                    "item_key": item_key,
                    "subject": subject,
                    "fname": fname,
                    "remote_path": remote_path,
                    "size": size,
                    "started": datetime.now().isoformat(timespec="seconds"),
                }
                _comments_job_write(meta)
                meta_tmp = os.path.join(tmp_dir, "comments_update_run.json")
                with open(meta_tmp, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False)
                ssh_upload_binary(ssh, meta_tmp, COMMENTS_RUN_META)
                pipeline = COMMENTS_REMOTE_CMD_TMPL.format(filepath=remote_path)
                pid, err = _remote_bg_start(
                    ssh, pipeline, COMMENTS_RUN_LOG, COMMENTS_RUN_PID, COMMENTS_RUN_EXIT
                )
                if not pid:
                    _comments_log(f"не удалось запустить фон: {err}")
                    _comments_notify_fail_once(
                        item_key,
                        f"Synaptica: комментарии — ошибка запуска ({fname})",
                        _comments_result_body(
                            ok=False,
                            fname=fname,
                            subject=subject,
                            size=size,
                            remote_path=remote_path,
                            extra=str(err or "не удалось запустить фон"),
                        ),
                    )
                    _comments_job_clear()
                    return
                _comments_log(f"разметка запущена pid={pid} под {ssh}")
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            return
        except Exception as e:
            _comments_log(f"ошибка на письме: {e}")
            _comments_notify_fail_once(
                locals().get("item_key") or str(e),
                f"Synaptica: комментарии — ошибка ({locals().get('fname') or 'файл'})",
                _comments_result_body(
                    ok=False,
                    fname=str(locals().get("fname") or ""),
                    subject=str(locals().get("subject") or ""),
                    size=0,
                    remote_path="",
                    extra=str(e),
                ),
            )

    if verbose:
        _comments_log(
            "scan: "
            f"просмотрено={stats['scanned']}, тема={stats['subject_ok']}, "
            f"файл={stats['file_ok']}, уже={stats['already_done']}"
        )


def _write_pid():
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))


def _remove_pid():
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass


def _stop_requested() -> bool:
    """Проверка stop-файла: если файл есть — запрос на остановку."""
    try:
        return os.path.exists(STOP_FILE)
    except Exception:
        return False


def _sleep_with_check(interval_sec: int, check_stop) -> bool:
    """Спим interval_sec секунд, проверяя каждые 0.3 с флаг остановки — Ctrl+C срабатывает быстро."""
    chunk = 0.3
    elapsed = 0.0
    while elapsed < interval_sec:
        if check_stop():
            return True
        time.sleep(min(chunk, interval_sec - elapsed))
        elapsed += chunk
    return False


def run_daemon(ssh: str, log_path: str, code_valid_minutes: int, interval_sec: int) -> None:
    """Фоновый цикл: опрос лога, обработка новых pending."""
    stop = False

    def on_signal(*_):
        nonlocal stop
        stop = True

    def should_stop():
        return stop or _stop_requested()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    _write_pid()
    atexit.register(_remove_pid)
    _alerts_sent_in_session: set = set()  # in-memory дедуп отправленных алертов за сеанс
    _reset_sent_ids: set = _reset_sent_set()
    def _log(msg: str) -> None:
        _stderr(msg)

    _log(f"[reset_pas_admin] Запуск в фоне: опрос каждые {interval_sec} с, лог={log_path}")
    _log(f"[reset_pas_admin] Остановка: Ctrl+C, или kill $(cat {PID_FILE}), или touch {STOP_FILE}")
    _log(f"[reset_pas_admin] Почта: exchangelib {server} / {email}")
    if NEWS_ENABLED:
        n_proc = len(_news_processed_set())
        _log(
            f"[reset_pas_admin] Новости: по теме «{NEWS_SUBJECT_MATCH}», "
            f"вложение {NEWS_ATTACH_PREFIX}*.csv → {NEWS_REMOTE_DIR}, clean в фоне. "
            f"CSV обработано: {n_proc}. Рассылка: день=утро, файл news_сегодня, неделя=пт, месяц=1-е. "
            f"Prefs: {NEWS_PREFS_REMOTE_DIR}/{{login}}.json."
        )
    if PULSE_ENABLED:
        _log(
            f"[reset_pas_admin] Pulse: тема «{PULSE_SUBJECT_MATCH}», "
            f"вложение *{PULSE_ATTACH_SUBSTRING}*.pptx → {PULSE_REMOTE_PATH}. "
            f"Уже залито: {len(_pulse_processed_set())}."
        )
    if CALLS_UPLOAD_ENABLED:
        _log(
            f"[reset_pas_admin] Звонки: тема «{CALLS_UPLOAD_SUBJECT_MATCH}», "
            f"вложение .xlsx/.csv → {CALLS_UPLOAD_REMOTE_DIR}, пайплайн в фоне на сервере. "
            f"Уже обработано: {len(_calls_upload_processed_set())}."
        )
    if COMMENTS_UPDATE_ENABLED:
        _log(
            f"[reset_pas_admin] Комментарии: тема «{COMMENTS_SUBJECT_MATCH}», "
            f"pkl/rar/zip → {COMMENTS_VORONKI_DIR}, разметка в фоне. "
            f"Уже обработано: {len(_comments_processed_set())}."
        )
    _log("[reset_pas_admin] Опрос: pending / code_set / verified / к обработке — одна строка на цикл.")

    try:
        while not should_stop():
            try:
                if _stop_requested():
                    break
                content = ssh_cat(ssh, log_path)
                pending_by_id, code_set_ids, verified_ids, stats = parse_log(content)
                code_set_ids |= _reset_sent_ids
                pending_list = get_pending_without_code(pending_by_id, code_set_ids, verified_ids)
                stale_n = sum(1 for _rid, data in pending_list if _pending_too_old(data))
                pending_list = [
                    (rid, data)
                    for rid, data in pending_list
                    if rid not in _reset_sent_ids and not _pending_too_old(data)
                ]
                pending_list = _newest_pending_per_email(pending_list)
                _log(
                    "[reset_pas_admin] опрос: "
                    f"pending={stats['pending']}, code_set={stats['code_set']}, "
                    f"verified={stats['verified']}, stale={stale_n}, "
                    f"к обработке={len(pending_list)}"
                )
                if pending_list:
                    details = ", ".join(
                        f"{_short_id(rid)}->{data.get('email') or '-'}"
                        for rid, data in pending_list
                    )
                    _log(f"[reset_pas_admin] коды будут выданы: {details}")
                for rid, data in pending_list:
                    if rid in _reset_sent_ids:
                        continue
                    if process_one_pending(ssh, log_path, rid, data["email"], code_valid_minutes):
                        _reset_sent_ids.add(rid)
                try:
                    process_alerts(ssh, ALERTS_LOG_PATH, _alerts_sent_in_session)
                except Exception as e:
                    _log(f"[alerts] Ошибка обработки алертов светофора: {e}")
                try:
                    process_fi_weekly(ssh)
                except Exception as e:
                    _log(f"[fi_weekly] Ошибка недельной выгрузки FI: {e}")
                try:
                    process_notes_weekly(ssh)
                except Exception as e:
                    _log(f"[notes_weekly] Ошибка недельного отчёта по заметкам: {e}")
                try:
                    process_news_csv(ssh)
                except Exception as e:
                    _log(f"[news] Ошибка обработки CSV с новостями: {e}")
                try:
                    process_pulse_pptx(ssh)
                except Exception as e:
                    _log(f"[pulse] Ошибка обработки weekly pulse: {e}")
                try:
                    process_calls_upload(ssh)
                except Exception as e:
                    _log(f"[calls_upload] Ошибка обработки Excel звонков: {e}")
                try:
                    process_comments_update(ssh)
                except Exception as e:
                    _log(f"[comments_update] Ошибка обработки воронки комментариев: {e}")
                try:
                    process_scheduled_news_digests(ssh)
                except Exception as e:
                    _log(f"[news] Ошибка рассылки дайджестов: {e}")
            except Exception as e:
                _log(f"[reset_pas_admin] Ошибка цикла: {e}")
            if _sleep_with_check(interval_sec, should_stop):
                break
    finally:
        _remove_pid()
        if _stop_requested():
            try:
                os.remove(STOP_FILE)
            except Exception:
                pass
        _log("[reset_pas_admin] Остановлен.")


def main():
    if RESET_SSH == "user@myserver.example.com" or not RESET_SSH:
        sys.exit("Задайте RESET_SSH в блоке CONFIG.")
    run_daemon(RESET_SSH, RESET_LOG_PATH, RESET_CODE_MINS, RESET_POLL_INTERVAL)


if __name__ == "__main__":
    main()


# --- Запуск на Windows ---
#
# Запуск (в фоне, без консоли; можно закрыть терминал/VS Code):
#   pythonw reset_pas_admin.py
#
# Остановка (PowerShell):
#   Get-Process python* | Where-Object {$_.Path -like "*sybapica_reset*"} | Stop-Process
#
# Проверить, что запустилось нормально:
#   — В папке со скриптом должен появиться файл  reset_pas_admin.pid  с числом (PID процесса).
#   — Что процесс жив:  Get-Process -Id (Get-Content reset_pas_admin.pid)  (PowerShell; ошибки нет — процесс работает).
#
# Если что-то не так (ошибка при старте, нет .pid, непонятное поведение):
#   Запустить один раз в консоли (чтобы увидеть вывод):  python reset_pas_admin.py
#   Сообщения об ошибках и статусе пишутся в stderr; после отладки снова запускать через pythonw.
