#!/usr/bin/env python3
"""
Backend server for DBFCM Tools Chrome extension
Provides waitlist data and availability checking
Run this on your Windows machine where you have SQL Server access
"""
import sys
sys.dont_write_bytecode = True  # prevent __pycache__ from appearing in the extension folder

from http.server import HTTPServer, BaseHTTPRequestHandler
import html as _html
import re
import subprocess
import json
import os
import platform
import shlex
import tempfile
import threading
from datetime import datetime, timedelta
import urllib.parse
import urllib.request
import urllib.error
import socket
import logging
import time
import uuid

import db_utils
from db_utils import run_query_rows, normalize_phone, author_code, configure_from_config

# Detect whether we're running on Windows or WSL/Linux
IS_WINDOWS = platform.system() == 'Windows'

# Logging — console + file in the extension folder
_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_LOG_PATH, encoding='utf-8'),
    ]
)
log = logging.getLogger('kennel')

# Per-machine config — loaded from config.<HOSTNAME>.json, then config.local.json (both gitignored).
# This allows multiple machines sharing the same OneDrive folder to have separate configs.
def _load_machine_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    hostname = socket.gethostname().upper()
    defaults = {
        'wsl_claude_path': '/home/noah/.local/bin/claude',
        'sql_server':      'desktop-bikigbr,2721',
        'sql_database':    'wkennel7',
        'sql_auth':        'sql',
        'sql_user':        'noah',
        'sql_password':    'noah',
        'noah_phone_numbers': ['5106465763', '5103310678'],
        'noah_cell_primary':  '5106465763',
    }
    # Try hostname-specific file first, then generic fallback
    candidates = [
        os.path.join(script_dir, f'config.{hostname}.json'),
        os.path.join(script_dir, 'config.local.json'),
    ]
    for config_path in candidates:
        if os.path.exists(config_path):
            try:
                with open(config_path) as f:
                    data = json.load(f)
                log.info(f'Loaded config from {os.path.basename(config_path)}')
                return {**defaults, **data}
            except Exception as e:
                log.warning(f'Could not load {os.path.basename(config_path)}: {e} — trying next')
    log.warning(f'No config file found (tried config.{hostname}.json, config.local.json) — using defaults. '
                'Copy config.local.json.example to get started.')
    return defaults

_cfg = _load_machine_config()
configure_from_config(_cfg)
WSL_CLAUDE_PATH = _cfg['wsl_claude_path']

# MCP config path — written dynamically by _generate_mcp_config() at startup
MCP_CONFIG_WSL_PATH = None

# Comma-separated tool names auto-approved for Noah-bot (no interactive permission prompt)
MCP_ALLOWED_TOOLS = ','.join([
    'mcp__kennel-db__get_appointments',
    'mcp__kennel-db__search_client_or_pet',
    'mcp__kennel-db__get_open_slots',
    'mcp__kennel-db__get_waitlist',
    'mcp__kennel-db__get_groomer_schedule',
    'mcp__kennel-db__append_note',
    'mcp__kennel-db__create_appointment',
    'mcp__kennel-db__reassign_bather',
    'mcp__kennel-db__add_to_knowledge_base',
    'mcp__kennel-db__draft_sms',
])

# Noah-bot session state
_claude_session_id = None
_system_prompt_file = None     # WSL-accessible path passed to claude CLI
_system_prompt_content = None  # raw string (retained for potential future use)

# Persistent WSL shell — stays alive for the life of the backend process
_wsl_shell_proc = None
_wsl_shell_lock = threading.Lock()
_SHELL_SENTINEL = '__NOAHBOT_DONE__'

def _win_to_wsl_path(win_path):
    """Convert a Windows path like C:\\Foo\\bar to /mnt/c/Foo/bar for WSL"""
    drive = win_path[0].lower()
    rest = win_path[2:].replace('\\', '/')
    return f'/mnt/{drive}{rest}'

def _get_ext_dir() -> str:
    """Directory where backend_server.py lives — the extension folder, synced via OneDrive."""
    return os.path.dirname(os.path.abspath(__file__))

def _get_wsl_ext_dir() -> str:
    """WSL-accessible path to the extension folder."""
    ext = _get_ext_dir()
    return _win_to_wsl_path(ext) if IS_WINDOWS else ext

def _generate_mcp_config():
    """Write noahbot_mcp_config.json into the extension folder with the correct WSL path.
    Works on any machine — paths are computed from __file__, not hardcoded."""
    global MCP_CONFIG_WSL_PATH
    wsl_ext = _get_wsl_ext_dir()
    mcp_server_path = f"{wsl_ext}/noahbot_mcp_server.py"
    config = {
        "mcpServers": {
            "kennel-db": {
                "command": "python3",
                "args": [mcp_server_path]
            }
        }
    }
    config_win_path = os.path.join(_get_ext_dir(), 'noahbot_mcp_config.json')
    with open(config_win_path, 'w') as f:
        json.dump(config, f, indent=2)
    MCP_CONFIG_WSL_PATH = _win_to_wsl_path(config_win_path) if IS_WINDOWS else config_win_path
    print(f"[Know-a-bot] MCP config -> {config_win_path}")
    print(f"[Know-a-bot] MCP server -> {mcp_server_path}")

def build_noahbot_system_prompt():
    """Build system prompt from staff docs; caches content string and writes file for CLI fallback."""
    global _system_prompt_file, _system_prompt_content

    # Staff docs — check extension folder's staff/ subdir first (OneDrive-synced, works on all machines).
    # If that doesn't exist yet, fall back to the WSL path (dev machine only).
    ext_staff = os.path.join(_get_ext_dir(), 'staff')
    if os.path.isdir(ext_staff):
        docs_dir = ext_staff
        print(f"[Know-a-bot] Staff docs -> {ext_staff}")
    elif IS_WINDOWS:
        docs_dir = r'\\wsl$\Ubuntu\home\noah\wkennel7\staff'
        print(f"[Know-a-bot] Staff docs -> WSL path (extension/staff/ not found)")
    else:
        docs_dir = '/home/noah/wkennel7/staff'

    doc_files = [
        'EMPLOYEE_OPERATIONS_GUIDE.md',
        'SCHEDULING_QUICK_REFERENCE.md',
        'ROSIE_AI_FAQ.md',
        'WKENNEL7_GROOMING_LEXICON.md',
        'SCHEDULING_CHEATSHEET.md',
        'KNOWLEDGE_BASE.md',   # staff-curated rules added via Know-a-bot
    ]

    content = (
        "You are Know-a-bot, an assistant for employees of Dog's Best Friend and the Cat's Meow "
        "grooming salon. Answer questions using the reference materials below. Be concise and "
        "practical. If something isn't covered in the materials, say so.\n\n"
        "You also have access to live database tools. Use them whenever staff ask about "
        "current appointments, clients, pets, waitlist, or groomer schedules — don't guess, look it up. "
        "Employee IDs: Tomoko=85, Kumi=59, Mandilyn=95, Elmer=8.\n\n"
        "FORMATTING: Plain text only. No markdown — no **, no ##, no bullet dashes, no tables. "
        "Use simple punctuation and line breaks to organize information.\n\n"
        "CAT SERVICES POLICY: We currently have no cat groomer on staff. Do not book or suggest "
        "any cat grooming services. The only exception is Sadie Donnelly's nail trim, which is "
        "still accepted. If anyone asks about cat grooming, explain we are not currently offering it.\n\n"
        "DRAFTING SMS: You can draft SMS messages using the draft_sms tool.\n"
        "- To message a client: use their full name as the recipient.\n"
        "- To escalate to Noah (the owner): use 'Noah' as the recipient and include the original\n"
        "  employee question in the 'context' field. When Noah replies by text, his answer will\n"
        "  be automatically added to the knowledge base.\n"
        "- If you genuinely cannot answer a question after checking all available info, offer to\n"
        "  escalate to Noah. Let the employee confirm before calling the tool.\n"
        "- All drafted messages appear in the SMS tab for staff review before sending.\n\n"
        "CONFIRMATION RULE: Before calling any tool that writes data "
        "(create_appointment, append_note, reassign_bather, add_to_knowledge_base, or draft_sms), you MUST first send a confirmation message to the user. "
        "The message must: (1) describe in plain language exactly what you are about to do, "
        "(2) explain the real-world consequence in simple terms (e.g. 'This will add a real appointment "
        "to the live system that the client and groomers will see'), and (3) ask the user to confirm "
        "before you proceed. Do not call the tool until you receive explicit confirmation.\n\n---\n\n"
    )

    for filename in doc_files:
        filepath = os.path.join(docs_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content += f"# {filename}\n\n{f.read()}\n\n---\n\n"
            print(f"[Noah-bot] Loaded {filename}")
        except FileNotFoundError:
            print(f"[Noah-bot] Warning: {filepath} not found, skipping")
        except Exception as e:
            print(f"[Noah-bot] Warning: Could not read {filepath}: {e}")

    # Write the prompt file somewhere the claude CLI (running in WSL) can read it.
    # On Windows: write to Windows temp dir, then compute its /mnt/... WSL path.
    # On Linux/WSL: write directly to /tmp/.
    if IS_WINDOWS:
        win_path = os.path.join(tempfile.gettempdir(), 'noahbot_system.txt')
        with open(win_path, 'w', encoding='utf-8') as f:
            f.write(content)
        _system_prompt_file = _win_to_wsl_path(win_path)
    else:
        _system_prompt_file = '/tmp/noahbot_system.txt'
        with open(_system_prompt_file, 'w', encoding='utf-8') as f:
            f.write(content)

    _system_prompt_content = content  # cache for direct API mode

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Know-a-bot system prompt built "
          f"({len(content):,} chars)")

# ===== Direct Anthropic API + persistent MCP subprocess =====

_chat_history = []          # [{"role": "user"/"assistant", "content": ...}, ...]
_mcp_proc = None            # persistent MCP server subprocess
_mcp_proc_lock = threading.Lock()
_mcp_tools_cache = None     # tool defs in Anthropic API format (cached after first fetch)

def _get_anthropic_key():
    """Get API key from environment; on Windows also tries WSL if not found locally."""
    key = os.environ.get('ANTHROPIC_API_KEY')
    if not key and IS_WINDOWS:
        try:
            r = subprocess.run(['wsl', 'bash', '-c', 'echo $ANTHROPIC_API_KEY'],
                               capture_output=True, text=True, timeout=5)
            key = r.stdout.strip()
        except Exception:
            pass
    return key or None

def _start_mcp_proc():
    """Spawn the MCP server subprocess and perform the initialize handshake."""
    wsl_script = f"{_get_wsl_ext_dir()}/noahbot_mcp_server.py"
    cmd = (['wsl', 'python3', '-u', wsl_script] if IS_WINDOWS else ['python3', '-u', wsl_script])
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL)
    init_req = json.dumps({
        "jsonrpc": "2.0", "id": 0, "method": "initialize",
        "params": {"protocolVersion": "2024-11-05",
                   "clientInfo": {"name": "backend", "version": "1.0"}}
    }) + "\n"
    proc.stdin.write(init_req.encode()); proc.stdin.flush()
    proc.stdout.readline()  # consume initialize response
    notif = json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n"
    proc.stdin.write(notif.encode()); proc.stdin.flush()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Know-a-bot] MCP server started (PID {proc.pid})")
    return proc

def _ensure_mcp_proc():
    """Return running MCP process, starting/restarting if needed. Caller must hold _mcp_proc_lock."""
    global _mcp_proc
    if _mcp_proc is None or _mcp_proc.poll() is not None:
        _mcp_proc = _start_mcp_proc()
    return _mcp_proc

def _mcp_rpc(method, params, req_id=1):
    """Send one JSON-RPC request and read one response. Caller must hold _mcp_proc_lock."""
    proc = _ensure_mcp_proc()
    req = json.dumps({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}) + "\n"
    proc.stdin.write(req.encode()); proc.stdin.flush()
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError("MCP process closed stdout unexpectedly")
    return json.loads(line)

def _get_mcp_tools():
    """Return tool defs in Anthropic API format, fetching and caching on first call."""
    global _mcp_tools_cache
    if _mcp_tools_cache is not None:
        return _mcp_tools_cache
    with _mcp_proc_lock:
        resp = _mcp_rpc("tools/list", {}, req_id=2)
    tools = []
    for t in resp.get("result", {}).get("tools", []):
        tools.append({
            "name": t["name"],
            "description": t["description"],
            "input_schema": t["inputSchema"],  # Anthropic uses snake_case
        })
    _mcp_tools_cache = tools
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Know-a-bot] {len(tools)} MCP tools loaded")
    return tools

def _call_mcp_tool(name, input_args):
    """Call an MCP tool by name and return (text_result, is_error)."""
    with _mcp_proc_lock:
        resp = _mcp_rpc("tools/call", {"name": name, "arguments": input_args}, req_id=3)
    result = resp.get("result", {})
    content = result.get("content", [])
    text = content[0]["text"] if content and content[0].get("type") == "text" else "(no result)"
    return text, result.get("isError", False)

# ===== Persistent WSL shell =====

def _start_wsl_shell():
    """Spawn a long-lived bash process (via WSL on Windows, native on Linux)."""
    cmd = (['wsl', 'bash', '--norc', '--noprofile']
           if IS_WINDOWS else ['bash', '--norc', '--noprofile'])
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL)
    # Make sure ~/.local/bin is on PATH (needed for sqlcmd at ~/sqlcmd etc.)
    proc.stdin.write(b'export PATH="$PATH:$HOME/.local/bin"\n')
    proc.stdin.flush()
    return proc

def _ensure_wsl_shell():
    """Return running shell process, starting/restarting if needed. Caller must hold _wsl_shell_lock."""
    global _wsl_shell_proc
    if _wsl_shell_proc is None or _wsl_shell_proc.poll() is not None:
        _wsl_shell_proc = _start_wsl_shell()
    return _wsl_shell_proc

def _shell_run(cmd: str):
    """Run cmd in the persistent WSL shell; return (stdout_output, exit_code)."""
    with _wsl_shell_lock:
        proc = _ensure_wsl_shell()
        full = f'{cmd}; echo "EXIT:$?"; echo "{_SHELL_SENTINEL}"\n'
        proc.stdin.write(full.encode())
        proc.stdin.flush()
        lines = []
        exit_code = 0
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError("WSL shell process died unexpectedly")
            decoded = line.decode('utf-8', errors='replace').rstrip('\n')
            if decoded == _SHELL_SENTINEL:
                break
            if decoded.startswith('EXIT:'):
                try:
                    exit_code = int(decoded[5:])
                except ValueError:
                    pass
            else:
                lines.append(decoded)
    return '\n'.join(lines), exit_code

# ── SMS Draft+Approve state ──────────────────────────────────────────────────
# Drafts keyed by str(inbound MessageId): {draft_id, message_id, client_id,
# client_name, phone, their_message, draft, timestamp}
_sms_drafts = {}
_sms_drafts_lock = threading.Lock()
_sms_last_seen_id = 0   # watermark: last inbound MessageId processed

_SMS_DRAFTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sms_drafts.json')

# Pending appointments briefings cache: appointment_id (str) → briefing dict
_pending_briefings = {}
_pending_briefings_lock = threading.Lock()

# ── Centralized TTL cache ─────────────────────────────────────────────────────
# All repeated SQL fetches go through canonical _get_*() functions that use this
# cache. Never write inline SQL for data that multiple endpoints need.
#
# Naming convention:
#   _get_<resource>(<params>)   — public-ish; checks cache, fetches if miss
#   _fetch_<resource>(<params>) — private; raw SQL only, called by _get_* only
#
# Cache invalidation: call _cache.delete(key) or _cache.delete_prefix(prefix)
#   after any write that would stale the cached data (e.g. after appt_book()).

_TTL_DOSSIER       = 60      # seconds — client dossier (pets, visits, cadence)
_TTL_COMPACT_AVAIL = 1800    # 30 min  — SMS compact availability text
_TTL_HOLIDAYS      = 86400   # 24 hrs  — calendar holiday/closure dates

class _TTLCache:
    def __init__(self):
        self._store = {}
        self._lock  = threading.Lock()

    def get(self, key):
        with self._lock:
            entry = self._store.get(key)
            if entry and time.time() < entry['exp']:
                return entry['val']
            if entry:
                del self._store[key]
            return None

    def set(self, key, value, ttl):
        with self._lock:
            self._store[key] = {'val': value, 'exp': time.time() + ttl}

    def delete(self, key):
        with self._lock:
            self._store.pop(key, None)

    def delete_prefix(self, prefix):
        """Invalidate all keys starting with prefix (e.g. 'holidays:' after a closure change)."""
        with self._lock:
            for k in [k for k in self._store if k.startswith(prefix)]:
                del self._store[k]

_cache = _TTLCache()

# ── Noah's personal cell numbers (for inbound routing) ───────────────────────
# Texts from these numbers go to KB ingestion, not the staff SMS queue.
NOAH_PHONE_NUMBERS = set(_cfg['noah_phone_numbers'])
NOAH_CELL_PRIMARY  = _cfg['noah_cell_primary']  # used for outbound escalation drafts

def _is_noah_phone(phone: str) -> bool:
    """Return True if the phone number belongs to Noah's personal cell."""
    return normalize_phone(phone) in NOAH_PHONE_NUMBERS

# ── Sent escalations tracking ─────────────────────────────────────────────────
# Tracks escalations sent to Noah so we can match his reply to the original question.
_sent_escalations = {}  # draft_id -> {escalation_context, sent_at, matched}
_PENDING_ESCALATIONS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'staff', 'pending_escalations.json'
)

def _save_pending_escalations():
    """Persist _sent_escalations to disk."""
    try:
        os.makedirs(os.path.dirname(_PENDING_ESCALATIONS_FILE), exist_ok=True)
        with open(_PENDING_ESCALATIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(_sent_escalations, f, indent=2)
    except Exception as e:
        log.warning(f'[SMS] Could not save pending escalations: {e}')

def _load_pending_escalations():
    """Load persisted escalations, expiring entries older than 7 days."""
    global _sent_escalations
    if not os.path.exists(_PENDING_ESCALATIONS_FILE):
        return
    try:
        with open(_PENDING_ESCALATIONS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return
        cutoff = (datetime.now() - timedelta(days=7)).isoformat()
        _sent_escalations = {
            k: v for k, v in data.items()
            if v.get('sent_at', '') >= cutoff and not v.get('matched')
        }
        if _sent_escalations:
            log.info(f'[SMS] Loaded {len(_sent_escalations)} pending escalation(s)')
    except Exception as e:
        log.warning(f'[SMS] Could not load pending escalations: {e}')

def _save_sms_drafts():
    """Persist _sms_drafts to disk so pending drafts survive a backend restart."""
    try:
        with _sms_drafts_lock:
            snapshot = dict(_sms_drafts)
        with open(_SMS_DRAFTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(snapshot, f, indent=2)
    except Exception as e:
        log.warning(f'[SMS] Could not save drafts to disk: {e}')

def _load_sms_drafts():
    """Restore persisted drafts on startup. Also restores the watermark."""
    global _sms_drafts, _sms_last_seen_id
    if not os.path.exists(_SMS_DRAFTS_FILE):
        return
    try:
        with open(_SMS_DRAFTS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return
        with _sms_drafts_lock:
            _sms_drafts = data
        if _sms_drafts:
            _sms_last_seen_id = max(int(v['message_id']) for v in _sms_drafts.values())
        log.info(f'[SMS] Restored {len(_sms_drafts)} pending draft(s) from disk '
                 f'(watermark={_sms_last_seen_id})')
    except Exception as e:
        log.warning(f'[SMS] Could not load saved drafts: {e}')

def _ensure_audit_tables():
    """Create AgentAuditLog and AgentRunLog if they don't exist. Safe to run every startup."""
    run_query_rows(
        "IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='AgentAuditLog') "
        "CREATE TABLE AgentAuditLog ("
        "  AuditSeq INT IDENTITY(1,1) PRIMARY KEY, "
        "  AuditTimestamp DATETIME DEFAULT GETDATE(), "
        "  AgentName VARCHAR(50), ActionType VARCHAR(20), RiskTier INT DEFAULT 0, "
        "  TargetTable VARCHAR(100), TargetKeys VARCHAR(500), Description VARCHAR(2000), "
        "  SnapshotBefore VARCHAR(MAX), SqlExecuted VARCHAR(MAX), RollbackSql VARCHAR(MAX), "
        "  Status VARCHAR(20) DEFAULT 'EXECUTED', VerifiedBy VARCHAR(50), "
        "  VerifiedAt DATETIME, ErrorMessage VARCHAR(1000), SessionId VARCHAR(100)"
        ")"
    )
    run_query_rows(
        "IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='AgentRunLog') "
        "CREATE TABLE AgentRunLog ("
        "  RunSeq INT IDENTITY(1,1) PRIMARY KEY, "
        "  AgentName VARCHAR(50), RunTimestamp DATETIME DEFAULT GETDATE(), "
        "  RunType VARCHAR(20), ChecksRun INT DEFAULT 0, "
        "  IssuesFound INT DEFAULT 0, IssuesAutoFixed INT DEFAULT 0, "
        "  Summary VARCHAR(2000), DurationMs INT"
        ")"
    )
    log.info('[Audit] Audit tables verified')

def _build_multipart(fields):
    """Build multipart/form-data body from a plain string dict. Returns (body_bytes, content_type)."""
    boundary = uuid.uuid4().hex
    parts = []
    for name, value in fields.items():
        parts.append(
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="{name}"\r\n'
            f'\r\n'
            f'{value}\r\n'
        )
    parts.append(f'--{boundary}--\r\n')
    body = ''.join(parts).encode('utf-8')
    return body, f'multipart/form-data; boundary={boundary}'

def _sms_send_via_kcapp(phone, message, client_id, cookies_dict):
    """POST a message to KCApp SMS API (stdlib urllib only).  Returns (new_message_id, error_str)."""
    url = "https://dbfcm.mykcapp.com/SMS/SMSSendFromFront"
    fields = {
        "phoneNumber": str(phone),
        "Message":     str(message),
        "MediaLinks":  "",
        "ClientId":    str(client_id),
        "MessageId":   "0",
    }
    body, content_type = _build_multipart(fields)
    cookie_header = '; '.join(f'{k}={v}' for k, v in cookies_dict.items())
    headers = {
        'Content-Type':      content_type,
        'Cookie':            cookie_header,
        'x-requested-with':  'XMLHttpRequest',
        'origin':            'https://dbfcm.mykcapp.com',
        'referer':           'https://dbfcm.mykcapp.com/',
        'user-agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/133.0.0.0 Safari/537.36'
        ),
    }
    req = urllib.request.Request(url, data=body, headers=headers, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        if data.get('Status') == 1:
            new_id = (data.get('ReturnedObject') or {}).get('MessageId')
            return new_id, None
        return None, data.get('Message', f"KCApp Status={data.get('Status')}")
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code}: {e.reason}"
    except Exception as e:
        return None, str(e)

def _sms_attribute_to_claude(message_id):
    """Set SendFromEmployeeId=105 (Claude Code) on a newly-sent SMS."""
    run_query_rows(f"UPDATE SMSMessages SET SendFromEmployeeId=105 "
               f"WHERE MessageId={int(message_id)}")

def _sms_mark_handled(message_id):
    """Mark an inbound message as handled."""
    run_query_rows(f"UPDATE SMSMessages SET IsHandled=1, MarkedHandledEmployeeId=105 "
               f"WHERE MessageId={int(message_id)}")

def _sms_get_client_context(client_id):
    """Return dict with client name, pets, upcoming appts, and recent conversation."""
    cid = int(client_id)

    client_rows = run_query_rows(
        f"SELECT CLFirstName, CLLastName FROM Clients WHERE CLSeq={cid}")
    if not client_rows or len(client_rows[0]) < 2:
        return None
    first_name = client_rows[0][0]
    last_name  = client_rows[0][1]

    pet_rows = run_query_rows(
        f"SELECT p.PtPetName, ISNULL(b.BrBreed,'') "
        f"FROM Pets p LEFT JOIN Breeds b ON p.PtBreedID=b.BrSeq "
        f"WHERE p.PtOwnerCode={cid} AND (p.PtDeleted IS NULL OR p.PtDeleted=0) "
        f"AND (p.PtInactive IS NULL OR p.PtInactive=0) "
        f"AND (p.PtDeceased IS NULL OR p.PtDeceased=0)")
    pets = [f"{r[0]} ({r[1]})" if len(r) > 1 and r[1] else r[0] for r in pet_rows]

    appt_rows = run_query_rows(
        f"SELECT TOP 5 "
        f"CONVERT(VARCHAR(10),gl.GLDate,120), "
        f"REPLACE(CONVERT(VARCHAR(5),DATEADD(MINUTE,DATEDIFF(MINUTE,'1899-12-30',gl.GLInTime),0),108),'1899-12-30 ',''), "
        f"p.PtPetName, ISNULL(e.USFNAME,''), "
        f"CASE WHEN gl.GLOthersID>0 THEN 'Handstrip' "
        f"     WHEN gl.GLBath=-1 AND gl.GLGroom=-1 THEN 'Full groom' "
        f"     WHEN gl.GLBath=-1 THEN 'Bath only' "
        f"     WHEN gl.GLGroom=-1 THEN 'Groom only' ELSE 'Service' END "
        f"FROM GroomingLog gl "
        f"INNER JOIN Pets p ON gl.GLPetID=p.PtSeq "
        f"LEFT JOIN Employees e ON gl.GLGroomerID=e.USSEQN "
        f"WHERE p.PtOwnerCode={cid} "
        f"AND gl.GLDate>=CAST(GETDATE() AS DATE) "
        f"AND (gl.GLDeleted IS NULL OR gl.GLDeleted=0) "
        f"ORDER BY gl.GLDate,gl.GLInTime")
    appts = []
    for r in appt_rows:
        if len(r) >= 5:
            appts.append(f"{r[0]} at {r[1]}: {r[2]} ({r[3]}, {r[4]})")

    conv_rows = run_query_rows(
        f"SELECT TOP 10 "
        f"CASE WHEN IsSendSMSByBusiness=1 THEN 'Us' ELSE 'Client' END, "
        f"LEFT(Message,120) "
        f"FROM SMSMessages WHERE ClientId={cid} ORDER BY MessageId DESC")
    recent = []
    for r in conv_rows:
        if len(r) >= 2:
            recent.append(f"{r[0]}: {r[1]}")
    recent.reverse()

    return {
        'first_name': first_name,
        'last_name':  last_name,
        'pets':       pets,
        'upcoming_appointments': appts,
        'recent_conversation':   recent,
    }


def _sms_get_client_dossier(client_id):
    """Rich client snapshot for the SMS card. Cached 60 seconds.

    Returned dict shape:
      pets          — list of {name, breed_code, last_groom, weeks_since, service, groomer}
      last_visit    — "Feb 1 (25d ago)" or None
      next_appt     — "Mar 15 — Fido w/ Tomoko" or None
      warning       — CLWarning string or None
      is_new_client — bool (< 3 lifetime appointments)
      future_count  — int
      preferred_day — "Saturday" or None
      preferred_time— "10:00 AM" or None
      avg_cadence_days — float or None
      suggested_next— "Apr 7 (Sat, ~6 wks)" or None   (only when future_count == 0)
    """
    from datetime import date as _date, timedelta as _timedelta
    from collections import Counter

    cid = int(client_id)
    cached = _cache.get(f'dossier:{cid}')
    if cached is not None:
        return cached

    MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

    def friendly(date_str):
        try:
            y, m, d = date_str.split('-')
            return f"{MON[int(m)-1]} {int(d)}"
        except Exception:
            return date_str

    result = {
        'pets':             [],
        'last_visit':       None,
        'next_appt':        None,
        'warning':          None,
        'is_new_client':    False,
        'future_count':     0,
        'preferred_day':    None,
        'preferred_time':   None,
        'avg_cadence_days': None,
        'suggested_next':   None,
    }

    # ── Query 1: client stats + future appt count ─────────────────────────
    hdr = run_query_rows(f"""
SELECT
    ISNULL(c.CLWarning,''),
    ISNULL(CAST(s.AvgCadenceDays AS varchar),''),
    ISNULL(s.PreferredDay,''),
    ISNULL(s.PreferredTime,''),
    ISNULL(CAST(s.ApptCount12Mo AS varchar),''),
    CAST((SELECT COUNT(*)
          FROM GroomingLog gl2
          INNER JOIN Pets p2 ON gl2.GLPetID=p2.PtSeq
          WHERE p2.PtOwnerCode=c.CLSeq
          AND gl2.GLDate > CAST(GETDATE() AS DATE)
          AND (gl2.GLDeleted IS NULL OR gl2.GLDeleted=0)
          AND (gl2.GLWaitlist IS NULL OR gl2.GLWaitlist=0)
          AND (gl2.GLNoShow IS NULL OR gl2.GLNoShow=0)) AS varchar)
FROM Clients c
LEFT JOIN DBFCMClientStats s ON c.CLSeq=s.ClientID
WHERE c.CLSeq={cid}
""")
    if hdr and hdr[0] and len(hdr[0]) >= 6:
        r = hdr[0]
        result['warning']          = r[0].strip() or None
        cadence_s                  = r[1].strip()
        result['preferred_day']    = r[2].strip() or None
        result['preferred_time']   = r[3].strip() or None
        appt_12mo_s                = r[4].strip()
        future_s                   = r[5].strip()
        try:
            result['avg_cadence_days'] = float(cadence_s) if cadence_s else None
        except ValueError:
            pass
        try:
            result['future_count'] = int(future_s) if future_s else 0
        except ValueError:
            pass
        # is_new_client: no stats row yet or zero appointments in last 12 months
        try:
            result['is_new_client'] = (not appt_12mo_s or int(appt_12mo_s) == 0)
        except ValueError:
            result['is_new_client'] = True

    # ── Query 2: pets + per-pet last groom date + birthdate ──────────────
    pet_rows = run_query_rows(f"""
SELECT
    CAST(p.PtSeq AS varchar),
    p.PtPetName,
    ISNULL(b.BrBreed,''),
    ISNULL(CONVERT(VARCHAR(10),
        (SELECT TOP 1 gl.GLDate
         FROM GroomingLog gl
         WHERE gl.GLPetID=p.PtSeq
         AND gl.GLDate < CAST(GETDATE() AS DATE)
         AND (gl.GLDeleted IS NULL OR gl.GLDeleted=0)
         AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist=0)
         ORDER BY gl.GLDate DESC), 120), ''),
    ISNULL(CONVERT(VARCHAR(10), p.PtBirthdate, 120), '')
FROM Pets p
LEFT JOIN Breeds b ON p.PtBreedID=b.BrSeq
WHERE p.PtOwnerCode={cid}
AND (p.PtDeleted IS NULL OR p.PtDeleted=0)
AND (p.PtInactive IS NULL OR p.PtInactive=0)
AND (p.PtDeceased IS NULL OR p.PtDeceased=0)
ORDER BY p.PtPetName
""")
    pet_ids  = []
    pet_data = {}
    for r in pet_rows:
        if len(r) < 5:
            continue
        pid        = r[0].strip()
        pname      = r[1].strip()
        breed      = r[2].strip()
        last_groom = r[3].strip()
        birthdate  = r[4].strip()
        if not pname:
            continue

        # Split "Golden Retriever (LGLH)" → breed_name="Golden Retriever", code="LGLH"
        if '(' in breed and ')' in breed:
            breed_name = breed[:breed.rfind('(')].strip()
            code       = breed[breed.rfind('(')+1:breed.rfind(')')]
        else:
            breed_name = breed
            code       = ''

        # Parse size and hair coat from code: "LGLH" → size="LG", coat="LH"
        size, coat = '', ''
        if len(code) >= 4:
            size = code[:2] if code[:2] in ('XS','SM','MD','LG','XL') else code[:2]
            coat = code[2:4] if code[2:4] in ('SH','LH') else code[2:4]
        elif len(code) == 2:
            coat = code  # e.g. just "SH" or "LH"

        # Compute age from birthdate
        age_str = ''
        if birthdate and birthdate not in ('NULL', ''):
            try:
                bdate = _date.fromisoformat(birthdate)
                days  = (_date.today() - bdate).days
                if days >= 365:
                    yrs = days // 365
                    age_str = f"{yrs}y"
                elif days >= 30:
                    mos = days // 30
                    age_str = f"{mos}mo"
            except Exception:
                pass

        pet_ids.append(pid)
        pet_data[pid] = {
            'name':       pname,
            'breed_name': breed_name,
            'size':       size,
            'coat':       coat,
            'age':        age_str,
            'last_groom': last_groom,
            'service':    None,
            'groomer':    None,
        }

    # ── Query 3: appointment history → service type + groomer preference ───
    total_appts = 0
    if pet_ids:
        pid_list = ','.join(pid for pid in pet_ids)
        hist = run_query_rows(f"""
SELECT TOP 40
    CAST(gl.GLPetID AS varchar),
    ISNULL(CAST(gl.GLBath AS varchar),'0'),
    ISNULL(CAST(gl.GLGroom AS varchar),'0'),
    ISNULL(CAST(gl.GLOthersID AS varchar),'0'),
    ISNULL(CAST(gl.GLNailsID AS varchar),'0'),
    ISNULL(e.USFNAME,'')
FROM GroomingLog gl
LEFT JOIN Employees e ON gl.GLGroomerID=e.USSEQN
WHERE gl.GLPetID IN ({pid_list})
AND gl.GLDate < CAST(GETDATE() AS DATE)
AND (gl.GLDeleted IS NULL OR gl.GLDeleted=0)
AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist=0)
ORDER BY gl.GLDate DESC
""")
        svc_counts = {}
        grm_counts = {}
        for r in hist:
            if len(r) < 6:
                continue
            pid       = r[0].strip()
            gl_bath   = r[1].strip()
            gl_groom  = r[2].strip()
            others_id = r[3].strip()
            nails_id  = r[4].strip()
            groomer   = r[5].strip()
            total_appts += 1

            # True handstrip: GLOthersID > 0 AND no bath/groom flags (confirmed Oct 2025 pattern)
            if others_id not in ('0', '', 'NULL') and gl_bath in ('0', '') and gl_groom in ('0', ''):
                svc = 'Handstrip'
            elif nails_id not in ('0', '', 'NULL') and gl_bath in ('0', '') and gl_groom in ('0', ''):
                svc = 'Nails only'
            elif gl_bath == '-1' and gl_groom == '-1':
                svc = 'Full service'
            elif gl_bath == '-1':
                svc = 'Bath only'
            elif gl_groom == '-1':
                svc = 'Groom only'
            else:
                continue

            if pid not in svc_counts:
                svc_counts[pid] = Counter()
                grm_counts[pid] = Counter()
            svc_counts[pid][svc] += 1
            if groomer:
                grm_counts[pid][groomer] += 1

        for pid in pet_ids:
            if pid in svc_counts and svc_counts[pid]:
                pet_data[pid]['service'] = svc_counts[pid].most_common(1)[0][0]
            if pid in grm_counts and grm_counts[pid]:
                top, top_n = grm_counts[pid].most_common(1)[0]
                total_for_pet = sum(grm_counts[pid].values())
                # Only flag preference if groomer is consistent (>50% AND at least 2 visits)
                if top_n >= 2 and top_n / total_for_pet > 0.5:
                    pet_data[pid]['groomer'] = top

        # Override is_new_client if DBFCMClientStats was missing but history exists
        if result['is_new_client'] and total_appts > 2:
            result['is_new_client'] = False

    # ── Build enriched pet list ────────────────────────────────────────────
    all_last_grooms = []
    for pid in pet_ids:
        pd  = pet_data[pid]
        lg  = pd['last_groom']
        wks = None
        if lg:
            try:
                wks = (_date.today() - _date.fromisoformat(lg)).days // 7
            except Exception:
                pass
            all_last_grooms.append(lg)
        result['pets'].append({
            'name':        pd['name'],
            'breed_name':  pd['breed_name'],
            'size':        pd['size'],
            'coat':        pd['coat'],
            'age':         pd['age'],
            'last_groom':  lg,
            'weeks_since': wks,
            'service':     pd['service'],
            'groomer':     pd['groomer'],
        })

    # ── Client-level last visit (most recent across all pets) ──────────────
    if all_last_grooms:
        most_recent = max(all_last_grooms)
        try:
            delta = (_date.today() - _date.fromisoformat(most_recent)).days
            result['last_visit'] = f"{friendly(most_recent)} ({delta}d ago)"
        except Exception:
            result['last_visit'] = most_recent

    # ── Next scheduled appointment ─────────────────────────────────────────
    na = run_query_rows(f"""
SELECT TOP 1 CONVERT(VARCHAR(10),gl.GLDate,120), p.PtPetName, ISNULL(e.USFNAME,'')
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID=p.PtSeq
LEFT JOIN Employees e ON gl.GLGroomerID=e.USSEQN
WHERE p.PtOwnerCode={cid}
AND gl.GLDate >= CAST(GETDATE() AS DATE)
AND (gl.GLDeleted IS NULL OR gl.GLDeleted=0)
AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist=0)
ORDER BY gl.GLDate, gl.GLInTime
""")
    if na and na[0] and len(na[0]) >= 3 and na[0][0].strip():
        try:
            na_str, pname, grm = na[0][0].strip(), na[0][1].strip(), na[0][2].strip()
            result['next_appt'] = f"{friendly(na_str)} — {pname}" + (f" w/ {grm}" if grm else "")
        except Exception:
            result['next_appt'] = na[0][0].strip()

    # ── Suggested next date (only when no future appts booked) ────────────
    if result['future_count'] == 0 and result['avg_cadence_days']:
        try:
            cadence = result['avg_cadence_days']
            pday    = result['preferred_day']
            target  = _date.today() + _timedelta(days=cadence)
            if pday:
                day_map = {'monday':0,'tuesday':1,'wednesday':2,'thursday':3,
                           'friday':4,'saturday':5,'sunday':6}
                wd = day_map.get(pday.lower())
                if wd is not None:
                    snap = (wd - target.weekday()) % 7
                    target = target + _timedelta(days=snap)
            wks_str  = f"~{round(cadence / 7)} wks"
            pday_str = f"{pday}, " if pday else ""
            pt_str   = f" {result['preferred_time']}" if result['preferred_time'] else ""
            result['suggested_next'] = f"{friendly(target.isoformat())} ({pday_str}{wks_str}{pt_str})"
        except Exception:
            pass

    # ── Query: ClientNotes — last 10, newest first ────────────────────────
    cn_rows = run_query_rows(f"""
SELECT TOP 10 CONVERT(varchar,CNDate,23), ISNULL(CNSubject,''), ISNULL(CNBy,''), ISNULL(CNNotes,'')
FROM ClientNotes WHERE CNClientSeq={cid}
ORDER BY CNDate DESC, CNSeq DESC
""")
    result['client_notes'] = [
        {'date': r[0], 'subject': r[1].strip(), 'by': r[2].strip(), 'text': r[3].strip()}
        for r in cn_rows if r and len(r) >= 4
    ]

    # ── Query: phone + inactive (needed by pending tab) ───────────────────
    extra = run_query_rows(f"SELECT ISNULL(CLPhone1,''), ISNULL(CAST(CLInactive AS varchar),'') FROM Clients WHERE CLSeq={cid}")
    if extra and extra[0] and len(extra[0]) >= 2:
        result['phone']    = extra[0][0].strip()
        result['inactive'] = extra[0][1].strip() == '-1'
    else:
        result['phone']    = ''
        result['inactive'] = False

    _cache.set(f'dossier:{cid}', result, _TTL_DOSSIER)
    return result


def _get_pet_context(pet_id):
    """Return pet warning/groom/notes and PetNotes rows for a given pet ID."""
    pid = int(pet_id)
    result = {'pet_warning': '', 'pet_groom': '', 'pet_notes': '',
              'is_new_pet': False, 'pet_notes_rows': []}

    rows = run_query_rows(
        f"SELECT ISNULL(PtWarning,''), ISNULL(PtGroom,''), ISNULL(PtNotes,''), ISNULL(PtPetName,'') "
        f"FROM Pets WHERE PtSeq={pid}")
    if rows and rows[0] and len(rows[0]) >= 4:
        r = rows[0]
        result['pet_warning'] = r[0].strip()
        result['pet_groom']   = r[1].strip()
        result['pet_notes']   = r[2].strip()
        result['is_new_pet']  = ':NEW' in r[3].upper()

    pn_rows = run_query_rows(f"""
SELECT TOP 10 CONVERT(varchar,PNDate,23), ISNULL(PNSubject,''), ISNULL(PNBy,''), ISNULL(PNNotes,'')
FROM PetNotes WHERE PNPetSeq={pid}
ORDER BY PNDate DESC, PNSeq DESC
""")
    result['pet_notes_rows'] = [
        {'date': r[0], 'subject': r[1].strip(), 'by': r[2].strip(), 'text': r[3].strip()}
        for r in pn_rows if r and len(r) >= 4
    ]
    return result


# ── Scheduling-aware draft helpers ───────────────────────────────────────────

_APPT_KEYWORDS = {
    'appointment', 'schedule', 'book', 'available', 'availability', 'opening',
    'come in', 'bring', 'slot', 'sooner', 'earlier', 'later', 'reschedule',
    'cancel', 'move', 'change', 'groom', 'bath', 'haircut', 'trim',
    'next week', 'next month', 'this week', 'monday', 'tuesday', 'wednesday',
    'thursday', 'friday', 'saturday', 'jan', 'feb', 'mar', 'apr', 'jun',
    'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
}

def _sms_is_appointment_related(message):
    """Return True if the message is about scheduling."""
    msg = message.lower()
    return any(kw in msg for kw in _APPT_KEYWORDS)

def _sms_load_scheduling_doc():
    """Return first 3500 chars of SCHEDULING_QUICK_REFERENCE.md."""
    doc = os.path.join(_get_ext_dir(), 'staff', 'SCHEDULING_QUICK_REFERENCE.md')
    try:
        with open(doc, encoding='utf-8') as f:
            return f.read()[:3500]
    except Exception:
        return ''

def _get_holidays(start_date_str, days=45):
    """Canonical holiday fetch — cached 24h. Use this instead of inline Calendar queries."""
    key = f'holidays:{start_date_str}:{days}'
    cached = _cache.get(key)
    if cached is not None:
        return cached
    rows = run_query_rows(
        f"SELECT CONVERT(VARCHAR(10), Date, 120) FROM Calendar "
        f"WHERE Date BETWEEN '{start_date_str}' "
        f"AND DATEADD(day,{days},'{start_date_str}') "
        f"AND Styleset IN ('HOLIDAY','CLOSED')"
    )
    result = {r[0] for r in rows if r}
    _cache.set(key, result, _TTL_HOLIDAYS)
    return result


def _sms_get_compact_availability():
    """Return a compact text block of the next ~8 open slots per active groomer.

    14:30 is intentionally excluded — it is only offered when a human explicitly
    requests it, not in auto-generated SMS suggestions.
    """
    cached = _cache.get('compact_avail')
    if cached is not None:
        return cached

    import datetime as dt
    today   = dt.date.today()
    end     = today + dt.timedelta(days=45)
    today_s = today.isoformat()
    end_s   = end.isoformat()

    # 14:30 excluded from auto-suggestions (offer only if client specifically asks)
    STD_SLOTS = ['08:30', '10:00', '11:30', '13:30']
    GROOMERS  = [
        (59, 'Kumi',     'handstrip only'),
        (85, 'Tomoko',   ''),
        (95, 'Mandilyn', 'LG/XL default'),
    ]

    def slot_min(s):
        h, m = int(s[:2]), int(s[3:])
        return h * 60 + m

    # Closed / holiday dates
    hols = _get_holidays(today_s, 45)

    # LIMIT-blocked dates
    limits = {r[0] for r in run_query_rows(
        f"SELECT DISTINCT CONVERT(VARCHAR(10),GLDate,120) FROM GroomingLog "
        f"WHERE GLPetID=12120 AND GLDate>'{today_s}' AND GLDate<='{end_s}' "
        f"AND (GLDeleted IS NULL OR GLDeleted=0)") if r}

    # Build blocked slots using real appointment durations (start + end time).
    # A standard slot is blocked if any existing appointment overlaps it —
    # i.e. appt_start <= slot_start < appt_end.
    taken = set()
    appt_rows = run_query_rows(
        f"SELECT GLGroomerID, CONVERT(VARCHAR(10),GLDate,120), "
        f"CONVERT(VARCHAR(5),DATEADD(MINUTE,DATEDIFF(MINUTE,'1899-12-30',GLInTime),0),108), "
        f"CONVERT(VARCHAR(5),DATEADD(MINUTE,DATEDIFF(MINUTE,'1899-12-30',GLOutTime),0),108) "
        f"FROM GroomingLog WHERE GLDate>'{today_s}' AND GLDate<='{end_s}' "
        f"AND (GLDeleted IS NULL OR GLDeleted=0) "
        f"AND (GLWaitlist IS NULL OR GLWaitlist=0) "
        f"AND GLGroomerID IS NOT NULL "
        f"AND GLInTime IS NOT NULL AND GLOutTime IS NOT NULL")
    for r in appt_rows:
        if len(r) < 4:
            continue
        try:
            gid      = int(r[0])
            date_s   = r[1]
            start_m  = slot_min(r[2][:5])
            end_m    = slot_min(r[3][:5])
            if end_m <= start_m:  # bad data guard
                end_m = start_m + 90
            for s in STD_SLOTS:
                sm = slot_min(s)
                if start_m <= sm < end_m:
                    taken.add((gid, date_s, s))
        except Exception:
            pass

    def working_days_for(gid):
        """Return set of date-strings when this groomer is scheduled."""
        working = set()
        for r in run_query_rows(
                f"SELECT CONVERT(VARCHAR(10),GroomerSchWEDate,120),"
                f"GroomerSchtueIn,GroomerSchwedIn,GroomerSchthurIn,"
                f"GroomerSchfriIn,GroomerSchsatIn "
                f"FROM GroomerSched WHERE GroomerSchID={gid} "
                f"AND GroomerSchWEDate>=DATEADD(day,-6,'{today_s}') "
                f"AND GroomerSchWEDate<=DATEADD(day,7,'{end_s}')"):
            if len(r) < 6: continue
            try: we = dt.date.fromisoformat(r[0])
            except: continue
            for offset, val in [(-4,r[1]),(-3,r[2]),(-2,r[3]),(-1,r[4]),(0,r[5])]:
                if val and val.strip() and val.strip().upper() not in ('NULL',''):
                    d = we + dt.timedelta(days=offset)
                    if today < d <= end:
                        working.add(d.isoformat())
        return working

    lines = []
    for gid, name, note in GROOMERS:
        working = working_days_for(gid)
        found   = []
        for i in range(1, 46):
            d  = today + dt.timedelta(days=i)
            ds = d.isoformat()
            if d.weekday() == 0 or ds in hols or ds in limits or ds not in working:
                continue
            for s in STD_SLOTS:
                if (gid, ds, s) not in taken:
                    found.append(f"{d.strftime('%a %b')} {d.day} {s}")
            if len(found) >= 8:
                break
        label = f"{name} ({note})" if note else name
        lines.append(f"{label}: " + (', '.join(found[:8]) if found else 'no slots in next 45 days'))
    result_text = '\n'.join(lines)
    _cache.set('compact_avail', result_text, _TTL_COMPACT_AVAIL)
    return result_text

def _run_one_shot_claude(system_text, user_msg, timeout=60):
    """Run a one-shot claude -p call.  Returns the result string or None."""
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as sf:
            sf.write(system_text)
            sys_path = sf.name
        if IS_WINDOWS:
            sys_wsl = _win_to_wsl_path(sys_path)
            cmd = ['wsl', WSL_CLAUDE_PATH, '-p',
                   '--system-prompt-file', sys_wsl, '--output-format', 'json', user_msg]
        else:
            cmd = [WSL_CLAUDE_PATH, '-p',
                   '--system-prompt-file', sys_path, '--output-format', 'json', user_msg]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        os.unlink(sys_path)
        if result.returncode == 0:
            return json.loads(result.stdout.strip()).get('result', '').strip()
        print(f"[Claude] error: {result.stderr[:200]}")
    except Exception as e:
        print(f"[Claude] exception: {e}")
    return None

def _sms_lookup_client(name_query):
    """Look up a client by 'FirstName LastName' or 'pet:PetName'. Returns dict or None."""
    q = name_query.strip().replace("'", "").replace('"', '')  # sanitize quotes
    if q.lower().startswith('pet:'):
        pet = q[4:].strip()
        rows = run_query_rows(
            f"SELECT TOP 1 c.CLSeq, c.CLFirstName, c.CLLastName, c.CLPhone1 "
            f"FROM Clients c INNER JOIN Pets p ON p.PtOwnerCode=c.CLSeq "
            f"WHERE p.PtPetName LIKE '%{pet}%' "
            f"AND (c.CLDeleted IS NULL OR c.CLDeleted=0) "
            f"AND (p.PtDeleted IS NULL OR p.PtDeleted=0)")
    else:
        parts = q.split()
        if len(parts) >= 2:
            fname, lname = parts[0], parts[-1]
            rows = run_query_rows(
                f"SELECT TOP 1 CLSeq, CLFirstName, CLLastName, CLPhone1 FROM Clients "
                f"WHERE CLFirstName LIKE '%{fname}%' AND CLLastName LIKE '%{lname}%' "
                f"AND (CLDeleted IS NULL OR CLDeleted=0)")
        else:
            rows = run_query_rows(
                f"SELECT TOP 1 CLSeq, CLFirstName, CLLastName, CLPhone1 FROM Clients "
                f"WHERE (CLFirstName LIKE '%{q}%' OR CLLastName LIKE '%{q}%') "
                f"AND (CLDeleted IS NULL OR CLDeleted=0)")
    if not rows or not rows[0] or len(rows[0]) < 4:
        return None
    r = rows[0]
    phone = str(r[3] or '').strip().replace('(','').replace(')','').replace('-','').replace(' ','')
    return {'client_id': int(r[0]), 'client_name': f"{r[1]} {r[2]}", 'phone': phone}


def _suggest_next_date(avg_cadence_days, preferred_day):
    """Compute suggested next appointment date from today + cadence, snapped to preferred day."""
    from datetime import date, timedelta
    cadence = float(avg_cadence_days) if avg_cadence_days else 42.0
    target = date.today() + timedelta(days=cadence)
    if preferred_day:
        day_map = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
                   'friday': 4, 'saturday': 5, 'sunday': 6}
        wd = day_map.get(preferred_day.lower())
        if wd is not None:
            delta = (wd - target.weekday()) % 7
            target = target + timedelta(days=delta)
    return target.strftime('%m/%d/%Y')


# ── PENDING TAB — RESERVED FOR FUTURE IMPLEMENTATION ─────────────────────────
# Frontend removed Feb 2026. Backend logic preserved as implementation record.
# See git history (popup.js) for frontend reference.

# ── Pending Appointments Intelligence ─────────────────────────────────────────

def _parse_pending_html(html):
    """Extract pending appointment data from KCApp pendinglist HTML using regex."""
    appointments = []

    # Find all pending appointment block IDs (div id="pendingapp_XXXXXXX")
    appt_ids = re.findall(r'id=["\']pendingapp_(\d+)["\']', html)
    if not appt_ids:
        return appointments

    for appt_id in appt_ids:
        appt = {'appointment_id': int(appt_id), 'contract_terms': ''}

        # Extract a chunk of HTML for this appointment block
        idx = html.find(f'pendingapp_{appt_id}')
        if idx == -1:
            continue
        chunk = html[idx:idx + 10000]

        # Date/time string: e.g. "4/29 at 10am", "03/15/2026 at 8:30am"
        m = re.search(r'(\d{1,2}/\d{1,2}(?:/\d{2,4})?\s+at\s+\d{1,2}(?::\d{2})?\s*[ap]m)',
                      chunk, re.IGNORECASE)
        appt['date_str'] = m.group(1).strip() if m else ''

        # Client: href="/#/clients/details/123" with anchor text
        m = re.search(r'href=["\'](?:/#|#)?/clients/details/(\d+)["\'][^>]*>([^<]+)', chunk)
        if m:
            appt['client_id'] = int(m.group(1))
            appt['client_name'] = m.group(2).strip()
        else:
            appt['client_id'] = None
            appt['client_name'] = ''

        # Pet: href="/#/pets/details/123"
        m = re.search(r'href=["\'](?:/#|#)?/pets/details/(\d+)["\'][^>]*>([^<]+)', chunk)
        if m:
            appt['pet_id'] = int(m.group(1))
            appt['pet_name'] = m.group(2).strip()
        else:
            appt['pet_id'] = None
            appt['pet_name'] = ''

        # Requested employee
        m = re.search(r'<em>Employee:\s*([^<]+)</em>', chunk, re.IGNORECASE)
        appt['employee_requested'] = m.group(1).strip() if m else 'Any Groomer'

        # Services (Items-Description spans) — unescape HTML entities (&nbsp; etc.)
        services = re.findall(r'class=["\']Items-Description["\'][^>]*>([^<]+)', chunk)
        appt['services'] = [_html.unescape(s).strip() for s in services if s.strip()]

        # Contract terms — look for "accepted"/"rejected" text near term keywords
        terms_m = re.search(r'(REJECTED|ACCEPTED)[^<]*(?:rabies|vaccination|contract|terms)[^<]*',
                             chunk, re.IGNORECASE)
        if terms_m:
            appt['contract_terms'] = terms_m.group(0).strip()

        # Default denial / waitlist email textarea content
        m = re.search(
            rf'id=["\']denyconfirmation_email_{appt_id}_default["\'][^>]*>(.*?)</textarea>',
            chunk, re.DOTALL)
        appt['denial_email'] = _html.unescape(re.sub(r'<[^>]+>', '', m.group(1)).strip()) if m else ''

        m = re.search(
            rf'id=["\']waitlistconfirmation_email_{appt_id}_default["\'][^>]*>(.*?)</textarea>',
            chunk, re.DOTALL)
        appt['waitlist_email'] = _html.unescape(re.sub(r'<[^>]+>', '', m.group(1)).strip()) if m else ''

        appointments.append(appt)

    return appointments


def _parse_requested_date(date_str):
    """Parse '4/29 at 10am' or '03/15/2026 at 8:30am' → 'YYYY-MM-DD'. Returns None if unparseable."""
    m = re.search(r'(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?', date_str or '')
    if not m:
        return None
    month, day = int(m.group(1)), int(m.group(2))
    year_raw = m.group(3)
    year = int(year_raw) if year_raw else datetime.now().year
    if year < 100:
        year += 2000
    try:
        dt = datetime(year, month, day)
        if dt < datetime.now() and not year_raw:
            dt = datetime(year + 1, month, day)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return None


def _enrich_pending_appt(appt):
    """Add DB context to a parsed appointment dict (mutates in place)."""
    client_id = appt.get('client_id')
    pet_id    = appt.get('pet_id')

    dossier = _sms_get_client_dossier(client_id) if client_id else {}
    pet_ctx = _get_pet_context(pet_id) if pet_id else {}

    appt['db'] = {
        'client_warning':  dossier.get('warning') or '',
        'client_notes':    dossier.get('client_notes', []),
        'client_discount': '',
        'client_inactive': dossier.get('inactive', False),
        'phone':           dossier.get('phone', ''),
        'is_new_client':   dossier.get('is_new_client', True),
        'pet_warning':     pet_ctx.get('pet_warning', ''),
        'pet_groom':       pet_ctx.get('pet_groom', ''),
        'pet_notes':       pet_ctx.get('pet_notes', ''),
        'is_new_pet':      pet_ctx.get('is_new_pet', False),
        'history':         [],
        'total_appts':     0,
        'day_groomer_load': {},
        'requested_date':  '',
    }

    # CLInvoiceWarning — not in dossier
    if client_id:
        r = run_query_rows(f"SELECT ISNULL(CLInvoiceWarning,'') FROM Clients WHERE CLSeq={client_id}")
        if r and r[0]:
            appt['db']['client_discount'] = r[0][0].strip()

    # Per-pet appointment history with no-show status
    if pet_id:
        rows = run_query_rows(
            f"SELECT TOP 12 CONVERT(varchar,gl.GLDate,23), ISNULL(e.USFNAME,'Unknown'), "
            f"CASE WHEN gl.GLNoShow=-1 THEN 'NOSHOW' ELSE 'OK' END "
            f"FROM GroomingLog gl LEFT JOIN Employees e ON gl.GLGroomerID=e.USSEQN "
            f"WHERE gl.GLPetID={pet_id} AND (gl.GLDeleted IS NULL OR gl.GLDeleted=0) "
            f"AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist=0) ORDER BY gl.GLDate DESC")
        appt['db']['history']     = [{'date': r[0], 'groomer': r[1], 'status': r[2]}
                                      for r in rows if r and len(r) >= 3]
        appt['db']['total_appts'] = len(appt['db']['history'])

    # Day-of groomer load
    requested_date = _parse_requested_date(appt.get('date_str', ''))
    if requested_date:
        appt['db']['requested_date'] = requested_date
        rows = run_query_rows(
            f"SELECT e.USFNAME, COUNT(*) FROM GroomingLog gl "
            f"INNER JOIN Employees e ON gl.GLGroomerID=e.USSEQN "
            f"WHERE gl.GLDate='{requested_date}' AND (gl.GLDeleted IS NULL OR gl.GLDeleted=0) "
            f"AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist=0) GROUP BY e.USFNAME")
        appt['db']['day_groomer_load'] = {r[0]: int(r[1]) for r in rows if r and len(r) >= 2}


def _build_pending_prompt(appointments):
    """Build the combined one-shot Claude prompt for all pending appointments."""
    lines = [
        "You are a grooming salon assistant at Dog's Best Friend & The Cat's Meow (Albany, CA).",
        "Analyze the following online appointment requests and provide a briefing for each.",
        "",
        "BUSINESS RULES:",
        "- Open Tue–Sat 8:30am–5:30pm. Closed Sun/Mon.",
        "- GROOMER ASSIGNMENT by size (from the size code in the service name):",
        "  XS/SM → Tomoko (ID 85); MD → Tomoko or Mandilyn; LG/XL → Mandilyn (ID 95).",
        "  New clients (0 prior appointments) always go to Mandilyn regardless of size.",
        "- Kumi (ID 59): handstrip ONLY. A pet is a handstrip candidate ONLY when '#' appears",
        "  in the pet name as stored in our DB. Do NOT infer handstrip from breed name alone.",
        "  If '#' is NOT in the pet name, never suggest or discuss handstrip.",
        "- Elmer (ID 8): primary bather. Josh (ID 91): backup bather.",
        "- More than 5 dogs/day for a groomer = at capacity — flag it.",
        "- ':NEW' in pet name = not yet had first completed appointment.",
        "- No cat grooming (exception: Sadie Donnelly nail trim only).",
        "- VACCINATIONS: we take the client's word — we do not require proof of vaccination.",
        "  Clients are welcome to bring documentation but it is not required or asked for.",
        "  A rejected rabies vaccine contract term should be mentioned as a note only;",
        "  do NOT make it an action step or ask the client to provide proof.",
        "- DUPLICATE APPOINTMENTS: only flag if two of the pet's upcoming appointments are",
        "  within 3 weeks of each other AND the same service type (e.g. two full grooms).",
        "  A bath and a full-groom appointment even close together is normal and intentional.",
        "  Multiple future appointments spread out are completely normal — do not flag them.",
        "- If a client has always booked with one specific groomer, note 'Any Groomer' requests.",
        "",
        "YOUR PRIMARY ANALYSIS for each request (cover all four in your briefing):",
        "1. DATE/TIME — Is the requested day a business day (Tue–Sat)? Is the time within",
        "   8:30am–5:30pm? Flag if the date falls on a Sunday or Monday.",
        "2. GROOMER FIT — Extract the size code from the service name (e.g. 'MDLH' → MD).",
        "   Is the requested groomer (or 'Any Groomer') appropriate for that size?",
        "   For 'Any Groomer', recommend the best fit and note if they are available.",
        "3. CAPACITY — How many dogs does the target groomer already have that day?",
        "   Flag if at or near capacity (≥5).",
        "4. CLIENT/PET FLAGS — Any warnings, inactive status, no-shows, or notes staff",
        "   should see before approving.",
        "",
    ]

    for i, appt in enumerate(appointments, 1):
        db = appt.get('db', {})
        history = db.get('history', [])

        groomer_counts = {}
        noshow_count = 0
        for h in history:
            g = h.get('groomer', 'Unknown')
            groomer_counts[g] = groomer_counts.get(g, 0) + 1
            if h.get('status') == 'NOSHOW':
                noshow_count += 1
        groomer_hist = ', '.join(f"{g}: {c}" for g, c in groomer_counts.items()) or 'None'
        last_visit = history[0]['date'] if history else 'Never'

        day_load = db.get('day_groomer_load', {})
        day_load_str = (', '.join(f"{g}: {c}" for g, c in day_load.items())
                        if day_load else 'No appointments yet that day')

        # Compute day-of-week label for the requested date so Claude can check business hours
        requested_date = db.get('requested_date', '')
        day_of_week = ''
        if requested_date:
            try:
                from datetime import datetime as _dt
                day_of_week = _dt.strptime(requested_date, '%Y-%m-%d').strftime('%A')
            except Exception:
                pass

        pet_name_display = appt.get('pet_name', 'Unknown')
        is_handstrip_candidate = '#' in pet_name_display

        lines += [
            "---", "",
            f"APPOINTMENT {i} (ID: {appt['appointment_id']}):",
            "REQUEST:",
            f"- Client: {appt.get('client_name', 'Unknown')} "
            f"(ID {appt.get('client_id', '?')}) | Phone: {db.get('phone', 'unknown')}",
            f"- Pet: {pet_name_display} (ID {appt.get('pet_id', '?')}) "
            f"| Handstrip candidate (#): {'YES' if is_handstrip_candidate else 'NO'}",
            f"- Requested date: {appt.get('date_str', 'Unknown')}"
            + (f" ({day_of_week})" if day_of_week else ""),
            f"- Groomer preference: {appt.get('employee_requested', 'Any Groomer')}",
            f"- Services: {', '.join(appt.get('services', [])) or 'Not specified'}",
        ]
        if appt.get('contract_terms'):
            lines.append(f"- Contract terms (note only — no proof required): {appt['contract_terms']}")

        lines += [
            "",
            "DATABASE CONTEXT:",
            f"- New client: {'Yes' if db.get('is_new_client') else 'No'} "
            f"({db.get('total_appts', 0)} prior completed appointments)",
            f"- New pet (:NEW in name): {'Yes' if db.get('is_new_pet') else 'No'}",
            f"- Groomer history: {groomer_hist}",
            f"- Last visit: {last_visit}  |  No-shows: {noshow_count}",
            f"- Client warnings: {db.get('client_warning') or 'none'}",
            f"- Client discount: {db.get('client_discount') or 'none'}",
            f"- Client inactive flag: {'Yes' if db.get('client_inactive') else 'No'}",
            f"- Pet warnings: {db.get('pet_warning') or 'none'}",
            f"- Pet groom notes: {db.get('pet_groom') or 'none'}",
            f"- Groomer load on {day_of_week or requested_date}: {day_load_str}",
            "",
        ]

    lines += [
        "---", "",
        "For EACH appointment output EXACTLY this format:",
        "### BRIEFING {appointment_id}",
        "[One-sentence summary of what is being requested]",
        "[Flag bullets: start each with ⚠️ for concerns, ✓ for OK items]",
        "Action steps:",
        "1. [first concrete action]",
        "2. [next step]",
        "(add more steps as needed)",
        "### END {appointment_id}",
        "",
        "Replace {appointment_id} with the actual numeric ID. Output ONLY the briefings — no preamble.",
    ]
    return '\n'.join(lines)


def _get_pending_briefings_from_claude(appointments):
    """Call Claude with the combined pending prompt. Returns list of briefing dicts."""
    if not appointments:
        return []

    prompt = _build_pending_prompt(appointments)
    system_text = (
        "You are a knowledgeable grooming salon assistant. Provide concise, actionable briefings "
        "for appointment requests. Focus on flags and action steps staff need right now. "
        "Output ONLY the briefings in the specified format — no preamble, no summary at the end."
    )

    t0 = datetime.now()
    log.info(f"[Pending] Calling Claude for {len(appointments)} appointment(s)...")

    # Write system prompt to a temp file (same as _run_one_shot_claude).
    # Pass the user message via stdin instead of as a CLI arg — multi-line prompts get
    # mangled by Windows list2cmdline when passed as an argument through WSL.
    raw = None
    sys_path = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as sf:
            sf.write(system_text)
            sys_path = sf.name

        if IS_WINDOWS:
            sys_wsl = _win_to_wsl_path(sys_path)
            cmd = ['wsl', WSL_CLAUDE_PATH, '-p',
                   '--system-prompt-file', sys_wsl, '--output-format', 'json']
        else:
            cmd = [WSL_CLAUDE_PATH, '-p',
                   '--system-prompt-file', sys_path, '--output-format', 'json']

        result = subprocess.run(cmd, input=prompt, capture_output=True,
                                encoding='utf-8', errors='replace', timeout=120)
        if sys_path:
            os.unlink(sys_path)

        if result.returncode == 0 and result.stdout.strip():
            try:
                raw = json.loads(result.stdout.strip()).get('result', '').strip()
            except json.JSONDecodeError as e:
                log.warning(f"[Pending] JSON parse error: {e}. stdout={result.stdout[:200]}")
        else:
            log.warning(f"[Pending] CLI exit={result.returncode} stderr={result.stderr[:300]}")
    except subprocess.TimeoutExpired:
        log.warning("[Pending] Claude CLI timed out after 120s")
        if sys_path and os.path.exists(sys_path):
            os.unlink(sys_path)
    except Exception as e:
        log.warning(f"[Pending] Claude CLI exception: {e}")
        if sys_path and os.path.exists(sys_path):
            os.unlink(sys_path)

    elapsed = (datetime.now() - t0).seconds
    log.info(f"[Pending] Claude call done in {elapsed}s, got {len(raw or '')} chars")

    if not raw:
        log.warning("[Pending] Claude returned no output")
        return [{
            **{k: v for k, v in a.items() if k != 'db'},
            'briefing': '(AI analysis unavailable — check backend logs)',
            'denial_email': a.get('denial_email', ''),
            'waitlist_email': a.get('waitlist_email', ''),
        } for a in appointments]

    # Parse response: ### BRIEFING {id} ... ### END {id}
    briefing_map = {}
    for m in re.finditer(r'###\s*BRIEFING\s+(\d+)\s*\n(.*?)###\s*END\s+\1', raw, re.DOTALL):
        briefing_map[m.group(1)] = m.group(2).strip()

    results = []
    for appt in appointments:
        aid = str(appt['appointment_id'])
        briefing = briefing_map.get(aid, f'(Could not parse briefing for ID {aid})')
        results.append({
            'appointment_id': appt['appointment_id'],
            'date_str':           appt.get('date_str', ''),
            'client_name':        appt.get('client_name', ''),
            'client_id':          appt.get('client_id'),
            'pet_name':           appt.get('pet_name', ''),
            'pet_id':             appt.get('pet_id'),
            'employee_requested': appt.get('employee_requested', 'Any Groomer'),
            'services':           appt.get('services', []),
            'contract_terms':     appt.get('contract_terms', ''),
            'briefing':           briefing,
            'denial_email':       appt.get('denial_email', ''),
            'waitlist_email':     appt.get('waitlist_email', ''),
        })
    return results


def _sms_regen_with_feedback(ctx, their_message, original_draft, feedback):
    """Regenerate a draft SMS reply given user feedback on the previous draft."""
    if not ctx:
        return None

    pets_str  = ', '.join(ctx['pets']) if ctx['pets'] else 'no pets on file'
    appts_str = '; '.join(ctx['upcoming_appointments']) if ctx['upcoming_appointments'] else 'none upcoming'
    conv_str  = '\n'.join(ctx['recent_conversation'][-8:]) if ctx['recent_conversation'] else 'No recent messages'

    avail_block = ''
    sched_rules = ''
    if _sms_is_appointment_related(their_message):
        try:
            avail_text  = _sms_get_compact_availability()
            avail_block = f"\n\nREAL OPEN SLOTS (use these exact dates/times when proposing):\n{avail_text}"
            sched_rules = '\n\nSCHEDULING RULES:\n' + _sms_load_scheduling_doc()
        except Exception as e:
            print(f"[SMS] Availability lookup error in regen: {e}")

    system = (
        "You are drafting SMS replies for Dog's Best Friend grooming salon. Write as Noah, the owner — "
        "small family business, direct and friendly, the way a real person texts. "
        "Style rules: start with 'Hi [FirstName]', no emojis, no exclamation marks, no 'I'd be happy to', "
        "no 'Great news', no corporate filler. Short sentences. Say what you mean. "
        "IMPORTANT: No cat grooming — we have no cat groomer. Only exception: Sadie Donnelly nail trim. "
        "For appointment requests: offer at most ONE or TWO specific slots, not a menu of options. "
        "Pick the best fit and offer it. Only use slots from the REAL OPEN SLOTS list — never invent times. "
        "Respond with ONLY the message text — no quotes, no label, no explanation."
        f"{sched_rules}"
    )
    user_msg = (
        f"Client: {ctx['first_name']} {ctx['last_name']}\n"
        f"Pets: {pets_str}\n"
        f"Upcoming appointments: {appts_str}\n"
        f"Recent conversation:\n{conv_str}"
        f"{avail_block}\n\n"
        f"They just wrote: \"{their_message}\"\n\n"
        f"Previous draft: \"{original_draft}\"\n"
        f"User feedback: {feedback}\n\n"
        f"Revise the draft based on the feedback."
    )
    return _run_one_shot_claude(system, user_msg, timeout=90)


def _sms_generate_draft(ctx, their_message):
    """Call Claude to generate a scheduling-aware draft SMS reply."""
    if not ctx:
        return None

    pets_str  = ', '.join(ctx['pets']) if ctx['pets'] else 'no pets on file'
    appts_str = '; '.join(ctx['upcoming_appointments']) if ctx['upcoming_appointments'] else 'none upcoming'
    conv_str  = '\n'.join(ctx['recent_conversation'][-8:]) if ctx['recent_conversation'] else 'No recent messages'

    # For appointment-related messages, pull real availability + scheduling rules
    avail_block = ''
    sched_rules = ''
    if _sms_is_appointment_related(their_message):
        print(f"[SMS] Message is appointment-related — pulling availability")
        try:
            avail_text  = _sms_get_compact_availability()
            avail_block = f"\n\nREAL OPEN SLOTS (use these exact dates/times when proposing):\n{avail_text}"
            sched_rules = '\n\nSCHEDULING RULES:\n' + _sms_load_scheduling_doc()
        except Exception as e:
            print(f"[SMS] Availability lookup error: {e}")

    system = (
        "You are drafting SMS replies for Dog's Best Friend grooming salon. Write as Noah, the owner — "
        "small family business, direct and friendly, the way a real person texts. "
        "Style rules: start with 'Hi [FirstName]', no emojis, no exclamation marks, no 'I'd be happy to', "
        "no 'Great news', no corporate filler. Short sentences. Say what you mean. "
        "IMPORTANT: No cat grooming — we have no cat groomer. Only exception: Sadie Donnelly nail trim. "
        "For appointment requests: offer at most ONE or TWO specific slots, not a menu of options. "
        "Pick the best fit and offer it. Only use slots from the REAL OPEN SLOTS list — never invent times. "
        "Respond with ONLY the message text — no quotes, no label, no explanation."
        f"{sched_rules}"
    )
    user_msg = (
        f"Client: {ctx['first_name']} {ctx['last_name']}\n"
        f"Pets: {pets_str}\n"
        f"Upcoming appointments: {appts_str}\n"
        f"Recent conversation:\n{conv_str}"
        f"{avail_block}\n\n"
        f"They just wrote: \"{their_message}\"\n\n"
        f"Draft a reply."
    )

    return _run_one_shot_claude(system, user_msg, timeout=90)

def _append_to_knowledge_base(category: str, content: str) -> bool:
    """Append a new entry to staff/KNOWLEDGE_BASE.md. Returns True on success."""
    kb_path = os.path.join(_get_ext_dir(), 'staff', 'KNOWLEDGE_BASE.md')
    try:
        os.makedirs(os.path.dirname(kb_path), exist_ok=True)
        if not os.path.exists(kb_path):
            with open(kb_path, 'w', encoding='utf-8') as f:
                f.write('# DBFCM Staff Knowledge Base\n\nBusiness rules and policies.\n\n')
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        entry = f'\n## [{timestamp}] {category}\n\n{content}\n'
        with open(kb_path, 'a', encoding='utf-8') as f:
            f.write(entry)
        log.info(f"[KB] Added entry under '{category}': {content[:80]}")
        return True
    except Exception as e:
        log.warning(f'[KB] Could not write to knowledge base: {e}')
        return False


def _extract_kb_from_noah_reply(message: str, escalation_context: str):
    """Use Claude to determine if Noah's SMS is KB-worthy. Returns (category, content) or None."""
    kb_path = os.path.join(_get_ext_dir(), 'staff', 'KNOWLEDGE_BASE.md')
    try:
        with open(kb_path, 'r', encoding='utf-8') as f:
            kb_text = f.read()[:3000]
    except FileNotFoundError:
        kb_text = '(empty)'

    esc_line = ''
    if escalation_context:
        esc_line = f'\nThis message is a reply to the employee question: "{escalation_context}"'

    system = (
        "You are a knowledge base curator for a pet grooming salon. "
        "Analyze the owner's text message and determine if it contains a policy, rule, or "
        "operational fact worth adding to the staff knowledge base.\n"
        "Respond with ONLY one of these two formats:\n\n"
        "Format 1 (KB-worthy):\n"
        "CATEGORY: [one of: Policies, Scheduling, Pricing, Services, Staff, Clients, Other]\n"
        "CONTENT: [clear, concise statement of the rule or fact]\n\n"
        "Format 2 (not KB-worthy — chitchat, acknowledgment, or already covered):\n"
        "NOT_KB"
    )
    user_msg = (
        f"Current knowledge base (first 3000 chars):\n{kb_text}\n\n"
        f"{esc_line}\n"
        f"Owner's message: \"{message}\"\n\n"
        "Is this new, useful staff knowledge not already covered? "
        "If yes, return CATEGORY and CONTENT. If no, return NOT_KB."
    )

    raw = _run_one_shot_claude(system, user_msg, timeout=45)
    if not raw:
        return None

    raw = raw.strip()
    if raw.upper().startswith('NOT_KB') or 'NOT_KB' in raw[:30]:
        return None

    cat_match     = re.search(r'CATEGORY:\s*(.+)', raw)
    content_match = re.search(r'CONTENT:\s*(.+)', raw, re.DOTALL)
    if cat_match and content_match:
        return (cat_match.group(1).strip(), content_match.group(1).strip())
    return None


def _handle_noah_inbound(msg_id: int, phone: str, message: str, timestamp: str):
    """Handle an inbound text from Noah: mark handled, check for KB content, optionally append."""
    # Mark as handled immediately — don't show in staff SMS queue
    _sms_mark_handled(msg_id)

    log.info(f"[SMS] Noah inbound msg_id={msg_id}: '{message[:80]}'")

    # Find the most recent unmatched escalation context. Check two sources:
    # 1. _sent_escalations — escalations that were already sent via the extension
    # 2. _sms_drafts — unsent escalation drafts (Know-a-bot may have created it but staff hasn't sent it yet)
    escalation_context = None
    matched_draft_id   = None

    # Source 1: sent escalations (most reliable — staff already reviewed and sent)
    sorted_escs = sorted(
        _sent_escalations.items(),
        key=lambda x: x[1].get('sent_at', ''),
        reverse=True,
    )
    for draft_id, esc in sorted_escs:
        if not esc.get('matched'):
            escalation_context = esc.get('escalation_context', '')
            matched_draft_id   = draft_id
            log.info(f"[SMS] Matched sent escalation {draft_id}: context='{escalation_context[:60]}'")
            break

    # Source 2: unsent escalation drafts (fallback when staff hasn't sent yet)
    if not escalation_context:
        with _sms_drafts_lock:
            unsent = sorted(
                [d for d in _sms_drafts.values() if d.get('is_escalation')],
                key=lambda d: d.get('timestamp', ''),
                reverse=True,
            )
        if unsent:
            escalation_context = unsent[0].get('escalation_context', '')
            log.info(f"[SMS] Using unsent escalation context: '{escalation_context[:60]}'")

    if not escalation_context:
        log.info("[SMS] No escalation context found — KB extraction will run without context")

    # Ask Claude if the message is KB-worthy
    kb_result = _extract_kb_from_noah_reply(message, escalation_context)
    log.info(f"[SMS] KB extraction result: {kb_result}")

    if kb_result:
        category, content = kb_result
        success = _append_to_knowledge_base(category, content)
        log.info(f"[SMS] KB append success={success}: [{category}] {content[:80]}")
        if success and matched_draft_id:
            _sent_escalations[matched_draft_id]['matched'] = True
            _save_pending_escalations()

    log.info(f"[SMS] Noah inbound processed: msg_id={msg_id}, kb_added={bool(kb_result)}")


def _sms_poll_inbound():
    """Check for new inbound SMS messages and generate drafts. Called every 30s."""
    global _sms_last_seen_id

    # On first run, just set the watermark — don't retroactively draft old messages
    if _sms_last_seen_id == 0:
        rows = run_query_rows("SELECT ISNULL(MAX(MessageId),0) FROM SMSMessages")
        if rows and rows[0]:
            try:
                _sms_last_seen_id = int(rows[0][0])
            except (ValueError, TypeError):
                _sms_last_seen_id = 0
        print(f"[SMS] Poller initialized, watermark MessageId={_sms_last_seen_id}")
        return

    rows = run_query_rows(
        f"SELECT TOP 20 MessageId, ClientId, Phone, Message, "
        f"CONVERT(VARCHAR(19),TimeReceivedOrSent,120) "
        f"FROM SMSMessages "
        f"WHERE IsSendSMSByBusiness=0 AND IsHandled=0 "
        f"AND MessageId>{_sms_last_seen_id} "
        f"ORDER BY MessageId ASC")

    new_max = _sms_last_seen_id
    for row in rows:
        if len(row) < 5:
            continue
        try:
            msg_id    = int(row[0])
            client_id = int(row[1]) if row[1] else 0
            phone     = row[2]
            message   = row[3]
            timestamp = row[4]
        except (ValueError, IndexError):
            continue

        new_max = max(new_max, msg_id)

        # Route Noah's personal cell texts to KB ingestion, not the staff SMS queue
        if _is_noah_phone(phone):
            log.info(f"[SMS] Noah inbound MessageId={msg_id}, routing to KB handler")
            _handle_noah_inbound(msg_id, phone, message, timestamp)
            continue

        draft_key = str(msg_id)

        with _sms_drafts_lock:
            if draft_key in _sms_drafts:
                continue  # already processed

        print(f"[SMS] New inbound MessageId={msg_id} ClientId={client_id}")

        ctx         = _sms_get_client_context(client_id) if client_id else None
        client_name = f"{ctx['first_name']} {ctx['last_name']}" if ctx else f"Client {client_id or phone}"
        draft_text  = _sms_generate_draft(ctx, message) if ctx else None

        # Store the prior conversation thread (exclude the trigger message itself)
        prior_thread = ctx['recent_conversation'][:-1] if ctx and ctx.get('recent_conversation') else []

        with _sms_drafts_lock:
            _sms_drafts[draft_key] = {
                'draft_id':           draft_key,
                'message_id':         msg_id,
                'client_id':          client_id,
                'client_name':        client_name,
                'phone':              phone,
                'their_message':      message,
                'recent_conversation': prior_thread,
                'draft':              draft_text or '',
                'timestamp':          timestamp,
            }
        _save_sms_drafts()
        log.info(f"[SMS] Draft ready for {client_name}: {(draft_text or '')[:60]}…")

    _sms_last_seen_id = new_max

def _start_sms_poller():
    """Start background thread polling for inbound SMS every 30 seconds."""
    def _loop():
        while True:
            try:
                _sms_poll_inbound()
            except Exception as e:
                print(f"[SMS] Poller error: {e}")
            time.sleep(30)
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    print("[SMS] Inbound poller started (30s interval)")

class WaitlistHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Parse URL
        parsed_path = urllib.parse.urlparse(self.path)

        # Enable CORS for extension
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

        if parsed_path.path == '/api/waitlist':
            data = self.get_waitlist()
            self.wfile.write(json.dumps(data).encode())
        elif parsed_path.path == '/api/groomers':
            data = self.get_groomers()
            self.wfile.write(json.dumps(data).encode())
        elif parsed_path.path == '/api/availability':
            query_params = urllib.parse.parse_qs(parsed_path.query)
            groomer_id = query_params.get('groomer_id', [None])[0]
            include_230 = query_params.get('include_230', ['1'])[0] == '1'
            if groomer_id:
                data = self.get_availability(int(groomer_id), include_230)
            else:
                data = {'error': 'groomer_id required'}
            self.wfile.write(json.dumps(data).encode())
        elif parsed_path.path == '/api/conflicts':
            data = self.get_conflicts()
            self.wfile.write(json.dumps(data).encode())
        elif parsed_path.path == '/api/conflicts/cached':
            data = self.get_conflicts_cached()
            self.wfile.write(json.dumps(data).encode())
        elif parsed_path.path == '/api/refresh-client-stats':
            data = self.refresh_client_stats_endpoint()
            self.wfile.write(json.dumps(data).encode())
        elif parsed_path.path == '/api/sms/drafts':
            data = self.sms_get_drafts()
            self.wfile.write(json.dumps(data).encode())
        elif parsed_path.path == '/api/client/dossier':
            params = urllib.parse.parse_qs(parsed_path.query)
            try:
                client_id = int(params.get('client_id', [0])[0])
            except (ValueError, TypeError):
                client_id = 0
            if not client_id:
                self.wfile.write(json.dumps({'error': 'client_id required'}).encode())
                return
            dossier = _sms_get_client_dossier(client_id)
            self.wfile.write(json.dumps(dossier).encode())
        elif parsed_path.path == '/api/checkout/today':
            data = self.get_checkout_today()
            self.wfile.write(json.dumps(data).encode())
        else:
            self.wfile.write(json.dumps({'error': 'Not found'}).encode())

    def do_HEAD(self):
        # Handle HEAD requests (used by extension to check if server is running)
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def do_POST(self):
        # Handle POST requests for updating data
        parsed_path = urllib.parse.urlparse(self.path)
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length).decode('utf-8')

        try:
            data = json.loads(post_data) if post_data else {}
        except json.JSONDecodeError:
            self.send_error_response(400, 'Invalid JSON')
            return

        if parsed_path.path == '/api/restart':
            self.send_json_response({'success': True, 'message': 'Restarting…'})
            def _restart():
                import time
                time.sleep(0.4)
                subprocess.Popen(
                    [sys.executable, os.path.abspath(__file__)] + sys.argv[1:],
                    creationflags=subprocess.CREATE_NEW_CONSOLE if IS_WINDOWS else 0
                )
                os._exit(0)
            threading.Thread(target=_restart, daemon=True).start()
            return
        elif parsed_path.path == '/api/waitlist/update-notes':
            result = self.update_notes(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/chat':
            message = data.get('message', '').strip()
            if not message:
                self.send_json_response({'success': False, 'error': 'Message is required'})
                return
            result = self.get_chat_response(message)
            self.send_json_response(result)
        elif parsed_path.path == '/api/chat/reset':
            result = self.reset_chat()
            self.send_json_response(result)
        elif parsed_path.path == '/api/sms/send':
            result = self.sms_send(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/sms/post-send':
            result = self.sms_post_send(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/sms/dismiss':
            result = self.sms_dismiss_draft(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/sms/regen':
            result = self.sms_regen_draft(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/sms/send':
            result = self.sms_send_via_kc(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/sms/queue-outbound':
            result = self.sms_queue_outbound(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/sms/compose':
            result = self.sms_compose(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/sms/draft-from-knowabot':
            result = self.sms_draft_from_knowabot(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/sms/extract-appt':
            result = self.sms_extract_appt(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/appt/book':
            result = self.appt_book(data)
            self.send_json_response(result)
        elif parsed_path.path == '/api/notes/add':
            entity_type = data.get('type', '')
            try:
                entity_id = int(data.get('id', 0))
            except (ValueError, TypeError):
                entity_id = 0
            subject    = (data.get('subject', '') or '')[:50].strip()
            notes_text = (data.get('notes', '') or '').strip()
            author     = author_code(data.get('author', ''))
            if not entity_id or not notes_text:
                self.send_error_response(400, 'id and notes are required')
                return
            if not subject:
                subject = notes_text[:47].rstrip() + ('…' if len(notes_text) > 47 else '')
            safe_subj  = subject.replace("'", "''")
            safe_notes = notes_text.replace("'", "''")
            if entity_type == 'client':
                run_query_rows(f"INSERT INTO ClientNotes "
                           f"(CNClientSeq,CNDate,CNSubject,CNBy,CNNotes,CNLOCSEQ) "
                           f"VALUES ({entity_id},GETDATE(),'{safe_subj}','{author}','{safe_notes}',1)")
                _cache.delete(f'dossier:{entity_id}')
            elif entity_type == 'pet':
                run_query_rows(f"INSERT INTO PetNotes "
                           f"(PNPetSeq,PNDate,PNSubject,PNBy,PNNotes,PNLOCSEQ) "
                           f"VALUES ({entity_id},GETDATE(),'{safe_subj}','{author}','{safe_notes}',1)")
            else:
                self.send_error_response(400, 'type must be client or pet')
                return
            self.send_json_response({'ok': True})
        elif parsed_path.path == '/api/pending/analyze':
            result = self.analyze_pending_appointments(data.get('html', ''))
            self.send_json_response(result)
        else:
            self.send_error_response(404, 'Not found')

    def send_json_response(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def send_error_response(self, status, message):
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({'error': message}).encode())

    def do_OPTIONS(self):
        # Handle preflight CORS requests
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, HEAD, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def get_waitlist(self):
        """Fetch waitlist from SQL Server"""
        query = """
        SELECT
            gl.GLSeq,
            CONVERT(varchar, gl.GLDate, 23) as ApptDate,
            CONVERT(varchar, gl.GLDateEntered, 23) as WLDate,
            CONVERT(varchar, gl.GLInTime, 108) as Time,
            p.PtPetName,
            p.PtSeq,
            c.CLSeq,
            c.CLLastName,
            ISNULL(c.CLAddress1, '') as Address,
            ISNULL(c.CLCity, '') as City,
            ISNULL(c.CLState, '') as State,
            ISNULL(c.CLZip, '') as Zip,
            ISNULL(b.BrBreed, '') as Breed,
            ISNULL(pt.PTypeName, '') as PetType,
            ISNULL(c.CLPhone1, '') as Phone,
            CASE
                WHEN gl.GLOthersID > 0 THEN 'Handstrip'
                WHEN gl.GLBath = -1 AND gl.GLGroom = 0 THEN 'Bath'
                WHEN gl.GLBath = 0 AND gl.GLGroom = -1 THEN 'Groom'
                WHEN gl.GLBath = -1 AND gl.GLGroom = -1 THEN 'Full Service'
                ELSE 'Other'
            END as ServiceType,
            CASE
                WHEN gl.GLOthersID > 0 THEN ISNULL(e3.USFNAME, '')
                WHEN gl.GLGroomerID > 0 THEN ISNULL(e1.USFNAME, '')
                ELSE ''
            END as Groomer,
            REPLACE(REPLACE(ISNULL(gl.GLDescription, ''), CHAR(13), ' '), CHAR(10), ' ') as Notes,
            ISNULL(c.CLWarning, '') as ClientWarning,
            ISNULL(p.PtWarning, '') as PetWarning,
            ISNULL(p.PTGroomWarning, '') as GroomWarning,
            (SELECT TOP 1 CONVERT(varchar, past.GLDate, 23)
             FROM GroomingLog past
             WHERE past.GLPetID = p.PtSeq
             AND past.GLCompleted = -1
             AND (past.GLDeleted IS NULL OR past.GLDeleted = 0)
             AND past.GLDate < GETDATE()
             ORDER BY past.GLDate DESC) as LastCompletedDate,
            (SELECT TOP 1 REPLACE(REPLACE(ISNULL(past.GLDescription, ''), CHAR(13), ' '), CHAR(10), ' ')
             FROM GroomingLog past
             WHERE past.GLPetID = p.PtSeq
             AND past.GLCompleted = -1
             AND (past.GLDeleted IS NULL OR past.GLDeleted = 0)
             AND past.GLDate < GETDATE()
             ORDER BY past.GLDate DESC) as LastCompletedNotes,
            (SELECT TOP 1 CONVERT(varchar, future.GLDate, 23)
             FROM GroomingLog future
             WHERE future.GLPetID = p.PtSeq
             AND (future.GLDeleted IS NULL OR future.GLDeleted = 0)
             AND future.GLWaitlist = 0
             AND future.GLDate > CAST(GETDATE() AS DATE)
             ORDER BY future.GLDate ASC) as NextScheduledDate,
            (SELECT TOP 1 ISNULL(emp.USFNAME, 'Unknown')
             FROM GroomingLog past
             LEFT JOIN Employees emp ON past.GLGroomerID = emp.USSEQN
             WHERE past.GLPetID = p.PtSeq
             AND past.GLCompleted = -1
             AND (past.GLDeleted IS NULL OR past.GLDeleted = 0)
             AND past.GLDate < GETDATE()
             ORDER BY past.GLDate DESC) as LastGroomer,
            (SELECT COUNT(*)
             FROM GroomingLog past
             WHERE past.GLPetID = p.PtSeq
             AND past.GLCompleted = -1
             AND (past.GLDeleted IS NULL OR past.GLDeleted = 0)) as TotalVisits,
            (SELECT STUFF((
                SELECT '|' + ISNULL(emp.USFNAME, 'Unknown') + ':' + CAST(COUNT(*) as varchar)
                FROM GroomingLog past
                LEFT JOIN Employees emp ON past.GLGroomerID = emp.USSEQN
                WHERE past.GLPetID = p.PtSeq
                AND past.GLCompleted = -1
                AND (past.GLDeleted IS NULL OR past.GLDeleted = 0)
                AND past.GLGroomerID > 0
                GROUP BY emp.USFNAME
                ORDER BY COUNT(*) DESC
                FOR XML PATH('')), 1, 1, '')) as GroomerStats
        FROM GroomingLog gl
        INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
        INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
        LEFT JOIN Breeds b ON p.PtBreedID = b.BrSeq
        LEFT JOIN PetTypes pt ON p.PtCat = pt.PTypeSeq
        LEFT JOIN Employees e1 ON gl.GLGroomerID = e1.USSEQN
        LEFT JOIN Employees e3 ON gl.GLOthersID = e3.USSEQN
        WHERE gl.GLWaitlist = -1
        AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
        ORDER BY gl.GLSeq
        """

        cmd = [
            'sqlcmd',
            '-S', db_utils.SQL_SERVER,
            '-d', db_utils.SQL_DATABASE,
            *db_utils.SQL_AUTH_ARGS,
            '-Q', query,
            '-s', '\t',  # Use TAB delimiter instead of pipe
            '-W',
            '-h', '-1'
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode != 0:
                return {
                    'error': 'SQL Server error',
                    'details': result.stderr,
                    'count': 0,
                    'waitlist': []
                }

            # Parse results
            lines = result.stdout.strip().split('\n')
            waitlist = []

            for line in lines:
                if '\t' in line and not line.startswith('--'):
                    row = line.split('\t')
                    if len(row) >= 27:
                        waitlist.append({
                            'glseq': row[0].strip(),
                            'appt_date': row[1].strip(),
                            'wl_date': row[2].strip() if row[2].strip() else 'N/A',
                            'time': row[3].strip(),
                            'pet_name': row[4].strip(),
                            'pet_id': row[5].strip(),
                            'client_id': row[6].strip(),
                            'last_name': row[7].strip(),
                            'address': row[8].strip(),
                            'city': row[9].strip(),
                            'state': row[10].strip(),
                            'zip': row[11].strip(),
                            'breed': row[12].strip(),
                            'pet_type': row[13].strip(),
                            'phone': row[14].strip(),
                            'service_type': row[15].strip(),
                            'groomer': row[16].strip(),
                            'notes': row[17].strip(),
                            'client_warning': row[18].strip(),
                            'pet_warning': row[19].strip(),
                            'groom_warning': row[20].strip(),
                            'last_completed_date': row[21].strip() if len(row) > 21 and row[21].strip() else None,
                            'last_completed_notes': row[22].strip() if len(row) > 22 and row[22].strip() else None,
                            'next_scheduled_date': row[23].strip() if len(row) > 23 and row[23].strip() else None,
                            'last_groomer': row[24].strip() if len(row) > 24 and row[24].strip() else None,
                            'total_visits': int(row[25].strip()) if len(row) > 25 and row[25].strip().isdigit() else 0,
                            'groomer_stats': row[26].strip() if len(row) > 26 and row[26].strip() else None
                        })

            return {
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'count': len(waitlist),
                'waitlist': waitlist
            }

        except subprocess.TimeoutExpired:
            return {
                'error': 'SQL query timeout',
                'count': 0,
                'waitlist': []
            }
        except Exception as e:
            return {
                'error': str(e),
                'count': 0,
                'waitlist': []
            }

    def update_notes(self, data):
        """Update GLDescription for a waitlist appointment"""
        glseq = data.get('glseq')
        notes = data.get('notes', '')

        if not glseq:
            return {'success': False, 'error': 'Missing glseq'}

        # Escape single quotes for SQL
        notes_escaped = notes.replace("'", "''")

        query = f"UPDATE GroomingLog SET GLDescription = '{notes_escaped}' WHERE GLSeq = {glseq}"

        cmd = [
            'sqlcmd',
            '-S', db_utils.SQL_SERVER,
            '-d', db_utils.SQL_DATABASE,
            *db_utils.SQL_AUTH_ARGS,
            '-Q', query
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode != 0:
                return {'success': False, 'error': result.stderr}

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Updated notes for GLSeq {glseq}")
            return {'success': True, 'glseq': glseq}

        except subprocess.TimeoutExpired:
            return {'success': False, 'error': 'SQL query timeout'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_groomers(self):
        """Get list of active groomers (excludes bathers and non-groomers)"""
        # Active groomers only: Nancy(2), Kumi(59), Tomoko(85), Mandilyn(95)
        # Excluded: Rachel(34) inactive, Natalia(104) inactive, Elmer(8) bather, Josh(91) bather, Noah(94) manager
        query = """
        SELECT USSEQN, USFNAME, USLNAME
        FROM Employees
        WHERE (USDeleted IS NULL OR USDeleted = 0)
        AND (USInactive IS NULL OR USInactive = 0)
        AND USSEQN IN (2, 59, 85, 95)
        ORDER BY USFNAME
        """

        cmd = [
            'sqlcmd', '-S', db_utils.SQL_SERVER, '-d', db_utils.SQL_DATABASE,
            *db_utils.SQL_AUTH_ARGS,
            '-Q', query, '-s', '\t', '-W', '-h', '-1'
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                return {'error': 'SQL Server error', 'groomers': []}

            groomers = []
            for line in result.stdout.strip().split('\n'):
                if '\t' in line:
                    row = line.split('\t')
                    if len(row) >= 3:
                        groomers.append({
                            'id': int(row[0].strip()),
                            'name': f"{row[1].strip()} {row[2].strip()}".strip()
                        })

            return {'groomers': groomers}
        except Exception as e:
            return {'error': str(e), 'groomers': []}

    def _run_query(self, query, use_tabs=True):
        """Run a sqlcmd query and return raw output lines"""
        cmd = [
            'sqlcmd', '-S', db_utils.SQL_SERVER, '-d', db_utils.SQL_DATABASE,
            *db_utils.SQL_AUTH_ARGS,
            '-Q', query, '-W', '-h', '-1'
        ]
        if use_tabs:
            cmd.extend(['-s', '\t'])
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return []
        return [l for l in result.stdout.strip().split('\n') if l.strip() and not l.startswith('--')]

    def get_availability(self, groomer_id, include_230=True):
        """Find all days with available slots for a groomer over 12 months - BULK QUERY VERSION"""
        import time
        t0 = time.time()

        if include_230:
            time_slots = ['08:30', '10:00', '11:30', '13:30', '14:30']
        else:
            time_slots = ['08:30', '10:00', '11:30', '13:30']

        start_date = datetime.now().date() + timedelta(days=1)
        end_date = start_date + timedelta(days=365)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        # --- BULK QUERY 1: All holidays in range ---
        holidays = set()
        try:
            lines = self._run_query(f"""
                SELECT CONVERT(varchar, Date, 23)
                FROM Calendar
                WHERE Date BETWEEN '{start_str}' AND '{end_str}'
                AND Styleset = 'HOLIDAY'
            """, use_tabs=False)
            for line in lines:
                d = line.strip()
                if d and len(d) == 10:
                    holidays.add(d)
        except:
            pass

        # --- BULK QUERY 2: All blocked dates for this groomer ---
        blocked_dates = set()
        try:
            lines = self._run_query(f"""
                SELECT CONVERT(varchar, BTDate, 23)
                FROM BlockedTime
                WHERE BTGroomerID = {groomer_id}
                AND BTDate BETWEEN '{start_str}' AND '{end_str}'
            """, use_tabs=False)
            for line in lines:
                d = line.strip()
                if d and len(d) == 10:
                    blocked_dates.add(d)
        except:
            pass

        # --- BULK QUERY 3: GroomerSched records covering the range ---
        # Map: date_str -> whether groomer is NOT scheduled (NULL means unavailable)
        # GroomerSchWEDate is the Saturday ending each week
        not_scheduled_dates = set()
        try:
            lines = self._run_query(f"""
                SELECT
                    CONVERT(varchar, gs.GroomerSchWEDate, 23) as WeekEnd,
                    CASE WHEN gs.GroomerSchsunIn IS NULL THEN 1 ELSE 0 END as Sun,
                    CASE WHEN gs.GroomerSchMonIn IS NULL THEN 1 ELSE 0 END as Mon,
                    CASE WHEN gs.GroomerSchtueIn IS NULL THEN 1 ELSE 0 END as Tue,
                    CASE WHEN gs.GroomerSchwedIn IS NULL THEN 1 ELSE 0 END as Wed,
                    CASE WHEN gs.GroomerSchthurIn IS NULL THEN 1 ELSE 0 END as Thu,
                    CASE WHEN gs.GroomerSchfriIn IS NULL THEN 1 ELSE 0 END as Fri,
                    CASE WHEN gs.GroomerSchsatIn IS NULL THEN 1 ELSE 0 END as Sat
                FROM GroomerSched gs
                WHERE gs.GroomerSchID = {groomer_id}
                AND gs.GroomerSchWEDate BETWEEN
                    DATEADD(day, 7 - DATEPART(dw, '{start_str}'), '{start_str}')
                    AND DATEADD(day, 7 - DATEPART(dw, '{end_str}'), DATEADD(day, 7, '{end_str}'))
            """)
            # Day-of-week mapping: column index -> offset from Saturday (WeekEnd)
            # Columns: WeekEnd, Sun, Mon, Tue, Wed, Thu, Fri, Sat
            # Offsets from Saturday: Sun=-6, Mon=-5, Tue=-4, Wed=-3, Thu=-2, Fri=-1, Sat=0
            day_offsets = [-6, -5, -4, -3, -2, -1, 0]
            for line in lines:
                if '\t' not in line:
                    continue
                row = line.split('\t')
                if len(row) < 8:
                    continue
                week_end_str = row[0].strip()
                if not week_end_str or len(week_end_str) != 10:
                    continue
                try:
                    week_end = datetime.strptime(week_end_str, '%Y-%m-%d').date()
                except ValueError:
                    continue
                for i, offset in enumerate(day_offsets):
                    val = row[i + 1].strip()
                    if val == '1':  # NULL in schedule = not scheduled
                        actual_date = week_end + timedelta(days=offset)
                        not_scheduled_dates.add(actual_date.strftime('%Y-%m-%d'))
        except:
            pass

        # --- BULK QUERY 4: All appointments for this groomer in range ---
        # Includes pet type for size breakdown and service type detection
        all_appointments = {}  # date_str -> list of appointment dicts
        try:
            lines = self._run_query(f"""
                SELECT
                    CONVERT(varchar, gl.GLDate, 23) as ApptDate,
                    CONVERT(varchar, gl.GLInTime, 108) as StartTime,
                    CONVERT(varchar, gl.GLOutTime, 108) as EndTime,
                    p.PtPetName,
                    c.CLLastName,
                    ISNULL(pt.PTypeName, '') as PetType,
                    CASE
                        WHEN gl.GLOthersID > 0 THEN 'Handstrip'
                        WHEN gl.GLBath = -1 AND gl.GLGroom = 0 THEN 'Bath'
                        WHEN gl.GLNailsID > 0 AND gl.GLBath = 0 AND gl.GLGroom = 0 THEN 'Nails'
                        WHEN gl.GLBath = -1 AND gl.GLGroom = -1 THEN 'Full'
                        WHEN gl.GLGroom = -1 THEN 'Groom'
                        ELSE 'Other'
                    END as ServiceType
                FROM GroomingLog gl
                INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
                INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
                LEFT JOIN PetTypes pt ON p.PtCat = pt.PTypeSeq
                WHERE gl.GLDate BETWEEN '{start_str}' AND '{end_str}'
                AND (gl.GLGroomerID = {groomer_id} OR gl.GLBatherID = {groomer_id} OR gl.GLOthersID = {groomer_id})
                AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
                ORDER BY gl.GLDate, gl.GLInTime
            """)
            for line in lines:
                if '\t' not in line:
                    continue
                row = line.split('\t')
                if len(row) < 7:
                    continue
                date_key = row[0].strip()
                appt = {
                    'time': row[1].strip(),
                    'end_time': row[2].strip(),
                    'pet_name': row[3].strip(),
                    'client': row[4].strip(),
                    'pet_type': row[5].strip(),
                    'service': row[6].strip()
                }
                if date_key not in all_appointments:
                    all_appointments[date_key] = []
                all_appointments[date_key].append(appt)
        except:
            pass

        elapsed_queries = time.time() - t0
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Bulk queries completed in {elapsed_queries:.2f}s "
              f"(holidays={len(holidays)}, blocked={len(blocked_dates)}, "
              f"unsched={len(not_scheduled_dates)}, appt_days={len(all_appointments)})")

        # --- PROCESS IN MEMORY ---
        available_days = []
        days_checked = 0

        while days_checked < 365:
            check_date = start_date + timedelta(days=days_checked)
            day_of_week = check_date.weekday()  # 0=Mon, 6=Sun
            days_checked += 1

            # Skip Sunday (6) and Monday (0)
            if day_of_week in [0, 6]:
                continue

            date_str = check_date.strftime('%Y-%m-%d')

            # Check holiday
            if date_str in holidays:
                continue

            # Check blocked
            if date_str in blocked_dates:
                continue

            # Check not scheduled
            if date_str in not_scheduled_dates:
                continue

            # Get appointments for this day from in-memory data
            day_appointments = all_appointments.get(date_str, [])

            # Check which slots are available
            def is_slot_blocked(slot):
                slot_minutes = int(slot[:2]) * 60 + int(slot[3:5])
                for appt in day_appointments:
                    start = appt['time'][:5]
                    end = appt.get('end_time', '')[:5]
                    if not end:
                        end = start
                    start_minutes = int(start[:2]) * 60 + int(start[3:5])
                    end_minutes = int(end[:2]) * 60 + int(end[3:5])
                    if start_minutes <= slot_minutes < end_minutes:
                        return True
                return False

            available_times = [slot for slot in time_slots if not is_slot_blocked(slot)]

            if not available_times:
                continue

            # Build summary from in-memory appointment data
            day_summary = self._build_day_summary(day_appointments)

            available_days.append({
                'date': date_str,
                'day_of_week': check_date.strftime('%A'),
                'available_times': available_times,
                'total_booked': day_summary['total'],
                'size_breakdown': day_summary['sizes'],
                'special_types': day_summary['special'],
                'appointments': day_appointments
            })

        elapsed_total = time.time() - t0
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Availability search completed in {elapsed_total:.2f}s "
              f"(found {len(available_days)} available days)")

        return {
            'groomer_id': groomer_id,
            'days': available_days
        }

    def get_conflicts(self):
        """Scan next 120 days for all groomers and find slots shown as available that would overlap"""
        import time
        t0 = time.time()

        GROOMERS = {59: 'Kumi', 85: 'Tomoko', 95: 'Mandilyn'}
        STANDARD_SLOTS = ['08:30', '10:00', '11:30', '13:30', '14:30']
        APPT_DURATION = 90

        start_date = datetime.now().date() + timedelta(days=1)
        end_date = start_date + timedelta(days=120)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        def time_to_minutes(t):
            parts = t.strip().split(':')
            return int(parts[0]) * 60 + int(parts[1])

        def minutes_to_time_display(m):
            h = m // 60
            mins = m % 60
            ampm = 'AM' if h < 12 else 'PM'
            display_h = h % 12 or 12
            return f"{display_h}:{mins:02d} {ampm}"

        # Bulk query: holidays
        holidays = set()
        try:
            lines = self._run_query(f"""
                SELECT CONVERT(varchar, Date, 23)
                FROM Calendar
                WHERE Date BETWEEN '{start_str}' AND '{end_str}'
                AND Styleset = 'HOLIDAY'
            """, use_tabs=False)
            for line in lines:
                d = line.strip()
                if d and len(d) == 10:
                    holidays.add(d)
        except:
            pass

        all_conflicts = []

        for groomer_id, groomer_name in GROOMERS.items():
            # Blocked dates
            blocked_dates = set()
            try:
                lines = self._run_query(f"""
                    SELECT CONVERT(varchar, BTDate, 23)
                    FROM BlockedTime
                    WHERE BTGroomerID = {groomer_id}
                    AND BTDate BETWEEN '{start_str}' AND '{end_str}'
                """, use_tabs=False)
                for line in lines:
                    d = line.strip()
                    if d and len(d) == 10:
                        blocked_dates.add(d)
            except:
                pass

            # GroomerSched
            not_scheduled_dates = set()
            try:
                lines = self._run_query(f"""
                    SELECT
                        CONVERT(varchar, gs.GroomerSchWEDate, 23) as WeekEnd,
                        CASE WHEN gs.GroomerSchsunIn IS NULL THEN 1 ELSE 0 END,
                        CASE WHEN gs.GroomerSchMonIn IS NULL THEN 1 ELSE 0 END,
                        CASE WHEN gs.GroomerSchtueIn IS NULL THEN 1 ELSE 0 END,
                        CASE WHEN gs.GroomerSchwedIn IS NULL THEN 1 ELSE 0 END,
                        CASE WHEN gs.GroomerSchthurIn IS NULL THEN 1 ELSE 0 END,
                        CASE WHEN gs.GroomerSchfriIn IS NULL THEN 1 ELSE 0 END,
                        CASE WHEN gs.GroomerSchsatIn IS NULL THEN 1 ELSE 0 END
                    FROM GroomerSched gs
                    WHERE gs.GroomerSchID = {groomer_id}
                    AND gs.GroomerSchWEDate BETWEEN
                        DATEADD(day, 7 - DATEPART(dw, '{start_str}'), '{start_str}')
                        AND DATEADD(day, 7 - DATEPART(dw, '{end_str}'), DATEADD(day, 7, '{end_str}'))
                """)
                day_offsets = [-6, -5, -4, -3, -2, -1, 0]
                for line in lines:
                    if '\t' not in line:
                        continue
                    row = line.split('\t')
                    if len(row) < 8:
                        continue
                    week_end_str = row[0].strip()
                    if not week_end_str or len(week_end_str) != 10:
                        continue
                    try:
                        week_end = datetime.strptime(week_end_str, '%Y-%m-%d').date()
                    except ValueError:
                        continue
                    for i, offset in enumerate(day_offsets):
                        if row[i + 1].strip() == '1':
                            actual_date = week_end + timedelta(days=offset)
                            not_scheduled_dates.add(actual_date.strftime('%Y-%m-%d'))
            except:
                pass

            # All appointments
            all_appointments = {}
            try:
                lines = self._run_query(f"""
                    SELECT
                        CONVERT(varchar, gl.GLDate, 23) as ApptDate,
                        CONVERT(varchar, gl.GLInTime, 108) as StartTime,
                        CONVERT(varchar, gl.GLOutTime, 108) as EndTime,
                        p.PtPetName,
                        c.CLLastName,
                        ISNULL(pt.PTypeName, '') as PetType,
                        CASE
                            WHEN gl.GLOthersID > 0 THEN 'Handstrip'
                            WHEN gl.GLBath = -1 AND gl.GLGroom = 0 THEN 'Bath'
                            WHEN gl.GLNailsID > 0 AND gl.GLBath = 0 AND gl.GLGroom = 0 THEN 'Nails'
                            WHEN gl.GLBath = -1 AND gl.GLGroom = -1 THEN 'Full'
                            WHEN gl.GLGroom = -1 THEN 'Groom'
                            ELSE 'Other'
                        END as ServiceType
                    FROM GroomingLog gl
                    INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
                    INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
                    LEFT JOIN PetTypes pt ON p.PtCat = pt.PTypeSeq
                    WHERE gl.GLDate BETWEEN '{start_str}' AND '{end_str}'
                    AND (gl.GLGroomerID = {groomer_id} OR gl.GLBatherID = {groomer_id} OR gl.GLOthersID = {groomer_id})
                    AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
                    ORDER BY gl.GLDate, gl.GLInTime
                """)
                for line in lines:
                    if '\t' not in line:
                        continue
                    row = line.split('\t')
                    if len(row) < 7:
                        continue
                    date_key = row[0].strip()
                    appt = {
                        'time': row[1].strip(),
                        'end_time': row[2].strip(),
                        'pet_name': row[3].strip(),
                        'client': row[4].strip(),
                        'pet_type': row[5].strip(),
                        'service': row[6].strip()
                    }
                    if date_key not in all_appointments:
                        all_appointments[date_key] = []
                    all_appointments[date_key].append(appt)
            except:
                pass

            # Check each business day
            days_checked = 0
            while days_checked < 120:
                check_date = start_date + timedelta(days=days_checked)
                days_checked += 1
                if check_date.weekday() in [0, 6]:
                    continue
                date_str = check_date.strftime('%Y-%m-%d')
                if date_str in holidays or date_str in blocked_dates or date_str in not_scheduled_dates:
                    continue

                day_appts = all_appointments.get(date_str, [])
                if not day_appts:
                    continue

                for slot in STANDARD_SLOTS:
                    slot_min = time_to_minutes(slot)

                    # Current extension logic: is slot start within any appointment range?
                    shows_available = True
                    for appt in day_appts:
                        a_start = time_to_minutes(appt['time'][:5])
                        a_end_str = appt.get('end_time', '')[:5]
                        a_end = time_to_minutes(a_end_str) if a_end_str else a_start
                        if a_start <= slot_min < a_end:
                            shows_available = False
                            break

                    if not shows_available:
                        continue

                    # Proper overlap check: would a 90-min appt here conflict?
                    slot_end = slot_min + APPT_DURATION
                    overlapping = []
                    for appt in day_appts:
                        a_start = time_to_minutes(appt['time'][:5])
                        a_end_str = appt.get('end_time', '')[:5]
                        a_end = time_to_minutes(a_end_str) if a_end_str else a_start + APPT_DURATION
                        if slot_min < a_end and slot_end > a_start:
                            overlapping.append({
                                'time_display': f"{minutes_to_time_display(a_start)}-{minutes_to_time_display(a_end)}",
                                'pet_name': appt['pet_name'],
                                'client': appt['client'],
                                'service': appt['service']
                            })

                    if overlapping:
                        all_conflicts.append({
                            'groomer': groomer_name,
                            'groomer_id': groomer_id,
                            'date': date_str,
                            'day_of_week': check_date.strftime('%A'),
                            'slot': slot,
                            'slot_display': minutes_to_time_display(slot_min),
                            'conflicts_with': overlapping
                        })

        elapsed = time.time() - t0
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Conflict check completed in {elapsed:.2f}s "
              f"(found {len(all_conflicts)} conflicts)")

        result = {
            'last_checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'date_range': f"{start_str} to {end_str}",
            'count': len(all_conflicts),
            'conflicts': all_conflicts
        }

        # Write summary (no full conflict list) to cache file so other machines can see status
        try:
            cache_path = os.path.join(_get_ext_dir(), 'conflict_cache.json')
            cache_data = {
                'last_checked': result['last_checked'],
                'date_range': result['date_range'],
                'count': result['count'],
            }
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f)
        except Exception as e:
            print(f"[conflicts] Could not write cache: {e}")

        return result

    def get_conflicts_cached(self):
        """Return last conflict check summary from cache file (fast, no DB query)."""
        try:
            cache_path = os.path.join(_get_ext_dir(), 'conflict_cache.json')
            if not os.path.exists(cache_path):
                return {}
            with open(cache_path) as f:
                return json.load(f)
        except Exception:
            return {}

    def _build_day_summary(self, appointments):
        """Build size/service summary from in-memory appointment list"""
        sizes = {'XS': 0, 'SM': 0, 'MD': 0, 'LG': 0, 'XL': 0}
        special = {'handstrip': 0, 'bath_only': 0, 'nails_only': 0}

        for appt in appointments:
            pt = appt.get('pet_type', '').upper()
            svc = appt.get('service', '')

            if 'XS' in pt:
                sizes['XS'] += 1
            elif 'SM' in pt or 'SMALL' in pt:
                sizes['SM'] += 1
            elif 'MD' in pt or 'MEDIUM' in pt:
                sizes['MD'] += 1
            elif 'LG' in pt or 'LARGE' in pt:
                sizes['LG'] += 1
            elif 'XL' in pt or 'EXTRA LARGE' in pt:
                sizes['XL'] += 1

            if svc == 'Handstrip':
                special['handstrip'] += 1
            elif svc == 'Bath':
                special['bath_only'] += 1
            elif svc == 'Nails':
                special['nails_only'] += 1

        return {
            'total': len(appointments),
            'sizes': sizes,
            'special': special
        }

    def refresh_client_stats_endpoint(self):
        """Trigger a manual refresh of DBFCMClientStats in SQL Server."""
        try:
            cmd = _wsl_python3_cmd()
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            output = result.stdout.strip() + result.stderr.strip()
            if result.returncode != 0:
                return {'success': False, 'error': output}
            # Parse row count from last output line ("Done. N rows written...")
            count = None
            for line in result.stdout.strip().splitlines():
                if 'rows' in line:
                    import re
                    m = re.search(r'(\d+) rows', line)
                    if m:
                        count = int(m.group(1))
            return {'success': True, 'clients_refreshed': count, 'output': output}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_chat_response(self, message):
        """Send a message to Claude CLI via direct subprocess (no shell — avoids all quoting issues)."""
        global _claude_session_id, _system_prompt_file

        if _claude_session_id is None and not _system_prompt_file:
            return {'success': False, 'error': 'System prompt not initialized. Restart the backend.'}

        # Build command as a Python list — Python passes args directly, no shell involved,
        # so apostrophes in paths (Dog's, Cat's) are never a problem.
        if IS_WINDOWS:
            base = ['wsl', WSL_CLAUDE_PATH, '-p']
        else:
            base = [WSL_CLAUDE_PATH, '-p']

        if _claude_session_id is None:
            session_args = ['--system-prompt-file', _system_prompt_file]
        else:
            session_args = ['--resume', _claude_session_id]

        cmd = base + session_args + [
            '--mcp-config', MCP_CONFIG_WSL_PATH,
            '--allowedTools', MCP_ALLOWED_TOOLS,
            '--output-format', 'json',
            message,
        ]

        session_label = 'new' if _claude_session_id is None else _claude_session_id[:8] + '...'
        t0 = datetime.now()
        print(f"[{t0.strftime('%H:%M:%S')}] [Know-a-bot] Claude CLI starting (session={session_label})")

        try:
            result = subprocess.run(cmd, capture_output=True, timeout=240)
        except subprocess.TimeoutExpired:
            return {'success': False, 'error': 'Claude CLI timed out after 4 minutes.'}
        except Exception as e:
            return {'success': False, 'error': f'Subprocess error: {e}'}

        elapsed = (datetime.now() - t0).seconds
        stdout = result.stdout.decode('utf-8', errors='replace')
        stderr = result.stderr.decode('utf-8', errors='replace')
        exit_code = result.returncode
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Know-a-bot] Claude CLI done in {elapsed}s (exit {exit_code})")
        if stderr:
            print(f"[Know-a-bot] stderr: {stderr[:600]}")

        if not stdout.strip():
            return {'success': False, 'error': f'Claude CLI returned no output (exit {exit_code}). See backend console for details.'}

        try:
            data = json.loads(stdout.strip())
        except json.JSONDecodeError as e:
            return {'success': False, 'error': f'Failed to parse response: {e}. Output: {stdout[:200]}'}

        reply_text = data.get('result', '')

        if _claude_session_id is None and 'session_id' in data:
            _claude_session_id = data['session_id']
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [Know-a-bot] session started ({_claude_session_id[:8]}...)")

        return {'success': True, 'reply': reply_text}

    def reset_chat(self):
        """Clear the session so the next message starts fresh."""
        global _claude_session_id
        _claude_session_id = None
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Know-a-bot] conversation reset")
        return {'success': True}

    # ── SMS Draft+Approve handlers ─────────────────────────────────────────────

    def get_checkout_today(self):
        """Return today's unchecked-out appointments grouped by client, with card + tip info."""
        query = """
SELECT
    gl.GLSeq,
    CONVERT(varchar, CAST(gl.GLInTime AS time), 100) AS InTime,
    p.PtPetName,
    c.CLSeq AS ClientID,
    c.CLFirstName + ' ' + c.CLLastName AS ClientName,
    ISNULL(e1.USFNAME, '') AS Groomer,
    ISNULL(c.CLCCMask,  '') AS Card1,  ISNULL(c.CLCCAltDesc,  '') AS Card1Desc,
    ISNULL(c.CLCCMask2, '') AS Card2,  ISNULL(c.CLCCAltDesc2, '') AS Card2Desc,
    ISNULL(c.CLCCMask3, '') AS Card3,  ISNULL(c.CLCCAltDesc3, '') AS Card3Desc,
    ISNULL(CAST(s.AvgTipPct    AS varchar), '') AS AvgTipPct,
    ISNULL(CAST(s.AvgTipAmount AS varchar), '') AS AvgTipAmt,
    ISNULL(CAST(s.LastTipPct   AS varchar), '') AS LastTipPct,
    ISNULL(CAST(s.LastTipAmount AS varchar), '') AS LastTipAmt,
    ISNULL(s.TipMethod,    '') AS TipMethod,
    ISNULL(s.PreferredDay, '') AS PreferredDay,
    ISNULL(CAST(s.AvgCadenceDays AS varchar), '') AS AvgCadenceDays,
    ISNULL((SELECT TOP 1 CONVERT(varchar, gl2.GLDate, 101)
            FROM GroomingLog gl2
            INNER JOIN Pets p2 ON gl2.GLPetID = p2.PtSeq
            WHERE p2.PtOwnerCode = c.CLSeq
            AND gl2.GLDate > CAST(GETDATE() AS DATE)
            AND (gl2.GLDeleted IS NULL OR gl2.GLDeleted = 0)
            AND (gl2.GLWaitlist IS NULL OR gl2.GLWaitlist = 0)
            AND (gl2.GLNoShow IS NULL OR gl2.GLNoShow = 0)
            ORDER BY gl2.GLDate), '') AS NextAppt,
    ISNULL(CAST((SELECT COUNT(*)
            FROM GroomingLog gl2
            INNER JOIN Pets p2 ON gl2.GLPetID = p2.PtSeq
            WHERE p2.PtOwnerCode = c.CLSeq
            AND gl2.GLDate > CAST(GETDATE() AS DATE)
            AND (gl2.GLDeleted IS NULL OR gl2.GLDeleted = 0)
            AND (gl2.GLWaitlist IS NULL OR gl2.GLWaitlist = 0)
            AND (gl2.GLNoShow IS NULL OR gl2.GLNoShow = 0)) AS varchar), '0') AS FutureApptCount,
    CASE WHEN EXISTS (
            SELECT 1 FROM GroomingLog gl2
            INNER JOIN Pets p2 ON gl2.GLPetID = p2.PtSeq
            WHERE p2.PtOwnerCode = c.CLSeq
            AND gl2.GLDate > CAST(GETDATE() AS DATE)
            AND (gl2.GLDeleted IS NULL OR gl2.GLDeleted = 0)
            AND (gl2.GLWaitlist IS NULL OR gl2.GLWaitlist = 0)
            AND (gl2.GLNoShow IS NULL OR gl2.GLNoShow = 0)
            AND (
                EXISTS (SELECT 1 FROM Calendar cal
                        WHERE cal.Date = gl2.GLDate
                        AND cal.Styleset IN ('HOLIDAY', 'CLOSED'))
                OR (gl2.GLGroomerID IS NOT NULL AND EXISTS (
                        SELECT 1 FROM BlockedTime bt
                        WHERE bt.BTGroomerID = gl2.GLGroomerID
                        AND bt.BTDate = gl2.GLDate))
            )
        ) THEN '1' ELSE '0' END AS HasConflict,
    ISNULL(CAST(gl.GLCompleted AS varchar), '0') AS Completed,
    CAST(p.PtSeq AS varchar) AS PetSeq
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
LEFT JOIN Employees e1 ON gl.GLGroomerID = e1.USSEQN
LEFT JOIN DBFCMClientStats s ON c.CLSeq = s.ClientID
WHERE gl.GLDate = CAST(GETDATE() AS DATE)
  AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
  AND (gl.GLNoShow IS NULL OR gl.GLNoShow = 0)
  AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist = 0)
  AND p.PtSeq != 12120
  AND NOT EXISTS (
      SELECT 1 FROM Receipts r
      WHERE r.RPCLIENTID = c.CLSeq
      AND CAST(r.RPDATE AS DATE) = CAST(GETDATE() AS DATE)
  )
ORDER BY gl.GLInTime, c.CLSeq
"""
        rows = run_query_rows(query.strip())

        # Group by ClientID — one card per client, list pets
        from collections import OrderedDict
        clients = OrderedDict()

        for row in rows:
            if len(row) < 18:
                continue
            (glseq, in_time, pet_name, client_id, client_name, groomer,
             card1, card1_desc, card2, card2_desc, card3, card3_desc,
             avg_tip_pct, avg_tip_amt, last_tip_pct, last_tip_amt,
             tip_method, preferred_day, avg_cadence_days,
             next_appt, future_appt_count, has_conflict,
             completed) = row[:23] if len(row) >= 23 else (row + [''] * 23)[:23]
            pet_id = row[23] if len(row) > 23 else ''

            if client_id not in clients:
                # Build cards list (only non-empty masks)
                cards = []
                for mask, desc in [(card1, card1_desc), (card2, card2_desc), (card3, card3_desc)]:
                    if mask and mask not in ('', 'NULL'):
                        cards.append({
                            'last4': mask,
                            'desc': desc if desc and desc not in ('', 'NULL') else None
                        })

                def _float_or_none(v):
                    try:
                        return float(v) if v and v not in ('', 'NULL') else None
                    except Exception:
                        return None

                future_count = int(future_appt_count) if future_appt_count and future_appt_count.isdigit() else 0
                pref_day_val = preferred_day if preferred_day and preferred_day not in ('', 'NULL') else None
                cadence_val  = _float_or_none(avg_cadence_days)
                suggested = None
                if future_count == 0:
                    suggested = _suggest_next_date(cadence_val, pref_day_val)

                cid = client_id
                cn_rows = run_query_rows(
                    f"SELECT TOP 3 CONVERT(varchar,CNDate,23), ISNULL(CNSubject,''), ISNULL(CNNotes,'') "
                    f"FROM ClientNotes WHERE CNClientSeq={cid} ORDER BY CNDate DESC, CNSeq DESC")
                client_notes = [
                    {'date': r[0], 'subject': r[1].strip(), 'text': r[2].strip()}
                    for r in cn_rows if r and len(r) >= 3
                ]

                clients[client_id] = {
                    'client_id':        client_id,
                    'client_name':      client_name,
                    'in_time':          in_time,
                    'pets':             [],
                    'cards':            cards,
                    'avg_tip_pct':      _float_or_none(avg_tip_pct),
                    'avg_tip_amt':      _float_or_none(avg_tip_amt),
                    'last_tip_pct':     _float_or_none(last_tip_pct),
                    'last_tip_amt':     _float_or_none(last_tip_amt),
                    'tip_method':       tip_method if tip_method and tip_method not in ('', 'NULL') else None,
                    'preferred_day':    pref_day_val,
                    'avg_cadence_days': cadence_val,
                    'next_appt':        next_appt if next_appt and next_appt not in ('', 'NULL') else None,
                    'future_appt_count': future_count,
                    'has_conflict':     has_conflict == '1',
                    'suggested_next':   suggested,
                    'client_notes':     client_notes,
                }

            clients[client_id]['pets'].append({
                'name':    pet_name,
                'pet_id':  pet_id,
                'groomer': groomer if groomer else None,
                'done':    completed == '-1',
            })

        return {'clients': list(clients.values()), 'count': len(clients)}

    def sms_get_drafts(self):
        """Return list of pending SMS drafts sorted oldest first, enriched with client dossier."""
        with _sms_drafts_lock:
            drafts = list(_sms_drafts.values())
        drafts.sort(key=lambda d: d['message_id'])

        # Enrich each non-escalation draft with a fresh client dossier (cached 60s).
        # Deduplicate by client_id so multi-draft clients only query once.
        seen = {}
        enriched = []
        for d in drafts:
            cid = d.get('client_id')
            if cid and not d.get('is_escalation'):
                if cid not in seen:
                    try:
                        seen[cid] = _sms_get_client_dossier(cid)
                    except Exception as e:
                        log.warning(f"[SMS] Dossier fetch failed for client {cid}: {e}")
                        seen[cid] = None
                d = dict(d)  # shallow copy — don't mutate the shared draft dict
                d['dossier'] = seen[cid]
            enriched.append(d)

        return {'drafts': enriched, 'count': len(enriched)}

    def sms_send(self, data):
        """Send an SMS via KCApp, attribute to Claude, mark inbound handled."""
        client_id = data.get('client_id', 0)
        phone     = data.get('phone', '').strip()
        message   = data.get('message', '').strip()
        cookies   = data.get('cookies', {})
        draft_id  = str(data.get('draft_id', ''))

        if not phone or not message:
            return {'success': False, 'error': 'phone and message are required'}
        if not cookies:
            return {'success': False, 'error': 'cookies required — are you logged in to KCApp?'}

        new_msg_id, error = _sms_send_via_kcapp(phone, message, client_id or 0, cookies)
        if error:
            return {'success': False, 'error': error}

        if new_msg_id:
            _sms_attribute_to_claude(new_msg_id)

        if draft_id:
            # Track escalation before removing from drafts
            with _sms_drafts_lock:
                draft_info = dict(_sms_drafts.get(draft_id, {}))
            if draft_info.get('is_escalation'):
                _sent_escalations[draft_id] = {
                    'escalation_context': draft_info.get('escalation_context', ''),
                    'sent_at':            datetime.now().isoformat(),
                    'matched':            False,
                }
                _save_pending_escalations()
            try:
                _sms_mark_handled(int(draft_id))
            except (ValueError, TypeError):
                pass
            with _sms_drafts_lock:
                _sms_drafts.pop(draft_id, None)

        print(f"[SMS] Sent to {phone} (new MessageId={new_msg_id}), draft {draft_id} resolved")
        return {'success': True, 'message_id': new_msg_id}

    def sms_post_send(self, data):
        """After extension sends via KCApp tab injection: attribute to Claude + clean up draft."""
        draft_id     = str(data.get('draft_id', ''))
        kcapp_msg_id = data.get('kcapp_message_id')

        if kcapp_msg_id:
            _sms_attribute_to_claude(kcapp_msg_id)

        if draft_id:
            # Track escalation before removing from drafts
            with _sms_drafts_lock:
                draft_info = dict(_sms_drafts.get(draft_id, {}))
            if draft_info.get('is_escalation'):
                _sent_escalations[draft_id] = {
                    'escalation_context': draft_info.get('escalation_context', ''),
                    'sent_at':            datetime.now().isoformat(),
                    'matched':            False,
                }
                _save_pending_escalations()
            try:
                _sms_mark_handled(int(draft_id))
            except (ValueError, TypeError):
                pass
            with _sms_drafts_lock:
                _sms_drafts.pop(draft_id, None)
            _save_sms_drafts()

        log.info(f"[SMS] post-send: attributed msg {kcapp_msg_id} to Claude, draft {draft_id} resolved")
        return {'success': True, 'message_id': kcapp_msg_id}

    def sms_compose(self, data):
        """Natural-language SMS compose: parse instruction → look up client → draft → queue."""
        instruction = data.get('instruction', '').strip()
        if not instruction:
            return {'success': False, 'error': 'instruction required'}

        system = (
            "You are composing SMS messages for Dog's Best Friend grooming salon (Noah, owner). "
            "Given a natural language instruction, return ONLY valid JSON with exactly two keys: "
            '{"client": "FirstName LastName  OR  pet:PetName", "draft": "the SMS text"} '
            "SMS voice: start with Hi [FirstName], concise, no emojis, no exclamation marks, use we/us. "
            "Return ONLY the JSON object — no explanation, no markdown."
        )
        raw = _run_one_shot_claude(system, instruction, timeout=60)
        if not raw:
            return {'success': False, 'error': 'Claude did not respond'}

        import re as _re
        try:
            parsed = json.loads(raw.strip())
        except Exception:
            m = _re.search(r'\{[^{}]+\}', raw, _re.DOTALL)
            if m:
                try:
                    parsed = json.loads(m.group())
                except Exception:
                    return {'success': False, 'error': f'Could not parse Claude response: {raw[:120]}'}
            else:
                return {'success': False, 'error': f'Could not parse Claude response: {raw[:120]}'}

        client_query = parsed.get('client', '').strip()
        draft = parsed.get('draft', '').strip()
        if not client_query or not draft:
            return {'success': False, 'error': 'Claude returned incomplete data'}

        client = _sms_lookup_client(client_query)
        if not client:
            return {'success': False, 'error': f'Client not found: {client_query}'}

        draft_id = f"compose-{uuid.uuid4().hex[:8]}"
        with _sms_drafts_lock:
            _sms_drafts[draft_id] = {
                'draft_id':            draft_id,
                'message_id':          0,
                'client_id':           client['client_id'],
                'client_name':         client['client_name'],
                'phone':               client['phone'],
                'their_message':       '',
                'recent_conversation': [],
                'draft':               draft,
                'timestamp':           datetime.now().strftime('%Y-%m-%dT%H:%M'),
            }
        _save_sms_drafts()
        log.info(f"[SMS] Composed outbound for {client['client_name']}: {draft[:60]}")
        return {'success': True, 'draft_id': draft_id,
                'client_name': client['client_name'], 'draft': draft}

    def sms_send_via_kc(self, data):
        """Send SMS by proxying to KC's web API with session cookies from the extension."""
        phone      = data.get('phone', '').strip()
        message    = data.get('message', '').strip()
        client_id  = int(data.get('client_id', 0))
        cookie_str = data.get('cookies', '').strip()
        if not phone or not message:
            return {'success': False, 'error': 'phone and message required'}
        if not cookie_str:
            return {'success': False, 'error': 'KC session cookies required'}

        try:
            import urllib.request
            import urllib.parse
            boundary = '----ExtBoundary' + uuid.uuid4().hex[:8]
            parts = []
            for name, val in [('phoneNumber', phone), ('Message', message),
                              ('MediaLinks', ''), ('ClientId', str(client_id)),
                              ('MessageId', '0')]:
                parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{val}')
            body = '\r\n'.join(parts) + f'\r\n--{boundary}--\r\n'
            req = urllib.request.Request(
                'https://dbfcm.mykcapp.com/SMS/SMSSendFromFront',
                data=body.encode('utf-8'),
                headers={
                    'Content-Type': f'multipart/form-data; boundary={boundary}',
                    'Cookie': cookie_str,
                    'X-Requested-With': 'XMLHttpRequest',
                },
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read().decode('utf-8'))
            log.info(f"[SMS] send-via-kc: sent to {phone}, KC result: {result}")
            return {'success': True, 'kc_result': result}
        except Exception as e:
            log.error(f"[SMS] send-via-kc error: {e}")
            return {'success': False, 'error': str(e)}

    def sms_queue_outbound(self, data):
        """Manually queue an outbound SMS as a draft for review+send via the extension."""
        client_id   = int(data.get('client_id', 0))
        client_name = data.get('client_name', '').strip()
        phone       = data.get('phone', '').strip()
        message     = data.get('message', '').strip()
        if not phone or not message:
            return {'success': False, 'error': 'phone and message required'}
        draft_id = f"manual-{uuid.uuid4().hex[:8]}"
        with _sms_drafts_lock:
            _sms_drafts[draft_id] = {
                'draft_id':      draft_id,
                'message_id':    0,
                'client_id':     client_id,
                'client_name':   client_name,
                'phone':         phone,
                'their_message': '',   # outbound-only, no inbound thread
                'draft':         message,
                'timestamp':     datetime.now().strftime('%Y-%m-%dT%H:%M'),
            }
        _save_sms_drafts()
        log.info(f"[SMS] Queued outbound draft {draft_id} for {client_name}")
        return {'success': True, 'draft_id': draft_id}

    def sms_dismiss_draft(self, data):
        """Remove a draft without sending (user chose to skip)."""
        draft_id = str(data.get('draft_id', ''))
        if not draft_id:
            return {'success': False, 'error': 'draft_id required'}
        with _sms_drafts_lock:
            removed = _sms_drafts.pop(draft_id, None)
        if removed is not None:
            _save_sms_drafts()
        return {'success': True, 'removed': removed is not None}

    def sms_regen_draft(self, data):
        """Regenerate a draft reply using user feedback."""
        draft_id = str(data.get('draft_id', ''))
        feedback = data.get('feedback', '').strip()
        if not draft_id:
            return {'success': False, 'error': 'draft_id required'}
        if not feedback:
            return {'success': False, 'error': 'feedback required'}

        with _sms_drafts_lock:
            draft_info = dict(_sms_drafts.get(draft_id, {}))
        if not draft_info:
            return {'success': False, 'error': 'Draft not found'}

        client_id     = draft_info.get('client_id')
        their_message = draft_info.get('their_message', '')
        original_draft = draft_info.get('draft', '')

        ctx = _sms_get_client_context(client_id) if client_id else None
        if not ctx:
            return {'success': False, 'error': 'Could not load client context'}

        log.info(f"[SMS] Regen draft {draft_id} with feedback: {feedback[:60]}")
        new_draft = _sms_regen_with_feedback(ctx, their_message, original_draft, feedback)
        if not new_draft:
            return {'success': False, 'error': 'Claude did not return a revised draft'}

        with _sms_drafts_lock:
            if draft_id in _sms_drafts:
                _sms_drafts[draft_id]['draft'] = new_draft
        _save_sms_drafts()
        return {'success': True, 'draft': new_draft}

    def sms_draft_from_knowabot(self, data):
        """Queue an SMS draft from Know-a-bot — escalation to Noah or message to a client."""
        recipient = data.get('recipient', '').strip()
        message   = data.get('message', '').strip()
        context   = data.get('context', '').strip()

        if not recipient or not message:
            return {'success': False, 'error': 'recipient and message are required'}

        if recipient.lower() == 'noah':
            # Deduplication: if an unsent escalation draft already exists, reuse it.
            # This prevents double-drafts when Claude times out mid-run and the user retries.
            with _sms_drafts_lock:
                existing = next(
                    (d for d in _sms_drafts.values()
                     if d.get('is_escalation') and d['draft_id'] not in _sent_escalations),
                    None
                )
                if existing:
                    log.info(f"[SMS] Escalation deduplicated — reusing {existing['draft_id']}")
                    return {'success': True, 'draft_id': existing['draft_id']}

                # No existing unsent escalation — create a new one
                draft_id = f"escalation-{uuid.uuid4().hex[:8]}"
                _sms_drafts[draft_id] = {
                    'draft_id':           draft_id,
                    'message_id':         0,
                    'client_id':          None,
                    'client_name':        'Noah (owner)',
                    'phone':              NOAH_CELL_PRIMARY,
                    'their_message':      '',
                    'recent_conversation': [],
                    'draft':              message,
                    'timestamp':          datetime.now().strftime('%Y-%m-%dT%H:%M'),
                    'is_escalation':      True,
                    'escalation_context': context,
                }
            _save_sms_drafts()
            log.info(f"[SMS] Know-a-bot escalation queued (draft_id={draft_id}): {message[:60]}")
            return {'success': True, 'draft_id': draft_id}
        else:
            # Client SMS draft
            client = _sms_lookup_client(recipient)
            if not client:
                return {'success': False, 'error': f'Client not found: {recipient}'}
            draft_id = f"knowabot-{uuid.uuid4().hex[:8]}"
            with _sms_drafts_lock:
                _sms_drafts[draft_id] = {
                    'draft_id':           draft_id,
                    'message_id':         0,
                    'client_id':          client['client_id'],
                    'client_name':        client['client_name'],
                    'phone':              client['phone'],
                    'their_message':      '',
                    'recent_conversation': [],
                    'draft':              message,
                    'timestamp':          datetime.now().strftime('%Y-%m-%dT%H:%M'),
                }
            _save_sms_drafts()
            log.info(f"[SMS] Know-a-bot draft for {client['client_name']} (draft_id={draft_id}): {message[:60]}")
            return {'success': True, 'draft_id': draft_id}

    def sms_extract_appt(self, data):
        """Use Claude to extract appointment details from an SMS thread for booking pre-fill."""
        draft_id  = str(data.get('draft_id', ''))
        client_id = int(data.get('client_id', 0))

        # Load the draft + full recent conversation for context
        with _sms_drafts_lock:
            draft = _sms_drafts.get(draft_id, {})

        ctx = _sms_get_client_context(client_id) if client_id else None

        conv_lines = []
        if ctx:
            conv_lines = ctx.get('recent_conversation', [])[-12:]
        if draft.get('their_message'):
            conv_lines.append(f"Client: {draft['their_message']}")
        if draft.get('draft'):
            conv_lines.append(f"Us (draft): {draft['draft']}")
        conv_text = '\n'.join(conv_lines) if conv_lines else '(no conversation history)'

        # Client's pets for matching
        pet_rows = run_query_rows(
            f"SELECT PtSeq, PtPetName FROM Pets "
            f"WHERE PtOwnerCode={client_id} AND (PtDeleted IS NULL OR PtDeleted=0) "
            f"AND (PtInactive IS NULL OR PtInactive=0) AND (PtDeceased IS NULL OR PtDeceased=0)")
        pets = [{'id': int(r[0]), 'name': r[1]} for r in pet_rows if len(r) >= 2]
        pets_list = ', '.join(p['name'] for p in pets) or 'unknown'

        groomers = [
            {'id': 59, 'name': 'Kumi'},
            {'id': 85, 'name': 'Tomoko'},
            {'id': 95, 'name': 'Mandilyn'},
        ]

        system = (
            "Extract appointment booking details from a grooming salon SMS conversation. "
            "Return ONLY valid JSON with exactly these fields: "
            "{\"date\": \"YYYY-MM-DD or null\", \"time\": \"HH:MM (24-hour) or null\", "
            "\"pet_name\": \"name from the pets list or null\", "
            "\"groomer_name\": \"Kumi, Tomoko, Mandilyn, or null\", "
            "\"service_type\": \"full, bath_only, or handstrip, or null\"}. "
            "Use null for anything not clearly agreed upon. No markdown, no explanation."
        )
        user_msg = (
            f"Client's pets: {pets_list}\n"
            f"Available groomers: Kumi (handstrip only), Tomoko, Mandilyn\n\n"
            f"SMS conversation:\n{conv_text}\n\n"
            f"Extract the agreed appointment details."
        )

        extracted = {}
        try:
            raw = _run_one_shot_claude(system, user_msg, timeout=45)
            if raw:
                # Strip accidental code fences
                if '```' in raw:
                    raw = raw.split('```')[1].lstrip('json\n').strip()
                extracted = json.loads(raw)
        except Exception as e:
            print(f"[SMS/extract-appt] Parse error: {e}")

        # Match pet name → pet record
        matched_pet = None
        if extracted.get('pet_name'):
            nl = extracted['pet_name'].lower()
            for p in pets:
                if p['name'].lower() == nl or nl in p['name'].lower():
                    matched_pet = p
                    break
        if not matched_pet and len(pets) == 1:
            matched_pet = pets[0]

        # Match groomer name → ID
        gmap = {'kumi': (59, 'Kumi'), 'tomoko': (85, 'Tomoko'), 'mandilyn': (95, 'Mandilyn')}
        matched_gid, matched_gname = None, None
        if extracted.get('groomer_name'):
            for key, (gid, gname) in gmap.items():
                if key in extracted['groomer_name'].lower():
                    matched_gid, matched_gname = gid, gname
                    break

        return {
            'success':      True,
            'extracted':    extracted,
            'pet_id':       matched_pet['id']   if matched_pet else None,
            'pet_name':     matched_pet['name'] if matched_pet else None,
            'groomer_id':   matched_gid,
            'groomer_name': matched_gname,
            'pets':         pets,
            'groomers':     groomers,
        }

    def appt_book(self, data):
        """INSERT a new appointment into GroomingLog from an SMS booking form."""
        pet_id     = int(data.get('pet_id', 0))
        date       = data.get('date', '').strip()      # YYYY-MM-DD
        appt_time  = data.get('time', '').strip()      # HH:MM
        groomer_id = int(data.get('groomer_id', 0))
        service    = data.get('service_type', 'full')  # full | bath_only | handstrip

        if not all([pet_id, date, appt_time, groomer_id]):
            return {'success': False, 'error': 'pet_id, date, time, and groomer_id are required'}
        try:
            import datetime as dt
            dt.date.fromisoformat(date)
            hh, mm = int(appt_time[:2]), int(appt_time[3:5])
        except (ValueError, IndexError):
            return {'success': False, 'error': f'Invalid date/time: {date} {appt_time}'}

        # Pricing: historical first
        PRICE_TABLE = {
            7:(55,75), 8:(60,80), 9:(75,85), 10:(80,90), 11:(90,100),
            12:(45,55), 13:(45,60), 14:(55,70), 15:(65,80), 16:(75,90),
        }
        pr = run_query_rows(
            f"SELECT TOP 1 gl.GLRate, gl.GLBathRate, p.PtCat "
            f"FROM GroomingLog gl INNER JOIN Pets p ON gl.GLPetID=p.PtSeq "
            f"WHERE gl.GLPetID={pet_id} AND (gl.GLDeleted IS NULL OR gl.GLDeleted=0) "
            f"AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist=0) AND gl.GLRate>0 "
            f"ORDER BY gl.GLDate DESC")
        gl_rate, gl_bath_rate, pt_cat = 0.0, 0.0, 0
        if pr and len(pr[0]) >= 3:
            try:
                gl_rate      = float(pr[0][0])
                gl_bath_rate = float(pr[0][1])
                pt_cat       = int(pr[0][2])
            except (ValueError, TypeError):
                pass
        if gl_rate == 0:
            cr = run_query_rows(f"SELECT PtCat FROM Pets WHERE PtSeq={pet_id}")
            if cr and cr[0]:
                try: pt_cat = int(cr[0][0])
                except: pass
            gl_rate, gl_bath_rate = PRICE_TABLE.get(pt_cat, (0.0, 0.0))

        # Service flags
        if service == 'handstrip':
            gl_bath, gl_groom = 0, 0
            g_val = str(groomer_id)
            b_val = 'NULL'
            o_val = str(groomer_id)
        elif service == 'bath_only':
            gl_bath, gl_groom = -1, 0
            gl_rate = 0.0
            g_val = 'NULL'
            b_val = '8'   # Elmer
            o_val = 'NULL'
        else:  # full
            gl_bath, gl_groom = -1, -1
            g_val = str(groomer_id)
            b_val = '8'   # Elmer
            o_val = 'NULL'

        out_min  = hh * 60 + mm + 90  # 90-min standard block
        out_h, out_m = out_min // 60, out_min % 60
        in_time  = f"1899-12-30 {hh:02d}:{mm:02d}:00"
        out_time = f"1899-12-30 {out_h:02d}:{out_m:02d}:00"

        run_query_rows(
            f"INSERT INTO GroomingLog "
            f"(GLDate,GLInTime,GLOutTime,GLPetID,GLGroomerID,GLBatherID,GLOthersID,"
            f"GLBath,GLGroom,GLOthers,GLConfirmed,GLDeleted,GLWaitlist,GLTakenBy,GLRate,GLBathRate) "
            f"VALUES "
            f"('{date}','{in_time}','{out_time}',{pet_id},{g_val},{b_val},{o_val},"
            f"{gl_bath},{gl_groom},NULL,0,0,0,'CLD',{gl_rate},{gl_bath_rate})")

        _cache.delete('compact_avail')       # next SMS draft gets fresh slot list
        _cache.delete_prefix('holidays:')    # cheap; ensures holiday changes propagate

        seq_rows = run_query_rows(
            f"SELECT MAX(GLSeq) FROM GroomingLog "
            f"WHERE GLPetID={pet_id} AND GLDate='{date}' AND GLTakenBy='CLD'")
        new_seq = None
        if seq_rows and seq_rows[0]:
            try: new_seq = int(seq_rows[0][0])
            except: pass

        print(f"[SMS/Book] Created GLSeq={new_seq} PetID={pet_id} {date} {appt_time} svc={service}")
        return {'success': True, 'glseq': new_seq}

    def analyze_pending_appointments(self, html):
        """Parse pending HTML from KCApp, enrich with DB data, get Claude briefings."""
        global _pending_briefings

        if not html or not html.strip():
            return {'briefings': [], 'count': 0, 'error': 'No HTML provided'}

        appointments = _parse_pending_html(html)
        log.info(f"[Pending] Parsed {len(appointments)} appointment(s) from {len(html)}-byte HTML")

        if not appointments:
            snippet = html[:300].replace('\n', ' ')
            log.info(f"[Pending] No pendingapp_ divs found. HTML snippet: {snippet}")
            return {'briefings': [], 'count': 0}

        # Prune stale cache entries (IDs not in current list)
        current_ids = {str(a['appointment_id']) for a in appointments}
        with _pending_briefings_lock:
            stale = [k for k in list(_pending_briefings.keys()) if k not in current_ids]
            for k in stale:
                del _pending_briefings[k]
            cached_ids = set(_pending_briefings.keys())

        to_analyze = [a for a in appointments if str(a['appointment_id']) not in cached_ids]

        if to_analyze:
            log.info(f"[Pending] Enriching {len(to_analyze)} new appointment(s) with DB data...")
            for appt in to_analyze:
                _enrich_pending_appt(appt)

            log.info(f"[Pending] Requesting AI briefings for {len(to_analyze)} appointment(s)...")
            new_briefings = _get_pending_briefings_from_claude(to_analyze)

            with _pending_briefings_lock:
                for b in new_briefings:
                    _pending_briefings[str(b['appointment_id'])] = b
        else:
            log.info(f"[Pending] All {len(appointments)} appointment(s) served from cache")

        # Assemble final response preserving HTML order
        result_briefings = []
        with _pending_briefings_lock:
            for appt in appointments:
                aid = str(appt['appointment_id'])
                cached = _pending_briefings.get(aid)
                if cached:
                    result_briefings.append(cached)
                else:
                    result_briefings.append({
                        'appointment_id':   appt['appointment_id'],
                        'date_str':         appt.get('date_str', ''),
                        'client_name':      appt.get('client_name', ''),
                        'client_id':        appt.get('client_id'),
                        'pet_name':         appt.get('pet_name', ''),
                        'pet_id':           appt.get('pet_id'),
                        'employee_requested': appt.get('employee_requested', ''),
                        'services':         appt.get('services', []),
                        'contract_terms':   appt.get('contract_terms', ''),
                        'briefing':         '(Analysis unavailable)',
                        'denial_email':     '',
                        'waitlist_email':   '',
                    })

        return {'briefings': result_briefings, 'count': len(result_briefings)}

    def log_message(self, format, *args):
        # Custom logging
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {format % args}")

def _wsl_python3_cmd() -> list:
    """Return command to run refresh_client_stats.py from the extension folder in WSL."""
    script = f"{_get_wsl_ext_dir()}/refresh_client_stats.py"
    if IS_WINDOWS:
        return ['wsl', 'python3', script]
    else:
        return ['python3', script]


def _run_client_stats_refresh():
    """Run refresh_client_stats in a background thread at startup."""
    import threading

    def _do_refresh():
        try:
            cmd = _wsl_python3_cmd()
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                print(f"[DBFCMClientStats] Warning: {result.stderr.strip()}")
            else:
                for line in result.stdout.strip().splitlines():
                    print(line)
        except Exception as e:
            print(f"[DBFCMClientStats] Warning: Could not refresh client stats: {e}")

    t = threading.Thread(target=_do_refresh, daemon=True)
    t.start()


def run_server(port=8000):
    # Ensure audit tables exist in SQL Server
    _ensure_audit_tables()

    # Restore any SMS drafts that were pending before last restart
    _load_sms_drafts()

    # Restore pending escalations (to match Noah replies to original questions)
    _load_pending_escalations()

    # Generate MCP config JSON pointing to scripts in this extension folder
    _generate_mcp_config()

    # Build Know-a-bot system prompt from staff docs
    build_noahbot_system_prompt()

    # Pre-compute client stats into SQL Server DBFCMClientStats (runs in background)
    _run_client_stats_refresh()

    # Start SMS inbound poller (runs in background, polls every 30s)
    _start_sms_poller()

    server_address = ('', port)
    httpd = HTTPServer(server_address, WaitlistHandler)

    log.info('=' * 60)
    log.info('DBFCM Extension Backend Server')
    log.info('=' * 60)
    log.info(f'Server running on: http://localhost:{port}')
    log.info(f'Log file: {_LOG_PATH}')
    log.info('Press Ctrl+C to stop')
    log.info('=' * 60)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log.info('Server stopped.')
        httpd.server_close()

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    run_server(port)
