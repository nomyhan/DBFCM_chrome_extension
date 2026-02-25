#!/usr/bin/env python3
"""
Backend server for DBFCM Tools Chrome extension
Provides waitlist data and availability checking
Run this on your Windows machine where you have SQL Server access
"""
import sys
sys.dont_write_bytecode = True  # prevent __pycache__ from appearing in the extension folder

from http.server import HTTPServer, BaseHTTPRequestHandler
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
WSL_CLAUDE_PATH = _cfg['wsl_claude_path']
if _cfg['sql_auth'] == 'windows':
    SQL_AUTH_ARGS = ['-E']
else:
    SQL_AUTH_ARGS = ['-U', _cfg['sql_user'], '-P', _cfg['sql_password']]

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

# SQL Server connection settings (from config.local.json or defaults)
SQL_SERVER   = _cfg['sql_server']
SQL_DATABASE = _cfg['sql_database']

# ── SMS Draft+Approve state ──────────────────────────────────────────────────
# Drafts keyed by str(inbound MessageId): {draft_id, message_id, client_id,
# client_name, phone, their_message, draft, timestamp}
_sms_drafts = {}
_sms_drafts_lock = threading.Lock()
_sms_last_seen_id = 0   # watermark: last inbound MessageId processed
_SMS_DRAFTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sms_drafts.json')

# ── Noah's personal cell numbers (for inbound routing) ───────────────────────
# Texts from these numbers go to KB ingestion, not the staff SMS queue.
NOAH_PHONE_NUMBERS = {'5106465763', '5103310678'}
NOAH_CELL_PRIMARY  = '5106465763'  # used for outbound escalation drafts

def _normalize_phone(phone: str) -> str:
    """Strip all non-digit characters; strip leading '1' country code for US numbers."""
    digits = re.sub(r'\D', '', phone or '')
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    return digits

def _is_noah_phone(phone: str) -> bool:
    """Return True if the phone number belongs to Noah's personal cell."""
    return _normalize_phone(phone) in NOAH_PHONE_NUMBERS

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

def _sql_query(query):
    """Run a sqlcmd query and return rows as list of stripped-field lists."""
    cmd = ['sqlcmd', '-S', SQL_SERVER, '-d', SQL_DATABASE, *SQL_AUTH_ARGS,
           '-Q', f'SET NOCOUNT ON; {query}', '-W', '-h', '-1', '-s', '\t']
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except Exception:
        return []
    if result.returncode != 0:
        return []
    rows = []
    for line in result.stdout.strip().split('\n'):
        line = line.strip()
        if not line or line.startswith('--'):
            continue
        rows.append([c.strip() for c in line.split('\t')])
    return rows

def _ensure_audit_tables():
    """Create AgentAuditLog and AgentRunLog if they don't exist. Safe to run every startup."""
    _sql_query(
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
    _sql_query(
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
    _sql_query(f"UPDATE SMSMessages SET SendFromEmployeeId=105 "
               f"WHERE MessageId={int(message_id)}")

def _sms_mark_handled(message_id):
    """Mark an inbound message as handled."""
    _sql_query(f"UPDATE SMSMessages SET IsHandled=1, MarkedHandledEmployeeId=105 "
               f"WHERE MessageId={int(message_id)}")

def _sms_get_client_context(client_id):
    """Return dict with client name, pets, upcoming appts, and recent conversation."""
    cid = int(client_id)

    client_rows = _sql_query(
        f"SELECT CLFirstName, CLLastName FROM Clients WHERE CLSeq={cid}")
    if not client_rows or len(client_rows[0]) < 2:
        return None
    first_name = client_rows[0][0]
    last_name  = client_rows[0][1]

    pet_rows = _sql_query(
        f"SELECT p.PtPetName, ISNULL(b.BrBreed,'') "
        f"FROM Pets p LEFT JOIN Breeds b ON p.PtBreedID=b.BrSeq "
        f"WHERE p.PtOwnerCode={cid} AND (p.PtDeleted IS NULL OR p.PtDeleted=0)")
    pets = [f"{r[0]} ({r[1]})" if len(r) > 1 and r[1] else r[0] for r in pet_rows]

    appt_rows = _sql_query(
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

    conv_rows = _sql_query(
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

def _sms_get_compact_availability():
    """Return a compact text block of the next ~8 open slots per active groomer."""
    import datetime as dt
    today   = dt.date.today()
    end     = today + dt.timedelta(days=45)
    today_s = today.isoformat()
    end_s   = end.isoformat()

    STD_SLOTS = ['08:30', '10:00', '11:30', '13:30', '14:30']
    GROOMERS  = [
        (59, 'Kumi',     'handstrip only'),
        (85, 'Tomoko',   ''),
        (95, 'Mandilyn', 'LG/XL default'),
    ]

    # Closed / holiday dates
    hols = {r[0] for r in _sql_query(
        f"SELECT CONVERT(VARCHAR(10),Date,120) FROM Calendar "
        f"WHERE Date>'{today_s}' AND Date<='{end_s}' AND Styleset<>'None'") if r}

    # LIMIT-blocked dates
    limits = {r[0] for r in _sql_query(
        f"SELECT DISTINCT CONVERT(VARCHAR(10),GLDate,120) FROM GroomingLog "
        f"WHERE GLPetID=12120 AND GLDate>'{today_s}' AND GLDate<='{end_s}' "
        f"AND (GLDeleted IS NULL OR GLDeleted=0)") if r}

    # Already-taken slots: set of (groomer_id, 'YYYY-MM-DD', 'HH:MM')
    taken = set()
    for r in _sql_query(
            f"SELECT GLGroomerID, CONVERT(VARCHAR(10),GLDate,120), "
            f"CONVERT(VARCHAR(5),DATEADD(MINUTE,DATEDIFF(MINUTE,'1899-12-30',GLInTime),0),108) "
            f"FROM GroomingLog WHERE GLDate>'{today_s}' AND GLDate<='{end_s}' "
            f"AND (GLDeleted IS NULL OR GLDeleted=0) AND GLGroomerID IS NOT NULL"):
        if len(r) == 3:
            try: taken.add((int(r[0]), r[1], r[2]))
            except: pass

    def working_days_for(gid):
        """Return set of date-strings when this groomer is scheduled."""
        working = set()
        for r in _sql_query(
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
            for slot in STD_SLOTS:
                if (gid, ds, slot) not in taken:
                    found.append(f"{d.strftime('%a %b')} {d.day} {slot}")
            if len(found) >= 8:
                break
        label = f"{name} ({note})" if note else name
        lines.append(f"{label}: " + (', '.join(found[:8]) if found else 'no slots in next 45 days'))
    return '\n'.join(lines)

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
        rows = _sql_query(
            f"SELECT TOP 1 c.CLSeq, c.CLFirstName, c.CLLastName, c.CLPhone1 "
            f"FROM Clients c INNER JOIN Pets p ON p.PtOwnerCode=c.CLSeq "
            f"WHERE p.PtPetName LIKE '%{pet}%' "
            f"AND (c.CLDeleted IS NULL OR c.CLDeleted=0) "
            f"AND (p.PtDeleted IS NULL OR p.PtDeleted=0)")
    else:
        parts = q.split()
        if len(parts) >= 2:
            fname, lname = parts[0], parts[-1]
            rows = _sql_query(
                f"SELECT TOP 1 CLSeq, CLFirstName, CLLastName, CLPhone1 FROM Clients "
                f"WHERE CLFirstName LIKE '%{fname}%' AND CLLastName LIKE '%{lname}%' "
                f"AND (CLDeleted IS NULL OR CLDeleted=0)")
        else:
            rows = _sql_query(
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
        "You are drafting SMS replies for Dog's Best Friend grooming salon, writing as Noah (owner). "
        "Style: concise, start with 'Hi [FirstName]', no emojis, no exclamation marks, "
        "use 'we/us' for the business, professional but warm. "
        "IMPORTANT: We currently have no cat groomer on staff. Do not offer or book any cat grooming. "
        "The only exception is Sadie Donnelly's nail trim. "
        "When the client is asking about or proposing an appointment:\n"
        "  • If they propose a specific date/time, check the real open slots and confirm or offer the closest alternatives.\n"
        "  • If they ask for availability, offer 2-3 specific real open slots for the right groomer.\n"
        "  • Only suggest slots from the REAL OPEN SLOTS list — never invent times.\n"
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
        "You are drafting SMS replies for Dog's Best Friend grooming salon, writing as Noah (owner). "
        "Style: concise, start with 'Hi [FirstName]', no emojis, no exclamation marks, "
        "use 'we/us' for the business, professional but warm. "
        "IMPORTANT: We currently have no cat groomer on staff. Do not offer or book any cat grooming. "
        "The only exception is Sadie Donnelly's nail trim. "
        "When the client is asking about or proposing an appointment:\n"
        "  • If they propose a specific date/time, check the real open slots and confirm or offer the closest alternatives.\n"
        "  • If they ask for availability, offer 2-3 specific real open slots for the right groomer.\n"
        "  • Only suggest slots from the REAL OPEN SLOTS list — never invent times.\n"
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

    # Find the most recent unmatched sent escalation (if any)
    escalation_context = None
    matched_draft_id   = None
    sorted_escs = sorted(
        _sent_escalations.items(),
        key=lambda x: x[1].get('sent_at', ''),
        reverse=True,
    )
    for draft_id, esc in sorted_escs:
        if not esc.get('matched'):
            escalation_context = esc.get('escalation_context', '')
            matched_draft_id   = draft_id
            break

    # Ask Claude if the message is KB-worthy
    kb_result = _extract_kb_from_noah_reply(message, escalation_context)

    if kb_result:
        category, content = kb_result
        success = _append_to_knowledge_base(category, content)
        if success and matched_draft_id:
            _sent_escalations[matched_draft_id]['matched'] = True
            _save_pending_escalations()

    log.info(f"[SMS] Noah inbound processed: msg_id={msg_id}, kb_added={bool(kb_result)}")


def _sms_poll_inbound():
    """Check for new inbound SMS messages and generate drafts. Called every 30s."""
    global _sms_last_seen_id

    # On first run, just set the watermark — don't retroactively draft old messages
    if _sms_last_seen_id == 0:
        rows = _sql_query("SELECT ISNULL(MAX(MessageId),0) FROM SMSMessages")
        if rows and rows[0]:
            try:
                _sms_last_seen_id = int(rows[0][0])
            except (ValueError, TypeError):
                _sms_last_seen_id = 0
        print(f"[SMS] Poller initialized, watermark MessageId={_sms_last_seen_id}")
        return

    rows = _sql_query(
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
            '-S', SQL_SERVER,
            '-d', SQL_DATABASE,
            *SQL_AUTH_ARGS,
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
            '-S', SQL_SERVER,
            '-d', SQL_DATABASE,
            *SQL_AUTH_ARGS,
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
            'sqlcmd', '-S', SQL_SERVER, '-d', SQL_DATABASE,
            *SQL_AUTH_ARGS,
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
            'sqlcmd', '-S', SQL_SERVER, '-d', SQL_DATABASE,
            *SQL_AUTH_ARGS,
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
    ISNULL(CAST(gl.GLCompleted AS varchar), '0') AS Completed
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
LEFT JOIN Employees e1 ON gl.GLGroomerID = e1.USSEQN
LEFT JOIN DBFCMClientStats s ON c.CLSeq = s.ClientID
WHERE gl.GLDate = CAST(GETDATE() AS DATE)
  AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
  AND (gl.GLNoShow IS NULL OR gl.GLNoShow = 0)
  AND p.PtSeq != 12120
  AND NOT EXISTS (
      SELECT 1 FROM Receipts r
      WHERE r.RPCLIENTID = c.CLSeq
      AND CAST(r.RPDATE AS DATE) = CAST(GETDATE() AS DATE)
  )
ORDER BY gl.GLInTime, c.CLSeq
"""
        rows = _sql_query(query.strip())

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
                }

            clients[client_id]['pets'].append({
                'name':    pet_name,
                'groomer': groomer if groomer else None,
                'done':    completed == '-1',
            })

        return {'clients': list(clients.values()), 'count': len(clients)}

    def sms_get_drafts(self):
        """Return list of pending SMS drafts sorted oldest first."""
        with _sms_drafts_lock:
            drafts = list(_sms_drafts.values())
        drafts.sort(key=lambda d: d['message_id'])
        return {'drafts': drafts, 'count': len(drafts)}

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
        raw = _run_one_shot_claude(system, instruction, timeout=30)
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
            # Escalation draft to Noah's personal cell
            draft_id = f"escalation-{uuid.uuid4().hex[:8]}"
            with _sms_drafts_lock:
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
        pet_rows = _sql_query(
            f"SELECT PtSeq, PtPetName FROM Pets "
            f"WHERE PtOwnerCode={client_id} AND (PtDeleted IS NULL OR PtDeleted=0)")
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
        pr = _sql_query(
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
            cr = _sql_query(f"SELECT PtCat FROM Pets WHERE PtSeq={pet_id}")
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

        _sql_query(
            f"INSERT INTO GroomingLog "
            f"(GLDate,GLInTime,GLOutTime,GLPetID,GLGroomerID,GLBatherID,GLOthersID,"
            f"GLBath,GLGroom,GLOthers,GLConfirmed,GLDeleted,GLWaitlist,GLTakenBy,GLRate,GLBathRate) "
            f"VALUES "
            f"('{date}','{in_time}','{out_time}',{pet_id},{g_val},{b_val},{o_val},"
            f"{gl_bath},{gl_groom},NULL,0,0,0,'CLD',{gl_rate},{gl_bath_rate})")

        seq_rows = _sql_query(
            f"SELECT MAX(GLSeq) FROM GroomingLog "
            f"WHERE GLPetID={pet_id} AND GLDate='{date}' AND GLTakenBy='CLD'")
        new_seq = None
        if seq_rows and seq_rows[0]:
            try: new_seq = int(seq_rows[0][0])
            except: pass

        print(f"[SMS/Book] Created GLSeq={new_seq} PetID={pet_id} {date} {appt_time} svc={service}")
        return {'success': True, 'glseq': new_seq}

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
    log.info('Kennel Connection Backend Server')
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
