"""
db_utils.py — Shared database utilities for the DBFCM extension.

Auto-detects the execution environment (Windows host, WSL on BIKIGBR, or
remote WSL) and configures the sqlcmd connection accordingly.

Call configure_from_config(cfg) after loading config.local.json to override
the auto-detected SQL connection settings.

Exports:
    configure(server, database, auth_args)
    configure_from_config(cfg_dict)
    run_query(query, timeout) -> list[str]           # raw lines, raises on error
    run_query_rows(query, timeout) -> list[list[str]] # parsed rows, [] on error
    run_update(query, timeout) -> None                # DML via stdin, raises
    run_update_count(query, timeout) -> int           # DML via -Q, returns count
    cols(line) -> list[str]
    sql_str(value) -> str
    normalize_phone(raw) -> str
    format_phone(normalized) -> str
    AUTHOR_CODES: dict
    author_code(name) -> str
"""

import os
import platform
import re
import socket
import subprocess

# ── Platform-specific subprocess flags ────────────────────────────────────

_SUBPROCESS_KWARGS = {}
if platform.system() == 'Windows':
    _SUBPROCESS_KWARGS['creationflags'] = 0x08000000  # CREATE_NO_WINDOW

# ── Connection auto-detection ─────────────────────────────────────────────

_HOSTNAME = socket.gethostname().upper()

SQL_DATABASE = 'wkennel7'

if platform.system() == 'Windows':
    # Native Windows (backend_server.py on both machines)
    SQL_SERVER = '.\\WKENNEL'
    SQL_AUTH_ARGS = ['-E']  # Windows Authentication
    SQLCMD_BIN = 'sqlcmd'
else:
    # WSL / Linux (noahbot_mcp_server.py, refresh_client_stats.py)
    _SQLCMD_CANDIDATES = [
        os.path.expanduser('~/sqlcmd'),
        '/opt/mssql-tools18/bin/sqlcmd',
        '/opt/mssql-tools/bin/sqlcmd',
        '/usr/bin/sqlcmd',
    ]
    SQLCMD_BIN = next((p for p in _SQLCMD_CANDIDATES if os.path.isfile(p)), 'sqlcmd')

    if _HOSTNAME == 'DESKTOP-BIKIGBR':
        # WSL on the SQL Server host — hostname resolves to 127.0.1.1 which
        # can't reach Windows; use the default gateway IP instead.
        try:
            _gw = subprocess.run(['ip', 'route', 'show', 'default'],
                                 capture_output=True, text=True)
            _WIN_IP = _gw.stdout.split()[2]
        except Exception:
            _WIN_IP = '172.22.224.1'
        SQL_SERVER = f'{_WIN_IP},2721'
        SQL_AUTH_ARGS = ['-U', 'noah', '-P', 'noah', '-N', 'disable']
    else:
        SQL_SERVER = 'desktop-bikigbr,2721'
        SQL_AUTH_ARGS = ['-U', 'noah', '-P', 'noah']


def configure(server=None, database=None, auth_args=None):
    """Override auto-detected connection settings."""
    global SQL_SERVER, SQL_DATABASE, SQL_AUTH_ARGS
    if server is not None:
        SQL_SERVER = server
    if database is not None:
        SQL_DATABASE = database
    if auth_args is not None:
        SQL_AUTH_ARGS = auth_args


def configure_from_config(cfg):
    """Apply SQL settings from the extension's config.local.json dict.

    Expected keys: sql_server, sql_database, sql_auth, sql_user, sql_password.
    """
    global SQL_SERVER, SQL_DATABASE, SQL_AUTH_ARGS
    SQL_SERVER = cfg.get('sql_server', SQL_SERVER)
    SQL_DATABASE = cfg.get('sql_database', SQL_DATABASE)
    if cfg.get('sql_auth') == 'windows':
        SQL_AUTH_ARGS = ['-E']
    else:
        SQL_AUTH_ARGS = ['-U', cfg.get('sql_user', 'noah'),
                         '-P', cfg.get('sql_password', 'noah')]


# ── Internal helpers ──────────────────────────────────────────────────────

def _check_sql_errors(stdout):
    """Raise RuntimeError if sqlcmd stdout contains SQL error messages.

    sqlcmd returns exit code 0 even on SQL errors — must check stdout.
    """
    for line in stdout.split('\n'):
        s = line.strip()
        if s.startswith('Msg ') and ', Level ' in s:
            raise RuntimeError(f'SQL error: {s}')


# ── Query execution ──────────────────────────────────────────────────────

def run_query(query, timeout=30):
    """Run a SELECT query via sqlcmd. Returns raw output lines (tab-delimited).

    Raises RuntimeError on sqlcmd failure or SQL errors.
    Filters out separator lines (---) and row-count lines.
    """
    cmd = [
        SQLCMD_BIN,
        '-S', SQL_SERVER, '-d', SQL_DATABASE,
        *SQL_AUTH_ARGS,
        '-Q', query,
        '-s', '\t', '-W', '-h', '-1',
    ]
    result = subprocess.run(cmd, capture_output=True, timeout=timeout,
                            **_SUBPROCESS_KWARGS)
    stdout = result.stdout.decode('utf-8', errors='replace')
    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8', errors='replace')
        raise RuntimeError(f'sqlcmd error: {stderr.strip()}')
    _check_sql_errors(stdout)
    return [
        line for line in stdout.split('\n')
        if line.strip()
        and not line.strip().startswith('---')
        and not re.match(r'^\(\d+ rows? affected\)', line.strip())
    ]


def run_query_rows(query, timeout=30, raise_on_error=False):
    """Run a SELECT query and return parsed rows as list[list[str]].

    Each row is a list of stripped column values.
    By default, returns [] on any error (silent failure for API handlers).
    Pass raise_on_error=True to propagate exceptions.
    """
    try:
        lines = run_query(query, timeout=timeout)
    except Exception:
        if raise_on_error:
            raise
        return []
    return [cols(line) for line in lines]


def run_update(query, timeout=60):
    """Run a DML statement by piping SQL via stdin (handles long queries).

    Raises RuntimeError on sqlcmd failure or SQL errors.
    """
    cmd = [
        SQLCMD_BIN,
        '-S', SQL_SERVER, '-d', SQL_DATABASE,
        *SQL_AUTH_ARGS,
    ]
    result = subprocess.run(cmd, input=query.encode('utf-8'),
                            capture_output=True, timeout=timeout,
                            **_SUBPROCESS_KWARGS)
    stdout = result.stdout.decode('utf-8', errors='replace')
    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8', errors='replace')
        raise RuntimeError(f'sqlcmd error: {stderr.strip()}')
    _check_sql_errors(stdout)


def run_update_count(query, timeout=60):
    """Run a DML statement via -Q flag and return the rows-affected count.

    Raises RuntimeError on sqlcmd failure or SQL errors.
    """
    cmd = [
        SQLCMD_BIN,
        '-S', SQL_SERVER, '-d', SQL_DATABASE,
        *SQL_AUTH_ARGS,
        '-Q', query, '-W',
    ]
    result = subprocess.run(cmd, capture_output=True, timeout=timeout,
                            **_SUBPROCESS_KWARGS)
    stdout = result.stdout.decode('utf-8', errors='replace')
    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8', errors='replace')
        raise RuntimeError(f'sqlcmd error: {stderr.strip()}')
    _check_sql_errors(stdout)
    for line in stdout.splitlines():
        m = re.search(r'\((\d+) rows? affected\)', line)
        if m:
            return int(m.group(1))
    return 0


# ── Parsing helpers ───────────────────────────────────────────────────────

def cols(line):
    """Split a tab-delimited line into stripped fields."""
    return [c.strip() for c in line.split('\t')]


def sql_str(value):
    """Escape single quotes for a SQL string literal."""
    return (value or '').replace("'", "''")


# ── Phone utilities ───────────────────────────────────────────────────────

def normalize_phone(raw):
    """Normalize a phone number: strip non-digits, drop leading US country code."""
    digits = re.sub(r'\D', '', raw or '')
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    return digits


def format_phone(normalized):
    """Format a 10-digit number as (XXX) XXX-XXXX."""
    if len(normalized) == 10:
        return f'({normalized[:3]}) {normalized[3:6]}-{normalized[6:]}'
    return normalized


# ── Author codes ──────────────────────────────────────────────────────────

AUTHOR_CODES = {
    'noah': 'NMH', 'noah han': 'NMH',
    'tomoko': 'TOM', 'tomoko hirokawa': 'TOM',
    'kumi': 'KMT', 'kumi tachikake': 'KMT',
    'mandilyn': 'MDY', 'mandilyn yarbrough': 'MDY',
    'elmer': 'ELM', 'elmer rivera': 'ELM',
    'josh': 'JSH', 'josh han': 'JSH',
}


def author_code(name):
    """Look up 3-letter author code by name; defaults to 'EXT'."""
    return AUTHOR_CODES.get((name or '').lower().strip(), 'EXT')
