#!/usr/bin/env python3
"""
Noah-bot MCP Server — Kennel DB Tools
Implements Model Context Protocol (JSON-RPC 2.0 over stdio).
Claude calls these tools when answering staff questions about
appointments, clients, pets, waitlist, and groomer schedules.

Security: No arbitrary SQL. 6 named tools with pre-written queries,
validated inputs, and restricted column access.
"""

import sys
import json
import os
import subprocess
import re
import urllib.request
import urllib.error
from datetime import datetime, date

from db_utils import run_query, run_update, cols, sql_str

# ---------------------------------------------------------------------------
# Backend URL resolution (MCP server runs in WSL; backend may run on Windows)
# ---------------------------------------------------------------------------
def _get_wsl_windows_host_ip():
    """Return Windows host IP from WSL2 default gateway, or None if unavailable."""
    try:
        gw = subprocess.run(['ip', 'route', 'show', 'default'],
                            capture_output=True, text=True, timeout=3)
        return gw.stdout.split()[2]
    except Exception:
        try:
            with open('/etc/resolv.conf') as f:
                for line in f:
                    if line.startswith('nameserver'):
                        return line.split()[1]
        except Exception:
            pass
    return None

def _is_wsl2() -> bool:
    """Return True if running inside WSL2."""
    try:
        with open('/proc/version') as f:
            return 'microsoft' in f.read().lower()
    except Exception:
        return False

# Build backend URL candidates: localhost first, Windows host IP as fallback.
_BACKEND_URLS = ['http://localhost:8000']
if _is_wsl2():
    _win_ip_fallback = _get_wsl_windows_host_ip()
    if _win_ip_fallback:
        _BACKEND_URLS.append(f'http://{_win_ip_fallback}:8000')

VALID_GROOMER_IDS = {8, 59, 85, 91, 94, 95, 97}

# ---------------------------------------------------------------------------
# Input validation helpers
# ---------------------------------------------------------------------------

def _validate_date(s: str) -> str:
    """Validate and return a date string in YYYY-MM-DD format."""
    datetime.strptime(s, '%Y-%m-%d')  # raises ValueError on bad input
    return s

def _validate_groomer_id(v) -> int:
    """Validate groomer ID against known set."""
    gid = int(v)
    if gid not in VALID_GROOMER_IDS:
        raise ValueError(f"Unknown groomer_id {gid}. Valid IDs: {sorted(VALID_GROOMER_IDS)}")
    return gid

def _escape_like(s: str) -> str:
    """Escape LIKE wildcards and single quotes for SQL LIKE clauses."""
    s = s.replace("'", "''")
    s = s.replace("[", "[[]").replace("%", "[%]").replace("_", "[_]")
    return s

def _validate_note_text(s: str) -> str:
    """Validate note text: max 500 chars, escape quotes."""
    if len(s) > 500:
        raise ValueError("Note text exceeds 500 characters.")
    return sql_str(s.strip())

# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

def tool_get_appointments(args: dict) -> str:
    """Appointments for a date range, optionally filtered by groomer."""
    date_from = _validate_date(args.get('date_from', ''))
    date_to   = _validate_date(args.get('date_to', date_from))

    groomer_clause = ""
    if 'groomer_id' in args and args['groomer_id'] not in (None, ''):
        gid = _validate_groomer_id(args['groomer_id'])
        groomer_clause = f"AND (gl.GLGroomerID = {gid} OR gl.GLBatherID = {gid} OR gl.GLOthersID = {gid})"

    query = f"""
SELECT TOP 50
    CONVERT(varchar, gl.GLDate, 23) as ApptDate,
    CONVERT(varchar, gl.GLInTime, 108) as InTime,
    CONVERT(varchar, gl.GLOutTime, 108) as OutTime,
    p.PtPetName,
    c.CLLastName,
    c.CLFirstName,
    ISNULL(b.BrBreed, p.PtBreed) as Breed,
    ISNULL(e1.USFNAME, '') as Groomer,
    ISNULL(e2.USFNAME, '') as Bather,
    ISNULL(e3.USFNAME, '') as Handstrip,
    CASE
        WHEN gl.GLOthersID > 0 THEN 'Handstrip'
        WHEN gl.GLBath = -1 AND gl.GLGroom = 0 THEN 'Bath Only'
        WHEN gl.GLBath = 0 AND gl.GLGroom = -1 THEN 'Groom Only'
        WHEN gl.GLBath = -1 AND gl.GLGroom = -1 THEN 'Full Service'
        ELSE 'Other'
    END as ServiceType,
    CASE WHEN gl.GLConfirmed = -1 THEN 'Confirmed' ELSE 'Unconfirmed' END as Status,
    ISNULL(REPLACE(REPLACE(p.PtWarning, CHAR(13), ' '), CHAR(10), ' '), '') as PetWarning,
    ISNULL(REPLACE(REPLACE(c.CLWarning, CHAR(13), ' '), CHAR(10), ' '), '') as ClientWarning
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
LEFT JOIN Breeds b ON p.PtBreedID = b.BrSeq
LEFT JOIN Employees e1 ON gl.GLGroomerID = e1.USSEQN
LEFT JOIN Employees e2 ON gl.GLBatherID = e2.USSEQN
LEFT JOIN Employees e3 ON gl.GLOthersID = e3.USSEQN
WHERE gl.GLDate BETWEEN '{date_from}' AND '{date_to}'
AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist = 0)
{groomer_clause}
ORDER BY gl.GLDate, gl.GLInTime
"""
    lines = run_query(query, timeout=30)
    if not lines:
        return f"No appointments found between {date_from} and {date_to}."

    rows = []
    for line in lines:
        if '\t' not in line:
            continue
        c = cols(line)
        if len(c) < 14:
            continue
        time_display = f"{c[1][:5]}–{c[2][:5]}" if c[2] else c[1][:5]
        groomer_info = c[7] or c[9] or c[8] or "—"
        svc = c[10]
        status = c[11]
        warn = ""
        if c[12]:
            warn += f" [PET: {c[12]}]"
        if c[13]:
            warn += f" [CLIENT: {c[13]}]"
        rows.append(
            f"{c[0]} {time_display} | {c[3]} ({c[4]}, {c[5]}) | {c[6]} | "
            f"{svc} | {groomer_info} | {status}{warn}"
        )

    header = f"Appointments {date_from} to {date_to} ({len(rows)} found):\n"
    return header + "\n".join(rows)


def tool_search_client_or_pet(args: dict) -> str:
    """Search clients and pets by name, return full profile."""
    raw_name = args.get('name', '').strip()
    if not raw_name:
        raise ValueError("'name' parameter is required.")
    name = _escape_like(raw_name)

    # Main client/pet query
    query = f"""
SELECT TOP 50
    c.CLSeq,
    c.CLLastName,
    c.CLFirstName,
    ISNULL(c.CLPhone1, '') as Phone,
    ISNULL(c.CLEmail, '') as Email,
    ISNULL(REPLACE(REPLACE(c.CLWarning, CHAR(13), ' '), CHAR(10), ' '), '') as ClientWarning,
    ISNULL(REPLACE(REPLACE(c.CLNotes, CHAR(13), ' '), CHAR(10), ' '), '') as ClientNotes,
    p.PtSeq,
    p.PtPetName,
    ISNULL(b.BrBreed, p.PtBreed) as Breed,
    ISNULL(pt.PTypeName, '') as PetSize,
    ISNULL(REPLACE(REPLACE(p.PtWarning, CHAR(13), ' '), CHAR(10), ' '), '') as PetWarning,
    ISNULL(REPLACE(REPLACE(p.PtGroom, CHAR(13), ' '), CHAR(10), ' '), '') as GroomNotes,
    ISNULL(REPLACE(REPLACE(p.PtNotes, CHAR(13), ' '), CHAR(10), ' '), '') as PetNotes
FROM Clients c
LEFT JOIN Pets p ON p.PtOwnerCode = c.CLSeq
    AND (p.PtDeleted IS NULL OR p.PtDeleted = 0)
LEFT JOIN Breeds b ON p.PtBreedID = b.BrSeq
LEFT JOIN PetTypes pt ON p.PtCat = pt.PTypeSeq
WHERE (c.CLDeleted IS NULL OR c.CLDeleted = 0)
AND (
    c.CLLastName LIKE '%{name}%'
    OR c.CLFirstName LIKE '%{name}%'
    OR p.PtPetName LIKE '%{name}%'
)
ORDER BY c.CLLastName, c.CLFirstName, p.PtPetName
"""
    lines = run_query(query, timeout=30)
    if not lines or not any('\t' in l for l in lines):
        return f"No clients or pets found matching '{raw_name}'."

    # Collect client IDs for history lookup
    client_ids = []
    results = []
    last_client = None

    for line in lines:
        if '\t' not in line:
            continue
        c = cols(line)
        if len(c) < 14:
            continue
        cl_id = c[0]
        if cl_id not in client_ids:
            client_ids.append(cl_id)

        client_header = f"\n=== {c[1]}, {c[2]} (ID:{cl_id}) | Phone: {c[3]} | Email: {c[4]}"
        if cl_id != last_client:
            results.append(client_header)
            if c[5]:
                results.append(f"  ⚠️  CLIENT WARNING: {c[5]}")
            if c[6]:
                results.append(f"  Notes: {c[6]}")
            last_client = cl_id

        if c[7]:  # pet exists
            results.append(f"  Pet: {c[8]} ({c[9]}, {c[10]}) [ID:{c[7]}]")
            if c[11]:
                results.append(f"    Pet warning: {c[11]}")
            if c[12]:
                results.append(f"    Groom notes: {c[12]}")
            if c[13]:
                results.append(f"    Pet notes: {c[13]}")

    if not client_ids:
        return f"No clients or pets found matching '{raw_name}'."

    # Last 5 appointments for each matched client
    id_list = ','.join(client_ids[:10])
    hist_query = f"""
SELECT TOP 50
    c.CLSeq,
    CONVERT(varchar, gl.GLDate, 23) as ApptDate,
    ISNULL(e1.USFNAME, ISNULL(e3.USFNAME, '?')) as Groomer,
    p.PtPetName,
    CASE
        WHEN gl.GLOthersID > 0 THEN 'Handstrip'
        WHEN gl.GLBath = -1 AND gl.GLGroom = 0 THEN 'Bath Only'
        WHEN gl.GLBath = -1 AND gl.GLGroom = -1 THEN 'Full Service'
        ELSE 'Other'
    END as ServiceType
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
LEFT JOIN Employees e1 ON gl.GLGroomerID = e1.USSEQN
LEFT JOIN Employees e3 ON gl.GLOthersID = e3.USSEQN
WHERE c.CLSeq IN ({id_list})
AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist = 0)
AND gl.GLDate <= CONVERT(date, GETDATE())
ORDER BY c.CLSeq, gl.GLDate DESC
"""
    hist_lines = run_query(hist_query, timeout=30)

    # Group history by client, keep last 5 per client
    hist_by_client: dict[str, list[str]] = {}
    hist_count: dict[str, int] = {}
    for line in hist_lines:
        if '\t' not in line:
            continue
        h = cols(line)
        if len(h) < 5:
            continue
        cid = h[0]
        if hist_count.get(cid, 0) >= 5:
            continue
        hist_by_client.setdefault(cid, []).append(
            f"    {h[1]} — {h[3]} ({h[4]}) with {h[2]}"
        )
        hist_count[cid] = hist_count.get(cid, 0) + 1

    # Pull client stats from DBFCMClientStats in SQL Server (tip + cadence)
    stats_by_client: dict[str, str] = {}
    try:
        id_list_stats = ','.join(client_ids[:10])
        stats_lines = run_query(f"""
SELECT
    ClientID, TipMethod, LastTipAmount, LastTipPct,
    AvgTipAmount, AvgTipPct, TipReceiptCount, PreferredPayment,
    CardTipRate, AvgCadenceDays, PreferredDay, PreferredTime,
    ApptCount12Mo, LastApptDate
FROM DBFCMClientStats
WHERE ClientID IN ({id_list_stats})
""", timeout=15)
        for line in stats_lines:
            if '\t' not in line:
                continue
            s = cols(line)
            if len(s) < 14:
                continue
            cid   = s[0]
            parts = []
            tip_m = s[1] or 'Unknown'
            try:
                last_tip = float(s[2]) if s[2] and s[2] != 'NULL' else None
                last_pct = float(s[3]) if s[3] and s[3] != 'NULL' else None
                avg_tip  = float(s[4]) if s[4] and s[4] != 'NULL' else None
                avg_pct  = float(s[5]) if s[5] and s[5] != 'NULL' else None
                tip_cnt  = int(s[6])   if s[6] and s[6] != 'NULL' else 0
                pay_type = s[7] or 'Unknown'
                tip_rate = float(s[8]) if s[8] and s[8] != 'NULL' else 0.0
                cadence  = float(s[9]) if s[9] and s[9] != 'NULL' else None
                pref_day = s[10] or '?'
                pref_tm  = s[11] or '?'
                cnt_12mo = int(s[12])  if s[12] and s[12] != 'NULL' else 0
                last_dt  = s[13] or 'N/A'
            except (ValueError, IndexError):
                continue

            if last_tip:
                parts.append(
                    f"Tip history: {tip_m} | Last ${last_tip:.2f} ({last_pct:.0f}%) | "
                    f"Typical ${avg_tip:.2f} ({avg_pct:.0f}%) | "
                    f"{tip_cnt} card visit(s) | Payment: {pay_type} | "
                    f"Card tip rate: {tip_rate*100:.0f}%"
                )
            else:
                parts.append(f"Tip history: {tip_m} | Payment: {pay_type}")
            if cadence:
                cadence_wks = round(cadence / 7)
                parts.append(
                    f"Cadence: every ~{cadence_wks} week(s) | "
                    f"Preferred: {pref_day} {pref_tm} | "
                    f"{cnt_12mo} appt(s) past 12mo | Last: {last_dt}"
                )
            stats_by_client[cid] = " | ".join(parts)
    except Exception:
        pass  # DBFCMClientStats may not be populated yet — graceful degradation

    # Merge history and stats into results
    final = []
    current_client = None
    for line in results:
        final.append(line)
        # Detect client header lines (start with ===)
        if line.startswith('\n=== ') or line.startswith('=== '):
            # Extract ID
            m = re.search(r'\(ID:(\d+)\)', line)
            if m:
                current_client = m.group(1)
        elif line.startswith('  Pet:'):
            pass  # no insertion here
        elif line.startswith('  ⚠️') or line.startswith('  Notes'):
            # After client-level lines, insert stats and history
            pass

    # Rebuild with stats+history injected after client header
    output = []
    i = 0
    lines_list = results
    client_section_ids: list[str] = []
    inserted_stats: set[str] = set()

    for line in lines_list:
        output.append(line)
        if line.startswith('\n=== ') or line.startswith('=== '):
            m = re.search(r'\(ID:(\d+)\)', line)
            if m:
                cid = m.group(1)
                if cid not in inserted_stats:
                    # Insert stats right after client header
                    if cid in stats_by_client:
                        output.append(f"  📊 {stats_by_client[cid]}")
                    # Insert appointment history at end of this client block
                    if cid in hist_by_client:
                        output.append(f"  Recent appointments:")
                        output.extend(hist_by_client[cid])
                    inserted_stats.add(cid)

    return "\n".join(output)


def tool_get_open_slots(args: dict) -> str:
    """Find available booking slots on a given date per groomer."""
    target_date = _validate_date(args.get('date', ''))

    groomer_clause = ""
    groomer_filter = ""
    if 'groomer_id' in args and args['groomer_id'] not in (None, ''):
        gid = _validate_groomer_id(args['groomer_id'])
        groomer_clause = f"AND gs.GroomerSchID = {gid}"
        groomer_filter = f"AND (gl.GLGroomerID = {gid} OR gl.GLBatherID = {gid} OR gl.GLOthersID = {gid})"

    # Who's scheduled on target date?
    dt = datetime.strptime(target_date, '%Y-%m-%d')
    day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    day_col_map = {
        0: ('GroomerSchMonIn', 'GroomerSchMonOut'),
        1: ('GroomerSchtueIn', 'GroomerSchtueOut'),
        2: ('GroomerSchwedIn', 'GroomerSchwedOut'),
        3: ('GroomerSchthurIn', 'GroomerSchthurOut'),
        4: ('GroomerSchfriIn', 'GroomerSchfriOut'),
        5: ('GroomerSchsatIn', 'GroomerSchsatOut'),
        6: ('GroomerSchsunIn', 'GroomerSchsunOut'),
    }
    dow = dt.weekday()  # 0=Mon
    in_col, out_col = day_col_map[dow]

    sched_query = f"""
SELECT
    gs.GroomerSchID,
    e.USFNAME,
    gs.{in_col} as InTime,
    gs.{out_col} as OutTime
FROM GroomerSched gs
INNER JOIN Employees e ON gs.GroomerSchID = e.USSEQN
WHERE gs.{in_col} IS NOT NULL
AND gs.GroomerSchWEDate = (
    SELECT MIN(gs2.GroomerSchWEDate)
    FROM GroomerSched gs2
    WHERE gs2.GroomerSchID = gs.GroomerSchID
    AND gs2.GroomerSchWEDate >= DATEADD(day, 7 - DATEPART(dw, '{target_date}'), '{target_date}')
)
{groomer_clause}
ORDER BY e.USFNAME
"""
    sched_lines = run_query(sched_query, timeout=20)

    if not any('\t' in l for l in sched_lines):
        return f"No groomers scheduled on {target_date} ({day_names[dow]})."

    # Booked appointments
    appt_query = f"""
SELECT
    gl.GLGroomerID,
    gl.GLBatherID,
    gl.GLOthersID,
    CONVERT(varchar, gl.GLInTime, 108) as InTime,
    CONVERT(varchar, gl.GLOutTime, 108) as OutTime,
    p.PtPetName,
    c.CLLastName
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
WHERE gl.GLDate = '{target_date}'
AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist = 0)
{groomer_filter}
ORDER BY gl.GLInTime
"""
    appt_lines = run_query(appt_query, timeout=20)

    # Build booked slots per groomer
    booked: dict[str, list[dict]] = {}
    for line in appt_lines:
        if '\t' not in line:
            continue
        a = cols(line)
        if len(a) < 7:
            continue
        gr_id, ba_id, oth_id = a[0], a[1], a[2]
        for gid_str in set([gr_id, ba_id, oth_id]):
            if gid_str and gid_str != '0' and gid_str.isdigit():
                booked.setdefault(gid_str, []).append({
                    'start': a[3][:5],
                    'end': a[4][:5],
                    'pet': a[5],
                    'client': a[6],
                })

    ALL_SLOTS = ['08:30', '10:00', '11:30', '13:30', '14:30']

    def slot_blocked(slot: str, appts: list) -> bool:
        sm = int(slot[:2]) * 60 + int(slot[3:])
        for a in appts:
            try:
                s = int(a['start'][:2]) * 60 + int(a['start'][3:])
                e = int(a['end'][:2]) * 60 + int(a['end'][3:])
                if s <= sm < e:
                    return True
            except Exception:
                pass
        return False

    def fmt_time(t: str) -> str:
        try:
            h, m = int(t[:2]), int(t[3:5])
            ampm = 'AM' if h < 12 else 'PM'
            return f"{h % 12 or 12}:{m:02d} {ampm}"
        except Exception:
            return t

    output = [f"Slot availability for {target_date} ({day_names[dow]}):"]
    for line in sched_lines:
        if '\t' not in line:
            continue
        s = cols(line)
        if len(s) < 4:
            continue
        gid_str = s[0]
        gname = s[1]
        groomer_appts = booked.get(gid_str, [])
        open_slots = [sl for sl in ALL_SLOTS if not slot_blocked(sl, groomer_appts)]
        booked_slots = [sl for sl in ALL_SLOTS if slot_blocked(sl, groomer_appts)]

        output.append(f"\n{gname} (ID:{gid_str}):")
        if open_slots:
            output.append(f"  Open: {', '.join(fmt_time(s) for s in open_slots)}")
        else:
            output.append("  Fully booked")
        if groomer_appts:
            output.append(f"  Booked ({len(groomer_appts)}): " +
                ', '.join(f"{a['pet']} {a['client']} @{fmt_time(a['start'])}" for a in groomer_appts))

    return "\n".join(output)


def tool_get_waitlist(args: dict) -> str:
    """Get active waitlist entries, optionally filtered by groomer."""
    groomer_clause = ""
    if 'groomer_id' in args and args['groomer_id'] not in (None, ''):
        gid = _validate_groomer_id(args['groomer_id'])
        groomer_clause = f"AND (gl.GLGroomerID = {gid} OR gl.GLOthersID = {gid})"

    query = f"""
SELECT TOP 50
    gl.GLSeq,
    CONVERT(varchar, gl.GLDate, 23) as TargetDate,
    CONVERT(varchar, gl.GLDateEntered, 23) as EnteredDate,
    p.PtPetName,
    c.CLLastName,
    c.CLFirstName,
    ISNULL(c.CLPhone1, '') as Phone,
    ISNULL(b.BrBreed, p.PtBreed) as Breed,
    ISNULL(pt.PTypeName, '') as PetSize,
    CASE
        WHEN gl.GLOthersID > 0 THEN 'Handstrip'
        WHEN gl.GLBath = -1 AND gl.GLGroom = 0 THEN 'Bath Only'
        WHEN gl.GLBath = -1 AND gl.GLGroom = -1 THEN 'Full Service'
        ELSE 'Other'
    END as ServiceType,
    CASE
        WHEN gl.GLOthersID > 0 THEN ISNULL(e3.USFNAME, 'Kumi')
        WHEN gl.GLGroomerID > 0 THEN ISNULL(e1.USFNAME, '')
        ELSE 'Any'
    END as PreferredGroomer,
    ISNULL(REPLACE(REPLACE(gl.GLDescription, CHAR(13), ' '), CHAR(10), ' '), '') as Notes,
    ISNULL(REPLACE(REPLACE(p.PtWarning, CHAR(13), ' '), CHAR(10), ' '), '') as PetWarning
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
LEFT JOIN Breeds b ON p.PtBreedID = b.BrSeq
LEFT JOIN PetTypes pt ON p.PtCat = pt.PTypeSeq
LEFT JOIN Employees e1 ON gl.GLGroomerID = e1.USSEQN
LEFT JOIN Employees e3 ON gl.GLOthersID = e3.USSEQN
WHERE gl.GLWaitlist = -1
AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
{groomer_clause}
ORDER BY gl.GLDate, gl.GLSeq
"""
    lines = run_query(query, timeout=30)
    if not any('\t' in l for l in lines):
        return "Waitlist is empty."

    rows = []
    for line in lines:
        if '\t' not in line:
            continue
        w = cols(line)
        if len(w) < 13:
            continue
        row = (
            f"[{w[0]}] Target: {w[1]} | Entered: {w[2]} | "
            f"{w[3]} ({w[4]}, {w[5]}) | {w[6]} | {w[7]} {w[8]} | "
            f"{w[9]} | Preferred: {w[10]}"
        )
        if w[11]:
            row += f" | Notes: {w[11]}"
        if w[12]:
            row += f" | ⚠️ {w[12]}"
        rows.append(row)

    return f"Waitlist ({len(rows)} entries):\n" + "\n".join(rows)


def tool_get_groomer_schedule(args: dict) -> str:
    """Who's working which days in a date range, plus blocked days."""
    date_from = _validate_date(args.get('date_from', ''))
    date_to   = _validate_date(args.get('date_to', date_from))

    # GroomerSched — weekly schedule
    sched_query = f"""
SELECT
    gs.GroomerSchID,
    e.USFNAME,
    CONVERT(varchar, gs.GroomerSchWEDate, 23) as WeekEnd,
    CONVERT(varchar, gs.GroomerSchMonIn, 108) as MonIn,
    CONVERT(varchar, gs.GroomerSchtueIn, 108) as TueIn,
    CONVERT(varchar, gs.GroomerSchwedIn, 108) as WedIn,
    CONVERT(varchar, gs.GroomerSchthurIn, 108) as ThuIn,
    CONVERT(varchar, gs.GroomerSchfriIn, 108) as FriIn,
    CONVERT(varchar, gs.GroomerSchsatIn, 108) as SatIn
FROM GroomerSched gs
INNER JOIN Employees e ON gs.GroomerSchID = e.USSEQN
WHERE gs.GroomerSchWEDate BETWEEN
    DATEADD(day, 7 - DATEPART(dw, '{date_from}'), DATEADD(day, -6, '{date_from}'))
    AND DATEADD(day, 7 - DATEPART(dw, '{date_to}'), '{date_to}')
AND gs.GroomerSchID IN (59, 85, 95, 8)
ORDER BY gs.GroomerSchWEDate, e.USFNAME
"""
    sched_lines = run_query(sched_query, timeout=20)

    # BlockedTime
    blocked_query = f"""
SELECT
    e.USFNAME,
    CONVERT(varchar, bt.BTDate, 23) as BlockDate,
    ISNULL(bt.BTDescr, '') as Reason
FROM BlockedTime bt
INNER JOIN Employees e ON bt.BTGroomerID = e.USSEQN
WHERE bt.BTDate BETWEEN '{date_from}' AND '{date_to}'
AND bt.BTGroomerID IN (59, 85, 95, 8)
ORDER BY bt.BTDate, e.USFNAME
"""
    blocked_lines = run_query(blocked_query, timeout=20)

    day_labels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']

    output = [f"Groomer schedule {date_from} to {date_to}:"]
    output.append("\n--- Weekly Schedule ---")
    for line in sched_lines:
        if '\t' not in line:
            continue
        s = cols(line)
        if len(s) < 9:
            continue
        week_end = s[2]
        name = s[1]
        working = [day_labels[i] for i, v in enumerate(s[3:9]) if v and v != 'NULL']
        off = [day_labels[i] for i, v in enumerate(s[3:9]) if not v or v == 'NULL']
        output.append(
            f"  {name} (week ending {week_end}): "
            f"Working={','.join(working) or 'None'} | Off={','.join(off) or 'None'}"
        )

    output.append("\n--- Blocked Days ---")
    blocked_found = False
    for line in blocked_lines:
        if '\t' not in line:
            continue
        b = cols(line)
        if len(b) < 3:
            continue
        output.append(f"  {b[0]} blocked {b[1]}: {b[2] or 'No reason given'}")
        blocked_found = True
    if not blocked_found:
        output.append("  None in this range.")

    return "\n".join(output)


def tool_append_note(args: dict) -> str:
    """Insert a structured note into ClientNotes or PetNotes table."""
    entity_type = args.get('entity_type', '').lower().strip()
    entity_id   = args.get('entity_id', '')
    subject     = (args.get('subject', '') or '').strip()[:50]
    text        = args.get('text', '').strip()

    if entity_type not in ('client', 'pet'):
        raise ValueError("entity_type must be 'client' or 'pet'.")

    try:
        eid = int(entity_id)
    except (ValueError, TypeError):
        raise ValueError("entity_id must be a valid integer.")

    safe_text = _validate_note_text(text)

    if not subject:
        subject = safe_text[:47].rstrip() + ('…' if len(safe_text) > 47 else '')
    safe_subj = subject.replace("'", "''")
    safe_body = safe_text.replace("'", "''")

    if entity_type == 'client':
        check = run_query(
            f"SELECT CLFirstName + ' ' + CLLastName FROM Clients WHERE CLSeq={eid} "
            f"AND (CLDeleted IS NULL OR CLDeleted=0)",
            timeout=10
        )
        if not check or not any(l.strip() for l in check):
            raise ValueError(f"Client ID {eid} not found.")
        entity_name = check[0].strip()
        run_update(
            f"INSERT INTO ClientNotes (CNClientSeq,CNDate,CNSubject,CNBy,CNNotes,CNLOCSEQ) "
            f"VALUES ({eid},GETDATE(),'{safe_subj}','CLD','{safe_body}',1)",
            timeout=10
        )
        return f"Note added to {entity_name}'s record: \"{subject}\""

    else:  # pet
        check = run_query(
            f"SELECT PtPetName FROM Pets WHERE PtSeq={eid} "
            f"AND (PtDeleted IS NULL OR PtDeleted=0)",
            timeout=10
        )
        if not check or not any(l.strip() for l in check):
            raise ValueError(f"Pet ID {eid} not found.")
        entity_name = check[0].strip()
        run_update(
            f"INSERT INTO PetNotes (PNPetSeq,PNDate,PNSubject,PNBy,PNNotes,PNLOCSEQ) "
            f"VALUES ({eid},GETDATE(),'{safe_subj}','CLD','{safe_body}',1)",
            timeout=10
        )
        return f"Note added to {entity_name}'s record: \"{subject}\""


# Standard pricing table keyed by PtCat
_PRICE_TABLE = {
    7:  (30,  50),   # XS LH
    12: (20,  40),   # XS SH
    8:  (45,  75),   # SM LH
    13: (25,  60),   # SM SH
    9:  (75,  100),  # MD LH
    14: (40,  80),   # MD SH
    10: (100, 130),  # LG LH
    15: (50,  105),  # LG SH
    11: (135, 165),  # XL LH
    16: (60,  130),  # XL SH
}

VALID_SLOTS = {'08:30', '10:00', '11:30', '13:30', '14:30'}

def _parse_time_slot(s: str) -> str:
    """Accept '10:00', '10:00 AM', '1:30 PM', '13:30' — return 'HH:MM' 24h."""
    s = s.strip().upper()
    # Strip seconds if present (e.g. "10:00:00")
    s = s[:5] if len(s) > 5 and s[2] == ':' else s
    # Handle AM/PM
    if 'AM' in s or 'PM' in s:
        from datetime import datetime as _dt
        for fmt in ('%I:%M %p', '%I %p'):
            try:
                return _dt.strptime(s.replace('.', ''), fmt).strftime('%H:%M')
            except ValueError:
                pass
        raise ValueError(f"Cannot parse time '{s}'.")
    # Already 24h HH:MM
    if len(s) == 5 and s[2] == ':':
        return s
    raise ValueError(f"Cannot parse time '{s}'.")


def tool_create_appointment(args: dict) -> str:
    """Create a new appointment or waitlist entry. Always unconfirmed — confirmation only happens via reminder text."""
    pet_id       = int(args.get('pet_id', 0))
    date         = _validate_date(args.get('date', ''))
    raw_slot     = args.get('time_slot', '09:00')  # default for waitlist entries
    groomer_id   = _validate_groomer_id(args.get('groomer_id', ''))
    service_type = args.get('service_type', 'full').lower().strip()
    bather_id    = int(args.get('bather_id', 8))
    duration_min = int(args.get('duration_minutes', 90))
    is_waitlist  = bool(args.get('waitlist', False))

    # Validate service type
    if service_type not in ('full', 'bath_only', 'groom_only'):
        raise ValueError("service_type must be 'full', 'bath_only', or 'groom_only'.")

    # Validate bather
    if bather_id not in VALID_GROOMER_IDS:
        raise ValueError(f"Unknown bather_id {bather_id}.")

    # Parse and validate time slot
    slot = _parse_time_slot(raw_slot)
    if slot not in VALID_SLOTS:
        raise ValueError(
            f"'{slot}' is not a standard slot. Valid: {', '.join(sorted(VALID_SLOTS))}."
        )

    # Look up pet + client
    pet_rows = run_query(f"""
SELECT p.PtSeq, p.PtPetName, p.PtCat, c.CLSeq, c.CLFirstName, c.CLLastName
FROM Pets p
INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
WHERE p.PtSeq = {pet_id}
AND (p.PtDeleted IS NULL OR p.PtDeleted = 0)
""", timeout=10)
    if not pet_rows or not any('\t' in l for l in pet_rows):
        raise ValueError(f"Pet ID {pet_id} not found.")
    pr = cols(next(l for l in pet_rows if '\t' in l))
    pt_cat      = int(pr[2]) if pr[2].isdigit() else 0
    client_name = f"{pr[4]} {pr[5]}"
    pet_name    = pr[1]

    # Pricing: try last appointment first, fall back to standard table
    price_rows = run_query(f"""
SELECT TOP 1 GLRate, GLBathRate
FROM GroomingLog
WHERE GLPetID = {pet_id}
AND (GLDeleted IS NULL OR GLDeleted = 0)
AND (GLWaitlist IS NULL OR GLWaitlist = 0)
AND GLRate > 0
ORDER BY GLDate DESC
""", timeout=10)
    if price_rows and any('\t' in l for l in price_rows):
        pp = cols(next(l for l in price_rows if '\t' in l))
        try:
            gl_rate      = float(pp[0])
            gl_bath_rate = float(pp[1])
        except (ValueError, IndexError):
            gl_rate, gl_bath_rate = _PRICE_TABLE.get(pt_cat, (0, 0))
    else:
        gl_rate, gl_bath_rate = _PRICE_TABLE.get(pt_cat, (0, 0))

    # For bath-only, groom rate is 0
    if service_type == 'bath_only':
        gl_rate = 0.0

    slot_h, slot_m = int(slot[:2]), int(slot[3:])
    slot_start = slot_h * 60 + slot_m

    # Only check for slot conflicts on real appointments (not waitlist entries)
    if not is_waitlist:
        conflict_rows = run_query(f"""
SELECT p.PtPetName, CONVERT(varchar, gl.GLInTime, 108), CONVERT(varchar, gl.GLOutTime, 108)
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
WHERE gl.GLDate = '{date}'
AND (gl.GLGroomerID = {groomer_id} OR gl.GLBatherID = {groomer_id} OR gl.GLOthersID = {groomer_id})
AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist = 0)
""", timeout=10)

        slot_end = slot_start + duration_min
        for line in conflict_rows:
            if '\t' not in line:
                continue
            cf = cols(line)
            if len(cf) < 3:
                continue
            try:
                bh, bm = int(cf[1][:2]), int(cf[1][3:5])
                eh, em = int(cf[2][:2]), int(cf[2][3:5])
                b_start = bh * 60 + bm
                b_end   = eh * 60 + em
                if slot_start < b_end and slot_end > b_start:
                    raise ValueError(
                        f"Slot {slot} on {date} conflicts with existing appointment: "
                        f"{cf[0]} at {cf[1][:5]}–{cf[2][:5]}."
                    )
            except ValueError as e:
                if 'conflicts' in str(e):
                    raise

    # Build service flags
    if service_type == 'full':
        gl_bath, gl_groom, gl_others = -1, -1, 0
    elif service_type == 'bath_only':
        gl_bath, gl_groom, gl_others = -1, 0, 0
    else:  # groom_only
        gl_bath, gl_groom, gl_others = 0, -1, 0

    # Compute out time
    out_h   = (slot_start + duration_min) // 60
    out_m   = (slot_start + duration_min) % 60
    in_time  = f"1899-12-30 {slot_h:02d}:{slot_m:02d}:00"
    out_time = f"1899-12-30 {out_h:02d}:{out_m:02d}:00"

    gl_waitlist_val = -1 if is_waitlist else 0

    run_update(f"""
INSERT INTO GroomingLog
    (GLDate, GLInTime, GLOutTime, GLPetID, GLGroomerID, GLBatherID,
     GLBath, GLGroom, GLOthers, GLConfirmed, GLDeleted, GLWaitlist,
     GLTakenBy, GLRate, GLBathRate)
VALUES
    ('{date}', '{in_time}', '{out_time}', {pet_id}, {groomer_id}, {bather_id},
     {gl_bath}, {gl_groom}, {gl_others}, 0, 0, {gl_waitlist_val},
     'CLD', {gl_rate}, {gl_bath_rate})
""", timeout=15)

    # Get the new GLSeq
    seq_rows = run_query(f"""
SELECT TOP 1 GLSeq FROM GroomingLog
WHERE GLPetID = {pet_id} AND GLDate = '{date}'
AND GLTakenBy = 'CLD'
AND (GLDeleted IS NULL OR GLDeleted = 0)
ORDER BY GLSeq DESC
""", timeout=10)
    glseq = seq_rows[0].strip() if seq_rows else '?'

    def fmt_slot(s):
        h, m = int(s[:2]), int(s[3:])
        ampm = 'AM' if h < 12 else 'PM'
        return f"{h % 12 or 12}:{m:02d} {ampm}"

    svc_label = {'full': 'Full Service', 'bath_only': 'Bath Only', 'groom_only': 'Groom Only'}[service_type]
    groomer_names = {8: 'Elmer', 59: 'Kumi', 85: 'Tomoko', 91: 'Josh', 94: 'Noah', 95: 'Mandilyn', 97: 'Guest'}

    if is_waitlist:
        return (
            f"Waitlist entry created (GLSeq {glseq}):\n"
            f"  Pet:     {pet_name} (ID:{pet_id}) for {client_name}\n"
            f"  Target:  {date} (waitlist — not a firm booking)\n"
            f"  Service: {svc_label}\n"
            f"  Groomer: {groomer_names.get(groomer_id, str(groomer_id))}, "
            f"Bather: {groomer_names.get(bather_id, str(bather_id))}\n"
            f"  Pricing: ${gl_rate:.0f} + ${gl_bath_rate:.0f}\n"
            f"  Status:  On Waitlist (GLWaitlist=-1)"
        )
    return (
        f"Appointment created (GLSeq {glseq}):\n"
        f"  Pet:     {pet_name} (ID:{pet_id}) for {client_name}\n"
        f"  Date:    {date} at {fmt_slot(slot)}\n"
        f"  Service: {svc_label}\n"
        f"  Groomer: {groomer_names.get(groomer_id, str(groomer_id))}, "
        f"Bather: {groomer_names.get(bather_id, str(bather_id))}\n"
        f"  Pricing: ${gl_rate:.0f} + ${gl_bath_rate:.0f}\n"
        f"  Status:  Unconfirmed (confirmation comes via reminder text)"
    )


# ---------------------------------------------------------------------------
# Tool: reassign_bather
# ---------------------------------------------------------------------------

_EMPLOYEE_NAMES = {8: 'Elmer', 59: 'Kumi', 85: 'Tomoko', 91: 'Josh', 94: 'Noah', 95: 'Mandilyn', 97: 'Guest'}

def tool_reassign_bather(args: dict) -> str:
    """Reassign the bather on an existing appointment."""
    try:
        appt_id = int(args['appointment_id'])
    except (KeyError, ValueError):
        raise ValueError("appointment_id must be an integer (GLSeq).")
    new_bather_id = _validate_groomer_id(args.get('new_bather_id', ''))

    # Confirm appointment exists and fetch current details
    check_q = f"""
SELECT TOP 1
    gl.GLSeq,
    CONVERT(varchar, gl.GLDate, 23),
    p.PtPetName,
    c.CLLastName,
    ISNULL(e1.USFNAME, '(none)') as Groomer,
    ISNULL(e2.USFNAME, '(none)') as CurrentBather,
    gl.GLBatherID
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
LEFT JOIN Employees e1 ON gl.GLGroomerID = e1.USSEQN
LEFT JOIN Employees e2 ON gl.GLBatherID = e2.USSEQN
WHERE gl.GLSeq = {appt_id}
AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
"""
    rows = run_query(check_q)
    if not rows:
        raise ValueError(f"No active appointment found with GLSeq {appt_id}.")
    c = cols(rows[0])
    if len(c) < 7:
        raise ValueError(f"Unexpected query result for GLSeq {appt_id}.")

    appt_date    = c[1]
    pet_name     = c[2]
    client_name  = c[3]
    old_bather   = c[5]
    old_bather_id = c[6].strip()

    run_update(
        f"UPDATE GroomingLog SET GLBatherID = {new_bather_id} WHERE GLSeq = {appt_id}"
    )

    new_bather_name = _EMPLOYEE_NAMES.get(new_bather_id, str(new_bather_id))
    return (
        f"Bather reassigned for GLSeq {appt_id}:\n"
        f"  Pet:     {pet_name} ({client_name})\n"
        f"  Date:    {appt_date}\n"
        f"  Bather:  {old_bather} (ID:{old_bather_id}) → {new_bather_name} (ID:{new_bather_id})"
    )


# ---------------------------------------------------------------------------
# Knowledge base tool
# ---------------------------------------------------------------------------

_KB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'staff', 'KNOWLEDGE_BASE.md')

def tool_add_to_knowledge_base(args: dict) -> str:
    """Append an entry to staff/KNOWLEDGE_BASE.md."""
    category = args.get('category', 'General').strip()
    content  = args.get('content', '').strip()
    if not content:
        return 'Error: content is required.'

    os.makedirs(os.path.dirname(_KB_PATH), exist_ok=True)

    # Initialize file with header if it doesn't exist yet
    if not os.path.exists(_KB_PATH):
        with open(_KB_PATH, 'w', encoding='utf-8') as f:
            f.write('# DBFCM Staff Knowledge Base\n\n'
                    'Business rules, policies, and operational notes added by staff via Know-a-bot.\n\n')

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    entry = f'\n## [{timestamp}] {category}\n\n{content}\n'
    with open(_KB_PATH, 'a', encoding='utf-8') as f:
        f.write(entry)

    return (
        f'Added to knowledge base under "{category}" at {timestamp}.\n'
        f'Changes take effect after the next backend restart.'
    )


# ---------------------------------------------------------------------------
# SMS draft tool (outbound via backend queue)
# ---------------------------------------------------------------------------

def tool_draft_sms(args: dict) -> str:
    """Queue an SMS draft via the backend — for client messages or Noah escalations."""
    recipient = args.get('recipient', '').strip()
    message   = args.get('message', '').strip()
    context   = args.get('context', '').strip()

    if not recipient:
        raise ValueError("'recipient' parameter is required.")
    if not message:
        raise ValueError("'message' parameter is required.")

    payload = json.dumps({
        'recipient': recipient,
        'message':   message,
        'context':   context,
    }).encode('utf-8')

    last_error = None
    for base_url in _BACKEND_URLS:
        url = f"{base_url}/api/sms/draft-from-knowabot"
        req = urllib.request.Request(
            url, data=payload,
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            if data.get('success'):
                draft_id = data.get('draft_id', '?')
                if recipient.lower() == 'noah':
                    return (
                        f"Escalation drafted to Noah (draft_id={draft_id}). "
                        "It will appear in the SMS tab for staff to review and send. "
                        "When Noah replies by text, his answer will be automatically added to the knowledge base."
                    )
                else:
                    return (
                        f"SMS draft created for {recipient} (draft_id={draft_id}). "
                        "It will appear in the SMS tab for staff review before sending."
                    )
            else:
                return f"Error creating draft: {data.get('error', 'unknown error')}"
        except urllib.error.URLError as e:
            last_error = e
            continue
        except Exception as e:
            return f"Error: {e}"

    return f"Could not reach backend at any address ({', '.join(_BACKEND_URLS)}): {last_error}"


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

TOOLS = {
    "get_appointments": {
        "fn": tool_get_appointments,
        "description": "Get appointments for a date range, optionally filtered by groomer.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date_from": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD). Required."
                },
                "date_to": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD). Defaults to date_from if omitted."
                },
                "groomer_id": {
                    "type": "integer",
                    "description": "Optional: filter to one groomer. IDs: Tomoko=85, Kumi=59, Mandilyn=95, Elmer=8."
                }
            },
            "required": ["date_from"]
        }
    },
    "search_client_or_pet": {
        "fn": tool_search_client_or_pet,
        "description": "Search for a client or pet by name. Returns contact info, warnings, groom notes, tip history, and recent appointment history.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Last name, first name, or pet name to search (partial match)."
                }
            },
            "required": ["name"]
        }
    },
    "get_open_slots": {
        "fn": tool_get_open_slots,
        "description": "Find available booking slots on a given date for each groomer.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Target date (YYYY-MM-DD). Required."
                },
                "groomer_id": {
                    "type": "integer",
                    "description": "Optional: filter to one groomer."
                }
            },
            "required": ["date"]
        }
    },
    "get_waitlist": {
        "fn": tool_get_waitlist,
        "description": "Get active waitlist entries, optionally filtered by groomer.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "groomer_id": {
                    "type": "integer",
                    "description": "Optional: filter to one groomer's waitlist."
                }
            }
        }
    },
    "get_groomer_schedule": {
        "fn": tool_get_groomer_schedule,
        "description": "Get groomer working schedule and blocked days for a date range.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date_from": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD). Required."
                },
                "date_to": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD). Required."
                }
            },
            "required": ["date_from", "date_to"]
        }
    },
    "create_appointment": {
        "fn": tool_create_appointment,
        "description": (
            "Create a new appointment or waitlist entry in the database. "
            "For waitlist entries, set waitlist=true — no slot conflict check, date is a target date. "
            "ALWAYS confirm with the user before calling — show pet name, date, groomer, service type, "
            "and whether it is a real appointment or a waitlist entry. "
            "Never set appointments as confirmed; confirmation only comes via the reminder text system."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "pet_id": {
                    "type": "integer",
                    "description": "PtSeq of the pet being booked. Use search_client_or_pet to find it."
                },
                "date": {
                    "type": "string",
                    "description": "Appointment date or target date for waitlist entries (YYYY-MM-DD)."
                },
                "time_slot": {
                    "type": "string",
                    "description": "Start time. Standard slots: 08:30, 10:00, 11:30, 13:30, 14:30. Also accepts '10:00 AM' format. For waitlist entries defaults to 09:00."
                },
                "groomer_id": {
                    "type": "integer",
                    "description": "Groomer ID. Tomoko=85, Kumi=59, Mandilyn=95."
                },
                "service_type": {
                    "type": "string",
                    "enum": ["full", "bath_only", "groom_only"],
                    "description": "'full' (bath + groom), 'bath_only', or 'groom_only'. Default: 'full'."
                },
                "bather_id": {
                    "type": "integer",
                    "description": "Bather ID. Default: 8 (Elmer). Use 97 if Elmer is out."
                },
                "duration_minutes": {
                    "type": "integer",
                    "description": "Appointment block length in minutes. Default: 90."
                },
                "waitlist": {
                    "type": "boolean",
                    "description": "Set true to add as a waitlist entry (GLWaitlist=-1) instead of a firm booking. No slot conflict check. Date is a target date."
                }
            },
            "required": ["pet_id", "date", "groomer_id"]
        }
    },
    "reassign_bather": {
        "fn": tool_reassign_bather,
        "description": (
            "Reassign the bather on an existing appointment. "
            "Use get_appointments first to find the GLSeq. "
            "ALWAYS confirm with the user before calling — show the pet name, date, current bather, and new bather."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "appointment_id": {
                    "type": "integer",
                    "description": "GLSeq of the appointment to update."
                },
                "new_bather_id": {
                    "type": "integer",
                    "description": "Employee ID of the new bather. Elmer=8, Josh=91, Guest=97."
                }
            },
            "required": ["appointment_id", "new_bather_id"]
        }
    },
    "append_note": {
        "fn": tool_append_note,
        "description": (
            "Add a structured note to a client or pet record (writes to ClientNotes/PetNotes table). "
            "ALWAYS confirm with the user before calling — show exactly what will be written and where."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "entity_type": {
                    "type": "string",
                    "enum": ["client", "pet"],
                    "description": "'client' or 'pet'"
                },
                "entity_id": {
                    "type": "integer",
                    "description": "CLSeq for clients, PtSeq for pets."
                },
                "subject": {
                    "type": "string",
                    "description": "Short subject/title for the note (max 50 chars). Optional — auto-generated from text if omitted."
                },
                "text": {
                    "type": "string",
                    "description": "Note text (max 500 characters)."
                }
            },
            "required": ["entity_type", "entity_id", "text"]
        }
    },
    "add_to_knowledge_base": {
        "fn": tool_add_to_knowledge_base,
        "description": (
            "Add a business rule, policy, or operational note to the staff knowledge base. "
            "Use this when staff share information that should be remembered for future reference — "
            "e.g. 'we no longer do X', 'always charge Y for Z', 'client prefers ...'. "
            "ALWAYS confirm with the user before calling — show exactly what will be written."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Short category label, e.g. 'Services', 'Pricing', 'Client Notes', 'Scheduling'."
                },
                "content": {
                    "type": "string",
                    "description": "The knowledge to record. Be clear and specific."
                }
            },
            "required": ["category", "content"]
        }
    },
    "draft_sms": {
        "fn": tool_draft_sms,
        "description": (
            "Draft an SMS to a client (by name) or to Noah (the owner). "
            "Drafts appear in the SMS tab for staff review before sending. "
            "Use when you want to message a client, or to escalate an unanswered question to Noah. "
            "ALWAYS confirm with the user before calling — tell them exactly who will receive the SMS and what it says."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "recipient": {
                    "type": "string",
                    "description": "Who to text: 'Noah' for the owner, or a client full name (e.g. 'Jane Smith')."
                },
                "message": {
                    "type": "string",
                    "description": "The text message body."
                },
                "context": {
                    "type": "string",
                    "description": "Optional: the original employee question or context (for escalations — helps knowledge base matching when Noah replies)."
                }
            },
            "required": ["recipient", "message"]
        }
    }
}

# ---------------------------------------------------------------------------
# MCP JSON-RPC 2.0 protocol handler
# ---------------------------------------------------------------------------

def make_response(req_id, result):
    return {"jsonrpc": "2.0", "id": req_id, "result": result}

def make_error(req_id, code, message):
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}

def dispatch(req: dict):
    method = req.get("method", "")
    req_id = req.get("id")
    params = req.get("params", {})

    if method == "initialize":
        return make_response(req_id, {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "kennel-db", "version": "1.0.0"}
        })

    if method == "notifications/initialized":
        return None  # no response for notifications

    if method == "tools/list":
        tools_list = []
        for name, info in TOOLS.items():
            tools_list.append({
                "name": name,
                "description": info["description"],
                "inputSchema": info["inputSchema"]
            })
        return make_response(req_id, {"tools": tools_list})

    if method == "tools/call":
        name = params.get("name", "")
        arguments = params.get("arguments", {})

        if name not in TOOLS:
            return make_response(req_id, {
                "content": [{"type": "text", "text": f"Unknown tool: {name}"}],
                "isError": True
            })

        sys.stderr.write(f"[kennel-db] tools/call {name}\n")
        sys.stderr.flush()

        try:
            result_text = TOOLS[name]["fn"](arguments)
            return make_response(req_id, {
                "content": [{"type": "text", "text": result_text}]
            })
        except (ValueError, TypeError) as e:
            return make_response(req_id, {
                "content": [{"type": "text", "text": f"Validation error: {e}"}],
                "isError": True
            })
        except subprocess.TimeoutExpired:
            return make_response(req_id, {
                "content": [{"type": "text", "text": "Database query timed out. Please try again."}],
                "isError": True
            })
        except RuntimeError as e:
            return make_response(req_id, {
                "content": [{"type": "text", "text": f"Database error: {e}"}],
                "isError": True
            })
        except Exception as e:
            sys.stderr.write(f"[kennel-db] Unexpected error in {name}: {e}\n")
            sys.stderr.flush()
            return make_response(req_id, {
                "content": [{"type": "text", "text": f"Unexpected error: {e}"}],
                "isError": True
            })

    if method.startswith("notifications/"):
        return None  # ignore all notifications

    return make_error(req_id, -32601, f"Method not found: {method}")


def main():
    sys.stderr.write("[kennel-db] MCP server started\n")
    sys.stderr.flush()
    # Use readline() instead of 'for line in sys.stdin' to avoid block-buffering
    # on pipes — small JSON messages would otherwise sit in the buffer indefinitely.
    while True:
        line = sys.stdin.readline()
        if not line:
            break  # EOF — parent process closed the pipe
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError as e:
            resp = make_error(None, -32700, f"Parse error: {e}")
            print(json.dumps(resp), flush=True)
            continue

        resp = dispatch(req)
        if resp is not None:
            print(json.dumps(resp), flush=True)


if __name__ == "__main__":
    main()
