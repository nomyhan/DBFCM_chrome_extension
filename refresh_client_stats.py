#!/usr/bin/env python3
"""
refresh_client_stats.py — Pre-compute client tip + cadence stats.

Writes to: SQL Server wkennel7 → DBFCMClientStats table
  (DBFCM = Dog's Best Friend and the Cat's Meow — our addition, not part of KC schema)

Called:
  - On backend startup (runs in background thread, ~10-20s)
  - Via GET /api/refresh-client-stats (manual refresh)
  - Standalone: python3 refresh_client_stats.py
"""

import sys
from datetime import datetime, date
from collections import defaultdict

from db_utils import run_query, run_update, cols, SQL_DATABASE

TABLE       = "DBFCMClientStats"


def _sql_val(v) -> str:
    """Format a Python value as a SQL literal."""
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, float):
        return f"{v:.6f}"
    return str(int(v))


def _ensure_table() -> None:
    """Create DBFCMClientStats if it doesn't exist. Safe to call repeatedly."""
    run_update(f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '{TABLE}')
CREATE TABLE {TABLE} (
    -- DBFCM addition: pre-computed client tip and visit cadence stats for Noah-bot.
    -- This table is NOT part of the Kennel Connection schema and is not read
    -- or written by the KC application. Safe to truncate/rebuild at any time.
    ClientID          INT          NOT NULL PRIMARY KEY,
    ClientName        VARCHAR(120) NULL,
    LastUpdated       VARCHAR(20)  NULL,

    -- Tip behavior (card tips only; cash tips excluded as unreliable)
    LastTipAmount     FLOAT        NULL,
    LastTipPct        FLOAT        NULL,   -- last tip as % of service amount
    LastTipDate       VARCHAR(10)  NULL,
    AvgTipAmount      FLOAT        NULL,
    AvgTipPct         FLOAT        NULL,
    TipReceiptCount   INT          NULL,   -- number of card receipts on file
    PreferredPayment  VARCHAR(10)  NULL,   -- 'Card', 'Cash', or 'Mixed'
    TipMethod         VARCHAR(50)  NULL,   -- human-readable summary
    CardTipRate       FLOAT        NULL,   -- fraction of card visits with tip (0.0-1.0)

    -- Appointment cadence (last 24 months)
    LastApptDate      VARCHAR(10)  NULL,
    AvgCadenceDays    FLOAT        NULL,   -- median gap between visits
    PreferredDay      VARCHAR(12)  NULL,   -- e.g. 'Saturday'
    PreferredTime     VARCHAR(10)  NULL,   -- e.g. '10:00 AM'
    ApptCount12Mo     INT          NULL
)
""", timeout=30)


def refresh(verbose: bool = True) -> int:
    """
    Full refresh of DBFCMClientStats. Returns number of rows written.
    Safe to call repeatedly — truncates and reloads each time.
    """
    t0 = datetime.now()

    _ensure_table()

    # ------------------------------------------------------------------
    # QUERY 1 — Tips + payment behavior
    # RPAMOUNT = service amount only (excludes tip).
    # RPTip = card tip. RPPAYTYPE: 'Cash'/'Check' = cash; others = card.
    # ------------------------------------------------------------------
    tips_query = """
SELECT
    r.RPCLIENTID,
    CONVERT(varchar, r.RPDATE, 23) as ReceiptDate,
    r.RPAMOUNT,
    r.RPTip,
    r.RPPAYTYPE
FROM Receipts r
WHERE r.RPCLIENTID > 0
AND r.RPAMOUNT > 0
AND (r.RPDeleted IS NULL OR r.RPDeleted = 0)
ORDER BY r.RPCLIENTID, r.RPDATE DESC
"""
    if verbose:
        print(f"[{TABLE}] Querying tip data...", flush=True)
    tip_lines = run_query(tips_query, timeout=60)

    client_receipts: dict[str, list[dict]] = defaultdict(list)
    for line in tip_lines:
        if "\t" not in line:
            continue
        c = cols(line)
        if len(c) < 5:
            continue
        cid      = c[0]
        rdate    = c[1]
        try:
            amount = float(c[2]) if c[2] else 0.0
            tip    = float(c[3]) if c[3] else 0.0
        except ValueError:
            continue
        pay_type = c[4].upper() if c[4] else ""
        is_card  = pay_type not in ("CASH", "CHECK", "")
        client_receipts[cid].append({
            "date": rdate, "amount": amount, "tip": tip, "is_card": is_card,
        })

    tip_stats: dict[str, dict] = {}
    for cid, receipts in client_receipts.items():
        card_visits = [r for r in receipts if r["is_card"]]
        cash_visits = [r for r in receipts if not r["is_card"]]
        card_count  = len(card_visits)
        cash_count  = len(cash_visits)
        total       = len(receipts)

        if card_count > cash_count:
            preferred_payment = "Card"
        elif cash_count > card_count:
            preferred_payment = "Cash"
        elif total > 0:
            preferred_payment = "Mixed"
        else:
            preferred_payment = None

        card_with_tip = [r for r in card_visits if r["tip"] > 0]
        card_tip_rate = len(card_with_tip) / card_count if card_count > 0 else 0.0

        if card_count == 0 and cash_count > 0:
            tip_method = "Cash payer (tip not tracked)"
        elif card_tip_rate >= 0.60:
            tip_method = "Usually tips on card"
        elif card_tip_rate >= 0.20:
            tip_method = "Occasionally tips"
        elif card_count > 0:
            tip_method = "Rarely tips"
        else:
            tip_method = "No card receipts"

        last_tip_amount = last_tip_pct = last_tip_date = None
        avg_tip_amount  = avg_tip_pct  = None

        tipped = sorted(card_with_tip, key=lambda r: r["date"], reverse=True)
        if tipped:
            last = tipped[0]
            last_tip_amount = last["tip"]
            last_tip_pct    = (last["tip"] / last["amount"] * 100) if last["amount"] > 0 else 0.0
            last_tip_date   = last["date"]
            avg_tip_amount  = sum(r["tip"] for r in tipped) / len(tipped)
            avg_tip_pct     = sum(
                (r["tip"] / r["amount"] * 100) for r in tipped if r["amount"] > 0
            ) / len(tipped)

        tip_stats[cid] = {
            "last_tip_amount":   last_tip_amount,
            "last_tip_pct":      last_tip_pct,
            "last_tip_date":     last_tip_date,
            "avg_tip_amount":    avg_tip_amount,
            "avg_tip_pct":       avg_tip_pct,
            "tip_receipt_count": card_count,
            "preferred_payment": preferred_payment,
            "tip_method":        tip_method,
            "card_tip_rate":     card_tip_rate,
        }

    if verbose:
        print(f"[{TABLE}] Tips processed for {len(tip_stats)} clients.", flush=True)

    # ------------------------------------------------------------------
    # QUERY 2 — Appointment cadence (last 24 months)
    # ------------------------------------------------------------------
    today   = date.today()
    cutoff  = today.replace(year=today.year - 2).strftime("%Y-%m-%d")
    today_s = today.strftime("%Y-%m-%d")

    cadence_query = f"""
SELECT
    c.CLSeq,
    CONVERT(varchar, gl.GLDate, 23) as ApptDate,
    DATEPART(dw, gl.GLDate) as DayOfWeek,
    CONVERT(varchar, gl.GLInTime, 108) as InTime
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
WHERE gl.GLDate BETWEEN '{cutoff}' AND '{today_s}'
AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
AND (gl.GLWaitlist IS NULL OR gl.GLWaitlist = 0)
AND gl.GLPetID <> 12120
ORDER BY c.CLSeq, gl.GLDate
"""
    if verbose:
        print(f"[{TABLE}] Querying cadence data...", flush=True)
    cadence_lines = run_query(cadence_query, timeout=60)

    # SQL DATEPART(dw): 1=Sun, 2=Mon, ..., 7=Sat
    dw_names = {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
                5: "Thursday", 6: "Friday", 7: "Saturday"}

    def fmt_time_12h(t: str) -> str:
        try:
            h, m = int(t[:2]), int(t[3:5])
            ampm = "AM" if h < 12 else "PM"
            return f"{h % 12 or 12}:{m:02d} {ampm}"
        except Exception:
            return t

    client_appts: dict[str, list] = defaultdict(list)
    for line in cadence_lines:
        if "\t" not in line:
            continue
        c = cols(line)
        if len(c) < 4:
            continue
        cid    = c[0]
        adate  = c[1]
        try:
            dow = int(c[2])
        except ValueError:
            dow = 0
        intime = c[3][:5] if c[3] else ""
        client_appts[cid].append((adate, dow, intime))

    STANDARD_MINS = [8*60+30, 10*60, 11*60+30, 13*60+30, 14*60+30]
    one_year_ago  = today.replace(year=today.year - 1).strftime("%Y-%m-%d")

    cadence_stats: dict[str, dict] = {}
    for cid, appts in client_appts.items():
        seen_dates: dict[str, tuple] = {}
        for adate, dow, intime in appts:
            if adate not in seen_dates:
                seen_dates[adate] = (dow, intime)
        unique_dates = sorted(seen_dates.keys())

        last_appt = unique_dates[-1] if unique_dates else None

        gaps = []
        for i in range(1, len(unique_dates)):
            d1 = datetime.strptime(unique_dates[i-1], "%Y-%m-%d").date()
            d2 = datetime.strptime(unique_dates[i],   "%Y-%m-%d").date()
            gap = (d2 - d1).days
            if 7 <= gap <= 365:
                gaps.append(gap)

        avg_cadence = None
        if gaps:
            sg  = sorted(gaps)
            mid = len(sg) // 2
            avg_cadence = sg[mid] if len(sg) % 2 == 1 else (sg[mid-1] + sg[mid]) / 2

        dow_counts: dict[int, int] = defaultdict(int)
        for adate in unique_dates:
            dow_counts[seen_dates[adate][0]] += 1
        preferred_dow = max(dow_counts, key=dow_counts.get) if dow_counts else None
        preferred_day = dw_names.get(preferred_dow) if preferred_dow else None

        time_counts: dict[str, int] = defaultdict(int)
        for adate in unique_dates:
            intime = seen_dates[adate][1]
            if intime and len(intime) >= 5:
                try:
                    m = int(intime[:2]) * 60 + int(intime[3:5])
                    closest = min(STANDARD_MINS, key=lambda x: abs(x - m))
                    h, mn = closest // 60, closest % 60
                    time_counts[fmt_time_12h(f"{h:02d}:{mn:02d}")] += 1
                except Exception:
                    pass
        preferred_time = max(time_counts, key=time_counts.get) if time_counts else None

        cadence_stats[cid] = {
            "last_appt_date":   last_appt,
            "avg_cadence_days": avg_cadence,
            "preferred_day":    preferred_day,
            "preferred_time":   preferred_time,
            "appt_count_12mo":  sum(1 for d in unique_dates if d >= one_year_ago),
        }

    if verbose:
        print(f"[{TABLE}] Cadence processed for {len(cadence_stats)} clients.", flush=True)

    # ------------------------------------------------------------------
    # Fetch client names
    # ------------------------------------------------------------------
    name_lines = run_query("""
SELECT CLSeq, CLFirstName + ' ' + CLLastName
FROM Clients
WHERE (CLDeleted IS NULL OR CLDeleted = 0)
""", timeout=30)
    client_names: dict[str, str] = {}
    for line in name_lines:
        if "\t" not in line:
            continue
        c = cols(line)
        if len(c) >= 2:
            client_names[c[0]] = c[1]

    # ------------------------------------------------------------------
    # Write to SQL Server — TRUNCATE + batch INSERT (500 rows/batch)
    # ------------------------------------------------------------------
    all_cids = set(tip_stats.keys()) | set(cadence_stats.keys()) | set(client_names.keys())
    now_str  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for cid in all_cids:
        try:
            cid_int = int(cid)
        except ValueError:
            continue
        ts = tip_stats.get(cid, {})
        cs = cadence_stats.get(cid, {})
        rows.append((
            cid_int,
            client_names.get(cid, ""),
            now_str,
            ts.get("last_tip_amount"),
            ts.get("last_tip_pct"),
            ts.get("last_tip_date"),
            ts.get("avg_tip_amount"),
            ts.get("avg_tip_pct"),
            ts.get("tip_receipt_count"),
            ts.get("preferred_payment"),
            ts.get("tip_method"),
            ts.get("card_tip_rate"),
            cs.get("last_appt_date"),
            cs.get("avg_cadence_days"),
            cs.get("preferred_day"),
            cs.get("preferred_time"),
            cs.get("appt_count_12mo"),
        ))

    if verbose:
        print(f"[{TABLE}] Writing {len(rows)} rows to SQL Server...", flush=True)

    run_update(f"TRUNCATE TABLE {TABLE}", timeout=15)

    BATCH = 500
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i+BATCH]
        values = ",\n".join(
            f"({_sql_val(r[0])},{_sql_val(r[1])},{_sql_val(r[2])},"
            f"{_sql_val(r[3])},{_sql_val(r[4])},{_sql_val(r[5])},"
            f"{_sql_val(r[6])},{_sql_val(r[7])},{_sql_val(r[8])},"
            f"{_sql_val(r[9])},{_sql_val(r[10])},{_sql_val(r[11])},"
            f"{_sql_val(r[12])},{_sql_val(r[13])},{_sql_val(r[14])},"
            f"{_sql_val(r[15])},{_sql_val(r[16])})"
            for r in batch
        )
        run_update(f"""
INSERT INTO {TABLE} (
    ClientID, ClientName, LastUpdated,
    LastTipAmount, LastTipPct, LastTipDate,
    AvgTipAmount, AvgTipPct, TipReceiptCount,
    PreferredPayment, TipMethod, CardTipRate,
    LastApptDate, AvgCadenceDays, PreferredDay,
    PreferredTime, ApptCount12Mo
) VALUES
{values}
""", timeout=30)
        if verbose:
            print(f"[{TABLE}]   ...wrote rows {i+1}–{min(i+BATCH, len(rows))}", flush=True)

    elapsed = (datetime.now() - t0).total_seconds()
    if verbose:
        print(f"[{TABLE}] Done — {len(rows)} clients refreshed in {elapsed:.1f}s.", flush=True)
    return len(rows)


if __name__ == "__main__":
    n = refresh(verbose=True)
    print(f"Done. {n} rows written to {TABLE} in {SQL_DATABASE}.")
