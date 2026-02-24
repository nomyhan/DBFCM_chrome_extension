#!/usr/bin/env python3
"""Test script to verify warnings are being parsed correctly"""

import subprocess

SQL_SERVER = "desktop-bikigbr,2721"
SQL_DATABASE = "wkennel7"
SQL_USER = "noah"
SQL_PASSWORD = "noah"

query = """
SELECT
    gl.GLSeq,
    CONVERT(varchar, gl.GLDate, 23) as ApptDate,
    CONVERT(varchar, gl.GLDateEntered, 23) as WLDate,
    CONVERT(varchar, gl.GLInTime, 108) as Time,
    p.PtPetName,
    c.CLLastName,
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
    ISNULL(gl.GLDescription, '') as Notes,
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
    (SELECT TOP 1 ISNULL(past.GLDescription, '')
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
     AND future.GLDate >= GETDATE()
     ORDER BY future.GLDate ASC) as NextScheduledDate
FROM GroomingLog gl
INNER JOIN Pets p ON gl.GLPetID = p.PtSeq
INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq
LEFT JOIN Employees e1 ON gl.GLGroomerID = e1.USSEQN
LEFT JOIN Employees e3 ON gl.GLOthersID = e3.USSEQN
WHERE gl.GLWaitlist = -1
AND (gl.GLDeleted IS NULL OR gl.GLDeleted = 0)
AND gl.GLSeq IN (153944, 154096, 154124, 154126, 154313)
ORDER BY gl.GLSeq
"""

cmd = [
    'sqlcmd',
    '-S', SQL_SERVER,
    '-d', SQL_DATABASE,
    '-U', SQL_USER,
    '-P', SQL_PASSWORD,
    '-Q', query,
    '-s', '|',
    '-W',
    '-h', '-1'
]

result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

if result.returncode != 0:
    print(f"ERROR: {result.stderr}")
else:
    lines = result.stdout.strip().split('\n')
    print(f"Total lines: {len(lines)}\n")

    for i, line in enumerate(lines):
        if '|' in line and not line.startswith('--'):
            row = line.split('|')
            print(f"\nRow {i} - Column count: {len(row)}")
            print(f"  GLSeq: {row[0]}")
            print(f"  Pet: {row[4]} - {row[5]}")
            print(f"  ClientWarning: '{row[10][:50] if row[10] else 'EMPTY'}'")
            print(f"  PetWarning: '{row[11][:50] if row[11] else 'EMPTY'}'")
            print(f"  GroomWarning: '{row[12][:50] if row[12] else 'EMPTY'}'")
            print(f"  LastCompleted: {row[13] if len(row) > 13 else 'N/A'}")
            print(f"  NextScheduled: {row[15] if len(row) > 15 else 'N/A'}")
