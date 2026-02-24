#!/usr/bin/env python3
import subprocess

cmd = ['sqlcmd', '-S', 'desktop-bikigbr,2721', '-d', 'wkennel7', '-U', 'noah', '-P', 'noah',
       '-Q', "SELECT TOP 2 gl.GLSeq, p.PtPetName, c.CLLastName, ISNULL(c.CLWarning, '') as CW FROM GroomingLog gl INNER JOIN Pets p ON gl.GLPetID = p.PtSeq INNER JOIN Clients c ON p.PtOwnerCode = c.CLSeq WHERE gl.GLSeq IN (153944, 154096)",
       '-s', '\t', '-W', '-h', '-1']

result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
lines = result.stdout.strip().split('\n')

print(f"Total lines: {len(lines)}\n")
for i, line in enumerate(lines):
    if '\t' in line:
        parts = line.split('\t')
        print(f"Line {i}: {len(parts)} columns")
        if len(parts) >= 4:
            print(f"  GLSeq: {parts[0]}")
            print(f"  Pet: {parts[1]}")
            print(f"  Client: {parts[2]}")
            print(f"  Warning: '{parts[3][:50] if parts[3] else 'EMPTY'}'...")
        print()
