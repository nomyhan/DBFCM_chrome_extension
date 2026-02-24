# Know-a-bot Setup Instructions for Claude

This file is a prompt for Claude Code (the AI assistant). If you're setting up the Kennel
Connection backend on a new Windows machine, open a Claude Code session in this folder and
say: "Read CLAUDE_SETUP.md and follow the instructions."

---

## What you are doing

The Kennel Connection Chrome extension uses a local backend server (`backend_server.py`) that
runs on Windows. It connects to SQL Server and also spawns a Claude CLI process inside WSL to
power the Know-a-bot chat tab. Your job is to make sure all the dependencies are in place in
this machine's WSL and that the backend starts cleanly.

The extension folder (where this file lives) is on OneDrive and already contains everything
the backend needs — you do not need to clone any Git repo.

---

## Step 1 — Check WSL

Run this in a Windows terminal (PowerShell or cmd):

    wsl echo "WSL OK"

If you get an error, WSL is not installed. Install it:

    wsl --install

Then restart and come back. Otherwise continue.

---

## Step 2 — Check python3 in WSL

    wsl python3 --version

If missing:

    wsl sudo apt-get update && wsl sudo apt-get install -y python3

---

## Step 3 — Install sqlcmd in WSL

The backend scripts call sqlcmd inside WSL to talk to SQL Server. They check these paths in
order: `~/sqlcmd`, `/opt/mssql-tools18/bin/sqlcmd`, `/opt/mssql-tools/bin/sqlcmd`,
`/usr/bin/sqlcmd`.

First, check if sqlcmd is already present:

    wsl bash -c "ls ~/sqlcmd 2>/dev/null || which sqlcmd 2>/dev/null || echo NOT_FOUND"

If NOT_FOUND, install the standalone sqlcmd binary (simplest approach — no sudo needed):

    wsl bash -c "curl -L -o ~/sqlcmd https://github.com/microsoft/go-sqlcmd/releases/latest/download/sqlcmd-linux-amd64 && chmod +x ~/sqlcmd"

Verify it works and can reach SQL Server.

IMPORTANT: On desktop-bikigbr, WSL's hostname resolves to 127.0.1.1 (its own loopback), not
the Windows host. You must connect via the Windows host IP from the default gateway instead.
The scripts do this automatically, but to test manually:

    wsl bash -c "WIN_IP=\$(ip route show default | awk '{print \$3; exit}') && ~/sqlcmd -S \"\$WIN_IP,2721\" -d wkennel7 -U noah -P noah -N disable -Q \"SELECT 'connected' as status\" -W -h -1"

You should see `connected`. The `-N disable` flag is required — SQL Server 2014 on this path
has SSL/TLS negotiation issues without it.

If the test fails, check that SQL Server is running (SQL Server Configuration Manager →
SQL Server Services) and that TCP/IP on port 2721 is enabled.

---

## Step 4 — Install Claude CLI in WSL

The backend invokes the Claude CLI inside WSL to run Know-a-bot conversations.

Check if it's already installed:

    wsl bash -c "ls ~/.local/bin/claude 2>/dev/null || which claude 2>/dev/null || echo NOT_FOUND"

If NOT_FOUND, install it:

    wsl bash -c "curl -fsSL https://claude.ai/install.sh | sh"

After install, verify:

    wsl ~/.local/bin/claude --version

Note the full path where claude was installed (usually `~/.local/bin/claude`).

---

## Step 5 — Update WSL_CLAUDE_PATH in backend_server.py if needed

Open `backend_server.py` (in this folder) and find this line near the top:

    WSL_CLAUDE_PATH = '/home/noah/.local/bin/claude'

To get the actual path on this machine:

    wsl bash -c "which claude || ls ~/.local/bin/claude"

If the path shown is different from `/home/noah/.local/bin/claude` (for example, if the WSL
username is not `noah`), update `WSL_CLAUDE_PATH` in `backend_server.py` to match.

---

## Step 6 — Start the backend and verify

Start the backend server from PowerShell in this folder:

    python backend_server.py

Watch the startup output. You should see lines like:

    [Know-a-bot] MCP config → ...\waitlist_extension\noahbot_mcp_config.json
    [Know-a-bot] MCP server → /mnt/c/.../waitlist_extension/noahbot_mcp_server.py
    [Know-a-bot] Staff docs → ...\waitlist_extension\staff
    [Know-a-bot] System prompt built (N chars) → /mnt/c/.../noahbot_system.txt
    [DBFCMClientStats] Querying tip data...
    Server running on: http://localhost:8000

If staff docs show as missing ("not found, skipping"), the `staff/` subfolder inside this
extension folder may not have synced from OneDrive yet. Wait for OneDrive to finish syncing
and restart the backend.

If DBFCMClientStats shows an error, verify Step 3 (sqlcmd connectivity).

---

## Step 7 — Load the Chrome extension

1. Open Chrome → chrome://extensions
2. Enable Developer Mode
3. Click "Load unpacked" and select this folder (waitlist_extension)
4. Click the extension icon → Settings → set Backend Server URL to `http://localhost:8000`

---

## Troubleshooting

**"python3: can't open file ... refresh_client_stats.py"**
The script path is being computed wrong. Confirm `backend_server.py` is running from its
actual file location (not a copied/moved path). `_get_ext_dir()` uses `__file__` to find
the extension folder.

**"sqlcmd: command not found" in backend logs**
Step 3 wasn't completed. Install sqlcmd and run the gateway-IP test in Step 3 to confirm
connectivity.

**"claude CLI not found" error in Know-a-bot tab**
Step 4 wasn't completed, or `WSL_CLAUDE_PATH` in `backend_server.py` still points to the
wrong path. Update it per Step 5.

**Know-a-bot chat returns empty responses**
The Claude CLI needs an active API key. Inside WSL, run `claude` once interactively to
complete the auth flow, or set the `ANTHROPIC_API_KEY` environment variable.
