# WKennel7 Waitlist Viewer - Chrome/Edge Extension

A browser extension for quick access to WKennel7 waitlist appointments with direct LiveAccess links.

## Features

✅ **One-Click Access** - Click extension icon to view entire waitlist
✅ **Direct LiveAccess Links** - Click any appointment to open in LiveAccess
✅ **Live Data** - Fetches real-time data from SQL Server
✅ **Badge Counter** - Shows waitlist count on extension icon
✅ **ASAP Highlighting** - Urgent requests clearly marked
✅ **Color-Coded Services** - Easy visual identification
✅ **Responsive Design** - Works on any screen size
✅ **Portable** - Easy to install on multiple computers

---

## Quick Start

### Step 1: Install the Extension

**Chrome/Edge:**
1. Open browser and go to `chrome://extensions/` (or `edge://extensions/`)
2. Enable **Developer mode** (toggle in top-right corner)
3. Click **Load unpacked**
4. Select the `waitlist_extension` folder
5. Extension should now appear in your toolbar!

### Step 2: Start the Backend Server

The extension needs a local server to fetch data from SQL Server.

**On Windows (recommended):**
```cmd
cd C:\path\to\waitlist_extension
python backend_server.py
```

**On Linux/WSL:**
```bash
cd /home/noah/wkennel7/waitlist_extension
python3 backend_server.py
```

You should see:
```
========================================================
WKennel7 Waitlist Backend Server
========================================================
Server running on: http://localhost:8000
API endpoint: http://localhost:8000/api/waitlist
```

**Keep this window open!** The server needs to run for the extension to work.

### Step 3: Configure (First Time Only)

1. Click the extension icon in your browser toolbar
2. Click the ⚙️ (settings) icon
3. Set **Backend Server URL**: `http://localhost:8000/api/waitlist`
4. Set **LiveAccess URL**: `https://dbfcm.mykcapp.com/#/grooming/appointment/`
5. Click **Save**

### Step 4: Use It!

- Click the extension icon to view waitlist
- Click any appointment to open in LiveAccess
- Click 🔄 to refresh data
- Extension badge shows current waitlist count

---

## Installation Guide (Detailed)

### Requirements

- **Browser**: Chrome, Edge, Brave, or any Chromium-based browser
- **Python 3.x**: For running the backend server
- **sqlcmd**: SQL Server Command Line Tools
- **SQL Server Access**: Connection to WKennel7 database

### Files Included

```
waitlist_extension/
├── manifest.json          - Extension configuration
├── popup.html            - Extension popup interface
├── popup.css             - Styling
├── popup.js              - Frontend logic
├── background.js         - Background tasks (badge updates)
├── backend_server.py     - Python server for SQL data
├── icons/
│   ├── icon16.png       - Small icon
│   ├── icon48.png       - Medium icon
│   └── icon128.png      - Large icon
└── README.md            - This file
```

### Configuring SQL Server Connection

Edit `backend_server.py` (lines 9-12):

**For Remote SQL Server (default):**
```python
SQL_SERVER = "desktop-bikigbr,2721"
SQL_DATABASE = "wkennel7"
SQL_USER = "noah"
SQL_PASSWORD = "noah"
```

**For Local Windows SQL Server:**
```python
SQL_SERVER = ".\\WKENNEL"
SQL_DATABASE = "wkennel7"
# Comment out SQL_USER and SQL_PASSWORD
# Add '-E' flag in cmd list for Windows Authentication
```

Update lines 78-84 in `backend_server.py`:
```python
cmd = [
    'sqlcmd',
    '-S', SQL_SERVER,
    '-d', SQL_DATABASE,
    '-E',  # Windows Authentication
    '-Q', query,
    # ... rest of command
]
```

---

## Usage

### Opening the Waitlist

**Method 1:** Click extension icon in toolbar
**Method 2:** Use keyboard shortcut (if configured)
**Method 3:** Right-click extension icon → Click extension name

### Viewing Appointment Details

Each waitlist item shows (ordered by GLSeq priority):
- **GLSeq** - Appointment ID (clickable)
- **Pet Name & Client** - Who the appointment is for
- **Service Type** - Full Service, Handstrip, Bath, or Groom
- **Phone** - Contact number
- **Groomer** - Preferred groomer (if specified)
- **Notes** - Special requests, ASAP indicators
- **Added Date** - When added to waitlist

### Opening in LiveAccess

Click anywhere on an appointment card to open that appointment in LiveAccess in a new tab.

### Refreshing Data

Click the 🔄 button in the header to fetch latest data from SQL Server.

The extension also auto-updates the badge count every 5 minutes.

---

## Running Backend Server Automatically

### Windows - Start with Computer

Create a file `start_backend.bat`:

```batch
@echo off
cd /d "C:\path\to\waitlist_extension"
start "WKennel7 Backend" python backend_server.py
```

**Option 1: Task Scheduler**
1. Open Task Scheduler
2. Create Basic Task
3. Trigger: At log on
4. Action: Start a program → Select `start_backend.bat`

**Option 2: Startup Folder**
1. Press `Win + R`
2. Type: `shell:startup`
3. Copy `start_backend.bat` to this folder

### Linux/Mac - Run in Background

Create `start_backend.sh`:

```bash
#!/bin/bash
cd /home/noah/wkennel7/waitlist_extension
nohup python3 backend_server.py > backend.log 2>&1 &
echo $! > backend.pid
echo "Backend server started (PID: $(cat backend.pid))"
```

Make executable: `chmod +x start_backend.sh`

To stop: `kill $(cat backend.pid)`

---

## Porting to Another Computer

### Complete Setup (3 minutes)

1. **Copy Files**
   - Copy entire `waitlist_extension` folder
   - Can go anywhere (Desktop, Documents, etc.)

2. **Install Requirements**
   - Python 3: [python.org/downloads](https://python.org/downloads)
   - sqlcmd: [aka.ms/sqlcmd](https://aka.ms/sqlcmd)

3. **Configure SQL Connection**
   - Edit `backend_server.py` (lines 9-12)
   - Set correct server, database, credentials

4. **Install Extension**
   - Open `chrome://extensions/`
   - Enable Developer mode
   - Load unpacked → Select folder

5. **Start Backend**
   - Run `python backend_server.py`

6. **Configure Extension**
   - Click extension icon
   - Click ⚙️ settings
   - Set backend URL (usually `http://localhost:8000/api/waitlist`)
   - Set LiveAccess URL
   - Save

Done! Extension is now working on new computer.

---

## Troubleshooting

### Extension shows "Error Loading Waitlist"

**Check:**
1. Is backend server running?
2. Is backend URL correct in settings?
3. Can you access `http://localhost:8000/api/waitlist` in browser?

**Solution:**
- Start backend server: `python backend_server.py`
- Check settings (⚙️) for correct URL
- Check console for errors (F12)

### Backend server won't start

**Error: "sqlcmd not found"**
- Install SQL Server Command Line Tools
- Download: [aka.ms/sqlcmd](https://aka.ms/sqlcmd)

**Error: "Login failed"**
- Check SQL_SERVER, SQL_USER, SQL_PASSWORD in `backend_server.py`
- Test connection: `sqlcmd -S [server] -U [user] -P [password]`

**Error: "Address already in use"**
- Another program is using port 8000
- Use different port: `python backend_server.py 8001`
- Update extension settings to `http://localhost:8001/api/waitlist`

### LiveAccess links don't work

- Check LiveAccess URL in settings (⚙️)
- Verify format: `https://dbfcm.mykcapp.com/#/grooming/appointment/`
- Make sure you have internet access to reach the cloud application

### Badge count not showing

- Badge updates every 5 minutes automatically
- Click 🔄 to force update
- Check browser permissions for the extension

### Data looks old

- Click 🔄 refresh button
- Check "Last Updated" timestamp
- Verify backend server is responding
- Check backend server console for errors

---

## Advanced Configuration

### Custom Port

Run backend on different port:
```bash
python backend_server.py 9000
```

Update extension settings:
```
Backend URL: http://localhost:9000/api/waitlist
```

### Network Access

To access from other computers on network:

1. Find your IP address: `ipconfig` (Windows) or `ifconfig` (Linux/Mac)
2. Start server: `python backend_server.py`
3. On other computer, set extension backend URL to:
   ```
   http://[YOUR_IP]:8000/api/waitlist
   ```

**Note:** May require firewall configuration.

### HTTPS Support

For production use, consider:
- Running behind nginx/Apache reverse proxy
- Using SSL certificates
- Proper authentication

---

## Development

### Modifying the Extension

After making changes:
1. Go to `chrome://extensions/`
2. Click 🔄 reload button on the extension
3. Close and reopen popup to see changes

### Testing Backend

Test API endpoint directly:
```bash
curl http://localhost:8000/api/waitlist
```

Should return JSON with waitlist data.

### Debug Mode

Open popup, press F12 to open DevTools for debugging JavaScript.

Check backend console for server-side errors.

---

## Security Notes

- Backend runs on localhost by default (only accessible from your computer)
- SQL credentials in `backend_server.py` - keep file secure
- Extension only has permissions for specified LiveAccess URL
- No data is stored or transmitted to external servers

---

## Support

For issues or questions:
- Check troubleshooting section above
- Review backend server console output
- Check browser extension console (F12 in popup)

---

## Version History

**v1.0.0** - 2025-10-22
- Initial release
- Waitlist viewing
- LiveAccess integration
- Badge counter
- Settings panel
- ASAP highlighting

---

**Created by:** Noah Han
**Last Updated:** 2025-10-22
**License:** Internal Use - WKennel7
