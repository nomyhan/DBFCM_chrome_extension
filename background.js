// Background service worker for the extension
// Handles side panel opening, badge notifications, and daily reminders

// Open side panel when extension icon is clicked
chrome.action.onClicked.addListener((tab) => {
    chrome.sidePanel.open({ windowId: tab.windowId });
    updateBadge();
});

// ── Daily 3 PM print-cards reminder ──────────────────────────────────────────

const PRINT_REMINDER_ALARM = 'dailyPrintReminder';

function schedulePrintReminder() {
    chrome.alarms.get(PRINT_REMINDER_ALARM, (existing) => {
        if (existing) return;
        const now = new Date();
        const target = new Date();
        target.setHours(15, 0, 0, 0); // 3:00 PM today
        if (now >= target) {
            target.setDate(target.getDate() + 1); // already past 3 PM — start tomorrow
        }
        chrome.alarms.create(PRINT_REMINDER_ALARM, {
            when: target.getTime(),
            periodInMinutes: 24 * 60, // repeat every 24 hours
        });
    });
}

function showPrintReminder() {
    // Only fire on business days (Tue=2 through Sat=6)
    const day = new Date().getDay(); // 0=Sun, 1=Mon, ..., 6=Sat
    if (day === 0 || day === 1) return;

    const nextDay = day === 6 ? 'Tuesday' : 'tomorrow';
    openReminderWindow(nextDay);
}

function openReminderWindow(nextDay = 'tomorrow') {
    const url = chrome.runtime.getURL(`reminder.html?nextDay=${nextDay}`);
    const W = 360, H = 300;
    chrome.windows.getLastFocused({ populate: false }, (win) => {
        const left = win ? Math.round(win.left + (win.width  - W) / 2) : 200;
        const top  = win ? Math.round(win.top  + (win.height - H) / 2) : 200;
        chrome.windows.create({ url, type: 'popup', width: W, height: H, left, top, focused: true });
    });
}

// ── Cookie relay to comm dashboard ──────────────────────────────────────────
// POST KCApp session cookies to the dashboard so it can send SMS on our behalf

const DASHBOARD_COOKIE_URL = 'http://localhost:5000/api/kc-cookies';

async function relayCookies() {
    try {
        const cookies = await chrome.cookies.getAll({ url: 'https://dbfcm.mykcapp.com/' });
        if (!cookies.length) return; // not logged in

        const cookieDict = {};
        for (const c of cookies) {
            cookieDict[c.name] = c.value;
        }

        await fetch(DASHBOARD_COOKIE_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Api-Key': 'dbfcm-relay-2026',
            },
            body: JSON.stringify({ cookies: cookieDict }),
        });
    } catch (e) {
        // Dashboard may not be running — silent fail
    }
}

// Update badge with waitlist count
async function updateBadge() {
    try {
        const result = await chrome.storage.sync.get(['config']);
        const config = result.config || {};
        const backendUrl = config.backendUrl || 'http://localhost:8000/api/waitlist';

        const response = await fetch(backendUrl);
        if (response.ok) {
            const data = await response.json();
            const count = data.count || 0;

            chrome.action.setBadgeText({ text: count > 0 ? count.toString() : '' });
            chrome.action.setBadgeBackgroundColor({ color: '#667eea' });
        }
    } catch (error) {
        console.error('Error updating badge:', error);
        chrome.action.setBadgeText({ text: '' });
    }
}

// Update badge on extension install/update
chrome.runtime.onInstalled.addListener(() => {
    console.log('DBFCM Extension Backend installed');
    updateBadge();
    relayCookies();
    schedulePrintReminder();
});

// Update badge periodically (every 5 minutes)
chrome.alarms.create('updateBadge', { periodInMinutes: 5 });

chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name === 'updateBadge') {
        updateBadge();
        relayCookies();
    }
    if (alarm.name === PRINT_REMINDER_ALARM) {
        showPrintReminder();
    }
});

