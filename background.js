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

function showPrintReminder(force = false) {
    // Only fire on business days (Tue=2 through Sat=6) unless forced for testing
    const day = new Date().getDay(); // 0=Sun, 1=Mon, ..., 6=Sat
    if (!force && (day === 0 || day === 1)) return;

    const nextDay = day === 6 ? 'Tuesday' : 'tomorrow';
    chrome.notifications.create(PRINT_REMINDER_ALARM, {
        type: 'basic',
        iconUrl: 'icons/icon128.png',
        title: '📋 Print Appointment Cards',
        message: `Time to print ${nextDay}'s appointment cards!`,
        priority: 1,
    });
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
    schedulePrintReminder();
});

// Update badge periodically (every 5 minutes)
chrome.alarms.create('updateBadge', { periodInMinutes: 5 });

chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name === 'updateBadge') {
        updateBadge();
    }
    if (alarm.name === PRINT_REMINDER_ALARM) {
        showPrintReminder();
    }
});

