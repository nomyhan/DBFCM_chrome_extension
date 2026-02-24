// Background service worker for the extension
// Handles side panel opening and badge notifications

// Open side panel when extension icon is clicked
chrome.action.onClicked.addListener((tab) => {
    chrome.sidePanel.open({ windowId: tab.windowId });
    updateBadge();
});

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
    console.log('WKennel7 Waitlist Viewer installed');
    updateBadge();
});

// Update badge periodically (every 5 minutes)
chrome.alarms.create('updateBadge', { periodInMinutes: 5 });

chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name === 'updateBadge') {
        updateBadge();
    }
});
