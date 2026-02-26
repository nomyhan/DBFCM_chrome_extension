// Default configuration
const DEFAULT_CONFIG = {
    backendUrl: 'http://localhost:8000',
    liveAccessUrl: 'https://dbfcm.mykcapp.com/#/grooming/appointment/'
};

// DOM elements
let settingsPanel, backendUrlInput, liveAccessUrlInput;
let waitlistContent, availabilityContent, conflictsContent, conflictsModal, conflictCacheStatus, waitlistCountSpan, lastUpdatedSpan;
let refreshBtn, settingsBtn, saveSettingsBtn, cancelSettingsBtn, backendStatusBtn;
let groomerSelect, include230Checkbox;
let chatMessages, chatInput, chatSendBtn, chatResetBtn;

// Current configuration
let config = { ...DEFAULT_CONFIG };

// Current tab
let currentTab = 'waitlist';

// Availability pagination state
let availabilityAllDays = [];
let availabilityShownCount = 0;
const AVAIL_PAGE_SIZE = 15;

// Initialize
document.addEventListener('DOMContentLoaded', async () => {
    // Get DOM elements
    settingsPanel = document.getElementById('settings-panel');
    backendUrlInput = document.getElementById('backend-url');
    liveAccessUrlInput = document.getElementById('liveaccess-url');
    waitlistContent = document.getElementById('waitlist-content');
    availabilityContent = document.getElementById('availability-content');
    conflictsContent = document.getElementById('conflicts-modal-content');
    conflictsModal = document.getElementById('conflicts-modal');
    conflictCacheStatus = document.getElementById('conflict-cache-status');
    waitlistCountSpan = document.getElementById('waitlist-count');
    lastUpdatedSpan = document.getElementById('last-updated');
    refreshBtn = document.getElementById('refresh-btn');
    settingsBtn = document.getElementById('settings-btn');
    saveSettingsBtn = document.getElementById('save-settings');
    cancelSettingsBtn = document.getElementById('cancel-settings');
    backendStatusBtn = document.getElementById('backend-status-btn');
    groomerSelect = document.getElementById('groomer-select');
    include230Checkbox = document.getElementById('include-230-slot');
    chatMessages = document.getElementById('chat-messages');
    chatInput = document.getElementById('chat-input');
    chatSendBtn = document.getElementById('chat-send-btn');
    chatResetBtn = document.getElementById('chat-reset-btn');

    // Load saved configuration
    await loadConfig();

    // Set up event listeners
    refreshBtn.addEventListener('click', refreshData);
    settingsBtn.addEventListener('click', toggleSettings);
    saveSettingsBtn.addEventListener('click', saveSettings);
    cancelSettingsBtn.addEventListener('click', () => {
        settingsPanel.classList.add('hidden');
        loadConfigToInputs();

    });
    backendStatusBtn.addEventListener('click', checkBackendStatus);

    document.getElementById('restart-backend-btn').addEventListener('click', restartBackend);
    document.getElementById('reload-extension-btn').addEventListener('click', () => chrome.runtime.reload());

    // Tab switching
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });

    // Groomer select change
    groomerSelect.addEventListener('change', () => {
        const groomerId = groomerSelect.value;
        if (groomerId) {
            loadAvailability(parseInt(groomerId));
        } else {
            showAvailabilityEmpty();
        }
    });

    // 2:30 slot checkbox change
    include230Checkbox.addEventListener('change', () => {
        const groomerId = groomerSelect.value;
        if (groomerId) {
            loadAvailability(parseInt(groomerId));
        }
    });

    // Calendar date links in availability tab (delegated — survives innerHTML replacements)
    availabilityContent.addEventListener('click', (e) => {
        const link = e.target.closest('.calendar-date-link');
        if (!link) return;
        e.preventDefault();
        e.stopPropagation();
        const slot = link.closest('.availability-slot');
        if (slot && slot.dataset.date) openCalendarOnDate(slot.dataset.date);
    });

    // Conflict check button (in settings panel)
    const runConflictsBtn = document.getElementById('run-conflicts-btn');
    if (runConflictsBtn) {
        runConflictsBtn.addEventListener('click', () => {
            openConflictsModal();
            loadConflicts();
        });
    }

    // Conflicts modal close
    document.getElementById('conflicts-modal-close').addEventListener('click', closeConflictsModal);

    // Chat tab
    chatSendBtn.addEventListener('click', sendChatMessage);
    chatInput.addEventListener('keypress', (e) => { if (e.key === 'Enter') sendChatMessage(); });
    chatResetBtn.addEventListener('click', resetChat);

    // Load initial data
    await loadWaitlist();
    await loadGroomers();

    // Detect KCApp operator for Know-a-bot (non-blocking)
    getKCAppOperator();

    // Check backend status periodically
    setInterval(updateBackendStatusIndicator, 10000);
});

// Switch tabs
function switchTab(tab) {
    currentTab = tab;

    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tab);
    });

    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `${tab}-tab`);
    });

    // Auto-load SMS tab when switched to
    if (tab === 'sms') loadSmsDrafts();
    // Auto-load Checkout tab when switched to
    if (tab === 'checkout') loadCheckout();
    // Refresh operator detection when switching to chat tab
    if (tab === 'chat') getKCAppOperator();
}

// Load configuration from storage
async function loadConfig() {
    try {
        const result = await chrome.storage.sync.get(['config']);
        if (result.config) {
            config = { ...DEFAULT_CONFIG, ...result.config };
        }
        loadConfigToInputs();
    } catch (error) {
        console.error('Error loading config:', error);
    }
}

// Load config values to input fields
function loadConfigToInputs() {
    backendUrlInput.value = config.backendUrl;
    liveAccessUrlInput.value = config.liveAccessUrl;
}

// Save configuration
async function saveSettings() {
    config.backendUrl = backendUrlInput.value.trim();
    config.liveAccessUrl = liveAccessUrlInput.value.trim();

    try {
        await chrome.storage.sync.set({ config });
        settingsPanel.classList.add('hidden');
        await loadWaitlist();
        await loadGroomers();
    } catch (error) {
        console.error('Error saving config:', error);
        showError(waitlistContent, 'Failed to save settings');
    }
}

// Toggle settings panel
function toggleSettings() {
    const isOpening = settingsPanel.classList.contains('hidden');
    settingsPanel.classList.toggle('hidden');
    if (isOpening) fetchCachedConflictStatus();
}

// Fetch and display cached conflict check status in settings panel
async function fetchCachedConflictStatus() {
    if (!conflictCacheStatus) return;
    try {
        const response = await fetch(`${config.backendUrl}/api/conflicts/cached`);
        if (!response.ok) { conflictCacheStatus.textContent = 'Not yet run'; return; }
        const data = await response.json();
        if (data.last_checked) {
            const count = data.count || 0;
            const label = count === 0 ? 'All clear' : `${count} conflict${count !== 1 ? 's' : ''}`;
            conflictCacheStatus.textContent = `${label} — ${data.last_checked}`;
            conflictCacheStatus.className = 'conflict-cache-status' + (count === 0 ? ' all-clear' : ' has-conflicts');
        } else {
            conflictCacheStatus.textContent = 'Not yet run';
            conflictCacheStatus.className = 'conflict-cache-status';
        }
    } catch (e) {
        conflictCacheStatus.textContent = 'Not yet run';
        conflictCacheStatus.className = 'conflict-cache-status';
    }
}

function openConflictsModal() {
    if (conflictsModal) conflictsModal.classList.remove('hidden');
}

function closeConflictsModal() {
    if (conflictsModal) conflictsModal.classList.add('hidden');
}

// Refresh data based on current tab
async function refreshData() {
    const refreshIcon = refreshBtn.querySelector('.refresh-icon');
    refreshIcon.classList.add('spinning');

    if (currentTab === 'waitlist') {
        await loadWaitlist();
    } else if (currentTab === 'availability') {
        const groomerId = groomerSelect.value;
        if (groomerId) {
            await loadAvailability(parseInt(groomerId));
        }
    } else if (currentTab === 'sms') {
        await loadSmsDrafts();
    } else if (currentTab === 'checkout') {
        await loadCheckout();
    }

    setTimeout(() => {
        refreshIcon.classList.remove('spinning');
    }, 500);
}

// Load groomers for dropdown
async function loadGroomers() {
    try {
        const response = await fetch(`${config.backendUrl}/api/groomers`);
        if (!response.ok) throw new Error('Failed to load groomers');

        const data = await response.json();
        if (data.groomers) {
            groomerSelect.innerHTML = '<option value="">-- Select Groomer --</option>';
            data.groomers.forEach(g => {
                groomerSelect.innerHTML += `<option value="${g.id}">${escapeHtml(g.name)}</option>`;
            });
        }
    } catch (error) {
        console.error('Error loading groomers:', error);
    }
}

// Load availability for a groomer
async function loadAvailability(groomerId) {
    try {
        showLoading(availabilityContent, 'Finding available slots...');

        const include230 = include230Checkbox && include230Checkbox.checked ? '1' : '0';
        const response = await fetch(`${config.backendUrl}/api/availability?groomer_id=${groomerId}&include_230=${include230}`);
        if (!response.ok) throw new Error('Failed to load availability');

        const data = await response.json();
        displayAvailability(data);
    } catch (error) {
        console.error('Error loading availability:', error);
        showError(availabilityContent, error.message);
    }
}

// Display availability days — resets pagination and renders first batch
function displayAvailability(data) {
    if (!data.days || data.days.length === 0) {
        availabilityContent.innerHTML = `
            <div class="empty-state">
                <h2>No Available Days</h2>
                <p>No available appointments found in the next 12 months.</p>
            </div>
        `;
        return;
    }

    availabilityAllDays = data.days;
    availabilityShownCount = 0;
    availabilityContent.innerHTML = '';
    appendAvailabilityPage();
}

function appendAvailabilityPage() {
    // Remove existing bottom indicator
    const existing = availabilityContent.querySelector('.avail-bottom-indicator');
    if (existing) existing.remove();

    const include230 = include230Checkbox && include230Checkbox.checked;
    const batch = availabilityAllDays.slice(availabilityShownCount, availabilityShownCount + AVAIL_PAGE_SIZE);
    availabilityShownCount += batch.length;

    const fragment = document.createDocumentFragment();
    batch.forEach(day => {
        const html = buildDayHtml(day, include230);
        if (html) {
            const wrapper = document.createElement('div');
            wrapper.innerHTML = html;
            fragment.appendChild(wrapper.firstElementChild);
        }
    });
    availabilityContent.appendChild(fragment);

    // Add bottom indicator
    const bottomEl = document.createElement('div');
    bottomEl.className = 'avail-bottom-indicator';
    if (availabilityShownCount < availabilityAllDays.length) {
        const remaining = availabilityAllDays.length - availabilityShownCount;
        bottomEl.innerHTML = `<div class="avail-load-more"><button class="avail-load-btn">Load more (${remaining} more days)</button></div>`;
        availabilityContent.appendChild(bottomEl);
        bottomEl.querySelector('.avail-load-btn').addEventListener('click', appendAvailabilityPage);
    } else {
        bottomEl.innerHTML = `<div class="avail-all-loaded">— All dates shown —</div>`;
        availabilityContent.appendChild(bottomEl);
    }
}

// Build HTML string for a single availability day card (returns null if no open slots)
function buildDayHtml(day, include230) {
    const sizes = day.size_breakdown;
    const special = day.special_types;

    const filteredAvailableTimes = include230
        ? day.available_times
        : day.available_times.filter(t => t !== '14:30');

    if (filteredAvailableTimes.length === 0) return null;

    let sizeTags = '';
    if (sizes.XS > 0) sizeTags += `<span class="size-tag size-xs">XS: ${sizes.XS}</span>`;
    if (sizes.SM > 0) sizeTags += `<span class="size-tag size-sm">SM: ${sizes.SM}</span>`;
    if (sizes.MD > 0) sizeTags += `<span class="size-tag size-md">MD: ${sizes.MD}</span>`;
    if (sizes.LG > 0) sizeTags += `<span class="size-tag size-lg">LG: ${sizes.LG}</span>`;
    if (sizes.XL > 0) sizeTags += `<span class="size-tag size-xl">XL: ${sizes.XL}</span>`;
    if (special.handstrip > 0) sizeTags += `<span class="size-tag size-handstrip">HS: ${special.handstrip}</span>`;
    if (special.bath_only > 0) sizeTags += `<span class="size-tag size-bath">Bath: ${special.bath_only}</span>`;
    if (special.nails_only > 0) sizeTags += `<span class="size-tag size-nails">Nails: ${special.nails_only}</span>`;

    let timelineEntries = [];
    if (day.appointments) {
        day.appointments.forEach(appt => {
            timelineEntries.push({
                time: appt.time.substring(0, 5),
                type: 'booked',
                appt: appt
            });
        });
    }
    filteredAvailableTimes.forEach(slot => {
        timelineEntries.push({ time: slot, type: 'available' });
    });
    timelineEntries.sort((a, b) => a.time.localeCompare(b.time));

    let timelineHtml = '';
    timelineEntries.forEach(entry => {
        if (entry.type === 'booked') {
            const appt = entry.appt;
            const size = extractSize(appt.pet_type);
            const sizeBadge = size ? `<span class="appt-size-tag size-${size.toLowerCase()}">${size}</span>` : '';
            timelineHtml += `
                <div class="day-appt-item booked">
                    <span class="day-appt-time">${formatTime(entry.time + ':00')}</span>
                    <span class="day-appt-pet">${escapeHtml(appt.pet_name)}</span>
                    <span class="day-appt-client">(${escapeHtml(appt.client)})</span>
                    ${sizeBadge}
                    <span class="day-appt-service ${appt.service.toLowerCase()}">${appt.service}</span>
                </div>
            `;
        } else {
            timelineHtml += `
                <div class="day-appt-item available">
                    <span class="day-appt-time">${formatTime(entry.time + ':00')}</span>
                    <span class="available-label">AVAILABLE</span>
                </div>
            `;
        }
    });

    return `
        <div class="availability-slot" data-date="${day.date}">
            <div class="slot-header">
                <div class="slot-datetime">
                    <a href="#" class="slot-date calendar-date-link">${formatDate(day.date)}</a>
                    <span class="slot-day">${day.day_of_week}</span>
                </div>
                <div class="slot-summary-inline">
                    <span class="available-count">${filteredAvailableTimes.length} open</span>
                    <span class="booked-count">${day.total_booked} booked</span>
                </div>
            </div>
            ${sizeTags ? `<div class="size-breakdown">${sizeTags}</div>` : ''}
            <div class="day-timeline">
                ${timelineHtml}
            </div>
        </div>
    `;
}

// Show empty availability state
function showAvailabilityEmpty() {
    availabilityContent.innerHTML = `
        <div class="empty-state">
            <h2>Select a Groomer</h2>
            <p>Choose a groomer to see their next available appointments</p>
        </div>
    `;
}

// Load waitlist data
async function loadWaitlist() {
    try {
        showLoading(waitlistContent, 'Loading waitlist...');

        const response = await fetch(`${config.backendUrl}/api/waitlist`);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        displayWaitlist(data);

    } catch (error) {
        console.error('Error loading waitlist:', error);
        showError(waitlistContent, error.message);
    }
}

// Returns the dominant groomer key if one groomer has ≥60% of visits (min 3 visits)
function getDominantGroomer(statsStr, totalVisits) {
    if (!statsStr || !totalVisits || totalVisits < 3) return null;
    const stats = parseGroomerStats(statsStr, totalVisits);
    if (!stats.length) return null;
    stats.sort((a, b) => b.count - a.count);
    const top = stats[0];
    if (top.percent < 60) return null;
    const name = top.name.toLowerCase();
    if (name.includes('kumi')) return 'kumi';
    if (name.includes('tomoko')) return 'tomoko';
    if (name.includes('mandilyn')) return 'mandilyn';
    return null;
}

// Assigns a waitlist item to a groomer group based on scheduling priority rules
function assignGroomerGroup(item) {
    const pref = (item.groomer || '').toLowerCase();
    const service = (item.service_type || '').toLowerCase();
    const petName = item.pet_name || '';
    const petType = (item.pet_type || '').toLowerCase();
    const isLargeOrXL = /\b(lg|xl)\b/.test(petType);

    // Scan all free-text fields for groomer name mentions
    const allText = [
        item.pet_warning || '',
        item.groom_warning || '',
        item.client_warning || '',
        item.notes || '',
    ].join(' ').toLowerCase();

    // 1. Handstrip → always Kumi
    if (service === 'handstrip' || petName.includes('#')) {
        return { group: 'kumi', reason: 'Handstrip' };
    }

    // 2. Explicit groomer preference (GLGroomerID field OR any warning/notes text)
    if (pref.includes('kumi')     || /\bkumi\b/.test(allText))     return { group: 'kumi',     reason: 'Requested Kumi' };
    if (pref.includes('tomoko')   || /\btomoko\b/.test(allText))   return { group: 'tomoko',   reason: 'Requested Tomoko' };
    if (pref.includes('mandilyn') || /\bmandilyn\b/.test(allText)) return { group: 'mandilyn', reason: 'Requested Mandilyn' };

    // 3. Strong groomer history (≥60% with one groomer)
    const dominant = getDominantGroomer(item.groomer_stats, item.total_visits);
    if (dominant === 'kumi')     return { group: 'kumi',     reason: 'Always with Kumi' };
    if (dominant === 'tomoko')   return { group: 'tomoko',   reason: 'Always with Tomoko' };
    if (dominant === 'mandilyn') return { group: 'mandilyn', reason: 'Always with Mandilyn' };

    // 4. LG/XL → Mandilyn
    if (isLargeOrXL) return { group: 'mandilyn', reason: 'LG/XL dog' };

    // 5. New dog (no history) → Mandilyn
    if (!item.last_completed_date || !item.total_visits) {
        return { group: 'mandilyn', reason: 'New client' };
    }

    // 6. No clear assignment
    return { group: 'flexible', reason: 'Any groomer' };
}

// Display waitlist
function displayWaitlist(data) {
    waitlistCountSpan.textContent = data.count || 0;
    lastUpdatedSpan.textContent = formatDateTime(data.last_updated);

    if (!data.waitlist || data.waitlist.length === 0) {
        waitlistContent.innerHTML = `
            <div class="empty-state">
                <h2>No Waitlist Appointments</h2>
                <p>The waitlist is currently empty.</p>
            </div>
        `;
        return;
    }

    // Group items by groomer
    const groups = { kumi: [], tomoko: [], mandilyn: [], flexible: [] };
    data.waitlist.forEach(item => {
        const { group, reason } = assignGroomerGroup(item);
        item._groupReason = reason;
        groups[group].push(item);
    });

    const buildItemHtml = (item) => {
        const serviceClass = item.service_type.toLowerCase().replace(' ', '-');
        const hasASAP = item.notes.toUpperCase().includes('ASAP');
        const liveAccessUrl = `${config.liveAccessUrl}?appid=${item.glseq}`;

        const hasWarnings = item.client_warning || item.pet_warning || item.groom_warning;
        const hasHistory = item.last_completed_date || item.next_scheduled_date || item.total_visits > 0;
        const itemId = `item-${item.glseq}`;

        const groomerStatsArray = parseGroomerStats(item.groomer_stats, item.total_visits);

        const breedInfo = item.breed || item.pet_type
            ? [item.breed, item.pet_type].filter(Boolean).join(' • ')
            : '';

        const petRecordUrl = `https://dbfcm.mykcapp.com/#/pets/details/${item.pet_id}`;
        const clientRecordUrl = `https://dbfcm.mykcapp.com/#/clients/details/${item.client_id}`;
        const clientTooltip = item.client_warning ? escapeHtml(item.client_warning) : 'No client warnings';

        const addressParts = [];
        if (item.address) addressParts.push(escapeHtml(item.address));
        if (item.city) addressParts.push(`<strong>${escapeHtml(item.city)}</strong>`);
        if (item.state) addressParts.push(escapeHtml(item.state));
        if (item.zip) addressParts.push(escapeHtml(item.zip));
        const fullAddress = addressParts.length > 0 ? addressParts.join(', ') : '';

        return `
            <div class="waitlist-item" data-url="${liveAccessUrl}" data-item-id="${itemId}">
                <div class="item-meta">
                    <span class="glseq-badge">#${escapeHtml(item.glseq)}</span>
                    <span class="group-reason-badge">${escapeHtml(item._groupReason)}</span>
                    <span class="added-date">Added ${formatDate(item.wl_date)}</span>
                </div>
                <div class="item-header">
                    <div class="item-title">
                        <a href="#" class="pet-name-link" data-pet-url="${petRecordUrl}">${escapeHtml(item.pet_name)}</a>
                        - <a href="#" class="client-name-link" data-client-url="${clientRecordUrl}" title="${clientTooltip}">${escapeHtml(item.last_name)}</a>
                        ${hasASAP ? '<span class="asap-badge">ASAP</span>' : ''}
                    </div>
                    <span class="service-badge service-${serviceClass}">
                        ${escapeHtml(item.service_type)}
                    </span>
                </div>
                ${item.phone ? `
                <div class="item-phone">
                    📞 ${escapeHtml(item.phone)}
                </div>
                ` : ''}
                ${fullAddress ? `
                <div class="item-address">
                    📍 Coming from: ${fullAddress}
                </div>
                ` : ''}
                ${breedInfo ? `
                <div class="item-breed">
                    🐕 ${escapeHtml(breedInfo)}
                </div>
                ` : ''}
                ${item.groomer ? `
                <div class="item-groomer">
                    ✂️ Preferred: ${escapeHtml(item.groomer)}
                </div>
                ` : ''}
                <div class="item-notes" data-glseq="${item.glseq}">
                    <div class="notes-display">
                        <span class="notes-text">${item.notes ? '📝 ' + escapeHtml(item.notes) : '<span class="no-notes">No notes</span>'}</span>
                        <button class="edit-notes-btn" title="Edit notes">✏️</button>
                    </div>
                    <div class="notes-edit hidden">
                        <textarea class="notes-textarea" rows="3">${escapeHtml(item.notes || '')}</textarea>
                        <div class="notes-edit-actions">
                            <button class="btn btn-primary save-notes-btn">Save</button>
                            <button class="btn btn-secondary cancel-notes-btn">Cancel</button>
                        </div>
                    </div>
                </div>

                ${hasWarnings || hasHistory ? `
                <button class="expand-toggle" data-item-id="${itemId}">
                    Show Details
                </button>
                <div class="expandable-content" id="details-${itemId}">
                    ${hasWarnings ? `
                    <div class="warnings-section">
                        ${item.client_warning ? `
                        <div class="warning-item">
                            <span class="warning-label">⚠️ Client:</span>
                            <span class="warning-value">${escapeHtml(item.client_warning)}</span>
                        </div>
                        ` : ''}
                        ${item.pet_warning ? `
                        <div class="warning-item">
                            <span class="warning-label">⚠️ Pet:</span>
                            <span class="warning-value">${escapeHtml(item.pet_warning)}</span>
                        </div>
                        ` : ''}
                        ${item.groom_warning ? `
                        <div class="warning-item">
                            <span class="warning-label">⚠️ Groom:</span>
                            <span class="warning-value">${escapeHtml(item.groom_warning)}</span>
                        </div>
                        ` : ''}
                    </div>
                    ` : ''}

                    ${hasHistory ? `
                    <div class="history-section">
                        ${item.total_visits > 0 ? `
                        <div class="history-item">
                            <span class="history-label">Total Visits:</span>
                            <span class="history-value">${item.total_visits}</span>
                        </div>
                        ` : ''}
                        ${item.last_completed_date ? `
                        <div class="history-item">
                            <span class="history-label">Last Visit:</span>
                            <span class="history-value">${formatDate(item.last_completed_date)}${item.last_groomer ? ` (${escapeHtml(item.last_groomer)})` : ''}</span>
                        </div>
                        ${item.last_completed_notes ? `
                        <div class="history-item">
                            <span class="history-label">Last Notes:</span>
                            <span class="history-value">${escapeHtml(item.last_completed_notes)}</span>
                        </div>
                        ` : ''}
                        ` : ''}
                        ${groomerStatsArray.length > 0 ? `
                        <div class="history-item groomer-breakdown">
                            <span class="history-label">Groomer History:</span>
                            <span class="history-value">${groomerStatsArray.map(g =>
                                `${escapeHtml(g.name)}: ${g.count} (${g.percent}%)`
                            ).join(', ')}</span>
                        </div>
                        ` : ''}
                        ${item.next_scheduled_date ? `
                        <div class="history-item">
                            <span class="history-label">Next Scheduled:</span>
                            <span class="history-value">${formatDate(item.next_scheduled_date)}</span>
                        </div>
                        ` : ''}
                    </div>
                    ` : ''}
                </div>
                ` : ''}
            </div>
        `;
    };

    const groupOrder = [
        { key: 'kumi',     label: '✂️ Kumi',        bg: '#fffbeb', border: '#d97706' },
        { key: 'tomoko',   label: '✂️ Tomoko',      bg: '#eff6ff', border: '#2563eb' },
        { key: 'mandilyn', label: '✂️ Mandilyn',    bg: '#f5f3ff', border: '#7c3aed' },
        { key: 'flexible', label: '↔ Any Groomer', bg: '#f8f9fa', border: '#9ca3af' },
    ];

    let html = '';
    groupOrder.forEach(({ key, label, bg, border }) => {
        const items = groups[key];
        if (!items.length) return;
        const isCollapsed = localStorage.getItem(`wl-group-${key}`) === 'true';
        html += `
            <div class="groomer-group${isCollapsed ? ' collapsed' : ''}">
                <div class="groomer-group-header" style="background:${bg};border-left:4px solid ${border};">
                    <label class="group-collapse-label" title="${isCollapsed ? 'Show' : 'Hide'} group">
                        <input type="checkbox" class="group-toggle" data-group="${key}" ${isCollapsed ? '' : 'checked'}>
                    </label>
                    <span class="groomer-group-name">${label}</span>
                    <span class="groomer-group-count">${items.length} dog${items.length !== 1 ? 's' : ''}</span>
                </div>
                <div class="groomer-group-body">
                    ${items.map(buildItemHtml).join('')}
                </div>
            </div>
        `;
    });

    waitlistContent.innerHTML = html;

    // Group collapse checkboxes
    document.querySelectorAll('.group-toggle').forEach(cb => {
        cb.addEventListener('change', (e) => {
            e.stopPropagation();
            const key = cb.getAttribute('data-group');
            const group = cb.closest('.groomer-group');
            const collapsed = !cb.checked;
            group.classList.toggle('collapsed', collapsed);
            localStorage.setItem(`wl-group-${key}`, collapsed);
        });
    });

    // Add click handlers for expand/collapse buttons
    document.querySelectorAll('.expand-toggle').forEach(button => {
        button.addEventListener('click', (e) => {
            e.stopPropagation();
            const itemId = button.getAttribute('data-item-id');
            const content = document.getElementById(`details-${itemId}`);

            if (content.classList.contains('expanded')) {
                content.classList.remove('expanded');
                button.classList.remove('expanded');
                button.textContent = 'Show Details';
            } else {
                content.classList.add('expanded');
                button.classList.add('expanded');
                button.textContent = 'Hide Details';
            }
        });
    });

    // Add click handlers for pet name links
    document.querySelectorAll('.pet-name-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            chrome.tabs.create({ url: link.getAttribute('data-pet-url') });
        });
    });

    // Add click handlers for client name links
    document.querySelectorAll('.client-name-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            chrome.tabs.create({ url: link.getAttribute('data-client-url') });
        });
    });

    // Add click handlers for notes editing
    document.querySelectorAll('.edit-notes-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const notesContainer = btn.closest('.item-notes');
            notesContainer.querySelector('.notes-display').classList.add('hidden');
            notesContainer.querySelector('.notes-edit').classList.remove('hidden');
            notesContainer.querySelector('.notes-textarea').focus();
        });
    });

    document.querySelectorAll('.cancel-notes-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const notesContainer = btn.closest('.item-notes');
            notesContainer.querySelector('.notes-display').classList.remove('hidden');
            notesContainer.querySelector('.notes-edit').classList.add('hidden');
        });
    });

    document.querySelectorAll('.save-notes-btn').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            e.stopPropagation();
            const notesContainer = btn.closest('.item-notes');
            const glseq = notesContainer.getAttribute('data-glseq');
            const textarea = notesContainer.querySelector('.notes-textarea');
            const newNotes = textarea.value.trim();

            btn.disabled = true;
            btn.textContent = 'Saving...';

            try {
                const response = await fetch(`${config.backendUrl}/api/waitlist/update-notes`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ glseq, notes: newNotes })
                });

                const result = await response.json();

                if (result.success) {
                    const notesText = notesContainer.querySelector('.notes-text');
                    notesText.innerHTML = newNotes ? '📝 ' + escapeHtml(newNotes) : '<span class="no-notes">No notes</span>';
                    notesContainer.querySelector('.notes-display').classList.remove('hidden');
                    notesContainer.querySelector('.notes-edit').classList.add('hidden');
                } else {
                    alert('Failed to save notes: ' + (result.error || 'Unknown error'));
                }
            } catch (error) {
                alert('Failed to save notes: ' + error.message);
            } finally {
                btn.disabled = false;
                btn.textContent = 'Save';
            }
        });
    });

    // Add click handlers to open LiveAccess
    document.querySelectorAll('.waitlist-item').forEach(item => {
        item.addEventListener('click', (e) => {
            if (e.target.closest('.expand-toggle') || e.target.closest('.expandable-content') ||
                e.target.closest('.pet-name-link') || e.target.closest('.client-name-link') || e.target.closest('.item-notes')) {
                return;
            }
            chrome.tabs.create({ url: item.getAttribute('data-url') });
        });
    });
}

// Global reference so a new click cancels any in-progress poll
let activePoll = null;

// Wait for the calendar picker to stabilize (app may restore a previous date on
// reload), then inject using the datetimepicker's own setDate API so its
// internal state is updated exactly as if the user had clicked the date.
function pollAndInjectDate(tabId, dateStr) {
    if (activePoll) { clearInterval(activePoll); activePoll = null; }

    let attempts = 0;
    let lastPickerValue = null;

    activePoll = setInterval(() => {
        attempts++;
        if (attempts > 30) { clearInterval(activePoll); activePoll = null; return; }

        // Phase 1: read the picker value and wait until it's stable
        chrome.scripting.executeScript({
            target: { tabId },
            world: 'MAIN',
            func: () => {
                const picker = document.getElementById('dateCalendarHidden');
                if (!picker || !picker.value) return null;
                if (!document.querySelector('.fc-view, .fc-view-container')) return null;
                return picker.value;
            }
        }, (results) => {
            const pickerValue = results && results[0] && results[0].result;
            if (!pickerValue) return; // still loading

            if (pickerValue === lastPickerValue) {
                // Stable — now inject using datetimepicker's own setDate so its
                // internal state matches, then fire change exactly as a real click would
                clearInterval(activePoll); activePoll = null;
                chrome.scripting.executeScript({
                    target: { tabId },
                    world: 'MAIN',
                    func: (date) => {
                        const picker = document.getElementById('dateCalendarHidden');
                        const [y, m, d] = date.split('-').map(Number);
                        if (typeof $ !== 'undefined' && $.fn.datetimepicker) {
                            // setDate updates datetimepicker's internal state (same as user clicking a date)
                            $(picker).datetimepicker('setDate', new Date(y, m - 1, d));
                        }
                        // Also set raw value and fire native change so the inline onchange handler fires
                        picker.value = date;
                        picker.dispatchEvent(new Event('change', { bubbles: true }));
                    },
                    args: [dateStr]
                });
            } else {
                lastPickerValue = pickerValue; // keep waiting
            }
        });
    }, 500);
}

// Navigate a tab to the calendar URL and poll until ready, then inject.
function navigateAndWait(tabId, dateStr, navigateFn) {
    if (activePoll) { clearInterval(activePoll); activePoll = null; }
    chrome.tabs.onUpdated.addListener(function onLoaded(updatedId, info) {
        if (updatedId !== tabId || info.status !== 'complete') return;
        chrome.tabs.onUpdated.removeListener(onLoaded);
        pollAndInjectDate(tabId, dateStr);
    });
    navigateFn();
}

// Open LiveAccess calendar tab and jump to a specific date.
function openCalendarOnDate(dateStr) {
    const calendarUrl = 'https://dbfcm.mykcapp.com/#/grooming/calendar';
    chrome.tabs.query({ url: 'https://dbfcm.mykcapp.com/*' }, (existingTabs) => {
        // Prefer a tab already showing the calendar
        const calendarTab = existingTabs.find(t => t.url.includes('#/grooming/calendar'));

        if (calendarTab) {
            // Already loaded — inject directly, no reload needed
            chrome.tabs.update(calendarTab.id, { active: true });
            chrome.windows.update(calendarTab.windowId, { focused: true });
            chrome.scripting.executeScript({
                target: { tabId: calendarTab.id },
                world: 'MAIN',
                func: (date) => {
                    const picker = document.getElementById('dateCalendarHidden');
                    if (!picker) return;
                    const [y, m, d] = date.split('-').map(Number);
                    if (typeof $ !== 'undefined' && $.fn.datetimepicker) {
                        $(picker).datetimepicker('setDate', new Date(y, m - 1, d));
                    }
                    picker.value = date;
                    picker.dispatchEvent(new Event('change', { bubbles: true }));
                },
                args: [dateStr]
            });
        } else if (existingTabs.length > 0) {
            // dbfcm tab open but not on calendar — navigate it there
            const tab = existingTabs[0];
            chrome.tabs.update(tab.id, { active: true });
            chrome.windows.update(tab.windowId, { focused: true });
            navigateAndWait(tab.id, dateStr, () => chrome.tabs.update(tab.id, { url: calendarUrl }));
        } else {
            // No dbfcm tab at all — open a new one
            chrome.tabs.create({ url: calendarUrl }, (tab) => {
                navigateAndWait(tab.id, dateStr, () => {});
            });
        }
    });
}

// Show loading state
function showLoading(container, message = 'Loading...') {
    container.innerHTML = `
        <div class="loading">
            <div class="spinner"></div>
            <p>${message}</p>
        </div>
    `;
}

// Show error
function showError(container, message) {
    container.innerHTML = `
        <div class="error">
            <h3>⚠️ Error</h3>
            <p><strong>Error:</strong> ${escapeHtml(message)}</p>
            <p style="margin-top: 12px;">
                <strong>Troubleshooting:</strong><br>
                1. Make sure the backend server is running<br>
                2. Check the Backend URL in settings (⚙️)<br>
                3. Verify you can access: <code>${escapeHtml(config.backendUrl)}</code>
            </p>
        </div>
    `;
}

// Utility functions
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function parseGroomerStats(statsStr, totalVisits) {
    if (!statsStr || !totalVisits) return [];
    return statsStr.split('|').map(entry => {
        const [name, countStr] = entry.split(':');
        const count = parseInt(countStr) || 0;
        const percent = Math.round((count / totalVisits) * 100);
        return { name: name.trim(), count, percent };
    });
}

function formatDate(dateStr) {
    if (!dateStr || dateStr === 'NULL' || dateStr === 'N/A') return 'N/A';
    try {
        // Parse as local time to avoid timezone offset issues
        // dateStr is in format YYYY-MM-DD
        const parts = dateStr.split('-');
        if (parts.length === 3) {
            const date = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
            return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
        }
        return dateStr;
    } catch {
        return dateStr;
    }
}

function formatTime(timeStr) {
    if (!timeStr) return '';
    try {
        const [hours, minutes] = timeStr.split(':');
        const hour = parseInt(hours);
        const ampm = hour >= 12 ? 'PM' : 'AM';
        const displayHour = hour % 12 || 12;
        return `${displayHour}:${minutes} ${ampm}`;
    } catch {
        return timeStr;
    }
}

function formatDateTime(dateTimeStr) {
    if (!dateTimeStr) return 'Never';
    try {
        const date = new Date(dateTimeStr);
        return date.toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: 'numeric',
            minute: '2-digit'
        });
    } catch {
        return dateTimeStr;
    }
}

function extractSize(petType) {
    if (!petType) return null;
    const sizeMatch = petType.match(/\b(XS|SM|MD|LG|XL)\b/i);
    return sizeMatch ? sizeMatch[1].toUpperCase() : null;
}

// Check backend status
async function updateBackendStatusIndicator() {
    try {
        const response = await fetch(`${config.backendUrl}/api/groomers`, {
            method: 'HEAD',
            mode: 'no-cors'
        });
        backendStatusBtn.classList.remove('backend-stopped');
        backendStatusBtn.classList.add('backend-running');
        backendStatusBtn.querySelector('.backend-icon').textContent = '▶️';
        backendStatusBtn.title = 'Backend server is running';
    } catch (error) {
        backendStatusBtn.classList.remove('backend-running');
        backendStatusBtn.classList.add('backend-stopped');
        backendStatusBtn.querySelector('.backend-icon').textContent = '⏹️';
        backendStatusBtn.title = 'Backend server is stopped - Click for instructions';
    }
}

// Acknowledged conflicts storage
let acknowledgedConflicts = {};

async function loadAcknowledgedConflicts() {
    try {
        const result = await chrome.storage.sync.get(['acknowledgedConflicts']);
        acknowledgedConflicts = result.acknowledgedConflicts || {};
        // Clean up old entries (dates in the past)
        const today = new Date().toISOString().slice(0, 10);
        let cleaned = false;
        for (const key of Object.keys(acknowledgedConflicts)) {
            const datePart = key.split('-').slice(1, 4).join('-'); // e.g., "2026-03-10"
            if (datePart < today) {
                delete acknowledgedConflicts[key];
                cleaned = true;
            }
        }
        if (cleaned) {
            await chrome.storage.sync.set({ acknowledgedConflicts });
        }
    } catch (error) {
        console.error('Error loading acknowledged conflicts:', error);
        acknowledgedConflicts = {};
    }
}

function getConflictKey(conflict) {
    return `${conflict.groomer_id}-${conflict.date}-${conflict.slot}`;
}

async function toggleAcknowledged(key, checked) {
    if (checked) {
        acknowledgedConflicts[key] = new Date().toISOString();
    } else {
        delete acknowledgedConflicts[key];
    }
    try {
        await chrome.storage.sync.set({ acknowledgedConflicts });
    } catch (error) {
        console.error('Error saving acknowledged state:', error);
    }
    updateConflictCounts();
}

function updateConflictCounts() {
    // Update the header badge and groomer counts
    const allItems = document.querySelectorAll('.conflict-item[data-conflict-key]');
    let totalUnacked = 0;

    // Count per groomer section
    document.querySelectorAll('.conflict-groomer-section').forEach(section => {
        const items = section.querySelectorAll('.conflict-item[data-conflict-key]');
        let sectionUnacked = 0;
        items.forEach(item => {
            const key = item.getAttribute('data-conflict-key');
            if (!acknowledgedConflicts[key]) {
                sectionUnacked++;
            }
        });
        totalUnacked += sectionUnacked;
        const countBadge = section.querySelector('.conflict-groomer-count');
        if (countBadge) {
            countBadge.textContent = sectionUnacked;
            countBadge.classList.toggle('all-clear', sectionUnacked === 0);
        }
    });

    const totalBadge = document.querySelector('.conflict-total-badge');
    if (totalBadge) {
        if (totalUnacked === 0) {
            totalBadge.textContent = 'All clear';
            totalBadge.classList.add('all-clear');
        } else {
            totalBadge.textContent = `${totalUnacked} conflict${totalUnacked !== 1 ? 's' : ''} to review`;
            totalBadge.classList.remove('all-clear');
        }
    }
}

// Load conflicts
async function loadConflicts() {
    try {
        showLoading(conflictsContent, 'Scanning next 120 days for conflicts...');

        await loadAcknowledgedConflicts();

        const response = await fetch(`${config.backendUrl}/api/conflicts`);
        if (!response.ok) throw new Error('Failed to load conflicts');

        const data = await response.json();
        displayConflicts(data);
        fetchCachedConflictStatus();
    } catch (error) {
        console.error('Error loading conflicts:', error);
        showError(conflictsContent, error.message);
    }
}

// Display conflicts
function displayConflicts(data) {
    if (!data.conflicts || data.conflicts.length === 0) {
        conflictsContent.innerHTML = `
            <div class="empty-state">
                <h2>No Conflicts Found</h2>
                <p>All availability slots look clean for the next 120 days.</p>
                <p style="margin-top: 8px; font-size: 12px; color: #888;">
                    Last checked: ${escapeHtml(data.last_checked || 'N/A')}<br>
                    Range: ${escapeHtml(data.date_range || 'N/A')}
                </p>
            </div>
        `;
        return;
    }

    // Group conflicts by groomer
    const byGroomer = {};
    data.conflicts.forEach(c => {
        if (!byGroomer[c.groomer]) {
            byGroomer[c.groomer] = [];
        }
        byGroomer[c.groomer].push(c);
    });

    // Count unacknowledged
    let totalUnacked = 0;
    data.conflicts.forEach(c => {
        if (!acknowledgedConflicts[getConflictKey(c)]) totalUnacked++;
    });

    let html = `
        <div class="conflicts-header">
            <div class="conflicts-summary">
                <span class="conflict-total-badge${totalUnacked === 0 ? ' all-clear' : ''}">
                    ${totalUnacked === 0 ? 'All clear' : `${totalUnacked} conflict${totalUnacked !== 1 ? 's' : ''} to review`}
                </span>
                <span class="conflicts-range">${escapeHtml(data.date_range)}</span>
            </div>
        </div>
    `;

    for (const [groomer, conflicts] of Object.entries(byGroomer)) {
        const groomerUnacked = conflicts.filter(c => !acknowledgedConflicts[getConflictKey(c)]).length;

        // Sort: unacknowledged first, then acknowledged
        const sorted = [...conflicts].sort((a, b) => {
            const aAck = acknowledgedConflicts[getConflictKey(a)] ? 1 : 0;
            const bAck = acknowledgedConflicts[getConflictKey(b)] ? 1 : 0;
            return aAck - bAck;
        });

        html += `
            <div class="conflict-groomer-section">
                <div class="conflict-groomer-header">
                    <span class="conflict-groomer-name">${escapeHtml(groomer)}</span>
                    <span class="conflict-groomer-count${groomerUnacked === 0 ? ' all-clear' : ''}">${groomerUnacked}</span>
                </div>
        `;

        sorted.forEach(conflict => {
            const conflictKey = getConflictKey(conflict);
            const isAcked = !!acknowledgedConflicts[conflictKey];

            let conflictItems = '';
            conflict.conflicts_with.forEach(c => {
                const serviceClass = c.service.toLowerCase();
                conflictItems += `
                    <div class="conflict-appt">
                        <span class="conflict-appt-time">${escapeHtml(c.time_display)}</span>
                        <span class="conflict-appt-pet">${escapeHtml(c.pet_name)}</span>
                        <span class="conflict-appt-client">(${escapeHtml(c.client)})</span>
                        <span class="day-appt-service ${serviceClass}">${escapeHtml(c.service)}</span>
                    </div>
                `;
            });

            html += `
                <div class="conflict-item${isAcked ? ' acknowledged' : ''}" data-conflict-key="${conflictKey}">
                    <div class="conflict-item-header">
                        <div class="conflict-date-info">
                            <label class="conflict-ack-label">
                                <input type="checkbox" class="conflict-ack-checkbox"
                                    data-key="${conflictKey}" ${isAcked ? 'checked' : ''}>
                                <span class="conflict-date">${formatDate(conflict.date)}</span>
                                <span class="conflict-day">${escapeHtml(conflict.day_of_week)}</span>
                            </label>
                        </div>
                        <span class="conflict-slot-badge">${escapeHtml(conflict.slot_display)} shows open</span>
                    </div>
                    <div class="conflict-details${isAcked ? ' collapsed' : ''}">
                        <div class="conflict-explanation">A 90-min booking here would overlap:</div>
                        ${conflictItems}
                    </div>
                    ${isAcked ? '<div class="conflict-ack-stamp">OK</div>' : ''}
                </div>
            `;
        });

        html += '</div>';
    }

    conflictsContent.innerHTML = html;

    // Add checkbox handlers
    document.querySelectorAll('.conflict-ack-checkbox').forEach(cb => {
        cb.addEventListener('change', async (e) => {
            e.stopPropagation();
            const key = cb.getAttribute('data-key');
            const item = cb.closest('.conflict-item');
            await toggleAcknowledged(key, cb.checked);
            item.classList.toggle('acknowledged', cb.checked);

            // Toggle OK stamp and collapse details
            let stamp = item.querySelector('.conflict-ack-stamp');
            const details = item.querySelector('.conflict-details');
            if (cb.checked) {
                if (!stamp) {
                    stamp = document.createElement('div');
                    stamp.className = 'conflict-ack-stamp';
                    stamp.textContent = 'OK';
                    item.appendChild(stamp);
                }
                if (details) details.classList.add('collapsed');
            } else {
                if (stamp) stamp.remove();
                if (details) details.classList.remove('collapsed');
            }
        });
    });
}

// ===== Noah-bot Chat Functions =====

function getChatInner() {
    let inner = chatMessages.querySelector('.chat-messages-inner');
    if (!inner) {
        inner = document.createElement('div');
        inner.className = 'chat-messages-inner';
        chatMessages.appendChild(inner);
    }
    return inner;
}

function appendChatBubble(role, text) {
    const inner = getChatInner();
    const bubble = document.createElement('div');
    bubble.className = `chat-bubble ${role}`;
    bubble.textContent = text;
    inner.appendChild(bubble);
    chatMessages.scrollTop = chatMessages.scrollHeight;
    return bubble;
}

// ── KCApp operator detection ──────────────────────────────────────────────────
// Cache: { name, fetchedAt } — refreshed every 5 minutes
let _kcappOperatorCache = null;

async function getKCAppOperator() {
    const CACHE_MS = 5 * 60 * 1000;
    if (_kcappOperatorCache && (Date.now() - _kcappOperatorCache.fetchedAt) < CACHE_MS) {
        return _kcappOperatorCache.name;
    }

    let name = 'Unknown';
    try {
        const tabs = await chrome.tabs.query({ url: 'https://dbfcm.mykcapp.com/*' });
        if (tabs.length) {
            const [injection] = await chrome.scripting.executeScript({
                target: { tabId: tabs[0].id },
                world: 'MAIN',
                func: async () => {
                    // Approach 1: try KCApp's session endpoint
                    try {
                        const r = await fetch('/api/user/current', { credentials: 'include' });
                        if (r.ok) {
                            const d = await r.json();
                            const n = d?.FullName || d?.Name || d?.UserName || d?.ReturnedObject?.FullName;
                            if (n) return n;
                        }
                    } catch (_) {}

                    // Approach 2: try /Account/GetCurrentUser (KC7 MVC pattern)
                    try {
                        const r = await fetch('/Account/GetCurrentUser', { credentials: 'include' });
                        if (r.ok) {
                            const d = await r.json();
                            const n = d?.FullName || d?.Name || d?.ReturnedObject?.FullName;
                            if (n) return n;
                        }
                    } catch (_) {}

                    // Approach 3: read from DOM — KC7 nav bar typically shows employee name
                    const selectors = [
                        '.navbar-right .username',
                        '.navbar .user-fullname',
                        '.nav-user-name',
                        '#user-display-name',
                        '.current-user',
                        '[data-user-name]',
                        '.navbar-right a.dropdown-toggle',
                        '.user-info .name',
                        '.header-user',
                        '#loggedInUser',
                        '.logged-in-as',
                        'li.dropdown > a[data-toggle="dropdown"]',
                    ];
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el) {
                            const txt = (el.dataset.userName || el.textContent || '').trim()
                                .replace(/\s+/g, ' ');
                            // Reject generic labels like "Account", "Menu", "Admin"
                            if (txt && txt.length > 2 && txt.length < 60
                                    && !['account','menu','admin','home','logout'].includes(txt.toLowerCase())) {
                                return txt;
                            }
                        }
                    }

                    // Approach 4: check window globals that KC7 may expose
                    const globals = ['currentUser', 'loggedInUser', 'employeeInfo', 'userInfo', 'AppUser'];
                    for (const g of globals) {
                        const v = window[g];
                        if (v) {
                            const n = (typeof v === 'string') ? v
                                : (v.FullName || v.Name || v.UserName || v.name || '');
                            if (n && n.length > 1) return n;
                        }
                    }

                    return null;
                }
            });
            if (injection?.result) name = injection.result;
        }
    } catch (e) {
        console.warn('[Know-a-bot] Could not detect KCApp operator:', e.message);
    }

    _kcappOperatorCache = { name, fetchedAt: Date.now() };
    updateOperatorBar(name);
    return name;
}

function updateOperatorBar(name) {
    const bar = document.getElementById('chat-operator-bar');
    const label = document.getElementById('chat-operator-label');
    if (!bar || !label) return;
    bar.classList.remove('operator-noah', 'operator-unknown');
    if (name === 'Unknown') {
        label.textContent = 'Logged in as: Unknown (open KCApp to identify)';
        bar.classList.add('operator-unknown');
    } else if (name.toLowerCase().includes('noah') || name.toLowerCase().includes('han')) {
        label.textContent = `Logged in as: ${name} (owner — full access)`;
        bar.classList.add('operator-noah');
    } else {
        label.textContent = `Logged in as: ${name}`;
    }
}

async function sendChatMessage() {
    const message = chatInput.value.trim();
    if (!message) return;

    chatInput.value = '';
    chatSendBtn.disabled = true;

    appendChatBubble('user', message);

    const thinkingBubble = appendChatBubble('thinking', 'Know-a-bot is thinking…');

    // Detect operator — run concurrently with the thinking display
    const operator_name = await getKCAppOperator();

    try {
        const response = await fetch(`${config.backendUrl}/api/chat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message, operator_name }),
            signal: AbortSignal.timeout(300000)  // 5 min — Claude CLI can be slow on first call
        });

        const data = await response.json();

        thinkingBubble.remove();

        if (data.success) {
            appendChatBubble('bot', data.reply);
        } else {
            appendChatBubble('error-msg', 'Error: ' + (data.error || 'Unknown error'));
        }
    } catch (error) {
        thinkingBubble.remove();
        appendChatBubble('error-msg', 'Error: ' + error.message);
    } finally {
        chatSendBtn.disabled = false;
        chatInput.focus();
    }
}

async function resetChat() {
    try {
        await fetch(`${config.backendUrl}/api/chat/reset`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
        });
    } catch (e) {
        // Best-effort — backend may not be running, that's fine
    }

    // Clear messages and show reset notice
    const inner = document.createElement('div');
    inner.className = 'chat-messages-inner';
    chatMessages.innerHTML = '';
    chatMessages.appendChild(inner);

    const systemBubble = document.createElement('div');
    systemBubble.className = 'chat-bubble system-msg';
    systemBubble.textContent = 'Conversation reset. Start a new question!';
    inner.appendChild(systemBubble);

    chatInput.focus();
}

async function restartBackend() {
    const btn = document.getElementById('restart-backend-btn');
    btn.textContent = '↺ Restarting…';
    btn.classList.add('working');
    btn.disabled = true;
    try {
        await fetch(`${config.backendUrl}/api/restart`, { method: 'POST',
            headers: { 'Content-Type': 'application/json' }, body: '{}' });
    } catch (e) {
        // Expected — server drops the connection as it exits
    }
    // Poll until backend is back up (up to 15s)
    let attempts = 0;
    const poll = setInterval(async () => {
        attempts++;
        try {
            await fetch(`${config.backendUrl}/api/groomers`);
            clearInterval(poll);
            btn.textContent = '✓ Backend ready';
            btn.classList.remove('working');
            setTimeout(() => {
                btn.textContent = '↺ Restart Backend';
                btn.disabled = false;
            }, 2000);
        } catch (e) {
            if (attempts >= 30) {
                clearInterval(poll);
                btn.textContent = '✗ Timed out';
                btn.classList.remove('working');
                setTimeout(() => { btn.textContent = '↺ Restart Backend'; btn.disabled = false; }, 3000);
            }
        }
    }, 500);
}

async function checkBackendStatus() {
    try {
        const response = await fetch(`${config.backendUrl}/api/groomers`);
        if (response.ok) {
            alert('✅ Backend Server Status: RUNNING\n\n' +
                  'The backend server is running and responding.\n\n' +
                  'To stop: Close the backend server window or press Ctrl+C');
        }
    } catch (error) {
        const instructions =
            '❌ Backend Server Status: STOPPED\n\n' +
            'The backend server is not running.\n\n' +
            'To start the backend:\n\n' +
            'Windows:\n' +
            '  • Double-click: start_backend.bat\n' +
            '  • Keep the window open\n\n' +
            'Linux/Mac:\n' +
            '  • Run: python3 backend_server.py\n' +
            '  • Keep the terminal open\n\n' +
            'The server must be running for the extension to work.';

        if (confirm(instructions + '\n\nOpen backend folder?')) {
            alert('Backend files location:\n\n' +
                  'Check where you installed the waitlist_extension folder.\n' +
                  'Look for start_backend.bat or backend_server.py');
        }
    }
}

// ── Checkout Tab ──────────────────────────────────────────────────────────────

document.getElementById('checkout-refresh-btn').addEventListener('click', loadCheckout);

async function loadCheckout() {
    const container = document.getElementById('checkout-content');
    showLoading(container, 'Loading today\'s appointments…');
    try {
        const resp = await fetch(`${config.backendUrl}/api/checkout/today`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        renderCheckout(data.clients || []);
    } catch (e) {
        container.innerHTML = `<div class="empty-state"><p style="color:#c00">Could not reach backend: ${escapeHtml(e.message)}</p></div>`;
    }
}

function renderCheckout(clients) {
    const container = document.getElementById('checkout-content');
    if (clients.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <h2>All done!</h2>
                <p>No remaining appointments for today.</p>
            </div>`;
        return;
    }

    container.innerHTML = '';
    clients.forEach(client => {
        const card = document.createElement('div');
        card.className = 'checkout-card';

        // Pets list
        const petsHtml = (client.pets || []).map(p => `
            <div class="checkout-pet-row">
                <span>${p.done ? '✅' : '🔄'}</span>
                <span class="checkout-pet-name">${escapeHtml(p.name)}</span>
                ${p.groomer ? `<span class="checkout-pet-groomer">→ ${escapeHtml(p.groomer)}</span>` : ''}
                ${p.done ? '<span class="checkout-done-badge">Ready</span>' : ''}
            </div>`).join('');

        // Cards on file
        let cardsHtml = '';
        if (client.cards && client.cards.length > 0) {
            cardsHtml = client.cards.map(c => {
                const label = (c.desc && c.desc.trim()) ? escapeHtml(c.desc) : `****${escapeHtml(c.last4)}`;
                return `<div class="checkout-card-row">
                    <span class="checkout-card-icon">💳</span>
                    <span class="checkout-card-value">${label}</span>
                </div>`;
            }).join('');
        } else {
            cardsHtml = `<div class="checkout-card-row"><span class="checkout-no-card">No card on file</span></div>`;
        }

        // Tip info
        let tipHtml = '';
        const avgPct  = client.avg_tip_pct  != null ? client.avg_tip_pct  : null;
        const avgAmt  = client.avg_tip_amt  != null ? client.avg_tip_amt  : null;
        const lastPct = client.last_tip_pct != null ? client.last_tip_pct : null;
        const lastAmt = client.last_tip_amt != null ? client.last_tip_amt : null;
        const method  = client.tip_method  || null;

        const hasTip = (avgPct != null || lastPct != null);
        if (hasTip) {
            const methodBadge = method ? `<span class="checkout-tip-method-badge">${escapeHtml(method)}</span>` : '';
            const avgLine = avgPct != null
                ? `<div class="checkout-tip-item">
                    <span class="checkout-tip-label">Avg tip</span>
                    <span class="checkout-tip-value">${Math.round(avgPct)}%${avgAmt != null ? ` ($${parseFloat(avgAmt).toFixed(0)})` : ''}${methodBadge}</span>
                   </div>`
                : '';
            const lastLine = lastPct != null
                ? `<div class="checkout-tip-item">
                    <span class="checkout-tip-label">Last</span>
                    <span class="checkout-tip-value">${Math.round(lastPct)}%${lastAmt != null ? ` ($${parseFloat(lastAmt).toFixed(0)})` : ''}</span>
                   </div>`
                : '';
            tipHtml = `<div class="checkout-tip-row">${avgLine}${lastLine}</div>`;
        }

        // Cadence / day preference
        let cadenceHtml = '';
        const prefDay  = client.preferred_day   || null;
        const cadWeeks = client.avg_cadence_days != null ? Math.round(client.avg_cadence_days / 7) : null;
        if (prefDay || cadWeeks) {
            const parts = [];
            if (prefDay)   parts.push(`Prefers ${escapeHtml(prefDay)}`);
            if (cadWeeks)  parts.push(`every ~${cadWeeks} wks`);
            cadenceHtml = `<div class="checkout-cadence-row">${parts.join('  ·  ')}</div>`;
        }

        // Next appointment + conflict
        let nextApptHtml = '';
        const count = client.future_appt_count || 0;
        const conflict = client.has_conflict;
        if (count > 0) {
            const countLabel = `${count} appt${count !== 1 ? 's' : ''} booked`;
            const nextLabel = client.next_appt ? ` · next ${escapeHtml(client.next_appt)}` : '';
            const conflictFlag = conflict ? ' <span class="checkout-conflict-badge">⚠️ CONFLICT</span>' : '';
            nextApptHtml = `<div class="checkout-cadence-row checkout-next-appt">${escapeHtml(countLabel)}${nextLabel}${conflictFlag}</div>`;
        } else {
            const suggestHtml = client.suggested_next
                ? `<div class="checkout-suggest-box">Suggest booking around <span class="checkout-suggest-date">${escapeHtml(client.suggested_next)}</span>${client.preferred_day ? ` (${escapeHtml(client.preferred_day)})` : ''}</div>`
                : '';
            nextApptHtml = `<div class="checkout-cadence-row checkout-no-appt">⚠️ No future appointment booked</div>${suggestHtml}`;
        }

        const hasFinancial = (client.cards && client.cards.length > 0) || hasTip || (prefDay || cadWeeks);

        card.innerHTML = `
            <div class="checkout-card-header">
                <span class="checkout-card-time">${escapeHtml(client.in_time || '')}</span>
                <span class="checkout-card-client">${escapeHtml(client.client_name)}</span>
            </div>
            <div class="checkout-card-body">
                ${petsHtml}
                ${hasFinancial ? '<hr class="checkout-divider">' : ''}
                <div class="checkout-card-info">
                    ${cardsHtml}
                    ${tipHtml}
                    ${cadenceHtml}
                    ${nextApptHtml}
                </div>
            </div>`;

        container.appendChild(card);
    });
}

// ── SMS Draft+Approve ─────────────────────────────────────────────────────────

document.getElementById('sms-refresh-btn').addEventListener('click', loadSmsDrafts);

// Compose bar
const smsComposeInput = document.getElementById('sms-compose-input');
const smsComposeBtn   = document.getElementById('sms-compose-btn');
const smsComposeStatus = document.getElementById('sms-compose-status');

smsComposeBtn.addEventListener('click', () => composeSmsMessage());
smsComposeInput.addEventListener('keydown', e => { if (e.key === 'Enter') composeSmsMessage(); });

async function composeSmsMessage() {
    const instruction = smsComposeInput.value.trim();
    if (!instruction) { smsComposeInput.focus(); return; }
    smsComposeBtn.disabled = true;
    smsComposeBtn.textContent = '…';
    smsComposeStatus.textContent = 'Drafting…';
    smsComposeStatus.className = 'sms-compose-status';
    try {
        const resp = await fetch(`${config.backendUrl}/api/sms/compose`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ instruction })
        });
        const data = await resp.json();
        if (data.success) {
            smsComposeInput.value = '';
            smsComposeStatus.textContent = `✓ Drafted for ${data.client_name}`;
            setTimeout(() => smsComposeStatus.classList.add('hidden'), 3000);
            await loadSmsDrafts();
        } else {
            smsComposeStatus.textContent = 'Error: ' + (data.error || 'Unknown error');
            smsComposeStatus.className = 'sms-compose-status error';
        }
    } catch (e) {
        smsComposeStatus.textContent = 'Could not reach backend: ' + e.message;
        smsComposeStatus.className = 'sms-compose-status error';
    } finally {
        smsComposeBtn.disabled = false;
        smsComposeBtn.textContent = 'Draft';
    }
}

async function loadSmsDrafts() {
    const container = document.getElementById('sms-content');
    container.innerHTML = '<div class="loading"><div class="spinner"></div><p>Loading drafts…</p></div>';
    try {
        const resp = await fetch(`${config.backendUrl}/api/sms/drafts`);
        const data = await resp.json();
        renderSmsDrafts(data.drafts || []);
    } catch (e) {
        container.innerHTML = `<div class="empty-state"><p style="color:#c00">Could not reach backend: ${e.message}</p></div>`;
    }
}

function updateSmsBadge(count) {
    const badge = document.getElementById('sms-badge');
    if (count > 0) {
        badge.textContent = count;
        badge.classList.remove('hidden');
    } else {
        badge.classList.add('hidden');
    }
}

function renderSmsDrafts(drafts) {
    const container = document.getElementById('sms-content');
    updateSmsBadge(drafts.length);

    if (drafts.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <h2>No pending drafts</h2>
                <p>When clients text in, Claude will draft replies here for your review.</p>
            </div>`;
        return;
    }

    container.innerHTML = '';
    drafts.forEach(d => {
        const card = document.createElement('div');
        card.className = 'sms-card';
        card.dataset.draftId = d.draft_id;

        const ts = d.timestamp ? d.timestamp.replace('T', ' ').slice(0, 16) : '';

        // Build prior thread HTML (messages before the trigger)
        const priorMsgs = d.recent_conversation || [];
        const threadHtml = priorMsgs.length > 0
            ? `<div class="sms-thread">${priorMsgs.map(m => {
                const isUs = m.startsWith('Us:');
                const text = m.replace(/^(Us|Client):\s*/, '');
                const label = isUs ? 'Us' : escapeHtml(d.client_name.split(' ')[0]);
                return `<div class="sms-thread-msg ${isUs ? 'us' : ''}">${label}: ${escapeHtml(text)}</div>`;
              }).join('')}</div>`
            : '';

        // Inbound trigger section (hidden for outbound-initiated drafts)
        const inboundHtml = d.their_message
            ? `<div class="sms-inbound-label">They wrote:</div>
               <div class="sms-inbound">${escapeHtml(d.their_message)}</div>`
            : '';

        // Build mini dossier HTML
        let dossierHtml = '';
        if (d.dossier) {
            const dos = d.dossier;
            const parts = [];

            // NEW CLIENT badge
            if (dos.is_new_client) {
                parts.push(`<div class="sms-dossier-new">★ NEW CLIENT</div>`);
            }

            // Warning
            if (dos.warning) {
                parts.push(`<div class="sms-dossier-warning">⚠ ${escapeHtml(dos.warning)}</div>`);
            }

            // Per-pet rows: Name · Breed · Size/Coat · Age · service · groomer · N wks ago
            (dos.pets || []).forEach(pet => {
                const wks  = pet.weeks_since != null ? pet.weeks_since : null;
                const over = wks != null && wks >= 8;

                // Build descriptor: e.g. "Golden Retriever · LG/LH · 4y"
                const descriptors = [];
                if (pet.breed_name) descriptors.push(escapeHtml(pet.breed_name));
                const sizeCoat = [pet.size, pet.coat].filter(Boolean).join('/');
                if (sizeCoat) descriptors.push(sizeCoat);
                if (pet.age)  descriptors.push(escapeHtml(pet.age));
                const desc = descriptors.length ? ` <span class="sms-dossier-breed">${descriptors.join(' · ')}</span>` : '';

                const svc = pet.service ? ` · ${escapeHtml(pet.service)}` : '';
                const grm = pet.groomer ? ` · ${escapeHtml(pet.groomer)}` : '';
                const age = wks != null
                    ? ` · <span class="${over ? 'sms-dossier-overdue' : ''}">${wks} wks ago</span>`
                    : '';
                parts.push(`<div class="sms-dossier-pet">${escapeHtml(pet.name)}${desc}${svc}${grm}${age}</div>`);
            });

            // Booking / future appts row
            const bookingParts = [];
            if (dos.future_count > 0) {
                bookingParts.push(`${dos.future_count} future booked`);
            }
            if (dos.next_appt) {
                bookingParts.push(`Next: ${escapeHtml(dos.next_appt)}`);
            }
            if (dos.suggested_next) {
                bookingParts.push(`Suggest: ${escapeHtml(dos.suggested_next)}`);
            }
            if (bookingParts.length) {
                parts.push(`<div class="sms-dossier-booking">${bookingParts.join('  ·  ')}</div>`);
            }

            // Cadence / preferences row
            const cadParts = [];
            if (dos.avg_cadence_days) {
                cadParts.push(`~${Math.round(dos.avg_cadence_days / 7)} wks cadence`);
            }
            if (dos.preferred_day) {
                cadParts.push(`prefers ${escapeHtml(dos.preferred_day)}`);
            }
            if (dos.preferred_time) {
                cadParts.push(escapeHtml(dos.preferred_time));
            }
            if (dos.last_visit) {
                cadParts.push(`last ${escapeHtml(dos.last_visit)}`);
            }
            if (cadParts.length) {
                parts.push(`<div class="sms-dossier-cadence">${cadParts.join('  ·  ')}</div>`);
            }

            if (parts.length) {
                dossierHtml = `<div class="sms-dossier">${parts.join('')}</div>`;
            }
        }

        card.innerHTML = `
            <div class="sms-card-header">
                <span>${escapeHtml(d.client_name)}</span>
                <span class="sms-timestamp">${escapeHtml(ts)}</span>
            </div>
            ${dossierHtml}
            ${threadHtml}
            ${inboundHtml}
            <div class="sms-draft-label">${d.draft ? 'Draft:' : 'Compose reply:'}</div>
            <textarea class="sms-draft-textarea" rows="3">${escapeHtml(d.draft)}</textarea>
            <div class="sms-feedback-row">
                <input type="text" class="sms-feedback-input" placeholder="Feedback for Claude (e.g. make it shorter, offer Sat instead)…">
                <button class="sms-btn-regen">↺ Regen</button>
            </div>
            <div class="sms-card-actions">
                <button class="sms-btn-dismiss">Dismiss</button>
                <button class="sms-btn-book">Book Appt</button>
                <button class="sms-btn-send">Send</button>
            </div>`;

        card.querySelector('.sms-btn-send').addEventListener('click', async () => {
            const textarea = card.querySelector('.sms-draft-textarea');
            const message = textarea.value.trim();
            if (!message) { alert('Message is empty.'); return; }
            await sendSmsDraft(d.draft_id, d.client_id, d.phone, message, card);
        });

        card.querySelector('.sms-btn-regen').addEventListener('click', async () => {
            const feedbackInput = card.querySelector('.sms-feedback-input');
            const feedback = feedbackInput.value.trim();
            if (!feedback) { feedbackInput.focus(); feedbackInput.placeholder = 'Enter feedback first…'; return; }
            await regenerateSmsDraft(d.draft_id, feedback, card);
            feedbackInput.value = '';
        });

        card.querySelector('.sms-btn-book').addEventListener('click', () => {
            openBookingModal(d.draft_id, d.client_id);
        });

        card.querySelector('.sms-btn-dismiss').addEventListener('click', async () => {
            await dismissSmsDraft(d.draft_id, card);
        });

        container.appendChild(card);
    });
}

async function sendSmsDraft(draftId, clientId, phone, message, cardEl) {
    const sendBtn = cardEl.querySelector('.sms-btn-send');
    sendBtn.disabled = true;
    sendBtn.textContent = 'Sending…';

    // Find an open KCApp tab — the injected function runs in that tab's context,
    // so KCApp session cookies are included automatically (no cookie API needed).
    const tabs = await chrome.tabs.query({ url: 'https://dbfcm.mykcapp.com/*' });
    if (!tabs.length) {
        alert('Please open the KCApp calendar (or any KCApp tab) and try again.');
        sendBtn.disabled = false;
        sendBtn.textContent = 'Send';
        return;
    }

    // POST the SMS directly from the KCApp tab context
    let kcappResult;
    try {
        const [injection] = await chrome.scripting.executeScript({
            target: { tabId: tabs[0].id },
            world: 'MAIN',
            func: async (phoneNumber, msg, cid) => {
                const fd = new FormData();
                fd.append('phoneNumber', String(phoneNumber));
                fd.append('Message', msg);
                fd.append('MediaLinks', '');
                fd.append('ClientId', String(cid));
                fd.append('MessageId', '0');
                const r = await fetch('/SMS/SMSSendFromFront', {
                    method: 'POST',
                    headers: { 'x-requested-with': 'XMLHttpRequest' },
                    body: fd,
                    credentials: 'include'
                });
                return r.json();
            },
            args: [phone, message, clientId || 0]
        });
        kcappResult = injection.result;
    } catch (e) {
        alert('Send failed: ' + e.message);
        sendBtn.disabled = false;
        sendBtn.textContent = 'Send';
        return;
    }

    if (!kcappResult || kcappResult.Status !== 1) {
        alert('KCApp rejected the message: ' + (kcappResult?.Message || JSON.stringify(kcappResult)));
        sendBtn.disabled = false;
        sendBtn.textContent = 'Send';
        return;
    }

    const newMessageId = (kcappResult.ReturnedObject || {}).MessageId;

    // Tell backend to attribute the message to Claude and clean up the draft
    try {
        const resp = await fetch(`${config.backendUrl}/api/sms/post-send`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ draft_id: draftId, kcapp_message_id: newMessageId, client_id: clientId })
        });
        const data = await resp.json();
        if (data.success) {
            cardEl.remove();
            const remaining = document.querySelectorAll('.sms-card').length;
            updateSmsBadge(remaining);
            if (remaining === 0) {
                document.getElementById('sms-content').innerHTML = `
                    <div class="empty-state"><h2>No pending drafts</h2>
                    <p>When clients text in, Claude will draft replies here for your review.</p></div>`;
            }
        } else {
            alert('Sent OK but cleanup failed: ' + (data.error || 'Unknown error'));
            cardEl.remove();
        }
    } catch (e) {
        // Message was sent — just log the cleanup failure, don't block the user
        console.error('post-send cleanup error:', e);
        cardEl.remove();
    }
}

async function dismissSmsDraft(draftId, cardEl) {
    try {
        await fetch(`${config.backendUrl}/api/sms/dismiss`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ draft_id: draftId })
        });
    } catch (e) {
        // best-effort
    }
    cardEl.remove();
    const remaining = document.querySelectorAll('.sms-card').length;
    updateSmsBadge(remaining);
    if (remaining === 0) {
        document.getElementById('sms-content').innerHTML = `
            <div class="empty-state"><h2>No pending drafts</h2>
            <p>When clients text in, Claude will draft replies here for your review.</p></div>`;
    }
}

async function regenerateSmsDraft(draftId, feedback, cardEl) {
    const regenBtn = cardEl.querySelector('.sms-btn-regen');
    const textarea = cardEl.querySelector('.sms-draft-textarea');
    regenBtn.disabled = true;
    regenBtn.textContent = '…';
    try {
        const resp = await fetch(`${config.backendUrl}/api/sms/regen`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ draft_id: draftId, feedback })
        });
        const data = await resp.json();
        if (data.success && data.draft) {
            textarea.value = data.draft;
            textarea.style.borderColor = '#2563eb';
            setTimeout(() => textarea.style.borderColor = '', 1200);
        } else {
            alert('Regen failed: ' + (data.error || 'Unknown error'));
        }
    } catch (e) {
        alert('Regen error: ' + e.message);
    } finally {
        regenBtn.disabled = false;
        regenBtn.textContent = '↺ Regen';
    }
}

// ── SMS → Appointment Booking ─────────────────────────────────────────────────

function openBookingModal(draftId, clientId) {
    const modal = document.getElementById('booking-modal');
    const body  = document.getElementById('booking-modal-body');
    body.innerHTML = '<div class="booking-extracting">Extracting details from conversation… (may take ~10s)</div>';
    modal.classList.remove('hidden');

    // Wire close button
    document.getElementById('booking-modal-close').onclick = () => {
        modal.classList.add('hidden');
    };

    // Call backend to extract details
    fetch(`${config.backendUrl}/api/sms/extract-appt`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ draft_id: draftId, client_id: clientId })
    })
    .then(r => r.json())
    .then(data => {
        if (!data.success) {
            body.innerHTML = `<div class="booking-error">Extraction failed: ${escapeHtml(data.error || 'unknown')}</div>`;
            return;
        }
        renderBookingForm(body, draftId, clientId, data);
    })
    .catch(e => {
        body.innerHTML = `<div class="booking-error">Error: ${escapeHtml(e.message)}</div>`;
    });
}

function renderBookingForm(container, draftId, clientId, extracted) {
    const pets     = extracted.pets     || [];
    const groomers = extracted.groomers || [];
    const ex       = extracted.extracted || {};

    const petOptions = pets.map(p =>
        `<option value="${p.id}" ${extracted.pet_id === p.id ? 'selected' : ''}>${escapeHtml(p.name)}</option>`
    ).join('');

    const groomerOptions = groomers.map(g =>
        `<option value="${g.id}" ${extracted.groomer_id === g.id ? 'selected' : ''}>${escapeHtml(g.name)}</option>`
    ).join('');

    const TIME_OPTIONS = ['08:30','10:00','11:30','13:30','14:30'].map(t =>
        `<option value="${t}" ${ex.time === t ? 'selected' : ''}>${t}</option>`
    ).join('');

    const SVC_OPTIONS = [
        ['full',      'Full groom'],
        ['bath_only', 'Bath only'],
        ['handstrip', 'Handstrip'],
    ].map(([val, lbl]) =>
        `<option value="${val}" ${ex.service_type === val ? 'selected' : ''}>${lbl}</option>`
    ).join('');

    container.innerHTML = `
        <div class="booking-form">
            <div class="booking-row">
                <label>Pet</label>
                <select id="bk-pet">${petOptions || '<option value="">— no pets found —</option>'}</select>
            </div>
            <div class="booking-row">
                <label>Date</label>
                <input type="date" id="bk-date" value="${escapeHtml(ex.date || '')}">
            </div>
            <div class="booking-row">
                <label>Time</label>
                <select id="bk-time">${TIME_OPTIONS}</select>
            </div>
            <div class="booking-row">
                <label>Groomer</label>
                <select id="bk-groomer">${groomerOptions}</select>
            </div>
            <div class="booking-row">
                <label>Service</label>
                <select id="bk-service">${SVC_OPTIONS}</select>
            </div>
            <div class="booking-actions">
                <button id="bk-cancel-btn" class="bk-btn-cancel">Cancel</button>
                <button id="bk-confirm-btn" class="bk-btn-confirm">Book</button>
            </div>
            <div id="bk-status" class="bk-status"></div>
        </div>`;

    document.getElementById('bk-cancel-btn').onclick = () => {
        document.getElementById('booking-modal').classList.add('hidden');
    };

    document.getElementById('bk-confirm-btn').onclick = async () => {
        const petId      = parseInt(document.getElementById('bk-pet').value);
        const date       = document.getElementById('bk-date').value;
        const time       = document.getElementById('bk-time').value;
        const groomerId  = parseInt(document.getElementById('bk-groomer').value);
        const service    = document.getElementById('bk-service').value;
        const status     = document.getElementById('bk-status');
        const confirmBtn = document.getElementById('bk-confirm-btn');

        if (!petId || !date || !time || !groomerId) {
            status.textContent = 'Please fill in all fields.';
            status.className = 'bk-status bk-error';
            return;
        }

        confirmBtn.disabled = true;
        confirmBtn.textContent = 'Booking…';
        status.textContent = '';

        try {
            const resp = await fetch(`${config.backendUrl}/api/appt/book`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pet_id: petId, date, time, groomer_id: groomerId, service_type: service })
            });
            const data = await resp.json();
            if (data.success) {
                status.textContent = `Booked! Appointment #${data.glseq}`;
                status.className = 'bk-status bk-success';
                confirmBtn.textContent = 'Done';
                setTimeout(() => document.getElementById('booking-modal').classList.add('hidden'), 1800);
            } else {
                status.textContent = 'Error: ' + (data.error || 'unknown');
                status.className = 'bk-status bk-error';
                confirmBtn.disabled = false;
                confirmBtn.textContent = 'Book';
            }
        } catch (e) {
            status.textContent = 'Error: ' + e.message;
            status.className = 'bk-status bk-error';
            confirmBtn.disabled = false;
            confirmBtn.textContent = 'Book';
        }
    };
}

