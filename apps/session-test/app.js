import { SessionCore } from '../dist/lib/session-core.js';

// Configuration for testing
const config = {
    idleTimeoutMs: 5000,      // 5 seconds idle
    absoluteTimeoutMs: 15000, // 15 seconds absolute expiry
    storageKey: 'session_test_key'
};

const session = new SessionCore(config);
const statusEl = document.getElementById('status');
const logList = document.getElementById('logList');

function log(msg) {
    const li = document.createElement('li');
    li.textContent = `[${new Date().toISOString().split('T')[1].slice(0, 8)}] ${msg}`;
    logList.prepend(li);
}

// Subscribe to state changes
session.observe().subscribe(state => {
    log(`State changed to: ${state}`);
    statusEl.textContent = state;

    // Remove all classes then add current state class
    statusEl.className = 'status-indicator';
    statusEl.classList.add(state);
});

// Button handlers
document.getElementById('logoutBtn').addEventListener('click', () => {
    log('Manual Logout clicked');
    session.logout();
});

document.getElementById('resetOnlyBtn').addEventListener('click', () => {
    // This is just to test manual interaction if needed, though mouse movement does this automatically
    log('Manual Reset/Activity simulated');
});

// Cleanup on unload (optional for test)
window.addEventListener('beforeunload', () => {
    session.destroy();
});
