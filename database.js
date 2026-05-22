const fs = require('fs');
const path = require('path');

const dbDir = './database';
const backupDir = './database/backups';

const files = {
    settings: path.join(dbDir, 'settings.json'),
    history: path.join(dbDir, 'history.json'),
    logs: path.join(dbDir, 'logs.json'),
    visitor: path.join(dbDir, 'visitor.json'),
    wa_sessions: path.join(dbDir, 'wa_sessions.json'),
};

// Auto create folder database
if (!fs.existsSync(dbDir)) fs.mkdirSync(dbDir, { recursive: true });
if (!fs.existsSync(backupDir)) fs.mkdirSync(backupDir, { recursive: true });

const defaultData = {
    settings: { 
        check_delay: 1000, cooldown_time: 10000, 
        active_email: 'your_email@gmail.com', active_pass: 'your_app_pass', 
        tg_channel: 'https://t.me/ADifixredChannel', tg_group: 'https://t.me/ADifixredGroup',
        auth_password: '', maintenance_mode: false, max_wa_sessions: 5
    },
    history: [], logs: [], visitor: [], wa_sessions: []
};

Object.keys(files).forEach(key => {
    if (!fs.existsSync(files[key])) {
        fs.writeFileSync(files[key], JSON.stringify(defaultData[key] || [], null, 2), 'utf8');
    }
});

function load(fileKey) {
    try { return JSON.parse(fs.readFileSync(files[fileKey], 'utf8')); } 
    catch (e) { return defaultData[fileKey] || {}; }
}

function save(fileKey, data) {
    try { fs.writeFileSync(files[fileKey], JSON.stringify(data, null, 2), 'utf8'); } 
    catch (e) { console.error(`Save error ${fileKey}:`, e.message); }
}

function getSetting(key) { return load('settings')[key]; }
function setSetting(key, value) { const s = load('settings'); s[key] = value; save('settings', s); }

module.exports = { load, save, getSetting, setSetting, files, backupDir };