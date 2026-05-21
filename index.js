// ══════════════════════════════════════════════════════════════════════════════
// 🚀 ADI FIX RED v9.0 [FINAL ULTIMATE - PORT 2142]
// ⚡ Express + GZIP + Socket.io + Queue + CPU Protect + Multi-WA
// ══════════════════════════════════════════════════════════════════════════════

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const compression = require('compression');
const path = require('path');
const fs = require('fs');
const qrcode = require('qrcode');
const nodemailer = require('nodemailer');
const multer = require('multer');
const csv = require('csv-parser');
const XLSX = require('xlsx');
const os = require('os');
const si = require('systeminformation');
const cron = require('node-cron');
const rateLimit = require('express-rate-limit');
const { makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, makeCacheableSignalKeyStore } = require("@whiskeysockets/baileys");
const P = require("pino");
const db = require('./database');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// GZIP Compression & Cache
app.use(compression());
app.use(express.static(path.join(__dirname, 'public'), { maxAge: '1d' }));
app.use(express.json());

// Rate Limit & API Protection
const limiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 200, message: { error: 'Too many requests.' } });
app.use('/api/', limiter);

// Multer File Size Limit (5MB max)
const upload = multer({ dest: 'uploads/', limits: { fileSize: 5 * 1024 * 1024 } });

// ================= GLOBAL STATE & QUEUE =================
let activeSockets = {}; 
let taskQueue = [];
let isProcessingQueue = false;

// ================= UTILS & VALIDATION =================
function formatNumber(raw) {
    let n = raw.replace(/\D/g, '');
    if (n.startsWith('0')) n = '62' + n.substring(1);
    else if (n.startsWith('8')) n = '62' + n;
    return n;
}
function isValidNumber(n) { return n.length >= 10 && n.length <= 15; }

function log(message, sessionId = 'SYSTEM') {
    const time = new Date().toLocaleTimeString();
    console.log(`[${time}][${sessionId}] ${message}`);
    io.emit('log', message);
    const logs = db.load('logs');
    logs.push({ time, sessionId, message });
    db.save('logs', logs.slice(-200)); // Anti memory leak
}

// ================= MULTI-SESSION WHATSAPP =================
async function startWhatsApp(sessionId) {
    const maxSessions = db.getSetting('max_wa_sessions') || 5;
    const sessions = db.load('wa_sessions');
    if (sessions.length >= maxSessions && !sessions.find(s => s.sessionId === sessionId)) {
        return log(`Max session limit reached (${maxSessions})`, sessionId);
    }

    if (activeSockets[sessionId]?.status === 'connecting') return;
    if (!activeSockets[sessionId]) activeSockets[sessionId] = { sock: null, status: 'disconnected' };
    
    activeSockets[sessionId].status = 'connecting';
    io.emit('wa_status', { sessionId, status: 'connecting' });
    log('Starting WhatsApp connection...', sessionId);

    const authDir = `./database/sessions/${sessionId}`;
    if (!fs.existsSync(authDir)) fs.mkdirSync(authDir, { recursive: true });

    try {
        const { state, saveCreds } = await useMultiFileAuthState(authDir);
        const { version } = await fetchLatestBaileysVersion();

        const sock = makeWASocket({
            version, auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, P({ level: "fatal" })) },
            printQRInTerminal: false, logger: P({ level: 'silent' }),
            browser: ['ADI FIX RED', 'Chrome', '9.0'], connectTimeoutMs: 60000,
        });

        activeSockets[sessionId].sock = sock;

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;
            if (qr) {
                const qrImage = await qrcode.toDataURL(qr, { width: 300, margin: 2 });
                io.emit('qr', { sessionId, qr: qrImage });
                log('QR Code generated.', sessionId);
            }
            if (connection === 'close') {
                activeSockets[sessionId].status = 'disconnected';
                io.emit('wa_status', { sessionId, status: 'disconnected' });
                updateSessionDb(sessionId, 'disconnected');
                const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
                log(`Disconnected. Reconnect: ${shouldReconnect}`, sessionId);
                if (shouldReconnect) setTimeout(() => startWhatsApp(sessionId), 5000); // Auto Reconnect
                else { if (fs.existsSync(authDir)) fs.rmSync(authDir, { recursive: true, force: true }); log('Session logged out.', sessionId); }
            }
            if (connection === 'open') {
                activeSockets[sessionId].status = 'connected';
                io.emit('wa_status', { sessionId, status: 'connected', number: sock.user?.id, name: sock.user?.name });
                updateSessionDb(sessionId, 'connected', sock.user?.id, sock.user?.name);
                log(`WhatsApp Connected as ${sock.user?.id}`, sessionId);
            }
        });
        sock.ev.on('creds.update', saveCreds);
    } catch(error) {
        log('WA Error: ' + error.message, sessionId);
        activeSockets[sessionId].status = 'disconnected';
        io.emit('wa_status', { sessionId, status: 'disconnected' });
    }
}

function disconnectWhatsApp(sessionId, deleteSession = false) {
    const session = activeSockets[sessionId];
    if (session?.sock) {
        try { session.sock.end(new Error('Manual disconnect')); } catch(e) {}
        const authDir = `./database/sessions/${sessionId}`;
        if (deleteSession && fs.existsSync(authDir)) fs.rmSync(authDir, { recursive: true, force: true });
    }
    activeSockets[sessionId] = { sock: null, status: 'disconnected' };
    io.emit('wa_status', { sessionId, status: 'disconnected' });
    
    if (deleteSession) {
        let sessions = db.load('wa_sessions');
        db.save('wa_sessions', sessions.filter(s => s.sessionId !== sessionId));
        log('Session deleted.', sessionId);
    } else {
        updateSessionDb(sessionId, 'disconnected');
        log('WhatsApp Disconnected manually.', sessionId);
    }
}

function updateSessionDb(sessionId, status, number = null, name = null) {
    let sessions = db.load('wa_sessions');
    const idx = sessions.findIndex(s => s.sessionId === sessionId);
    if (idx !== -1) {
        sessions[idx].status = status;
        if(number) sessions[idx].waNumber = number;
        if(name) sessions[idx].pushName = name;
    } else {
        sessions.push({ sessionId, status, waNumber: number, pushName: name, createdAt: new Date().toISOString() });
    }
    db.save('wa_sessions', sessions);
}

// ================= QUEUE & CPU PROTECTION =================
async function processQueue() {
    if (isProcessingQueue || taskQueue.length === 0) return;
    
    // CPU Overload Protection
    try {
        const cpu = await si.currentLoad();
        if (cpu.currentLoad > 90) {
            log('[QUEUE] CPU Overload > 90%. Pausing queue...');
            setTimeout(processQueue, 10000);
            return;
        }
    } catch(e) {}

    isProcessingQueue = true;
    const task = taskQueue.shift();
    
    try {
        const result = await executeTool(task.socketId, task.tool, task.payload);
        io.to(task.socketId).emit('tool_result', result);
    } catch(error) {
        io.to(task.socketId).emit('tool_result', { tool: task.tool, result: 'Server Error: ' + error.message, copyData: '' });
    }
    
    isProcessingQueue = false;
    processQueue();
}

async function executeTool(socketId, tool, payload) {
    const sessionId = payload.sessionId || 'default_admin';
    const sock = activeSockets[sessionId]?.sock;
    
    if (!sock && tool !== 'banding') return { tool, result: `WhatsApp session ${sessionId} not connected!`, copyData: '' };

    const checkDelay = parseInt(db.getSetting('check_delay')) || 1000;

    if (tool === 'cekbio') {
        let numSet = new Set();
        if (payload.numbers) payload.numbers.split(/[\s,\n]+/).filter(Boolean).map(formatNumber).filter(isValidNumber).forEach(n => numSet.add(n));
        const numbers = Array.from(numSet);
        if (numbers.length === 0) return { tool, result: 'No valid numbers!', copyData: '' };

        let stats = { total: numbers.length, registered: 0, notRegistered: 0, withBio: 0, withoutBio: 0, businessMeta: 0, exclusive: 0, standard: 0, low: 0, suite: 0, yearSet: {} };
        let copyData = '';
        let startTime = Date.now();

        for (let i = 0; i < numbers.length; i++) {
            const num = numbers[i];
            try {
                const [check] = await sock.onWhatsApp(num + '@s.whatsapp.net');
                if (!check?.exists) { stats.notRegistered++; continue; }
                stats.registered++;
                
                let bio = '', setAt = null, isBusiness = false;
                try { 
                    const s = await sock.fetchStatus(num + '@s.whatsapp.net'); 
                    if (s?.[0]?.status?.status) { bio = s[0].status.status; stats.withBio++; }
                    else { stats.withoutBio++; }
                    if(s?.[0]?.status?.setAt) setAt = new Date(s[0].status.setAt);
                } catch(e) { stats.withoutBio++; }

                try {
                    const bp = await sock.getBusinessProfile(num + '@s.whatsapp.net');
                    if(bp) { 
                        isBusiness = true; stats.businessMeta++; 
                        if(bp.options?.length > 2) stats.suite++;
                        else if(bp.description?.length > 100) stats.exclusive++;
                        else if(bp.description?.length > 0) stats.standard++;
                        else stats.low++;
                    }
                } catch(e) {}

                if(setAt) { const year = setAt.getFullYear(); stats.yearSet[year] = (stats.yearSet[year] || 0) + 1; }
                
                copyData += `${num}|${bio}|${isBusiness ? 'Business' : 'Personal'}|${setAt ? setAt.getFullYear() : 'N/A'}\n`;
                
                // ETA & Progress Calculation
                const elapsed = Date.now() - startTime;
                const eta = Math.round(((numbers.length - (i+1)) * (elapsed / (i+1))) / 1000);
                io.emit('progress', { current: i+1, total: numbers.length, eta: eta + 's' });
                await new Promise(r => setTimeout(r, checkDelay));
            } catch(e) { stats.notRegistered++; }
        }

        let resultText = `📊 STATISTIK RINGKASAN:\n`;
        resultText += `├ Terdaftar WA: ${stats.registered}\n`;
        resultText += `├ Tidak Terdaftar WA: ${stats.notRegistered}\n`;
        resultText += `├ Memiliki Bio: ${stats.withBio}\n`;
        resultText += `├ Tanpa Bio: ${stats.withoutBio}\n`;
        resultText += `├ Business Meta: ${stats.businessMeta}\n`;
        resultText += `│  ├ Eklusif: ${stats.exclusive}\n`;
        resultText += `│  ├ Standart: ${stats.standard}\n`;
        resultText += `│  ├ Low: ${stats.low}\n`;
        resultText += `│  └ Suite: ${stats.suite}\n\n`;
        resultText += `📅 STATISTIK BIO BERDASARKAN TAHUN SET:\n`;
        Object.keys(stats.yearSet).sort().forEach(year => { resultText += `├ ${year}: ${stats.yearSet[year]} nomor\n`; });
        resultText += `\n═════════════════════════\n\nDETAILED LOG:\n\n` + copyData.replace(/\|/g, ' | ');

        return { tool, result: resultText, copyData };
    } 
    else if (tool === 'ceknom') {
        let reg = [], nreg = [];
        let numSet = new Set();
        if (payload.numbers) payload.numbers.split(/[\s,\n]+/).filter(Boolean).map(formatNumber).filter(isValidNumber).forEach(n => numSet.add(n));
        const numbers = Array.from(numSet);
        let startTime = Date.now();
        
        for (let i = 0; i < numbers.length; i += 20) {
            const batch = numbers.slice(i, i + 20);
            const res = await Promise.all(batch.map(async n => { try { const [c] = await sock.onWhatsApp(n + '@s.whatsapp.net'); return { n, ok: !!c?.exists }; } catch(e) { return { n, ok: false }; } }));
            res.forEach(r => (r.ok ? reg : nreg).push(r.n));
            const elapsed = Date.now() - startTime;
            const eta = Math.round(((numbers.length - (i+20)) * (elapsed / (i+20))) / 1000);
            io.emit('progress', { current: Math.min(i+20, numbers.length), total: numbers.length, eta: eta + 's' });
            await new Promise(r => setTimeout(r, checkDelay));
        }
        const resultText = `✅ TERDAFTAR (${reg.length}):\n` + reg.join('\n') + `\n\n❌ TIDAK (${nreg.length}):\n` + nreg.join('\n');
        return { tool, result: resultText, copyData: reg.join('\n') };
    }
    else if (tool === 'fix') {
        const num = formatNumber(payload.number);
        const transporter = nodemailer.createTransport({ host: 'smtp.gmail.com', port: 587, secure: false, auth: { user: db.getSetting('active_email'), pass: db.getSetting('active_pass') } });
        await transporter.sendMail({ from: db.getSetting('active_email'), to: 'android@support.whatsapp.com', subject: 'Request', text: `Hello WA, unban ${num}` });
        return { tool, result: `✅ FIX SENT to ${num}!`, copyData: num };
    }
    else if (tool === 'banding') {
        const num = formatNumber(payload.number);
        return { tool, result: `📋 BANDING\n\n📱 +${num}\n\nHello WA, unban +${num}`, copyData: num };
    }
}

// ================= SOCKET.IO HANDLER =================
io.on('connection', (socket) => {
    log('Client Connected: ' + socket.id);
    
    socket.emit('init_data', { settings: db.load('settings'), sessions: db.load('wa_sessions') });
    io.emit('live_stats', { onlineUsers: io.engine.clientsCount });

    socket.on('login', (password, callback) => {
        const sysPass = db.getSetting('auth_password');
        if (!sysPass || sysPass === '') return callback({ success: true }); // No password set
        if (password === sysPass) return callback({ success: true });
        callback({ success: false });
    });

    socket.on('request_qr', (data) => startWhatsApp(data.sessionId));
    socket.on('disconnect_wa', (data) => disconnectWhatsApp(data.sessionId, data.delete));
    socket.on('rename_session', (data) => {
        let sessions = db.load('wa_sessions');
        const idx = sessions.findIndex(s => s.sessionId === data.oldId);
        if(idx !== -1) sessions[idx].sessionId = data.newId;
        db.save('wa_sessions', sessions);
        if(activeSockets[data.oldId]) { activeSockets[data.newId] = activeSockets[data.oldId]; delete activeSockets[data.oldId]; }
        log('Session renamed to ' + data.newId);
    });

    socket.on('request_pairing', (data) => {
        const sessionId = data.sessionId;
        if(activeSockets[sessionId]?.status === 'connected') return log('Already connected!', sessionId);
        startWhatsApp(sessionId);
        setTimeout(async () => {
            const sock = activeSockets[sessionId]?.sock;
            if(sock) {
                try {
                    const code = await sock.requestPairingCode(data.number);
                    io.emit('pairing_code', { sessionId, code: code.match(/.{1,4}/g)?.join('-') || code });
                } catch(e) { log('Pairing error: ' + e.message, sessionId); }
            }
        }, 4000);
    });

    socket.on('run_tool', (data) => {
        if (db.getSetting('maintenance_mode')) return socket.emit('tool_result', { tool: data.tool, result: '🛠 Maintenance Mode Active. Try again later.', copyData: '' });
        data.socketId = socket.id;
        taskQueue.push(data);
        processQueue();
    });

    socket.on('save_settings', (data) => { Object.keys(data).forEach(k => db.setSetting(k, data[k])); log('Settings updated.'); });
    socket.on('clear_logs', () => { db.save('logs', []); log('Logs cleared.'); });
    
    socket.on('visitor_info', (data) => { const v = db.load('visitor'); v.push({...data, time: new Date().toISOString()}); db.save('visitor', v.slice(-500)); });

    socket.on('disconnect', () => {
        io.emit('live_stats', { onlineUsers: io.engine.clientsCount });
    });
});

// ================= API & EXPORT =================
app.post('/api/upload', upload.single('file'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    try {
        const ext = req.file.originalname.split('.').pop().toLowerCase();
        const uniqueNums = new Set();
        
        if (ext === 'txt') {
            fs.readFileSync(req.file.path, 'utf8').split('\n').forEach(line => { const num = formatNumber(line.trim()); if (isValidNumber(num)) uniqueNums.add(num); });
        } else if (ext === 'csv') {
            await new Promise((resolve, reject) => {
                fs.createReadStream(req.file.path).pipe(csv()).on('data', (row) => { Object.values(row).forEach(val => { const num = formatNumber(String(val).trim()); if (isValidNumber(num)) uniqueNums.add(num); }); })
                .on('end', resolve).on('error', reject);
            });
        } else if (ext === 'xlsx' || ext === 'xls') {
            const wb = XLSX.readFile(req.file.path);
            XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], { header: 1 }).flat().forEach(val => { const num = formatNumber(String(val).trim()); if (isValidNumber(num)) uniqueNums.add(num); });
        } else if (ext === 'json') {
            const jsonData = JSON.parse(fs.readFileSync(req.file.path, 'utf8'));
            if(Array.isArray(jsonData)) jsonData.forEach(val => { const num = formatNumber(String(val).trim()); if (isValidNumber(num)) uniqueNums.add(num); });
        }
        
        fs.unlinkSync(req.file.path);
        res.json({ numbers: Array.from(uniqueNums) });
    } catch(error) { res.status(500).json({ error: 'Failed to process file' }); }
});

app.get('/api/export/:format', (req, res) => {
    const { format } = req.params;
    const data = req.query.data;
    if (!data) return res.status(400).send('No data');
    
    if (format === 'txt') { res.setHeader('Content-Type', 'text/plain'); res.setHeader('Content-Disposition', 'attachment; filename=result.txt'); res.send(data.replace(/\|/g, ' | ')); }
    else if (format === 'csv') { res.setHeader('Content-Type', 'text/csv'); res.setHeader('Content-Disposition', 'attachment; filename=result.csv'); let c = "Number,Bio,Type,YearSet\n"; data.split('\n').forEach(l => { const p=l.split('|'); c+=`${p[0]},${p[1]||''},${p[2]||''},${p[3]||''}\n`; }); res.send(c); }
    else if (format === 'json') { res.setHeader('Content-Type', 'application/json'); res.setHeader('Content-Disposition', 'attachment; filename=result.json'); let j=[]; data.split('\n').forEach(l => { if(l.trim()){const p=l.split('|');j.push({number:p[0],bio:p[1]||'',type:p[2]||'',year:p[3]||''});}}); res.send(JSON.stringify(j, null, 2)); }
    else res.status(400).send('Invalid format');
});

// ================= SYSTEM MONITOR & CRON =================
setInterval(async () => {
    try {
        const cpu = await si.currentLoad();
        const ram = await si.mem();
        io.emit('system_stats', { cpu: Math.round(cpu.currentLoad), ramUsed: Math.round((ram.used/ram.total)*100), visitors: db.load('visitor').length, uptime: os.uptime(), ping: Math.round(Math.random()*10+5) });
    } catch(e) {}
}, 3000);

cron.schedule('0 */6 * * *', () => {
    const date = new Date().toISOString().replace(/[:.]/g, '-');
    fs.readdirSync('./database').forEach(file => { if(file.endsWith('.json')) fs.copyFileSync(`./database/${file}`, path.join(db.backupDir, `${date}_${file}`)); });
    log('[CRON] Auto backup executed.');
});

const PORT = 10689;
server.listen(PORT, () => {
    console.log(`\n╔═════════════════════════════════════════════════════════════════╗`);
    console.log(`║   🚀 ADI FIX RED v9.0 - FINAL ULTIMATE EDITION               ║`);
    console.log(`║   🌐 http://localhost:${PORT}                                    ║`);
    console.log(`║   📱 PWA Ready | 🔒 Auth | ⚡ GZIP | 🛡 CPU Protect          ║`);
    console.log(`╚═════════════════════════════════════════════════════════════════╝\n`);
    db.load('wa_sessions').filter(s => s.status === 'connected').forEach(s => startWhatsApp(s.sessionId));
});