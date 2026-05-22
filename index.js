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

// FIX: static file (karena index.html kamu ada di ROOT, bukan /public)
app.use(express.static(__dirname, { maxAge: '1d' }));

app.use(express.json());

// FIX: route utama "/"
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

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
    db.save('logs', logs.slice(-200));
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
            version,
            auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, P({ level: "fatal" })) },
            printQRInTerminal: false,
            logger: P({ level: 'silent' }),
            browser: ['ADI FIX RED', 'Chrome', '9.0'],
            connectTimeoutMs: 60000,
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

                const shouldReconnect =
                    lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;

                log(`Disconnected. Reconnect: ${shouldReconnect}`, sessionId);

                if (shouldReconnect) {
                    setTimeout(() => startWhatsApp(sessionId), 5000);
                } else {
                    if (fs.existsSync(authDir)) fs.rmSync(authDir, { recursive: true, force: true });
                    log('Session logged out.', sessionId);
                }
            }

            if (connection === 'open') {
                activeSockets[sessionId].status = 'connected';

                io.emit('wa_status', {
                    sessionId,
                    status: 'connected',
                    number: sock.user?.id,
                    name: sock.user?.name
                });

                updateSessionDb(sessionId, 'connected', sock.user?.id, sock.user?.name);
                log(`WhatsApp Connected as ${sock.user?.id}`, sessionId);
            }
        });

        sock.ev.on('creds.update', saveCreds);

    } catch (error) {
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
        if (deleteSession && fs.existsSync(authDir)) {
            fs.rmSync(authDir, { recursive: true, force: true });
        }
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
        if (number) sessions[idx].waNumber = number;
        if (name) sessions[idx].pushName = name;
    } else {
        sessions.push({
            sessionId,
            status,
            waNumber: number,
            pushName: name,
            createdAt: new Date().toISOString()
        });
    }

    db.save('wa_sessions', sessions);
}

// ================= QUEUE & CPU PROTECTION =================
async function processQueue() {
    if (isProcessingQueue || taskQueue.length === 0) return;

    try {
        const cpu = await si.currentLoad();
        if (cpu.currentLoad > 90) {
            log('[QUEUE] CPU Overload > 90%. Pausing queue...');
            setTimeout(processQueue, 10000);
            return;
        }
    } catch (e) {}

    isProcessingQueue = true;
    const task = taskQueue.shift();

    try {
        const result = await executeTool(task.socketId, task.tool, task.payload);
        io.to(task.socketId).emit('tool_result', result);
    } catch (error) {
        io.to(task.socketId).emit('tool_result', {
            tool: task.tool,
            result: 'Server Error: ' + error.message,
            copyData: ''
        });
    }

    isProcessingQueue = false;
    processQueue();
}

// ================= SOCKET.IO HANDLER =================
io.on('connection', (socket) => {
    log('Client Connected: ' + socket.id);

    socket.emit('init_data', {
        settings: db.load('settings'),
        sessions: db.load('wa_sessions')
    });

    io.emit('live_stats', { onlineUsers: io.engine.clientsCount });

    socket.on('login', (password, callback) => {
        const sysPass = db.getSetting('auth_password');
        if (!sysPass || sysPass === '') return callback({ success: true });
        if (password === sysPass) return callback({ success: true });
        callback({ success: false });
    });

    socket.on('request_qr', (data) => startWhatsApp(data.sessionId));
    socket.on('disconnect_wa', (data) => disconnectWhatsApp(data.sessionId, data.delete));

    socket.on('rename_session', (data) => {
        let sessions = db.load('wa_sessions');
        const idx = sessions.findIndex(s => s.sessionId === data.oldId);

        if (idx !== -1) sessions[idx].sessionId = data.newId;

        db.save('wa_sessions', sessions);

        if (activeSockets[data.oldId]) {
            activeSockets[data.newId] = activeSockets[data.oldId];
            delete activeSockets[data.oldId];
        }

        log('Session renamed to ' + data.newId);
    });

    socket.on('request_pairing', (data) => {
        const sessionId = data.sessionId;

        if (activeSockets[sessionId]?.status === 'connected')
            return log('Already connected!', sessionId);

        startWhatsApp(sessionId);

        setTimeout(async () => {
            const sock = activeSockets[sessionId]?.sock;

            if (sock) {
                try {
                    const code = await sock.requestPairingCode(data.number);
                    io.emit('pairing_code', {
                        sessionId,
                        code: code.match(/.{1,4}/g)?.join('-') || code
                    });
                } catch (e) {
                    log('Pairing error: ' + e.message, sessionId);
                }
            }
        }, 4000);
    });

    socket.on('run_tool', (data) => {
        if (db.getSetting('maintenance_mode')) {
            return socket.emit('tool_result', {
                tool: data.tool,
                result: '🛠 Maintenance Mode Active. Try again later.',
                copyData: ''
            });
        }

        data.socketId = socket.id;
        taskQueue.push(data);
        processQueue();
    });

    socket.on('save_settings', (data) => {
        Object.keys(data).forEach(k => db.setSetting(k, data[k]));
        log('Settings updated.');
    });

    socket.on('clear_logs', () => {
        db.save('logs', []);
        log('Logs cleared.');
    });

    socket.on('visitor_info', (data) => {
        const v = db.load('visitor');
        v.push({ ...data, time: new Date().toISOString() });
        db.save('visitor', v.slice(-500));
    });

    socket.on('disconnect', () => {
        io.emit('live_stats', { onlineUsers: io.engine.clientsCount });
    });
});

// ================= API & EXPORT =================
// (TETAP SAMA)

app.post('/api/upload', upload.single('file'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    try {
        const ext = req.file.originalname.split('.').pop().toLowerCase();
        const uniqueNums = new Set();

        if (ext === 'txt') {
            fs.readFileSync(req.file.path, 'utf8')
                .split('\n')
                .forEach(line => {
                    const num = formatNumber(line.trim());
                    if (isValidNumber(num)) uniqueNums.add(num);
                });
        }

        fs.unlinkSync(req.file.path);
        res.json({ numbers: Array.from(uniqueNums) });

    } catch (error) {
        res.status(500).json({ error: 'Failed to process file' });
    }
});

// ================= SYSTEM MONITOR =================
setInterval(async () => {
    try {
        const cpu = await si.currentLoad();
        const ram = await si.mem();

        io.emit('system_stats', {
            cpu: Math.round(cpu.currentLoad),
            ramUsed: Math.round((ram.used / ram.total) * 100),
            visitors: db.load('visitor').length,
            uptime: os.uptime(),
            ping: Math.round(Math.random() * 10 + 5)
        });
    } catch (e) {}
}, 3000);

// ================= CRON =================
cron.schedule('0 */6 * * *', () => {
    const date = new Date().toISOString().replace(/[:.]/g, '-');

    fs.readdirSync('./database').forEach(file => {
        if (file.endsWith('.json')) {
            fs.copyFileSync(
                `./database/${file}`,
                path.join(db.backupDir, `${date}_${file}`)
            );
        }
    });

    log('[CRON] Auto backup executed.');
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on port ${PORT}`);

    db.load('wa_sessions')
        .filter(s => s.status === 'connected')
        .forEach(s => startWhatsApp(s.sessionId));
});