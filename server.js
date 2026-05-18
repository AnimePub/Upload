// server.js
const express = require("express");
const fs = require("fs");
const path = require("path");
const cron = require("node-cron");
const crypto = require("crypto");
const { runPipeline } = require("./pipeline");

const app = express();
const PORT = process.env.PORT || 3000;

const DATA_FILE = path.join(__dirname, "data", "settings.json");
const LOGS_DIR = path.join(__dirname, "logs");

// ─── Ensure dirs exist ────────────────────────────────────────────────────────
if (!fs.existsSync(path.join(__dirname, "data"))) fs.mkdirSync(path.join(__dirname, "data"));
if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR);

app.use(express.json());

// ─── Settings helpers ─────────────────────────────────────────────────────────
function loadSettings() {
  try {
    return JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  } catch {
    return { scheduleTime: "06:00", scheduleEnabled: false, delayMs: 3000, dashPassword: "", sessions: {} };
  }
}

function saveSettings(s) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(s, null, 2));
}



// ─── Cookie encryption (AES-256-GCM, key derived from dashboard password) ─────
// Cookie is encrypted at rest — even if settings.json is stolen, cookie is useless without the password

const CIPHER_ALG = "aes-256-gcm";

function deriveKey(password) {
  // Derive a 32-byte key from the dashboard password using SHA-256
  return crypto.createHash("sha256").update(password).digest();
}

function encryptCookie(plaintext, password) {
  const key = deriveKey(password);
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv(CIPHER_ALG, key, iv);
  const encrypted = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  // Store as iv:tag:ciphertext (all hex)
  return iv.toString("hex") + ":" + tag.toString("hex") + ":" + encrypted.toString("hex");
}

function decryptCookie(stored, password) {
  try {
    const [ivHex, tagHex, dataHex] = stored.split(":");
    const key = deriveKey(password);
    const iv = Buffer.from(ivHex, "hex");
    const tag = Buffer.from(tagHex, "hex");
    const data = Buffer.from(dataHex, "hex");
    const decipher = crypto.createDecipheriv(CIPHER_ALG, key, iv);
    decipher.setAuthTag(tag);
    return decipher.update(data) + decipher.final("utf8");
  } catch {
    return null; // Wrong password or corrupted data
  }
}

// ─── Auth helpers ─────────────────────────────────────────────────────────────
function hashPassword(pw) {
  return crypto.createHash("sha256").update(pw).digest("hex");
}

function generateToken() {
  return crypto.randomBytes(32).toString("hex");
}

function requireAuth(req, res, next) {
  const open = ["/api/login", "/api/setup", "/api/needs-setup"];
  if (open.includes(req.path)) return next();

  const s = loadSettings();
  if (!s.dashPassword) return res.status(401).json({ error: "No password set", needsSetup: true });

  // SSE passes token as query param (EventSource can't set headers)
  const token = req.headers["x-auth-token"] || req.query.token;
  const sessions = s.sessions || {};

  if (!token || !sessions[token]) return res.status(401).json({ error: "Unauthorized" });

  // Expire after 7 days
  if (Date.now() - sessions[token].created > 7 * 24 * 60 * 60 * 1000) {
    delete sessions[token];
    s.sessions = sessions;
    saveSettings(s);
    return res.status(401).json({ error: "Session expired" });
  }

  next();
}

// ─── Log helpers ──────────────────────────────────────────────────────────────
function todayKey() {
  return new Date().toISOString().slice(0, 10);
}

function logFile(date) {
  return path.join(LOGS_DIR, `${date}.json`);
}

function readLogs(date) {
  try {
    return JSON.parse(fs.readFileSync(logFile(date), "utf8"));
  } catch {
    return [];
  }
}

function appendLog(entry) {
  const date = entry.time.slice(0, 10);
  const logs = readLogs(date);
  logs.push(entry);
  fs.writeFileSync(logFile(date), JSON.stringify(logs));

  // Clean up logs older than 1 day
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const keepDates = new Set([
    todayKey(),
    yesterday.toISOString().slice(0, 10),
  ]);
  fs.readdirSync(LOGS_DIR).forEach((f) => {
    const d = f.replace(".json", "");
    if (!keepDates.has(d)) {
      try { fs.unlinkSync(path.join(LOGS_DIR, f)); } catch {}
    }
  });
}

// ─── SSE clients ──────────────────────────────────────────────────────────────
let sseClients = [];
let pipelineRunning = false;
let lastSummary = null;

function broadcast(data) {
  const payload = `data: ${JSON.stringify(data)}\n\n`;
  sseClients = sseClients.filter((res) => {
    try { res.write(payload); return true; } catch { return false; }
  });
}

// ─── Pipeline runner ──────────────────────────────────────────────────────────
function getYesterday() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}

function getToday() {
  return new Date().toISOString().slice(0, 10);
}

async function startPipeline(dateOverride, cookie) {
  if (pipelineRunning) return { error: "Pipeline already running" };

  const settings = loadSettings();

  // If cookie not passed (e.g. cron auto-run), decrypt from server storage
  if (!cookie) {
    if (!settings.encryptedCookie) return { error: "No cookie saved — go to Settings and save your anipub cookie" };
    cookie = decryptCookie(settings.encryptedCookie, settings.dashPassword);
    if (!cookie) return { error: "Failed to decrypt cookie — re-save it in Settings" };
  }
  pipelineRunning = true;
  lastSummary = null;
  broadcast({ type: "status", running: true });

  const date = dateOverride || getYesterday();

  runPipeline({
    date,
    cookie,
    delayMs: settings.delayMs || 3000,
    onLog: (entry) => {
      appendLog(entry);
      broadcast({ type: "log", entry });
    },
    onProgress: (p) => {
      broadcast({ type: "progress", ...p });
    },
    onDone: (summary) => {
      lastSummary = summary;
      pipelineRunning = false;
      broadcast({ type: "status", running: false, summary });
    },
  });

  return { ok: true, date };
}

// ─── Cron job ─────────────────────────────────────────────────────────────────
let cronJob = null;

function applyCron(settings) {
  if (cronJob) { cronJob.stop(); cronJob = null; }

  if (settings.scheduleEnabled && settings.scheduleTime) {
    const [hour, minute] = settings.scheduleTime.split(":").map(Number);
    const expr = `${minute} ${hour} * * *`;
    try {
      cronJob = cron.schedule(expr, () => {
        console.log(`[cron] Auto-run triggered at ${settings.scheduleTime}`);
        // Run fully on server — cookie is decrypted from storage automatically
        startPipeline(getYesterday(), null).then(result => {
          if (result?.error) {
            console.error("[cron] Pipeline failed:", result.error);
            broadcast({ type: "cron_error", error: result.error });
          }
        });
      });
      console.log(`[cron] Scheduled at ${settings.scheduleTime} (${expr})`);
    } catch (e) {
      console.error("[cron] Invalid time:", e.message);
    }
  }
}

// Apply saved cron on startup
applyCron(loadSettings());

// ─── Static (public files, no auth needed) ────────────────────────────────────
app.use(express.static(path.join(__dirname, "public")));

// ─── Public auth routes ───────────────────────────────────────────────────────

// Is password set up?
app.get("/api/needs-setup", (req, res) => {
  const s = loadSettings();
  res.json({ needsSetup: !s.dashPassword });
});

// First-time setup
app.post("/api/setup", (req, res) => {
  const s = loadSettings();
  if (s.dashPassword) return res.status(400).json({ error: "Already set up" });
  const { password } = req.body;
  if (!password || password.length < 4) return res.status(400).json({ error: "Min 4 characters" });
  s.dashPassword = hashPassword(password);
  s.sessions = {};
  saveSettings(s);
  res.json({ ok: true });
});

// Login
app.post("/api/login", (req, res) => {
  const s = loadSettings();
  if (!s.dashPassword) return res.status(400).json({ error: "Not set up", needsSetup: true });
  const { password } = req.body;
  if (!password || hashPassword(password) !== s.dashPassword) {
    return res.status(401).json({ error: "Wrong password" });
  }
  const token = generateToken();
  s.sessions = s.sessions || {};
  s.sessions[token] = { created: Date.now() };
  // Prune sessions older than 7 days
  for (const [t, sess] of Object.entries(s.sessions)) {
    if (Date.now() - sess.created > 7 * 24 * 60 * 60 * 1000) delete s.sessions[t];
  }
  saveSettings(s);
  res.json({ ok: true, token });
});

// ─── All routes below require auth ────────────────────────────────────────────
app.use(requireAuth);

// Logout
app.post("/api/logout", (req, res) => {
  const token = req.headers["x-auth-token"];
  const s = loadSettings();
  if (s.sessions && token) delete s.sessions[token];
  saveSettings(s);
  res.json({ ok: true });
});

// ─── Protected API routes ───────────────────────────────────────────────────────────────────────────────────

// GET settings (cookie masked)
app.get("/api/settings", (req, res) => {
  const s = loadSettings();
  res.json({ ...s, hasCookie: !!s.encryptedCookie, encryptedCookie: undefined, dashPassword: s.dashPassword ? "••••••••" : "", sessions: undefined });
});

// POST settings
app.post("/api/settings", (req, res) => {
  const current = loadSettings();
  const body = req.body;

  const updated = {
    ...current,
    scheduleTime: body.scheduleTime || current.scheduleTime,
    scheduleEnabled: typeof body.scheduleEnabled === "boolean" ? body.scheduleEnabled : current.scheduleEnabled,
    delayMs: Number(body.delayMs) || current.delayMs,
  };

  // Save cookie (encrypted with dashboard password)
  if (body.cookie && body.cookie !== "••••••••") {
    const pw = updated.dashPassword || current.dashPassword;
    updated.encryptedCookie = encryptCookie(body.cookie.trim(), pw);
  }

  // Change dashboard password — re-encrypt cookie with new password
  if (body.newPassword && body.newPassword.length >= 4) {
    const oldCookie = current.encryptedCookie
      ? decryptCookie(current.encryptedCookie, current.dashPassword)
      : null;
    updated.dashPassword = hashPassword(body.newPassword);
    updated.sessions = {}; // invalidate all sessions on password change
    if (oldCookie) {
      updated.encryptedCookie = encryptCookie(oldCookie, updated.dashPassword);
    }
  }

  saveSettings(updated);
  applyCron(updated);
  res.json({ ok: true });
});

// GET pipeline status
app.get("/api/status", (req, res) => {
  res.json({ running: pipelineRunning, summary: lastSummary });
});

// POST run pipeline — cookie optional in body; falls back to encrypted server-stored cookie
app.post("/api/run", async (req, res) => {
  const { date, cookie } = req.body;
  const result = await startPipeline(date || null, cookie || null);
  if (result.error) return res.status(400).json(result);
  res.json(result);
});

// GET logs for a date
app.get("/api/logs", (req, res) => {
  const date = req.query.date || todayKey();
  res.json(readLogs(date));
});

// GET list of available log dates
app.get("/api/logs/dates", (req, res) => {
  const dates = fs.readdirSync(LOGS_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((f) => f.replace(".json", ""))
    .sort()
    .reverse();
  res.json(dates);
});

// GET schedule preview for a date (no upload)
app.get("/api/preview", async (req, res) => {
  const date = req.query.date || getYesterday();
  try {
    const r = await fetch(`https://anikoto-api.onrender.com/schedule?time=${date}`);
    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// SSE endpoint — token via query param since EventSource can't set headers
app.get("/api/stream", (req, res) => {
  const token = req.query.token;
  const s = loadSettings();
  const sessions = s.sessions || {};
  if (!token || !sessions[token]) {
    res.status(401).end();
    return;
  }

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  res.write(`data: ${JSON.stringify({ type: "status", running: pipelineRunning })}\n\n`);

  sseClients.push(res);
  req.on("close", () => {
    sseClients = sseClients.filter((c) => c !== res);
  });
});

app.listen(PORT, () => {
  console.log(`\n✓ Anipub Dashboard running at http://localhost:${PORT}\n`);
});
