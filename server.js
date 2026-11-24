/**
 * 神修维基 社区聊天后端 v4
 * 多群聊(群主/管理员/禁言)、私聊、动态(可见性)、好友
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, 'data');
const UPLOAD_DIR = path.join(ROOT, 'uploads');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const MSGS_FILE = path.join(DATA_DIR, 'messages.json');
const EMOJIS_FILE = path.join(DATA_DIR, 'emojis.json');
const POSTS_FILE = path.join(DATA_DIR, 'posts.json');
const DMS_FILE = path.join(DATA_DIR, 'dms.json');
const GROUPS_FILE = path.join(DATA_DIR, 'groups.json');
const DEVICES_FILE = path.join(DATA_DIR, 'devices.json');
const REPORTS_FILE = path.join(DATA_DIR, 'reports.json');
const VIOL_FILE = path.join(DATA_DIR, 'violations.json');
const BADWORDS_FILE = path.join(DATA_DIR, 'badwords.json');
const BIND_FILE = path.join(DATA_DIR, 'device-bind.json');
const ADMIN_KEY = process.env.ADMIN_KEY || 'DWAS221217SLTX';
const AUTH_SECRET = process.env.AUTH_SECRET || 'XGDM-2026-SECRET';
const PORT = process.env.PORT || 3000;
const MAX_MSGS = 500;
const MAX_FILE = 20 * 1024 * 1024;
const MAX_EMOJI_VIDEO = 15 * 1024 * 1024;
const MAX_AUDIO = 5 * 1024 * 1024;

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });
if (!fs.existsSync(USERS_FILE)) fs.writeFileSync(USERS_FILE, '[]', 'utf8');
if (!fs.existsSync(MSGS_FILE)) fs.writeFileSync(MSGS_FILE, '[]', 'utf8');
if (!fs.existsSync(EMOJIS_FILE)) fs.writeFileSync(EMOJIS_FILE, '[]', 'utf8');
if (!fs.existsSync(POSTS_FILE)) fs.writeFileSync(POSTS_FILE, '[]', 'utf8');
if (!fs.existsSync(DMS_FILE)) fs.writeFileSync(DMS_FILE, '{}', 'utf8');
if (!fs.existsSync(GROUPS_FILE)) fs.writeFileSync(GROUPS_FILE, '{}', 'utf8');
if (!fs.existsSync(DEVICES_FILE)) fs.writeFileSync(DEVICES_FILE, '{}', 'utf8');
if (!fs.existsSync(REPORTS_FILE)) fs.writeFileSync(REPORTS_FILE, '[]', 'utf8');
if (!fs.existsSync(VIOL_FILE)) fs.writeFileSync(VIOL_FILE, '{}', 'utf8');
if (!fs.existsSync(BADWORDS_FILE)) fs.writeFileSync(BADWORDS_FILE, '{}', 'utf8');
if (!fs.existsSync(BIND_FILE)) fs.writeFileSync(BIND_FILE, '{}', 'utf8');

/* ===== 存储层：本地文件 / MongoDB（云部署时自动切换） ===== */
const MONGO_URI = process.env.MONGODB_URI || '';
const MONGO_DB = process.env.MONGO_DB || 'shenxiu';
let storeReady = false;
let storeColl = null;
const storeCache = new Map();
const dirty = new Map();
let flushTimer = null;

async function flushStore() {
  flushTimer = null;
  if (!storeReady || !dirty.size) return;
  const items = Array.from(dirty.entries());
  dirty.clear();
  for (const [k, data] of items) {
    try { await storeColl.updateOne({ _id: k }, { $set: { data } }, { upsert: true }); }
    catch (e) { console.error('Mongo 写入失败', k, e.message); dirty.set(k, data); }
  }
}
function queueFlush(key, data) {
  dirty.set(key, data);
  if (!flushTimer) flushTimer = setTimeout(flushStore, 400);
}
async function initStore() {
  if (!/^mongodb/.test(MONGO_URI)) return;
  const { MongoClient } = require('mongodb');
  const client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 20000 });
  await client.connect();
  storeColl = client.db(MONGO_DB).collection('kv');
  const docs = await storeColl.find({}).toArray();
  docs.forEach(d => storeCache.set(d._id, d.data));
  storeReady = true;
  setInterval(() => { if (dirty.size) flushStore(); }, 8000);
  console.log('已连接 MongoDB，载入数据表 ' + docs.length + ' 个');
}

function readJSON(file, fallback) {
  if (storeReady) {
    const k = path.basename(file);
    return storeCache.has(k) ? storeCache.get(k) : fallback;
  }
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch (e) { return fallback; }
}
function writeJSON(file, data) {
  if (storeReady) { queueFlush(path.basename(file), data); return; }
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
}
function getUsers() { return readJSON(USERS_FILE, []); }
function saveUsers(u) { writeJSON(USERS_FILE, u); }
/* 设备 → 账号绑定（一台设备只允许一个号；同一账号可多设备） */
function getBinds() { return readJSON(BIND_FILE, {}); }
function saveBinds(b) { writeJSON(BIND_FILE, b); }
function deviceOwner(deviceId) {
  if (!deviceId) return null;
  const b = getBinds()[deviceId];
  return (b && b.uid) ? b : null;
}
function bindDevice(deviceId, uid, nick) {
  if (!deviceId) return null;
  const owner = deviceOwner(deviceId);
  if (owner && owner.uid !== uid) return owner;
  const b = getBinds();
  b[deviceId] = { uid, nick: nick || '', time: Date.now() };
  saveBinds(b);
  return null;
}
function unbindByUid(uid) {
  const b = getBinds();
  const removed = [];
  Object.keys(b).forEach(dev => {
    if ((b[dev] || {}).uid === uid) { removed.push(dev); delete b[dev]; }
  });
  saveBinds(b);
  return removed;
}
function getMsgs() { return readJSON(MSGS_FILE, []); }
function saveMsgs(m) { writeJSON(MSGS_FILE, m.slice(-MAX_MSGS)); }
function getEmojis() { return readJSON(EMOJIS_FILE, []); }
function saveEmojis(l) { writeJSON(EMOJIS_FILE, l.slice(-200)); }
function getPosts() { return readJSON(POSTS_FILE, []); }
function savePosts(p) { writeJSON(POSTS_FILE, p.slice(-500)); }
function getDMs() { return readJSON(DMS_FILE, {}); }
function saveDMs(d) { writeJSON(DMS_FILE, d); }
function getGroups() { return readJSON(GROUPS_FILE, {}); }
function saveGroups(g) { writeJSON(GROUPS_FILE, g); }

function getDevices() { return readJSON(DEVICES_FILE, {}); }
function saveDevices(d) { writeJSON(DEVICES_FILE, d); }
function getReports() { return readJSON(REPORTS_FILE, []); }
function saveReports(r) { writeJSON(REPORTS_FILE, r.slice(-1000)); }

/* ===== 违规词机器人 + 自动封禁 ===== */
const DEFAULT_BADWORDS = {
  mild: ['加微信','加QQ','加qq','私聊我','代练','刷单','返利','兼职日结','日赚','免费领取','点击链接','扫码进群','傻逼','脑残','废物','去死','滚出去'],
  severe: ['赌博','博彩','下注','百家乐','六合彩','赌球','洗钱','跑分','代开发票','枪支','弹药','毒品','冰毒','大麻','摇头丸','色情','裸聊','约炮','卖淫','嫖娼','黄色网站','诈骗','杀猪盘','传销','暴恐','办假证','假证件'],
  threshold: 3,
  banOneDay: 24 * 3600 * 1000,
  banThirtyDay: 30 * 24 * 3600 * 1000
};
function getBadwords() {
  let b = readJSON(BADWORDS_FILE, null);
  if (!b || typeof b !== 'object' || Array.isArray(b)) b = {};
  if (!Array.isArray(b.mild) || !b.mild.length) b.mild = DEFAULT_BADWORDS.mild.slice();
  if (!Array.isArray(b.severe) || !b.severe.length) b.severe = DEFAULT_BADWORDS.severe.slice();
  b.threshold = Math.max(1, parseInt(b.threshold) || DEFAULT_BADWORDS.threshold);
  b.banOneDay = b.banOneDay || DEFAULT_BADWORDS.banOneDay;
  b.banThirtyDay = b.banThirtyDay || DEFAULT_BADWORDS.banThirtyDay;
  writeJSON(BADWORDS_FILE, b);
  return b;
}
function saveBadwords(b) { writeJSON(BADWORDS_FILE, b); }
function getViolations() { return readJSON(VIOL_FILE, {}); }
function saveViolations(v) { writeJSON(VIOL_FILE, v); }

function scanText(text) {
  const t = String(text || '').toLowerCase();
  const b = getBadwords();
  for (const w of b.severe) { if (w && t.includes(String(w).toLowerCase())) return { hit: true, level: 'severe', word: w }; }
  for (const w of b.mild) { if (w && t.includes(String(w).toLowerCase())) return { hit: true, level: 'mild', word: w }; }
  return { hit: false };
}
function banOf(uid) {
  const rec = getViolations()[uid];
  if (rec && rec.bannedUntil && rec.bannedUntil > Date.now()) {
    return { banned: true, until: rec.bannedUntil, reason: rec.reason || '违反社区规定' };
  }
  return { banned: false };
}
function punishViolation(uid, nick, word, level) {
  const v = getViolations();
  const now = Date.now();
  const rec = v[uid] || { uid, nick, count: 0, records: [], bannedUntil: 0, reason: '' };
  rec.nick = nick; rec.count = (rec.count || 0) + 1;
  rec.records = (rec.records || []).concat([{ word, level, time: now }]).slice(-30);
  let banMs = 0, reason = '';
  if (level === 'severe') {
    banMs = getBadwords().banThirtyDay;
    reason = '发送特大违规内容：' + word;
  } else {
    const recent = rec.records.filter(r => now - r.time < 24 * 3600 * 1000).length;
    if (recent >= getBadwords().threshold) {
      banMs = getBadwords().banOneDay;
      reason = '24小时内违规 ' + recent + ' 次：' + word;
    }
  }
  if (banMs && getCfg().badwordAutoBan) { rec.bannedUntil = now + banMs; rec.reason = reason; rec.lastBan = now; }
  v[uid] = rec; saveViolations(v);
  return { banned: banMs > 0, until: rec.bannedUntil || 0, reason, count: rec.count };
}
function banLeftHours(until) { return Math.max(1, Math.ceil((until - Date.now()) / 3600000)); }

/* ===== 在线人数上限 ===== */
const CONFIG_FILE = path.join(DATA_DIR, 'config.json');
function getMaxOnline() {
  const c = readJSON(CONFIG_FILE, {}) || {};
  const n = parseInt(c.maxOnline);
  return n > 0 ? n : 300;
}
function saveMaxOnline(n) {
  const c = readJSON(CONFIG_FILE, {}) || {};
  c.maxOnline = Math.max(1, parseInt(n) || 300);
  writeJSON(CONFIG_FILE, c);
  return c.maxOnline;
}
function onlineCount() { return typeof online !== 'undefined' && online && online.size ? online.size : 0; }

/* ===== 举报 / 观察期配置 ===== */
function getCfg() {
  const c = readJSON(CONFIG_FILE, {}) || {};
  const num = (v, d, max) => { const n = parseInt(v); return (n > 0 && n <= (max || 99999)) ? n : d; };
  return {
    maxOnline: c.maxOnline || 300,
    reportBanCount: num(c.reportBanCount, 3),     // 多少人举报 → 进入观察期
    probationDays: num(c.probationDays, 15, 3650), // 观察期天数
    banAfterReports: num(c.banAfterReports, 3),    // 10 天内举报次数达到 → 封号
    reportBanDays: num(c.reportBanDays, 7, 3650),  // 封号天数
    badwordAutoBan: c.badwordAutoBan === true,
    deviceOnly: c.deviceOnly !== false             // 一台设备只能登一个号
  };
}
function saveCfg(patch) {
  const c = readJSON(CONFIG_FILE, {}) || {};
  if (patch.reportBanCount !== undefined) { const n = parseInt(patch.reportBanCount); c.reportBanCount = n > 0 ? Math.min(n, 999) : 3; }
  if (patch.probationDays !== undefined) { const n = parseInt(patch.probationDays); c.probationDays = n > 0 ? Math.min(n, 3650) : 15; }
  if (patch.banAfterReports !== undefined) { const n = parseInt(patch.banAfterReports); c.banAfterReports = n > 0 ? Math.min(n, 999) : 3; }
  if (patch.reportBanDays !== undefined) { const n = parseInt(patch.reportBanDays); c.reportBanDays = n > 0 ? Math.min(n, 3650) : 7; }
  if (patch.badwordAutoBan !== undefined) c.badwordAutoBan = patch.badwordAutoBan === true || patch.badwordAutoBan === 'true';
  if (patch.deviceOnly !== undefined) c.deviceOnly = !(patch.deviceOnly === false || patch.deviceOnly === 'false');
  writeJSON(CONFIG_FILE, c);
  return getCfg();
}

/* 按被举报人聚合：独立举报人数量 */
function reportAggregate() {
  const list = getReports();
  const map = {};
  for (const r of list) {
    const key = String(r.targetUid || '').trim() || String(r.targetNick || '').trim();
    if (!key) continue;
    const rec = map[key] || (map[key] = { key, targetUid: String(r.targetUid || '').trim(), targetNick: String(r.targetNick || '').trim(), reporters: {}, count: 0, first: 0, last: 0, reasons: [] });
    if (r.targetNick) rec.targetNick = r.targetNick;
    if (r.targetUid) rec.targetUid = r.targetUid;
    const who = String(r.reporterUid || '').trim() || String(r.reporterNick || '').trim() || ('anon-' + r.id);
    rec.reporters[who] = true;
    rec.count++;
    if (r.reason && rec.reasons.indexOf(r.reason) < 0) rec.reasons.push(r.reason);
    rec.first = rec.first || r.time;
    rec.last = Math.max(rec.last, r.time || 0);
  }
  return Object.keys(map).map(k => {
    const x = map[k];
    return { key: k, targetUid: x.targetUid, targetNick: x.targetNick, reporters: Object.keys(x.reporters).length, count: x.count, first: x.first, last: x.last, reasons: x.reasons.slice(0, 5) };
  }).sort((a, b) => (b.reporters - a.reporters) || (b.count - a.count));
}

/* 某目标在 since 时间之后，有多少个不同的人举报 */
function reportersSince(key, since) {
  const set = {};
  for (const r of getReports()) {
    const k = String(r.targetUid || '').trim() || String(r.targetNick || '').trim();
    if (k !== key) continue;
    if ((r.time || 0) < since) continue;
    set[String(r.reporterUid || '').trim() || String(r.reporterNick || '').trim()] = true;
  }
  return Object.keys(set).length;
}

/* 举报人数达标则封号 */
function autoBanByReports(targetUid, targetNick, reporterCount, days, reason) {
  if (!targetUid) return null;
  const v = getViolations();
  const now = Date.now();
  const rec = v[targetUid] || { uid: targetUid, nick: targetNick || '', count: 0, records: [], bannedUntil: 0, reason: '' };
  rec.nick = targetNick || rec.nick || '';
  rec.bannedUntil = now + (days || 7) * 24 * 3600000;
  rec.reason = reason || ('被 ' + reporterCount + ' 人举报');
  rec.autoBan = 'report';
  rec.lastBan = now;
  v[targetUid] = rec;
  saveViolations(v);
  return rec;
}

function genCode(dev) {
  const s = String(dev).trim().toUpperCase() + '|' + AUTH_SECRET;
  let h1 = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) { h1 ^= s.charCodeAt(i); h1 = (h1 * 0x01000193) >>> 0; }
  let h2 = 5381;
  for (let i = s.length - 1; i >= 0; i--) { h2 = ((h2 << 5) + h2) ^ s.charCodeAt(i); h2 = h2 >>> 0; }
  const num = (h1 ^ h2) >>> 0;
  return (num.toString(36).toUpperCase() + '00000000').slice(0, 8);
}
function checkAdmin(key) { return String(key || '') === ADMIN_KEY; }

function hashPassword(pw, salt) {
  return crypto.createHash('sha256').update(salt + ':' + pw).digest('hex');
}

// 确保默认群存在
function ensureDefaultGroup() {
  const groups = getGroups() || {};
  if (!groups['default']) {
    groups['default'] = {
      name: '神修尼亚官方群',
      owner: '',
      admins: [],
      members: [],
      muted: {},
      createdAt: Date.now()
    };
    saveGroups(groups);
  }
}

const MIME = {
  '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
  '.gif': 'image/gif', '.webp': 'image/webp', '.svg': 'image/svg+xml', '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8', '.xml': 'application/xml; charset=utf-8', '.apk': 'application/vnd.android.package-archive', '.mp3': 'audio/mpeg', '.wav': 'audio/wav', '.m4a': 'audio/mp4',
  '.ogg': 'audio/ogg', '.mp4': 'video/mp4', '.webm': 'video/webm', '.mov': 'video/quicktime',
  '.apk': 'application/vnd.android.package-archive'
};
const ALLOWED_UPLOAD = {
  'image/png': '.png', 'image/jpeg': '.jpg', 'image/jpg': '.jpg', 'image/gif': '.gif', 'image/webp': '.webp',
  'video/mp4': '.mp4', 'video/webm': '.webm', 'video/quicktime': '.mov',
  'audio/mpeg': '.mp3', 'audio/wav': '.wav', 'audio/mp4': '.m4a', 'audio/ogg': '.ogg', 'audio/webm': '.mp3'
};

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, 'http://localhost');
  let pathname;
  try { pathname = decodeURIComponent(url.pathname); } catch (e) { pathname = url.pathname; }
  async function readBody() {
    let body = '';
    for await (const chunk of req) body += chunk;
    return JSON.parse(body);
  }

  if (pathname === '/api/register' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '请求格式错误' }); }
    const nick = String(data.nick || data.name || '').trim(), pwd = String(data.password || '');
    let uid = String(data.uid || '').trim();
    if (!nick || nick.length > 20) return json(res, 400, { ok: false, msg: '名字不能为空且最多20字' });
    if (pwd.length < 4) return json(res, 400, { ok: false, msg: '密码至少4位' });
    const users = getUsers();
    if (users.find(u => u.nick === nick)) return json(res, 400, { ok: false, msg: '该名字已被使用，换一个' });
    const deviceId = String(data.deviceId || '').trim();
    if (getCfg().deviceOnly && deviceId) {
      const owner = deviceOwner(deviceId);
      if (owner) return json(res, 400, { ok: false, deviceLock: true, msg: '该设备已绑定账号「' + (owner.nick || owner.uid) + '」，一台设备只能用一个号' });
    }
    if (!/^\d{12}$/.test(uid)) {
      do { uid = '9' + String(Math.floor(Math.random() * 1e11)).padStart(11, '0'); } while (users.find(u => u.uid === uid));
    } else if (users.find(u => u.uid === uid)) return json(res, 400, { ok: false, msg: '该ID已被占用' });
    const salt = crypto.randomBytes(8).toString('hex');
    users.push({ uid, nick, bio: '', password: hashPassword(pwd, salt), salt, avatar: '', friends: [], deviceId });
    if (deviceId) bindDevice(deviceId, uid, nick);
    saveUsers(users);
    const groups = getGroups();
    if (!groups['default'].members.includes(uid)) groups['default'].members.push(uid);
    saveGroups(groups);
    return json(res, 200, { ok: true, user: { uid, nick, bio: '', avatar: '', friends: [] } });
  }

  if (pathname === '/api/login' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '请求格式错误' }); }
    const account = String(data.account || data.uid || data.nick || '').trim(), pwd = String(data.password || '');
    const users = getUsers();
    const user = users.find(u => u.uid === account) || users.find(u => u.nick === account);
    if (!user || user.password !== hashPassword(pwd, user.salt)) return json(res, 400, { ok: false, msg: '名字或密码错误' });
    const loginBan = banOf(user.uid);
    if (loginBan.banned) return json(res, 403, { ok: false, banned: true, msg: '该账号已被封禁，剩余约 ' + banLeftHours(loginBan.until) + ' 小时。原因：' + loginBan.reason, until: loginBan.until, reason: loginBan.reason, hours: banLeftHours(loginBan.until) });
    // 一台设备只允许登录一个账号
    const deviceId = String(data.deviceId || '').trim();
    const cfg = getCfg();
    if (cfg.deviceOnly && deviceId) {
      const owner = deviceOwner(deviceId);
      if (owner && owner.uid !== user.uid) {
        return json(res, 403, { ok: false, deviceLock: true, msg: '该设备已绑定账号「' + (owner.nick || owner.uid) + '」，一台设备只能登录一个号' });
      }
      bindDevice(deviceId, user.uid, user.nick);
      if (!user.deviceId) user.deviceId = deviceId;
    }
    const onlineNow = onlineCount();
    const alreadyIn = typeof online !== 'undefined' && online && online.has(user.uid);
    if (!alreadyIn && onlineNow >= getMaxOnline()) {
      return json(res, 503, { ok: false, full: true, msg: '服务器人数已满（' + onlineNow + '/' + getMaxOnline() + '），请稍后再试' });
    }
    user.token = crypto.randomBytes(24).toString('hex');
    saveUsers(users);
    const wv = getViolations()[user.uid] || {};
    const watchLeft = (wv.watchStart && !(wv.bannedUntil > Date.now())) ? Math.max(0, Math.ceil((cfg.probationDays * 86400000 - (Date.now() - wv.watchStart)) / 86400000)) : 0;
    return json(res, 200, { ok: true, user: { uid: user.uid, nick: user.nick, bio: user.bio || '', avatar: user.avatar || '', token: user.token, friends: user.friends || [], watchDays: watchLeft, watchReason: watchLeft ? (wv.reason || '') : '' } });
  }

  if (pathname === '/api/autologin' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '请求格式错误' }); }
    const uid = String(data.uid || '').trim(), token = String(data.token || '');
    const users = getUsers();
    const user = users.find(u => u.uid === uid && u.token === token);
    if (!user) return json(res, 401, { ok: false, msg: '登录已过期' });
    const autoBan = banOf(user.uid);
    if (autoBan.banned) return json(res, 403, { ok: false, banned: true, msg: '该账号已被封禁，剩余约 ' + banLeftHours(autoBan.until) + ' 小时。原因：' + autoBan.reason, until: autoBan.until, reason: autoBan.reason, hours: banLeftHours(autoBan.until) });
    const cfgA = getCfg();
    const devA = String(data.deviceId || '').trim();
    if (cfgA.deviceOnly && devA) {
      const ownerA = deviceOwner(devA);
      if (ownerA && ownerA.uid !== user.uid) {
        return json(res, 403, { ok: false, deviceLock: true, msg: '该设备已绑定账号「' + (ownerA.nick || ownerA.uid) + '」，一台设备只能登录一个号' });
      }
      bindDevice(devA, user.uid, user.nick);
    }
    const wvA = getViolations()[user.uid] || {};
    const watchLeftA = wvA.watchStart ? Math.max(0, Math.ceil((cfgA.probationDays * 86400000 - (Date.now() - wvA.watchStart)) / 86400000)) : 0;
    return json(res, 200, { ok: true, user: { uid: user.uid, nick: user.nick, bio: user.bio || '', avatar: user.avatar || '', token: user.token, friends: user.friends || [], watchDays: watchLeftA, watchReason: watchLeftA ? (wvA.reason || '') : '' } });
  }

  if (pathname === '/api/avatar' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '错误' }); }
    const uid = String(data.uid || '').trim(), avatar = String(data.avatar || '');
    const users = getUsers();
    const user = users.find(u => u.uid === uid);
    if (!user) return json(res, 400, { ok: false, msg: '用户不存在' });
    user.avatar = avatar;
    saveUsers(users);
    broadcast({ type: 'profile-updated', data: { uid, nick: user.nick, avatar: user.avatar, bio: user.bio || '' } });
    return json(res, 200, { ok: true });
  }

  if (pathname === '/api/profile' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '错误' }); }
    const uid = String(data.uid || '').trim();
    const users = getUsers();
    const user = users.find(u => u.uid === uid);
    if (!user) return json(res, 400, { ok: false, msg: '用户不存在' });
    if (data.nick !== undefined) {
      const nick = String(data.nick).trim();
      if (!nick || nick.length > 20) return json(res, 400, { ok: false, msg: '昵称不能为空' });
      user.nick = nick;
    }
    if (data.bio !== undefined) {
      const bio = String(data.bio).trim();
      if (bio.length > 50) return json(res, 400, { ok: false, msg: '签名太长' });
      user.bio = bio;
    }
    saveUsers(users);
    broadcast({ type: 'profile-updated', data: { uid, nick: user.nick, avatar: user.avatar || '', bio: user.bio || '' } });
    return json(res, 200, { ok: true, user: { uid, nick: user.nick, bio: user.bio || '', avatar: user.avatar || '' } });
  }

  if (pathname === '/api/users' && req.method === 'GET') {
    const users = getUsers();
    return json(res, 200, { ok: true, list: users.map(u => ({ uid: u.uid, nick: u.nick, bio: u.bio || '', avatar: u.avatar || '' })) });
  }

  if (pathname === '/api/friend' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '错误' }); }
    const me = String(data.uid || '').trim(), fuid = String(data.friendUid || '').trim();
    const users = getUsers();
    const meObj = users.find(u => u.uid === me), fObj = users.find(u => u.uid === fuid);
    if (!meObj || !fObj) return json(res, 404, { ok: false, msg: '用户不存在' });
    meObj.friends = meObj.friends || [];
    fObj.friends = fObj.friends || [];
    if (!meObj.friends.includes(fuid)) meObj.friends.push(fuid);
    if (!fObj.friends.includes(me)) fObj.friends.push(me);
    saveUsers(users);
    return json(res, 200, { ok: true, msg: '已添加好友' });
  }

  if (pathname === '/api/groups' && req.method === 'GET') {
    const uid = url.searchParams.get('uid') || '';
    const groups = getGroups();
    const list = Object.entries(groups).map(([gid, g]) => ({
      id: gid, name: g.name, owner: g.owner, admins: g.admins || [],
      memberCount: (g.members || []).length,
      myRole: g.owner === uid ? 'owner' : (g.admins || []).includes(uid) ? 'admin' : (g.members || []).includes(uid) ? 'member' : 'none'
    }));
    return json(res, 200, { ok: true, list });
  }

  if (pathname === '/api/group/create' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '错误' }); }
    const uid = String(data.uid || '').trim(), name = String(data.name || '').trim();
    if (!name || name.length > 20) return json(res, 400, { ok: false, msg: '群名1-20字' });
    const gid = 'g-' + Date.now() + '-' + crypto.randomBytes(4).toString('hex');
    const groups = getGroups();
    groups[gid] = { name, owner: uid, admins: [], members: [uid], muted: {}, createdAt: Date.now() };
    saveGroups(groups);
    return json(res, 200, { ok: true, group: { id: gid, name, owner: uid } });
  }

  if (pathname.startsWith('/api/group/members/') && req.method === 'GET') {
    const gid = pathname.slice('/api/group/members/'.length);
    const groups = getGroups();
    const g = groups[gid];
    if (!g) return json(res, 404, { ok: false, msg: '群不存在' });
    const users = getUsers();
    const members = (g.members || []).map(uid => {
      const u = users.find(x => x.uid === uid) || {};
      const mutedUntil = (g.muted || {})[uid] || 0;
      return { uid, nick: u.nick || '?', avatar: u.avatar || '', role: g.owner === uid ? 'owner' : (g.admins || []).includes(uid) ? 'admin' : 'member', muted: mutedUntil > Date.now() ? Math.ceil((mutedUntil - Date.now())/1000) : 0 };
    });
    return json(res, 200, { ok: true, members, owner: g.owner, admins: g.admins || [] });
  }

  if (pathname === '/api/upload' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '错误' }); }
    const base64 = String(data.data || ''), mime = String(data.mime || '').toLowerCase();
    if (!base64) return json(res, 400, { ok: false, msg: '缺少数据' });
    const ext = ALLOWED_UPLOAD[mime];
    if (!ext) return json(res, 400, { ok: false, msg: '不支持的类型' });
    let buf;
    const m = base64.match(/^data:[a-z/]+;base64,(.+)$/i);
    buf = m ? Buffer.from(m[1], 'base64') : Buffer.from(base64, 'base64');
    const isAudio = mime.startsWith('audio/');
    if (isAudio && buf.length > MAX_AUDIO) return json(res, 400, { ok: false, msg: '语音过大' });
    if (!isAudio && buf.length > MAX_FILE) return json(res, 400, { ok: false, msg: '文件过大' });
    const filename = Date.now() + '-' + crypto.randomBytes(6).toString('hex') + ext;
    fs.writeFileSync(path.join(UPLOAD_DIR, filename), buf);
    return json(res, 200, { ok: true, url: '/uploads/' + filename, mime });
  }

  if (pathname === '/api/emojis' && req.method === 'GET') return json(res, 200, { ok: true, list: getEmojis() });
  if (pathname === '/api/emoji' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '错误' }); }
    const base64 = String(data.data || ''), mime = String(data.mime || ''), uid = String(data.uid || '').trim();
    if (!base64) return json(res, 400, { ok: false, msg: '缺少数据' });
    const ext = ALLOWED_UPLOAD[mime.toLowerCase()];
    if (!ext) return json(res, 400, { ok: false, msg: '不支持的类型' });
    let buf;
    const m = base64.match(/^data:[a-z/]+;base64,(.+)$/i);
    buf = m ? Buffer.from(m[1], 'base64') : Buffer.from(base64, 'base64');
    const isVideo = mime.toLowerCase().startsWith('video/');
    if (isVideo && buf.length > MAX_EMOJI_VIDEO) return json(res, 400, { ok: false, msg: '视频表情过大' });
    if (!isVideo && buf.length > 5 * 1024 * 1024) return json(res, 400, { ok: false, msg: '图片表情过大' });
    const filename = Date.now() + '-' + crypto.randomBytes(6).toString('hex') + ext;
    fs.writeFileSync(path.join(UPLOAD_DIR, filename), buf);
    const users = getUsers();
    const by = (users.find(u => u.uid === uid) || {}).nick || '未知';
    const emoji = { id: Date.now() + '-' + Math.floor(Math.random()*10000), type: isVideo?'video':'image', url: '/uploads/'+filename, by, time: Date.now() };
    const list = getEmojis();
    list.push(emoji);
    saveEmojis(list);
    return json(res, 200, { ok: true, emoji });
  }

  // 动态列表（按可见性过滤）
  if (pathname === '/api/posts' && req.method === 'GET') {
    const uid = url.searchParams.get('uid') || '';
    const users = getUsers();
    const me = users.find(u => u.uid === uid);
    const myFriends = me ? (me.friends || []) : [];
    const posts = getPosts().slice(-200).reverse();
    const enriched = posts.filter(p => {
      const vis = p.visibility || 'public';
      if (vis === 'public') return true;
      if (vis === 'private') return p.authorId === uid;
      if (vis === 'friends') return p.authorId === uid || myFriends.includes(p.authorId);
      return true;
    }).map(p => {
      const author = users.find(u => u.uid === p.authorId) || {};
      return { ...p, authorNick: author.nick || p.authorNick || '?', authorAvatar: author.avatar || '' };
    });
    return json(res, 200, { ok: true, list: enriched });
  }

  if (pathname === '/api/post' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '错误' }); }
    const uid = String(data.uid || '').trim();
    const type = data.type === 'image' || data.type === 'video' ? data.type : 'text';
    const content = String(data.content || '').slice(0, 2000);
    const visibility = ['public', 'friends', 'private'].includes(data.visibility) ? data.visibility : 'public';
    const users = getUsers();
    const author = users.find(u => u.uid === uid);
    if (!author) return json(res, 400, { ok: false, msg: '用户不存在' });
    if (type !== 'text' && !/^\/uploads\//.test(content)) return json(res, 400, { ok: false, msg: '文件地址无效' });
    const post = {
      id: 'post-' + Date.now() + '-' + Math.floor(Math.random()*10000),
      authorId: uid, authorNick: author.nick, authorAvatar: author.avatar || '',
      type, content, visibility, likes: [], comments: [], time: Date.now()
    };
    const posts = getPosts();
    posts.push(post);
    savePosts(posts);
    broadcast({ type: 'post-new', data: post });
    return json(res, 200, { ok: true, post });
  }

  if (pathname === '/api/post/like' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '错误' }); }
    const pid = String(data.id || ''), uid = String(data.uid || '');
    const posts = getPosts();
    const post = posts.find(p => p.id === pid);
    if (!post) return json(res, 404, { ok: false, msg: '动态不存在' });
    post.likes = post.likes || [];
    const idx = post.likes.indexOf(uid);
    let liked;
    if (idx >= 0) { post.likes.splice(idx, 1); liked = false; } else { post.likes.push(uid); liked = true; }
    savePosts(posts);
    return json(res, 200, { ok: true, liked, count: post.likes.length });
  }

  if (pathname === '/api/post/comment' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok: false, msg: '错误' }); }
    const pid = String(data.id || ''), uid = String(data.uid || '');
    const text = String(data.text || '').slice(0, 500), replyTo = String(data.replyTo || '');
    if (!text) return json(res, 400, { ok: false, msg: '留言不能为空' });
    const users = getUsers();
    const author = users.find(u => u.uid === uid);
    const posts = getPosts();
    const post = posts.find(p => p.id === pid);
    if (!post) return json(res, 404, { ok: false, msg: '动态不存在' });
    post.comments = post.comments || [];
    const comment = { id: 'c-' + Date.now() + '-' + Math.floor(Math.random()*10000), authorId: uid, authorNick: author?author.nick:'?', authorAvatar: author?author.avatar||'':'', text, replyTo, time: Date.now() };
    post.comments.push(comment);
    savePosts(posts);
    broadcast({ type: 'post-comment', data: { id: pid, comment } });
    return json(res, 200, { ok: true, comment });
  }

  if (pathname.startsWith('/api/dm/') && req.method === 'GET') {
    const otherUid = pathname.slice('/api/dm/'.length);
    const me = url.searchParams.get('me') || '';
    if (!me || !otherUid) return json(res, 400, { ok: false, msg: '参数缺失' });
    const dms = getDMs();
    const key = [me, otherUid].sort().join('|');
    return json(res, 200, { ok: true, list: (dms[key] || []).slice(-200) });
  }

  // ===== 授权（设备号） =====
  // 买家授权时绑定手机号，卖家查设备号即可带出
  if (pathname === '/api/bind-phone' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    const dev = String(data.deviceId || '').trim(), phone = String(data.phone || '').trim();
    if (!dev || !phone) return json(res, 400, { ok:false, msg:'缺少设备号或手机号' });
    if (!/^1\d{10}$/.test(phone)) return json(res, 400, { ok:false, msg:'手机号格式不正确' });
    const devices = getDevices();
    const rec = devices[dev] || { status:'unknown', time: Date.now() };
    rec.phone = phone;
    devices[dev] = rec;
    saveDevices(devices);
    return json(res, 200, { ok:true, msg:'手机号已绑定' });
  }
  if (pathname === '/api/auth-check' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    const dev = String(data.deviceId || '').trim();
    if (!dev) return json(res, 400, { ok:false, msg:'缺少设备号' });
    const d = getDevices()[dev];
    if (!d) return json(res, 200, { ok:true, status:'unknown' });
    if (d.status === 'banned') return json(res, 200, { ok:true, status:'banned' });
    return json(res, 200, { ok:true, status:'allowed' });
  }
  if (pathname === '/api/verify-code' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    const dev = String(data.deviceId || '').trim(), code = String(data.code || '').trim().toUpperCase().replace(/\s/g,'');
    if (!dev || !code) return json(res, 400, { ok:false, msg:'缺少参数' });
    const devices = getDevices();
    if (devices[dev] && devices[dev].status === 'banned') return json(res, 403, { ok:false, msg:'该设备已被禁用' });
    if (code !== genCode(dev)) return json(res, 400, { ok:false, msg:'通行码不正确' });
    devices[dev] = Object.assign({}, devices[dev] || {}, { status:'allowed', code, time: Date.now() });
    saveDevices(devices);
    return json(res, 200, { ok:true, msg:'授权成功' });
  }

  // ===== 卖家管理后台 =====
  if (pathname === '/api/admin/devices' && req.method === 'GET') {
    if (!checkAdmin(url.searchParams.get('key'))) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const devices = getDevices();
    const list = Object.entries(devices).map(([deviceId, d]) => Object.assign({ deviceId }, d));
    return json(res, 200, { ok:true, list });
  }
  // 查询设备号 = 自动锁定 + 记录统计
  if (pathname === '/api/admin/lock' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    if (!checkAdmin(data.key)) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const dev = String(data.deviceId || '').trim();
    if (!dev) return json(res, 400, { ok:false, msg:'缺少设备号' });
    const devices = getDevices();
    const prev = devices[dev] || {};
    const rec = Object.assign({}, prev, {
      status: 'banned',
      code: prev.code || genCode(dev),
      time: prev.time || Date.now(),
      phone: String(data.phone || prev.phone || '').slice(0, 24),
      lockCount: (prev.lockCount || 0) + 1,
      firstLock: prev.firstLock || Date.now(),
      lastLock: Date.now()
    });
    devices[dev] = rec;
    saveDevices(devices);
    return json(res, 200, { ok:true, device: Object.assign({ deviceId: dev }, rec) });
  }
  if (pathname === '/api/admin/allow' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    if (!checkAdmin(data.key)) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const dev = String(data.deviceId || '').trim();
    if (!dev) return json(res, 400, { ok:false, msg:'缺少设备号' });
    const devices = getDevices();
    const code = genCode(dev);
    devices[dev] = Object.assign({}, devices[dev] || {}, { status:'allowed', code, time: Date.now() });
    saveDevices(devices);
    return json(res, 200, { ok:true, deviceId: dev, code });
  }
  // ===== 违规词机器人管理 =====
  if (pathname === '/api/admin/violations' && req.method === 'GET') {
    if (!checkAdmin(url.searchParams.get('key'))) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const v = getViolations();
    const now = Date.now();
    const list = Object.keys(v).map(uid => {
      const r = v[uid] || {};
      return {
        uid, nick: r.nick || '', count: r.count || 0,
        banned: !!(r.bannedUntil && r.bannedUntil > now),
        until: r.bannedUntil || 0, reason: r.reason || '', lastBan: r.lastBan || 0,
        recent: (r.records || []).slice(-5)
      };
    });
    return json(res, 200, { ok:true, list, threshold: getBadwords().threshold });
  }
  if (pathname === '/api/admin/unban-user' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    if (!checkAdmin(data.key)) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const uid = String(data.uid || '').trim();
    const v = getViolations();
    if (!v[uid]) return json(res, 404, { ok:false, msg:'没有该用户的违规记录' });
    v[uid].bannedUntil = 0; v[uid].reason = ''; v[uid].count = 0;
    saveViolations(v);
    return json(res, 200, { ok:true });
  }
  if (pathname === '/api/admin/ban-user' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    if (!checkAdmin(data.key)) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const account = String(data.account || '').trim();
    if (!account) return json(res, 400, { ok:false, msg:'请填写要封的用户名或 UID' });
    const users = getUsers();
    let target = users.find(u => u.uid === account) || null;
    if (!target) target = users.find(u => String(u.nick || '') === account) || null;
    if (!target) return json(res, 404, { ok:false, msg:'找不到该用户：' + account });
    const hours = Math.max(0, parseInt(data.hours));
    const now = Date.now();
    const v = getViolations();
    const rec = v[target.uid] || { uid: target.uid, nick: target.nick || '', count: 0, records: [], bannedUntil: 0, reason: '' };
    rec.nick = target.nick || rec.nick || '';
    rec.bannedUntil = hours > 0 ? now + hours * 3600000 : now + 100 * 365 * 24 * 3600000;
    rec.reason = String(data.reason || '').trim() || (hours > 0 ? ('管理员封禁 ' + hours + ' 小时') : '管理员永久封禁');
    rec.manualBan = true; rec.lastBan = now;
    v[target.uid] = rec; saveViolations(v);
    return json(res, 200, { ok:true, uid: target.uid, nick: rec.nick, until: rec.bannedUntil, permanent: hours === 0, msg:'已封禁' });
  }
  if (pathname === '/api/admin/config') {
    if (req.method === 'GET') {
      if (!checkAdmin(url.searchParams.get('key'))) return json(res, 403, { ok:false, msg:'管理密钥错误' });
      const c = getCfg();
      return json(res, 200, { ok:true, maxOnline: c.maxOnline, onlineNow: onlineCount(), reportBanCount: c.reportBanCount, reportBanDays: c.reportBanDays, badwordAutoBan: c.badwordAutoBan });
    }
    let cfgData; try { cfgData = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    if (!checkAdmin(cfgData.key)) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    if (cfgData.maxOnline !== undefined && cfgData.reportBanCount === undefined && cfgData.reportBanDays === undefined && cfgData.badwordAutoBan === undefined) {
      const n = saveMaxOnline(cfgData.maxOnline);
      const c = getCfg();
      return json(res, 200, { ok:true, maxOnline: n, onlineNow: onlineCount(), reportBanCount: c.reportBanCount, reportBanDays: c.reportBanDays, badwordAutoBan: c.badwordAutoBan });
    }
    const c = saveCfg(cfgData);
    return json(res, 200, { ok:true, maxOnline: c.maxOnline, onlineNow: onlineCount(), reportBanCount: c.reportBanCount, reportBanDays: c.reportBanDays, badwordAutoBan: c.badwordAutoBan });
  }
  if (pathname === '/api/admin/report-stats' && req.method === 'GET') {
    if (!checkAdmin(url.searchParams.get('key'))) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const cfg = getCfg();
    const now = Date.now();
    const vio = getViolations();
    const list = reportAggregate().map(x => {
      const key = x.targetUid || x.key;
      const rec = vio[key] || {};
      const b = banOf(key);
      const watching = !!(rec.watchStart && !(rec.bannedUntil > now) && (now - rec.watchStart) <= cfg.probationDays * 86400000);
      return Object.assign({}, x, {
        banned: !!b.banned, until: b.until || 0, reason: b.reason || '',
        watching,
        watchLeft: watching ? Math.max(0, Math.ceil((cfg.probationDays * 86400000 - (now - rec.watchStart)) / 86400000)) : 0,
        watchNew: watching ? reportersSince(key, Math.max(rec.watchStart, now - 10 * 86400000)) : 0
      });
    });
    return json(res, 200, { ok:true, need: cfg.reportBanCount, days: cfg.reportBanDays, probationDays: cfg.probationDays, banAfter: cfg.banAfterReports, total: getReports().length, list });
  }
  if (pathname === '/api/admin/unbind-user' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    if (!checkAdmin(data.key)) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const acc = String(data.account || '').trim();
    const users = getUsers();
    const user = users.find(u => u.uid === acc) || users.find(u => (u.nick || '') === acc);
    if (!user) return json(res, 404, { ok:false, msg:'找不到该用户' });
    const old = user.deviceId || '';
    user.deviceId = '';
    saveUsers(users);
    const removedDevs = unbindByUid(user.uid);
    const msgDev = removedDevs.length ? ('（解除 ' + removedDevs.length + ' 台：' + removedDevs.join('、').slice(0, 80) + '）') : (old ? ('（原设备 ' + old + '）') : '');
    return json(res, 200, { ok:true, msg:'已解绑设备' + msgDev, uid: user.uid, nick: user.nick, devices: removedDevs });
  }
  if (pathname === '/api/admin/badwords') {
    if (req.method === 'GET') {
      if (!checkAdmin(url.searchParams.get('key'))) return json(res, 403, { ok:false, msg:'管理密钥错误' });
      return json(res, 200, { ok:true, words: getBadwords() });
    }
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    if (!checkAdmin(data.key)) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const b = getBadwords();
    const clean = arr => String(arr || '').split(/[\n,，、]/).map(x => x.trim()).filter(Boolean).slice(0, 500);
    if (data.mild !== undefined) b.mild = clean(data.mild);
    if (data.severe !== undefined) b.severe = clean(data.severe);
    if (data.threshold) b.threshold = Math.max(1, parseInt(data.threshold) || 3);
    saveBadwords(b);
    return json(res, 200, { ok:true, words: b });
  }
  if (pathname === '/api/admin/ban' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    if (!checkAdmin(data.key)) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const dev = String(data.deviceId || '').trim();
    if (!dev) return json(res, 400, { ok:false, msg:'缺少设备号' });
    const devices = getDevices();
    devices[dev] = Object.assign({}, devices[dev] || {}, { status:'banned', time: Date.now(), code: (devices[dev]||{}).code || genCode(dev) });
    saveDevices(devices);
    return json(res, 200, { ok:true, msg:'已封禁' });
  }
  if (pathname === '/api/admin/unban' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    if (!checkAdmin(data.key)) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const dev = String(data.deviceId || '').trim();
    if (!dev) return json(res, 400, { ok:false, msg:'缺少设备号' });
    const devices = getDevices();
    if (devices[dev]) { devices[dev].status = 'allowed'; saveDevices(devices); }
    return json(res, 200, { ok:true, msg:'已解封' });
  }
  if (pathname === '/api/admin/delete' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    if (!checkAdmin(data.key)) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const dev = String(data.deviceId || '').trim();
    const devices = getDevices();
    delete devices[dev]; saveDevices(devices);
    return json(res, 200, { ok:true, msg:'已删除该设备记录' });
  }
  if (pathname === '/api/admin/stats' && req.method === 'GET') {
    if (!checkAdmin(url.searchParams.get('key'))) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    const devices = getDevices();
    const arr = Object.entries(devices).map(([deviceId, d]) => Object.assign({ deviceId }, d));
    const allowed = arr.filter(d => d.status === 'allowed').length;
    const banned = arr.filter(d => d.status === 'banned').length;
    const locked = arr.filter(d => (d.lockCount || 0) > 0).sort((a, b) => (b.lastLock || 0) - (a.lastLock || 0));
    return json(res, 200, { ok:true, stats: { total: arr.length, allowed, banned, lockedCount: locked.length }, list: locked });
  }
  if (pathname === '/api/admin/reports' && req.method === 'GET') {
    if (!checkAdmin(url.searchParams.get('key'))) return json(res, 403, { ok:false, msg:'管理密钥错误' });
    return json(res, 200, { ok:true, list: getReports().slice(-200).reverse() });
  }

  // ===== 用户举报（需登录；3 人进观察期，观察期内 10 天多次举报即封号）=====
  if (pathname === '/api/report' && req.method === 'POST') {
    let data; try { data = await readBody(); } catch (e) { return json(res, 400, { ok:false, msg:'错误' }); }
    const reporterUid = String(data.reporterUid || '').trim();
    const token = String(data.token || '');
    const targetUid = String(data.targetUid || '').trim();
    const targetNick = String(data.targetNick || '').slice(0, 20);
    const reason = String(data.reason || '').slice(0, 200);
    const content = String(data.content || '').slice(0, 500);
    if (!targetUid && !targetNick) return json(res, 400, { ok:false, msg:'缺少被举报人' });
    // 加固 A：必须是已登录且 token 有效的真实账号
    const users = getUsers();
    const me = users.find(u => u.uid === reporterUid);
    if (!reporterUid || !me) return json(res, 401, { ok:false, msg:'请先登录后再举报' });
    if (!token || !me.token || me.token !== token) return json(res, 401, { ok:false, msg:'登录状态已失效，请重新登录后再举报' });
    // 加固 B：被举报人必须是系统内真实账号
    if (targetUid && !users.find(u => u.uid === targetUid)) return json(res, 404, { ok:false, msg:'被举报的用户不存在' });
    if (reporterUid === targetUid) return json(res, 400, { ok:false, msg:'不能举报自己' });

    const r = {
      id: 'rep-' + Date.now() + '-' + Math.floor(Math.random() * 10000),
      deviceId: String(data.deviceId || '').trim(),
      reporterUid, reporterNick: me.nick || '',
      targetUid, targetNick, reason, content, time: Date.now()
    };
    const list = getReports();
    list.push(r);
    saveReports(list);

    const cfg = getCfg();
    const key = targetUid || targetNick;
    const agg = reportAggregate().find(x => x.key === key);
    const reporters = agg ? agg.reporters : 1;
    const reasons = ((agg && agg.reasons) || [reason]).filter(Boolean).slice(0, 3).join('、') || '未填写理由';
    const now = Date.now();
    const v = getViolations();
    const rec = v[key] || { uid: key, nick: targetNick || '', count: 0, records: [], bannedUntil: 0, reason: '', watchStart: 0 };
    rec.nick = targetNick || rec.nick || '';

    let banned = false, watching = false, msg = '';

    if (rec.bannedUntil && rec.bannedUntil > now) {
      return json(res, 200, { ok:true, banned:true, reporting:true, reporters, need: cfg.reportBanCount, msg:'该用户已在封禁中，举报已记录' });
    }
    // 观察期过期自动结束
    if (rec.watchStart && now - rec.watchStart > cfg.probationDays * 86400000) rec.watchStart = 0;

    if (!rec.watchStart && reporters >= cfg.reportBanCount) {
      rec.watchStart = now; rec.watchReporters = reporters;
      rec.reason = '被 ' + reporters + ' 人举报：' + reasons;
      v[key] = rec; saveViolations(v);
      watching = true;
      msg = '举报已提交。该用户已被 ' + reporters + ' 人举报（' + reasons + '），已进入 ' + cfg.probationDays + ' 天观察期';
    } else if (rec.watchStart) {
      const since = Math.max(rec.watchStart, now - 10 * 86400000);
      const newReporters = reportersSince(key, since);
      if (newReporters >= cfg.banAfterReports) {
        rec.bannedUntil = now + cfg.reportBanDays * 86400000;
        rec.reason = '被 ' + reporters + ' 人举报（' + reasons + '）；观察期内 10 天又被 ' + newReporters + ' 人举报';
        rec.lastBan = now; rec.banSource = 'report';
        v[key] = rec; saveViolations(v);
        banned = true;
        msg = '举报已提交。该用户在观察期内 10 天被 ' + newReporters + ' 人举报，已封号 ' + cfg.reportBanDays + ' 天（原因：' + reasons + '）';
      } else {
        const leftDays = Math.max(1, Math.ceil((cfg.probationDays * 86400000 - (now - rec.watchStart)) / 86400000));
        rec.reason = '被 ' + reporters + ' 人举报：' + reasons;
        v[key] = rec; saveViolations(v);
        msg = '举报已提交。该用户在观察期内（剩余 ' + leftDays + ' 天），10 天内已有 ' + newReporters + '/' + cfg.banAfterReports + ' 人举报';
      }
    } else {
      msg = '举报已提交，累计 ' + reporters + '/' + cfg.reportBanCount + ' 人举报（满 ' + cfg.reportBanCount + ' 人进入 ' + cfg.probationDays + ' 天观察期）';
    }

    return json(res, 200, {
      ok:true, banned, watching, reporters,
      need: cfg.reportBanCount, banAfter: cfg.banAfterReports, days: cfg.reportBanDays,
      reason: reasons, msg
    });
  }

  if (req.method === 'GET' || req.method === 'HEAD') {
    let filePath = (pathname === '/' || pathname === '/index.html') ? path.join(ROOT, 'index.html') : path.join(ROOT, pathname);
    const resolved = path.resolve(filePath);
    if (!resolved.startsWith(path.resolve(ROOT))) { res.writeHead(403); return res.end('Forbidden'); }
    fs.stat(resolved, (err, stat) => {
      if (err || !stat.isFile()) { res.writeHead(404); return res.end('404'); }
      const ext = path.extname(resolved).toLowerCase();
      res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream', 'Content-Length': stat.size, 'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600' });
      if (req.method === 'HEAD') return res.end();
      fs.createReadStream(resolved).pipe(res);
    });
    return;
  }
  res.writeHead(405); res.end('Method Not Allowed');
});

function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}

const wss = new WebSocketServer({ server, path: '/ws' });
const online = new Map();

function broadcast(obj, exceptUid) {
  const payload = JSON.stringify(obj);
  for (const [uid, client] of online) {
    if (uid === exceptUid) continue;
    if (client.ws.readyState === 1) { try { client.ws.send(payload); } catch(e){} }
  }
}
function sendTo(uid, obj) {
  const c = online.get(uid);
  if (c && c.ws.readyState === 1) { try { c.ws.send(JSON.stringify(obj)); return true; } catch(e){} }
  return false;
}
function broadcastToGroup(gid, obj, exceptUid) {
  const groups = getGroups();
  const g = groups[gid];
  if (!g) return;
  const payload = JSON.stringify(obj);
  for (const uid of (g.members || [])) {
    if (uid === exceptUid) continue;
    const c = online.get(uid);
    if (c && c.ws.readyState === 1) { try { c.ws.send(payload); } catch(e){} }
  }
}
function onlineList() {
  return Array.from(online.values()).map(c => ({ uid: c.uid, nick: c.nick, avatar: c.avatar || '' }));
}
function sysMsg(text, gid) {
  return { id: 'sys-' + Date.now() + '-' + Math.floor(Math.random()*10000), senderId: 'system', sender: '系统', avatar: '', type: 'text', content: text, time: Date.now(), gid };
}

wss.on('connection', (ws) => {
  let client = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch(e){ return; }

    if (msg.type === 'auth') {
      const users = getUsers();
      const user = users.find(u => u.uid === msg.uid && u.nick === msg.nick);
      if (!user) { ws.send(JSON.stringify({type:'error',data:{msg:'身份校验失败'}})); return; }
      const prev = online.get(user.uid);
      if ((!prev || prev.ws !== ws) && online.size >= getMaxOnline()) {
        ws.send(JSON.stringify({ type: 'error', data: { full: true, msg: '服务器人数已满（' + online.size + '/' + getMaxOnline() + '），请稍后再试' } }));
        try { ws.close(); } catch (e) {}
        return;
      }
      client = { uid: user.uid, nick: user.nick, avatar: user.avatar || '', ws };
      const old = online.get(user.uid);
      if (old && old.ws !== ws) { try { old.ws.close(); } catch(e){} }
      online.set(user.uid, client);
      ws.send(JSON.stringify({ type: 'history', data: getMsgs().filter(m => !m.gid || m.gid === 'default') }));
      ws.send(JSON.stringify({ type: 'online', data: onlineList() }));
      broadcast({ type: 'system', data: sysMsg(`${user.nick} 加入了社区`) });
      broadcast({ type: 'online', data: onlineList() });
      return;
    }

    if (!client) return;
    const groups = getGroups();

    if (msg.type === 'chat') {
      const chatBan = banOf(client.uid);
      if (chatBan.banned) { ws.send(JSON.stringify({ type: 'banned', data: { until: chatBan.until, reason: chatBan.reason } })); return; }
      const gid = msg.data.gid || 'default';
      const g = groups[gid];
      if (!g) { ws.send(JSON.stringify({type:'error',data:{msg:'群不存在'}})); return; }
      if (!(g.members || []).includes(client.uid)) { ws.send(JSON.stringify({type:'error',data:{msg:'你不在这个群'}})); return; }
      const mutedUntil = (g.muted || {})[client.uid] || 0;
      if (mutedUntil > Date.now()) {
        const sec = Math.ceil((mutedUntil - Date.now())/1000);
        ws.send(JSON.stringify({type:'error',data:{msg:`你被禁言了，还剩${sec}秒`}})); return;
      }
      const content = String(msg.data.content || '');
      const t = msg.data.type;
      const scan = scanText(content);
      if (scan.hit) {
        const p = punishViolation(client.uid, client.nick, scan.word, scan.level);
        ws.send(JSON.stringify({ type: 'violation', data: {
          word: scan.word, level: scan.level, count: p.count, banned: p.banned, until: p.until, reason: p.reason,
          msg: p.banned ? ('消息含违规内容「' + scan.word + '」，累计违规 ' + p.count + ' 次，账号已被封禁') :
                 ('消息含违规内容「' + scan.word + '」，已被拦截（累计' + p.count + '次，满' + getBadwords().threshold + '次封号1天）')
        } }));
        if (p.banned) ws.send(JSON.stringify({ type: 'banned', data: { until: p.until, reason: p.reason } }));
        return;
      }
      const msgType = t === 'image' ? 'image' : t === 'video' ? 'video' : t === 'audio' ? 'audio' : 'text';
      if ((msgType === 'image' || msgType === 'video' || msgType === 'audio') && !/^\/uploads\//.test(content)) {
        ws.send(JSON.stringify({type:'error',data:{msg:'文件地址无效'}})); return;
      }
      if (msgType === 'text' && content.length > 2000) { ws.send(JSON.stringify({type:'error',data:{msg:'消息过长'}})); return; }
      const users = getUsers();
      const u = users.find(x => x.uid === client.uid);
      const chatMsg = {
        id: Date.now()+'-'+Math.floor(Math.random()*10000), gid,
        senderId: client.uid, sender: u?u.nick:client.nick, avatar: u&&u.avatar?u.avatar:'',
        type: msgType, content, time: Date.now(), replyTo: msg.data.replyTo || null
      };
      const msgs = getMsgs();
      msgs.push(chatMsg);
      saveMsgs(msgs);
      broadcastToGroup(gid, { type: 'chat', data: chatMsg });
      return;
    }

    if (msg.type === 'join') {
      const gid = msg.data.gid || 'default';
      const g = groups[gid];
      if (!g) return;
      if (!g.members.includes(client.uid)) {
        g.members.push(client.uid);
        saveGroups(groups);
      }
      const msgs = getMsgs().filter(m => m.gid === gid || (!m.gid && gid === 'default'));
      ws.send(JSON.stringify({ type: 'history', data: msgs }));
      broadcastToGroup(gid, { type: 'system', data: sysMsg(`${client.nick} 加入了群`, gid) }, client.uid);
      return;
    }

    if (msg.type === 'kick') {
      const gid = msg.data.gid, targetUid = msg.data.targetUid;
      const g = groups[gid];
      if (!g) return;
      const isOwner = g.owner === client.uid;
      const isAdmin = (g.admins || []).includes(client.uid);
      if (!isOwner && !isAdmin) { ws.send(JSON.stringify({type:'error',data:{msg:'无权操作'}})); return; }
      if (!isOwner && (g.owner === targetUid || (g.admins || []).includes(targetUid))) {
        ws.send(JSON.stringify({type:'error',data:{msg:'管理员只能踢普通成员'}})); return;
      }
      g.members = (g.members || []).filter(u => u !== targetUid);
      g.admins = (g.admins || []).filter(u => u !== targetUid);
      delete g.muted[targetUid];
      saveGroups(groups);
      broadcastToGroup(gid, { type: 'system', data: sysMsg(`${client.nick} 将某成员移出了群`, gid) });
      sendTo(targetUid, { type: 'kicked', data: { gid } });
      return;
    }

    if (msg.type === 'setAdmin') {
      const gid = msg.data.gid, targetUid = msg.data.targetUid, add = !!msg.data.add;
      const g = groups[gid];
      if (!g || g.owner !== client.uid) { ws.send(JSON.stringify({type:'error',data:{msg:'只有群主能设管理员'}})); return; }
      g.admins = g.admins || [];
      if (add && !g.admins.includes(targetUid)) g.admins.push(targetUid);
      if (!add) g.admins = g.admins.filter(u => u !== targetUid);
      saveGroups(groups);
      broadcastToGroup(gid, { type: 'system', data: sysMsg(add ? `某成员被设为管理员` : `某管理员被取消`, gid) });
      return;
    }

    if (msg.type === 'mute') {
      const gid = msg.data.gid, targetUid = msg.data.targetUid, seconds = Number(msg.data.seconds) || 0;
      const g = groups[gid];
      if (!g) return;
      const isOwner = g.owner === client.uid;
      const isAdmin = (g.admins || []).includes(client.uid);
      if (!isOwner && !isAdmin) { ws.send(JSON.stringify({type:'error',data:{msg:'无权操作'}})); return; }
      if (!isOwner && (g.owner === targetUid || (g.admins || []).includes(targetUid))) {
        ws.send(JSON.stringify({type:'error',data:{msg:'不能禁言管理员'}})); return;
      }
      g.muted = g.muted || {};
      if (seconds <= 0) delete g.muted[targetUid];
      else g.muted[targetUid] = Date.now() + seconds * 1000;
      saveGroups(groups);
      broadcastToGroup(gid, { type: 'system', data: sysMsg(seconds>0 ? `某成员被禁言${seconds}秒` : `某成员被解除禁言`, gid) });
      return;
    }

    if (msg.type === 'dm') {
      const dmBan = banOf(client.uid);
      if (dmBan.banned) { ws.send(JSON.stringify({ type: 'banned', data: { until: dmBan.until, reason: dmBan.reason } })); return; }
      const toUid = String(msg.data.toUid || '');
      const content = String(msg.data.content || '');
      if (!/^\/uploads\//.test(content)) {
        const scan = scanText(content);
        if (scan.hit) {
          const p = punishViolation(client.uid, client.nick, scan.word, scan.level);
          ws.send(JSON.stringify({ type: 'violation', data: {
            word: scan.word, level: scan.level, count: p.count, banned: p.banned, until: p.until, reason: p.reason,
            msg: p.banned ? ('消息含违规内容「' + scan.word + '」，累计违规 ' + p.count + ' 次，账号已被封禁') :
                   ('消息含违规内容「' + scan.word + '」，已被拦截')
          } }));
          if (p.banned) ws.send(JSON.stringify({ type: 'banned', data: { until: p.until, reason: p.reason } }));
          return;
        }
      }
      const t = msg.data && msg.data.type;
      const msgType = t === 'image' ? 'image' : t === 'video' ? 'video' : t === 'audio' ? 'audio' : 'text';
      if (!toUid) return;
      const dmMsg = {
        id: 'dm-'+Date.now()+'-'+Math.floor(Math.random()*10000),
        from: client.uid, fromNick: client.nick, fromAvatar: client.avatar || '',
        to: toUid, type: msgType, content, time: Date.now()
      };
      const dms = getDMs();
      const key = [client.uid, toUid].sort().join('|');
      if (!dms[key]) dms[key] = [];
      dms[key].push(dmMsg);
      saveDMs(dms);
      sendTo(toUid, { type: 'dm', data: dmMsg });
      sendTo(client.uid, { type: 'dm', data: dmMsg });
      return;
    }

    if (msg.type === 'delete') {
      const msgId = String(msg.data && msg.data.id || '');
      if (!msgId) return;
      const msgs = getMsgs();
      const idx = msgs.findIndex(m => m.id === msgId);
      if (idx < 0) return;
      if (msgs[idx].senderId !== client.uid) {
        ws.send(JSON.stringify({type:'error',data:{msg:'只能撤回自己的'}})); return;
      }
      const gid = msgs[idx].gid || 'default';
      msgs.splice(idx, 1);
      saveMsgs(msgs);
      broadcastToGroup(gid, { type: 'delete', data: { id: msgId } });
      return;
    }
  });

  ws.on('close', () => {
    if (client) {
      online.delete(client.uid);
      broadcast({ type: 'system', data: sysMsg(`${client.nick} 离开了社区`) });
      broadcast({ type: 'online', data: onlineList() });
    }
  });
  ws.on('error', () => {});
});

(async () => {
  try { await initStore(); }
  catch (e) { console.error('MongoDB 连接失败，回退本地文件存储:', e.message); storeReady = false; }
  ensureDefaultGroup();
  server.listen(PORT, '0.0.0.0', () => console.log('神修尼亚社区服务器已启动，端口 ' + PORT + (storeReady ? '（MongoDB 存储）' : '（本地文件存储）')));
})();
