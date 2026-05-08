// ----------------------------------------------------------------------
//  Scrap Metal Arena — multiplayer signaling + state-relay server
//
//  Run with:
//    npm install
//    node server.js
//
//  Then in the game's Settings, set the Server URL to ws://localhost:8080
//  (or wherever you deploy this — Render / Fly / Railway / your VPS).
// ----------------------------------------------------------------------

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Optional dependencies — only loaded if installed and configured.
// (`pg` for Postgres, `google-auth-library` for Google sign-in.)
let pg = null;
let GoogleOAuth2Client = null;
try { pg = require('pg'); } catch (e) { /* optional */ }
try { GoogleOAuth2Client = require('google-auth-library').OAuth2Client; } catch (e) { /* optional */ }

const PORT = parseInt(process.env.PORT || '8080', 10);
const wss = new WebSocket.Server({ port: PORT });

console.log(`[arena] WebSocket server listening on :${PORT}`);

// --- Postgres connection (Neon / any Postgres) ---
const DATABASE_URL = process.env.DATABASE_URL || '';
const HAS_DB = !!DATABASE_URL && !!pg;
const pgPool = HAS_DB
  ? new pg.Pool({
      connectionString: DATABASE_URL,
      // Neon (and most managed Postgres) require SSL. Allow self-signed certs.
      ssl: { rejectUnauthorized: false },
      max: 5,
    })
  : null;

if (HAS_DB) {
  console.log('[arena] using Postgres for accounts');
  pgPool.query(`
    CREATE TABLE IF NOT EXISTS accounts (
      username      TEXT PRIMARY KEY,
      salt          TEXT,
      password_hash TEXT,
      google_sub    TEXT UNIQUE,
      email         TEXT,
      display_name  TEXT,
      save          TEXT NOT NULL DEFAULT '',
      created_at    BIGINT NOT NULL,
      last_login    BIGINT NOT NULL
    );
  `).then(() => pgPool.query(
    `CREATE INDEX IF NOT EXISTS accounts_google_sub_idx ON accounts(google_sub);`
  )).then(() => {
    console.log('[arena] Postgres schema ready');
  }).catch((e) => {
    console.warn('[arena] schema init failed:', e.message);
  });
} else if (DATABASE_URL && !pg) {
  console.warn('[arena] DATABASE_URL is set but `pg` package is not installed — falling back to file storage');
} else {
  console.log('[arena] DATABASE_URL not set — using file storage (set it to enable Postgres)');
}

// --- Google OAuth verification ---
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '';
const googleClient = (GoogleOAuth2Client && GOOGLE_CLIENT_ID)
  ? new GoogleOAuth2Client(GOOGLE_CLIENT_ID)
  : null;
if (googleClient) {
  console.log('[arena] Google sign-in enabled');
} else if (GOOGLE_CLIENT_ID) {
  console.warn('[arena] GOOGLE_CLIENT_ID is set but `google-auth-library` is not installed');
}

// --- Registry ---
// Map<playerID, { ws, name, partyID, lastSeen, state }>
const players = new Map();
// Map<partyID, { hostID, members: Set<playerID>, started: boolean, startedAt }>
const parties = new Map();
let nextPartyID = 1;

// --- Global leaderboard ---
// Map<playerID, { name, wins, score, kills, updatedAt }>
// Persisted to disk every 60s. Survives reboots.
//
// Bumping the LEADERBOARD_VERSION wipes every existing entry on first boot
// after deploy — useful for "reset the leaderboard". The file gets rewritten
// fresh and the old version is renamed to leaderboard.<oldver>.bak just in case.
const LEADERBOARD_VERSION = 2;
const LEADERBOARD_FILE   = path.join(__dirname, 'leaderboard.json');
// Per-player progress JSON saves (cross-device cloud sync)
const CLOUD_DIR          = path.join(__dirname, 'cloud-saves');
try { fs.mkdirSync(CLOUD_DIR, { recursive: true }); } catch (e) {}

// --- Accounts: Postgres (preferred) with file fallback ---
// Keep an in-memory file-mode store for local dev / fallback, and a one-time
// migration from accounts.json into Postgres on first DB connection so
// existing users don't lose their accounts.
const ACCOUNTS_FILE = path.join(__dirname, 'accounts.json');
const fileAccounts = {};
function loadFileAccounts() {
  try {
    if (fs.existsSync(ACCOUNTS_FILE)) {
      Object.assign(fileAccounts, JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8')));
      console.log(`[arena] loaded ${Object.keys(fileAccounts).length} file-mode accounts`);
    }
  } catch (e) { console.warn('[arena] file account load failed:', e.message); }
}
function saveFileAccounts() {
  try { fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(fileAccounts), 'utf8'); }
  catch (e) { console.warn('[arena] file account save failed:', e.message); }
}
loadFileAccounts();
// Only persist file accounts when DB is NOT in use; otherwise the file is read-only legacy.
if (!HAS_DB) setInterval(saveFileAccounts, 30_000);

function hashPassword(pwd, salt) {
  return crypto.createHash('sha256').update(salt + ':' + pwd).digest('hex');
}
function makeToken() { return crypto.randomBytes(16).toString('hex'); }
const sessions = {}; // token -> username (in-memory; restart means re-login)

// One-time migration: pull the legacy accounts.json contents into Postgres.
// Idempotent — ON CONFLICT DO NOTHING means existing rows are untouched.
async function migrateFileAccountsToDB() {
  if (!HAS_DB) return;
  const usernames = Object.keys(fileAccounts);
  if (!usernames.length) return;
  let migrated = 0;
  for (const u of usernames) {
    const a = fileAccounts[u] || {};
    try {
      const r = await pgPool.query(
        `INSERT INTO accounts (username, salt, password_hash, save, created_at, last_login)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (username) DO NOTHING`,
        [u, a.salt || '', a.passwordHash || '', a.save || '',
         Number(a.createdAt) || Date.now(),
         Number(a.lastLogin) || Date.now()]
      );
      if (r.rowCount) migrated++;
    } catch (e) {
      console.warn(`[arena] migrate ${u} failed:`, e.message);
    }
  }
  if (migrated) console.log(`[arena] migrated ${migrated} file accounts → Postgres`);
}
// Run migration after the schema-init promise has settled
setTimeout(() => { migrateFileAccountsToDB().catch(() => {}); }, 1500);

// Storage abstraction. All methods async and return plain JS objects shaped
// like the legacy file-account record { salt, passwordHash, save, createdAt,
// lastLogin, googleSub?, email?, displayName? }.
const accountStore = {
  async get(username) {
    if (HAS_DB) {
      try {
        const r = await pgPool.query(
          `SELECT username, salt, password_hash, google_sub, email, display_name,
                  save, created_at, last_login
             FROM accounts WHERE username = $1`,
          [username]
        );
        if (!r.rows.length) return null;
        const row = r.rows[0];
        return {
          username: row.username,
          salt: row.salt || '',
          passwordHash: row.password_hash || '',
          googleSub: row.google_sub || null,
          email: row.email || null,
          displayName: row.display_name || null,
          save: row.save || '',
          createdAt: Number(row.created_at),
          lastLogin: Number(row.last_login),
        };
      } catch (e) { console.warn('[arena] db get failed:', e.message); return null; }
    }
    return fileAccounts[username] ? { username, ...fileAccounts[username] } : null;
  },
  async getByGoogleSub(sub) {
    if (HAS_DB) {
      try {
        const r = await pgPool.query(
          `SELECT username FROM accounts WHERE google_sub = $1`, [sub]
        );
        if (!r.rows.length) return null;
        return await this.get(r.rows[0].username);
      } catch (e) { console.warn('[arena] db getByGoogleSub failed:', e.message); return null; }
    }
    for (const u in fileAccounts) {
      if (fileAccounts[u].googleSub === sub) return { username: u, ...fileAccounts[u] };
    }
    return null;
  },
  async create(username, data) {
    const now = Date.now();
    if (HAS_DB) {
      await pgPool.query(
        `INSERT INTO accounts (username, salt, password_hash, google_sub, email, display_name,
                               save, created_at, last_login)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [username, data.salt || null, data.passwordHash || null,
         data.googleSub || null, data.email || null, data.displayName || null,
         data.save || '', now, now]
      );
    } else {
      fileAccounts[username] = {
        salt: data.salt || '', passwordHash: data.passwordHash || '',
        googleSub: data.googleSub || null, email: data.email || null,
        displayName: data.displayName || null,
        save: data.save || '', createdAt: now, lastLogin: now,
      };
      saveFileAccounts();
    }
  },
  async updateLogin(username) {
    const now = Date.now();
    if (HAS_DB) {
      try { await pgPool.query('UPDATE accounts SET last_login = $1 WHERE username = $2', [now, username]); }
      catch (e) {}
    } else if (fileAccounts[username]) {
      fileAccounts[username].lastLogin = now;
      saveFileAccounts();
    }
  },
  async updateSave(username, save) {
    if (HAS_DB) {
      try { await pgPool.query('UPDATE accounts SET save = $1 WHERE username = $2', [save, username]); }
      catch (e) { console.warn('[arena] db updateSave failed:', e.message); }
    } else if (fileAccounts[username]) {
      fileAccounts[username].save = save;
      saveFileAccounts();
    }
  },
  async linkGoogle(username, googleSub, email, displayName) {
    if (HAS_DB) {
      try {
        await pgPool.query(
          'UPDATE accounts SET google_sub = $1, email = $2, display_name = $3 WHERE username = $4',
          [googleSub, email || null, displayName || null, username]
        );
      } catch (e) { console.warn('[arena] db linkGoogle failed:', e.message); }
    } else if (fileAccounts[username]) {
      fileAccounts[username].googleSub = googleSub;
      fileAccounts[username].email = email || null;
      fileAccounts[username].displayName = displayName || null;
      saveFileAccounts();
    }
  },
};

// Rough "how much progress is in this save" score so we can refuse to
// overwrite a richer save with a poorer one. The dimensions chosen here
// (money, level, xp, garage size, upgrade count) are monotonic in normal
// gameplay — they only ever go up, they never go down — so a regression
// almost certainly means the client pushed a fresh-default save by mistake.
function _saveProgressScore(saveStr) {
  if (!saveStr || typeof saveStr !== 'string') return 0;
  let s;
  try { s = JSON.parse(saveStr); } catch (e) { return 0; }
  if (!s || typeof s !== 'object') return 0;
  let score = 0;
  if (typeof s.money === 'number')  score += Math.max(0, s.money);
  if (typeof s.level === 'number')  score += Math.max(0, s.level) * 5000;
  if (typeof s.xp === 'number')     score += Math.max(0, s.xp) * 10;
  if (Array.isArray(s.cars))        score += s.cars.length * 2000;
  if (s.ownedCars && typeof s.ownedCars === 'object') {
    score += Object.keys(s.ownedCars).length * 2000;
  }
  if (s.upgrades && typeof s.upgrades === 'object') {
    for (const k in s.upgrades) {
      const v = s.upgrades[k];
      if (typeof v === 'number') score += Math.max(0, v) * 200;
    }
  }
  return score;
}
const VERSION_FILE       = path.join(__dirname, 'leaderboard.version');
const leaderboard = new Map();
function loadLeaderboard() {
  try {
    // Check the on-disk version. If it's older than LEADERBOARD_VERSION, wipe.
    let onDiskVersion = 1;
    try {
      if (fs.existsSync(VERSION_FILE)) {
        onDiskVersion = parseInt(fs.readFileSync(VERSION_FILE, 'utf8'), 10) || 1;
      }
    } catch (e) {}
    if (onDiskVersion < LEADERBOARD_VERSION) {
      // Back up the old file (just in case), then wipe.
      try {
        if (fs.existsSync(LEADERBOARD_FILE)) {
          const bak = LEADERBOARD_FILE.replace('.json', `.${onDiskVersion}.bak`);
          fs.renameSync(LEADERBOARD_FILE, bak);
          console.log(`[arena] leaderboard reset — old file backed up to ${bak}`);
        }
      } catch (e) { /* ignore — proceed with wipe */ }
      try { fs.writeFileSync(VERSION_FILE, String(LEADERBOARD_VERSION), 'utf8'); } catch (e) {}
      // Leave the in-memory Map empty.
      console.log(`[arena] leaderboard wiped (version ${onDiskVersion} → ${LEADERBOARD_VERSION})`);
      return;
    }
    if (!fs.existsSync(LEADERBOARD_FILE)) return;
    const raw = fs.readFileSync(LEADERBOARD_FILE, 'utf8');
    const data = JSON.parse(raw);
    if (data && typeof data === 'object') {
      for (const id in data) leaderboard.set(id, data[id]);
      console.log(`[arena] loaded ${leaderboard.size} leaderboard entries`);
    }
  } catch (e) { console.warn('[arena] leaderboard load failed:', e.message); }
}
function saveLeaderboard() {
  try {
    const obj = {};
    for (const [id, v] of leaderboard) obj[id] = v;
    fs.writeFileSync(LEADERBOARD_FILE, JSON.stringify(obj), 'utf8');
  } catch (e) { console.warn('[arena] leaderboard save failed:', e.message); }
}
loadLeaderboard();
setInterval(saveLeaderboard, 60_000);
process.on('SIGTERM', () => { saveLeaderboard(); if (!HAS_DB) saveFileAccounts(); });
process.on('SIGINT', () => { saveLeaderboard(); if (!HAS_DB) saveFileAccounts(); process.exit(0); });

function getTopLeaderboard(limit = 50) {
  const arr = [];
  for (const [id, v] of leaderboard) {
    arr.push({
      id,
      name: v.name || '—',
      wins: v.wins || 0,
      score: v.score || 0,
      kills: v.kills || 0,
      updatedAt: v.updatedAt || 0,
    });
  }
  arr.sort((a, b) => (b.wins - a.wins) || (b.score - a.score) || (b.kills - a.kills));
  return arr.slice(0, limit);
}

function makePartyID() { return 'p' + (nextPartyID++); }

function send(ws, type, payload = {}) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  try { ws.send(JSON.stringify({ type, ...payload })); }
  catch (e) { /* ignore broken pipes */ }
}

function broadcastToParty(partyID, type, payload, exceptID) {
  const party = parties.get(partyID);
  if (!party) return;
  for (const memberID of party.members) {
    if (exceptID && memberID === exceptID) continue;
    const p = players.get(memberID);
    if (p) send(p.ws, type, payload);
  }
}

function getPartyView(partyID) {
  const party = parties.get(partyID);
  if (!party) return null;
  return {
    partyID,
    hostID: party.hostID,
    started: party.started,
    members: Array.from(party.members).map(id => {
      const p = players.get(id);
      return { id, name: p ? p.name : '—', online: !!p };
    }),
  };
}

function leaveParty(playerID) {
  const player = players.get(playerID);
  if (!player || !player.partyID) return;
  const partyID = player.partyID;
  const party = parties.get(partyID);
  player.partyID = null;
  if (!party) return;
  party.members.delete(playerID);
  if (party.hostID === playerID) {
    // Transfer host to next member, or close empty party
    if (party.members.size > 0) {
      party.hostID = party.members.values().next().value;
    }
  }
  if (party.members.size === 0) {
    parties.delete(partyID);
  } else {
    broadcastToParty(partyID, 'partyUpdate', { party: getPartyView(partyID) });
  }
}

wss.on('connection', (ws) => {
  let myID = null;

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    switch (msg.type) {

      // -------- Register / login --------
      case 'register': {
        const id = String(msg.playerID || '').trim();
        const name = String(msg.name || 'Anonymous').slice(0, 32);
        const carColor = (typeof msg.carColor === 'number') ? msg.carColor : 0x4cc9f0;
        if (!/^\d{6}$/.test(id)) {
          send(ws, 'error', { message: 'Invalid player ID (must be 6 digits)' });
          return;
        }
        // Kick old session with same ID
        const existing = players.get(id);
        if (existing && existing.ws !== ws) {
          send(existing.ws, 'kicked', { reason: 'Logged in elsewhere' });
          try { existing.ws.close(); } catch (e) {}
        }
        myID = id;
        players.set(id, { ws, name, partyID: null, lastSeen: Date.now(), carColor, state: null });
        send(ws, 'registered', { playerID: id, name });
        return;
      }

      // -------- Search by ID --------
      case 'searchPlayer': {
        const id = String(msg.playerID || '').trim();
        if (!myID) return;
        if (!/^\d{6}$/.test(id)) {
          send(ws, 'searchResult', { playerID: id, found: false, reason: 'Invalid ID' });
          return;
        }
        const found = players.get(id);
        if (!found) {
          send(ws, 'searchResult', { playerID: id, found: false, reason: 'Player offline' });
        } else {
          send(ws, 'searchResult', {
            playerID: id, found: true, name: found.name,
            inParty: !!found.partyID, inMatch: !!(found.partyID && parties.get(found.partyID)?.started),
          });
        }
        return;
      }

      // -------- Send invite --------
      case 'invite': {
        if (!myID) return;
        const targetID = String(msg.playerID || '').trim();
        if (targetID === myID) {
          send(ws, 'error', { message: "You can't invite yourself" });
          return;
        }
        const target = players.get(targetID);
        const me = players.get(myID);
        if (!target) { send(ws, 'error', { message: 'Player offline' }); return; }
        if (!me) return;
        // Create our party if we don't have one
        if (!me.partyID) {
          const pid = makePartyID();
          parties.set(pid, { hostID: myID, members: new Set([myID]), started: false, startedAt: 0 });
          me.partyID = pid;
          send(ws, 'partyUpdate', { party: getPartyView(pid) });
        }
        const party = parties.get(me.partyID);
        if (!party || party.hostID !== myID) {
          send(ws, 'error', { message: 'Only the party host can invite' });
          return;
        }
        if (party.started) {
          send(ws, 'error', { message: 'Match already started' });
          return;
        }
        if (party.members.has(targetID)) {
          send(ws, 'error', { message: 'Already in your party' });
          return;
        }
        send(target.ws, 'partyInvite', {
          fromID: myID, fromName: me.name, partyID: me.partyID,
        });
        send(ws, 'inviteSent', { toID: targetID });
        return;
      }

      // -------- Accept invite --------
      case 'acceptInvite': {
        if (!myID) return;
        const partyID = String(msg.partyID || '');
        const party = parties.get(partyID);
        const me = players.get(myID);
        if (!party || !me) { send(ws, 'error', { message: 'Invitation no longer valid' }); return; }
        if (party.started) { send(ws, 'error', { message: 'Match already started' }); return; }
        // Leave any current party first
        if (me.partyID && me.partyID !== partyID) leaveParty(myID);
        party.members.add(myID);
        me.partyID = partyID;
        broadcastToParty(partyID, 'partyUpdate', { party: getPartyView(partyID) });
        return;
      }

      // -------- Decline invite --------
      case 'declineInvite': {
        const fromID = String(msg.fromID || '');
        const from = players.get(fromID);
        if (from) send(from.ws, 'inviteDeclined', { byID: myID });
        return;
      }

      // -------- Leave party --------
      case 'leaveParty': {
        if (!myID) return;
        leaveParty(myID);
        send(ws, 'partyLeft', {});
        return;
      }

      // -------- Start match (host only) --------
      case 'startMatch': {
        if (!myID) return;
        const me = players.get(myID);
        if (!me || !me.partyID) return;
        const party = parties.get(me.partyID);
        if (!party || party.hostID !== myID) {
          send(ws, 'error', { message: 'Only host can start' });
          return;
        }
        party.started = true;
        party.startedAt = Date.now();
        broadcastToParty(me.partyID, 'matchStart', {
          partyID: me.partyID,
          seed: Math.floor(Math.random() * 0xffffffff),
          members: Array.from(party.members),
        });
        return;
      }

      // -------- End match (host only, or auto when only 1 alive) --------
      case 'endMatch': {
        if (!myID) return;
        const me = players.get(myID);
        if (!me || !me.partyID) return;
        const party = parties.get(me.partyID);
        if (!party) return;
        party.started = false;
        broadcastToParty(me.partyID, 'matchEnd', { winnerID: msg.winnerID || null });
        return;
      }

      // -------- Real-time state relay --------
      case 'state': {
        if (!myID) return;
        const me = players.get(myID);
        if (!me || !me.partyID) return;
        const party = parties.get(me.partyID);
        if (!party || !party.started) return;
        // Cache last state so late-joining peers can sync
        me.state = msg.state || null;
        // Forward to every other party member
        broadcastToParty(me.partyID, 'peerState', { fromID: myID, state: msg.state }, myID);
        return;
      }

      // -------- Damage event (client-authoritative) --------
      case 'hit': {
        if (!myID) return;
        const me = players.get(myID);
        if (!me || !me.partyID) return;
        const party = parties.get(me.partyID);
        if (!party || !party.started) return;
        const targetID = String(msg.targetID || '');
        const target = players.get(targetID);
        if (!target || target.partyID !== me.partyID) return;
        send(target.ws, 'youWereHit', {
          fromID: myID, damage: Number(msg.damage) || 0, weapon: msg.weapon || 'mg',
        });
        return;
      }

      // -------- Player died (client says so) --------
      case 'died': {
        if (!myID) return;
        const me = players.get(myID);
        if (!me || !me.partyID) return;
        broadcastToParty(me.partyID, 'peerDied', { fromID: myID, killerID: msg.killerID || null });
        return;
      }

      // -------- Peer fired a weapon (visual only — damage is via 'hit') --------
      case 'peerFire': {
        if (!myID) return;
        const me = players.get(myID);
        if (!me || !me.partyID) return;
        const party = parties.get(me.partyID);
        if (!party || !party.started) return;
        broadcastToParty(me.partyID, 'peerFire', {
          fromID: myID,
          weapon: msg.weapon || 'mg',
          x: Number(msg.x) || 0, y: Number(msg.y) || 0, z: Number(msg.z) || 0,
          dx: Number(msg.dx) || 0, dz: Number(msg.dz) || 0,
        }, myID);
        return;
      }

      // -------- Peer triggered a special ability (visual broadcast) --------
      case 'peerSpecial': {
        if (!myID) return;
        const me = players.get(myID);
        if (!me || !me.partyID) return;
        const party = parties.get(me.partyID);
        if (!party || !party.started) return;
        broadcastToParty(me.partyID, 'peerSpecial', {
          fromID: myID,
          type: String(msg.type || ''),
          x: Number(msg.x) || 0, y: Number(msg.y) || 0, z: Number(msg.z) || 0,
          radius: Number(msg.radius) || 0,
        }, myID);
        return;
      }

      // -------- Cloud save: persist a player's progress JSON to disk --------
      // Stored as cloud-saves/<playerID>.json. Same player ID on a different
      // device/domain means the same cloud save — that's the cross-domain
      // bridge. Capped at 100KB to prevent abuse.
      case 'cloudSave': {
        const id = String(msg.playerID || myID || '').trim();
        if (!/^\d{6}$/.test(id)) return;
        const save = typeof msg.save === 'string' ? msg.save : JSON.stringify(msg.save || {});
        if (save.length > 100000) return;
        try {
          if (!fs.existsSync(CLOUD_DIR)) fs.mkdirSync(CLOUD_DIR, { recursive: true });
          fs.writeFileSync(path.join(CLOUD_DIR, id + '.json'), save, 'utf8');
          send(ws, 'cloudSaveAck', { ok: true });
        } catch (e) {
          send(ws, 'cloudSaveAck', { ok: false, error: e.message });
        }
        return;
      }

      // -------- Cloud load: read back a player's progress JSON --------
      case 'cloudLoad': {
        const id = String(msg.playerID || myID || '').trim();
        if (!/^\d{6}$/.test(id)) {
          send(ws, 'cloudLoadResult', { ok: false, save: null, playerID: id });
          return;
        }
        let save = null;
        try {
          const p = path.join(CLOUD_DIR, id + '.json');
          if (fs.existsSync(p)) save = fs.readFileSync(p, 'utf8');
        } catch (e) {}
        send(ws, 'cloudLoadResult', { ok: true, save, playerID: id });
        return;
      }

      // -------- Account: register --------
      case 'accountRegister': {
        const username = String(msg.username || '').trim().toLowerCase();
        const password = String(msg.password || '');
        if (username.length < 3 || username.length > 16) {
          send(ws, 'accountAuthResult', { ok: false, error: `Username must be 3–16 characters (got ${username.length})` });
          return;
        }
        if (/\s/.test(username)) {
          send(ws, 'accountAuthResult', { ok: false, error: 'Username can\'t contain spaces' });
          return;
        }
        if (password.length < 4) {
          send(ws, 'accountAuthResult', { ok: false, error: 'Password too short (4+ chars)' });
          return;
        }
        try {
          const existing = await accountStore.get(username);
          if (existing) {
            send(ws, 'accountAuthResult', { ok: false, error: 'Username already taken' });
            return;
          }
          const salt = crypto.randomBytes(8).toString('hex');
          await accountStore.create(username, {
            salt, passwordHash: hashPassword(password, salt), save: '',
          });
          const token = makeToken();
          sessions[token] = username;
          send(ws, 'accountAuthResult', { ok: true, username, token, save: '' });
        } catch (e) {
          console.warn('[arena] register failed:', e.message);
          send(ws, 'accountAuthResult', { ok: false, error: 'Server error — try again in a moment' });
        }
        return;
      }

      // -------- Account: login --------
      case 'accountLogin': {
        const username = String(msg.username || '').trim().toLowerCase();
        const password = String(msg.password || '');
        try {
          const acc = await accountStore.get(username);
          if (!acc) {
            send(ws, 'accountAuthResult', { ok: false, error: 'Account not found' });
            return;
          }
          if (!acc.passwordHash || acc.passwordHash !== hashPassword(password, acc.salt)) {
            send(ws, 'accountAuthResult', { ok: false, error: 'Wrong password' });
            return;
          }
          const token = makeToken();
          sessions[token] = username;
          accountStore.updateLogin(username).catch(() => {});
          send(ws, 'accountAuthResult', { ok: true, username, token, save: acc.save || '' });
        } catch (e) {
          console.warn('[arena] login failed:', e.message);
          send(ws, 'accountAuthResult', { ok: false, error: 'Server error — try again in a moment' });
        }
        return;
      }

      // -------- Account: Google sign-in --------
      // Verifies a Google ID token, then either signs into an existing
      // Google-linked account or creates a new one. The username for new
      // Google accounts is derived from the email local-part (with a numeric
      // suffix if the base name is taken). Existing username/password
      // accounts can OPT to link Google later via 'accountLinkGoogle'.
      case 'accountGoogleSignIn': {
        if (!googleClient) {
          send(ws, 'accountAuthResult', { ok: false, error: 'Google sign-in not configured on server' });
          return;
        }
        const idToken = String(msg.idToken || '');
        if (!idToken) {
          send(ws, 'accountAuthResult', { ok: false, error: 'Missing ID token' });
          return;
        }
        try {
          const ticket = await googleClient.verifyIdToken({
            idToken, audience: GOOGLE_CLIENT_ID,
          });
          const payload = ticket.getPayload();
          if (!payload || !payload.sub) {
            send(ws, 'accountAuthResult', { ok: false, error: 'Invalid Google token' });
            return;
          }
          const sub = payload.sub;
          const email = payload.email || null;
          const displayName = payload.name || (email ? email.split('@')[0] : 'player');
          // 1) Find by Google sub
          let acc = await accountStore.getByGoogleSub(sub);
          let username;
          if (acc) {
            username = acc.username;
          } else {
            // 2) New Google account — derive a username from email
            const base = (email ? email.split('@')[0] : 'player')
              .toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 12) || 'player';
            let candidate = base;
            let n = 0;
            while (await accountStore.get(candidate)) {
              n += 1;
              candidate = (base + n).slice(0, 16);
              if (n > 9999) { candidate = 'p' + Date.now().toString(36); break; }
            }
            username = candidate;
            await accountStore.create(username, {
              googleSub: sub, email, displayName, save: '',
            });
            acc = await accountStore.get(username);
          }
          const token = makeToken();
          sessions[token] = username;
          accountStore.updateLogin(username).catch(() => {});
          send(ws, 'accountAuthResult', {
            ok: true, username, token, save: acc.save || '',
            email, displayName,
          });
        } catch (e) {
          console.warn('[arena] google sign-in failed:', e.message);
          send(ws, 'accountAuthResult', { ok: false, error: 'Google verification failed' });
        }
        return;
      }

      // -------- Account: resume an existing session via token --------
      case 'accountResume': {
        const token = String(msg.token || '');
        const username = sessions[token];
        if (!username) {
          send(ws, 'accountAuthResult', { ok: false, error: 'Session expired', expired: true });
          return;
        }
        try {
          const acc = await accountStore.get(username);
          if (!acc) {
            send(ws, 'accountAuthResult', { ok: false, error: 'Session expired', expired: true });
            return;
          }
          send(ws, 'accountAuthResult', { ok: true, username, token, save: acc.save || '' });
        } catch (e) {
          send(ws, 'accountAuthResult', { ok: false, error: 'Server error', expired: false });
        }
        return;
      }

      // -------- Account: push the player's progress JSON --------
      case 'accountSave': {
        const token = String(msg.token || '');
        const username = sessions[token];
        if (!username) return;
        const save = typeof msg.save === 'string' ? msg.save : JSON.stringify(msg.save || {});
        if (save.length > 100000) return;
        try {
          const acc = await accountStore.get(username);
          if (!acc) return;
          // REGRESSION GUARD: refuse a save that's significantly poorer than
          // what we already have. This is what destroyed real progress when
          // a fresh-default-empty local save got pushed up before we restored
          // from cloud. The threshold (5000) ignores noise at low levels
          // while still blocking obvious wipes.
          const oldScore = _saveProgressScore(acc.save);
          const newScore = _saveProgressScore(save);
          if (oldScore > 0 && newScore + 5000 < oldScore) {
            send(ws, 'accountSaveAck', { ok: false, error: 'rejected: would lose progress', oldScore, newScore });
            return;
          }
          await accountStore.updateSave(username, save);
          send(ws, 'accountSaveAck', { ok: true, score: newScore });
        } catch (e) {
          console.warn('[arena] save failed:', e.message);
        }
        return;
      }

      // -------- Account: read your own cloud save (diagnostic + recovery) --------
      // Auth via token if live, else fall back to username + password.
      // Returns the FULL raw save string so the client can offer "restore".
      case 'accountInspect': {
        let username = null;
        let refreshedToken = null;
        const token = String(msg.token || '');
        if (token && sessions[token]) {
          username = sessions[token];
        } else if (msg.username && msg.password) {
          const u = String(msg.username || '').trim().toLowerCase();
          const p = String(msg.password || '');
          try {
            const acc = await accountStore.get(u);
            if (acc && acc.passwordHash && acc.passwordHash === hashPassword(p, acc.salt)) {
              username = u;
              refreshedToken = makeToken();
              sessions[refreshedToken] = username;
            }
          } catch (e) {}
        }
        if (!username) {
          send(ws, 'accountInspectResult', { ok: false, error: 'auth failed (token expired and no/wrong password)' });
          return;
        }
        try {
          const acc = await accountStore.get(username);
          if (!acc) {
            send(ws, 'accountInspectResult', { ok: false, error: 'account vanished' });
            return;
          }
          const raw = acc.save || '';
          let parsed = null;
          try { parsed = raw ? JSON.parse(raw) : null; } catch (e) {}
          send(ws, 'accountInspectResult', {
            ok: true,
            username,
            hasSave: !!raw,
            save: raw,
            token: refreshedToken || undefined,
            score: _saveProgressScore(raw),
            summary: parsed ? {
              money: parsed.money,
              level: parsed.level,
              xp: parsed.xp,
              cars: Array.isArray(parsed.cars) ? parsed.cars.length : (parsed.ownedCars ? Object.keys(parsed.ownedCars).length : 0),
              savedAt: parsed.savedAt,
            } : null,
            rawSize: raw.length,
          });
        } catch (e) {
          send(ws, 'accountInspectResult', { ok: false, error: 'server error' });
        }
        return;
      }

      // -------- Heartbeat --------
      case 'ping': {
        send(ws, 'pong', { t: msg.t });
        return;
      }

      // -------- Submit a leaderboard score --------
      // Expects { wins, score, kills, name }. Stores the highest values seen
      // for this player so a single bad match can't tank their rank.
      case 'submitScore': {
        if (!myID) return;
        const wins  = Math.max(0, Math.min(1e6, Number(msg.wins)  || 0));
        const score = Math.max(0, Math.min(1e9, Number(msg.score) || 0));
        const kills = Math.max(0, Math.min(1e6, Number(msg.kills) || 0));
        const name  = String(msg.name || '').slice(0, 32) || 'Anonymous';
        const prev = leaderboard.get(myID) || { wins: 0, score: 0, kills: 0 };
        leaderboard.set(myID, {
          name,
          wins:  Math.max(prev.wins,  wins),
          score: Math.max(prev.score, score),
          kills: Math.max(prev.kills, kills),
          updatedAt: Date.now(),
        });
        send(ws, 'scoreSubmitted', { ok: true });
        return;
      }

      // -------- Fetch the top of the leaderboard --------
      case 'getLeaderboard': {
        const limit = Math.max(1, Math.min(200, Number(msg.limit) || 50));
        const top = getTopLeaderboard(limit);
        // Also include the requester's rank/entry if they're not in the top
        let myEntry = null, myRank = null;
        if (myID) {
          const idxInTop = top.findIndex(e => e.id === myID);
          if (idxInTop >= 0) {
            myRank = idxInTop + 1;
            myEntry = top[idxInTop];
          } else if (leaderboard.has(myID)) {
            // Compute the player's rank in the full sorted list
            const all = getTopLeaderboard(100000);
            const i = all.findIndex(e => e.id === myID);
            if (i >= 0) { myRank = i + 1; myEntry = all[i]; }
          }
        }
        send(ws, 'leaderboard', { top, myEntry, myRank });
        return;
      }
    }
  });

  ws.on('close', () => {
    if (myID) {
      leaveParty(myID);
      players.delete(myID);
    }
  });

  ws.on('error', (err) => {
    console.warn('[arena] socket error:', err.message);
  });
});

// --- Cleanup loop: drop stale players (no heartbeat for 60s) ---
setInterval(() => {
  const now = Date.now();
  for (const [id, p] of players) {
    if (p.ws.readyState !== WebSocket.OPEN) {
      leaveParty(id);
      players.delete(id);
    }
  }
}, 30000);
