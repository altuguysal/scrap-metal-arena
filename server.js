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

const PORT = parseInt(process.env.PORT || '8080', 10);
const wss = new WebSocket.Server({ port: PORT });

console.log(`[arena] WebSocket server listening on :${PORT}`);

// --- Registry ---
// Map<playerID, { ws, name, partyID, lastSeen, state }>
const players = new Map();
// Map<partyID, { hostID, members: Set<playerID>, started: boolean, startedAt }>
const parties = new Map();
let nextPartyID = 1;

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

  ws.on('message', (raw) => {
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

      // -------- Heartbeat --------
      case 'ping': {
        send(ws, 'pong', { t: msg.t });
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
