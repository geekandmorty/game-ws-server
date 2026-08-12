/**
 * qFoldIT session relay server.
 *
 * Responsibilities kept intentionally narrow:
 *  - relay player transforms, nicknames, and chat between connected clients
 *  - relay and persist the last known qFoldIT scientific-state/v1 payload for the
 *    shared session, so a client that joins late still sees the current experiment
 *
 * This server does not compute, validate, or own scientific truth. It only stores
 * the last broadcast canonical state in memory so newcomers can catch up. Authoritative
 * scientific computation stays with qFoldIT services, outside this relay.
 */

const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });

const SCIENTIFIC_STATE_SCHEMA = 'qfoldit.scientific-state/v1';
const UAG_SCENE_SCHEMA = 'qfoldit.uag/0.1';
const VALID_CHARACTERS = ['mannequin', 'xbot', 'ybot']; // keep in sync with CHARACTER_LIBRARY in js/game.js
const DEFAULT_CHARACTER = 'mannequin';

const clients = new Map();     // ws -> { id, nick, character }
const chatMessages = [];       // { from, to, text, timestamp }
let lastScienceState = null;   // last valid qfoldit.scientific-state/v1 payload shared in this session
let lastUagScene = null;       // last valid qfoldit.uag/0.1 payload shared in this session

function isValidScientificState(state) {
  return !!state && typeof state === 'object' && state.schema === SCIENTIFIC_STATE_SCHEMA;
}

function isValidUagScene(scene) {
  return !!scene && typeof scene === 'object' && scene.schema === UAG_SCENE_SCHEMA;
}

wss.on('connection', (ws) => {
  const id = Math.random().toString(36).substr(2, 8);
  const player = { id, nick: 'Player ' + id, character: DEFAULT_CHARACTER };
  clients.set(ws, player);

  ws.send(JSON.stringify({
    type: 'welcome',
    id: id,
    players: Array.from(clients.values()).map(p => ({ id: p.id, nick: p.nick, character: p.character })),
    scienceState: lastScienceState, // lets a newly joined client catch up on the current experiment
    uagScene: lastUagScene          // lets a newly joined client catch up on the current UAG scene, if any
  }));

  broadcast({ type: 'player-joined', id: id, nick: player.nick, character: player.character }, ws);

  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch (e) {
      return; // ignore malformed payloads
    }
    if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') return;

    const sender = clients.get(ws);
    if (!sender) return;

    switch (msg.type) {
      case 'nickname-change': {
        const newNick = typeof msg.nick === 'string' ? msg.nick.trim() : '';
        if (newNick && newNick !== sender.nick) {
          sender.nick = newNick;
          broadcast({ type: 'nickname-changed', id: sender.id, nick: newNick }, ws);
        }
        return;
      }

      case 'character-change': {
        const newCharacter = VALID_CHARACTERS.includes(msg.character) ? msg.character : null;
        if (newCharacter && newCharacter !== sender.character) {
          sender.character = newCharacter;
          broadcast({ type: 'character-changed', id: sender.id, character: newCharacter }, ws);
        }
        return;
      }

      case 'request-chat-history': {
        const partnerId = msg.partnerId;
        const history = chatMessages
          .filter(m => (m.from === sender.id && m.to === partnerId) || (m.from === partnerId && m.to === sender.id))
          .sort((a, b) => a.timestamp - b.timestamp);
        ws.send(JSON.stringify({ type: 'chat-history', partnerId, messages: history }));
        return;
      }

      case 'chat': {
        const chatEntry = { from: sender.id, to: msg.to, text: msg.text, timestamp: msg.timestamp };
        chatMessages.push(chatEntry);
        // Deliver to both participants so both clients stay in sync.
        broadcastTo([sender.id, msg.to], { type: 'chat', ...chatEntry });
        return;
      }

      // ---- qFoldIT scientific-state session sync ----
      case 'science-state': {
        if (!isValidScientificState(msg.state)) return; // reject anything that isn't canonical qFoldIT state
        lastScienceState = msg.state;
        broadcast({
          type: 'science-state',
          originId: sender.id,
          originNick: msg.originNick || sender.nick,
          state: lastScienceState
        }, ws);
        return;
      }

      case 'science-clear': {
        lastScienceState = null;
        broadcast({ type: 'science-clear', originId: sender.id, originNick: msg.originNick || sender.nick }, ws);
        return;
      }

      // ---- forward-compatible qfoldit.uag/0.1 scene sync ----
      // This relay does not validate UAG semantics, run canonical action mapping, or act as an
      // engine adapter (that contract belongs to the UEFN/Unity/UNIGINE Toolbelts per
      // qfoldit-engine-adapter-spec). It only checks the schema tag and relays/persists the
      // payload, the same way it already does for qfoldit.scientific-state/v1.
      case 'uag-scene': {
        if (!isValidUagScene(msg.scene)) return; // reject anything that isn't a tagged qfoldit.uag/0.1 payload
        lastUagScene = msg.scene;
        broadcast({
          type: 'uag-scene',
          originId: sender.id,
          originNick: msg.originNick || sender.nick,
          scene: lastUagScene
        }, ws);
        return;
      }

      case 'uag-clear': {
        lastUagScene = null;
        broadcast({ type: 'uag-clear', originId: sender.id, originNick: msg.originNick || sender.nick }, ws);
        return;
      }

      // ---- everything else (e.g. avatar 'pos' updates) is relayed as-is ----
      default: {
        msg.from = sender.id;
        broadcast(msg, ws);
      }
    }
  });

  ws.on('close', () => {
    const player = clients.get(ws);
    if (player) {
      broadcast({ type: 'player-left', id: player.id }, ws);
      clients.delete(ws);
    }
  });
});

function broadcast(message, senderWs) {
  const str = JSON.stringify(message);
  wss.clients.forEach((client) => {
    if (client !== senderWs && client.readyState === WebSocket.OPEN) {
      client.send(str);
    }
  });
}

// Deliver a message to specific participants only, identified by session id.
function broadcastTo(ids, message) {
  const str = JSON.stringify(message);
  ids.forEach(targetId => {
    for (const [wsClient, player] of clients.entries()) {
      if (player.id === targetId && wsClient.readyState === WebSocket.OPEN) {
        wsClient.send(str);
        break;
      }
    }
  });
}

console.log('qFoldIT session relay server started');
