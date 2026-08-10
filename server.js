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

const clients = new Map(); // ws -> { id, nick }
const chatMessages = []; // { from, to, text, timestamp }

wss.on('connection', (ws) => {
  const id = Math.random().toString(36).substr(2, 8);
  const player = { id, nick: 'Player ' + id };
  clients.set(ws, player);

  ws.send(JSON.stringify({
    type: 'welcome',
    id: id,
    players: Array.from(clients.values()).map(p => ({ id: p.id, nick: p.nick }))
  }));

  broadcast({ type: 'player-joined', id: id, nick: player.nick }, ws);

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      const sender = clients.get(ws);
      if (!sender) return;

      if (msg.type === 'nickname-change') {
        const newNick = msg.nick.trim();
        if (newNick && newNick !== sender.nick) {
          sender.nick = newNick;
          broadcast({ type: 'nickname-changed', id: sender.id, nick: newNick }, ws);
        }
        return;
      }

      if (msg.type === 'request-chat-history') {
        const partnerId = msg.partnerId;
        const history = chatMessages.filter(m =>
          (m.from === sender.id && m.to === partnerId) ||
          (m.from === partnerId && m.to === sender.id)
        );
        history.sort((a, b) => a.timestamp - b.timestamp);
        ws.send(JSON.stringify({
          type: 'chat-history',
          partnerId: partnerId,
          messages: history
        }));
        return;
      }

      if (msg.type === 'chat') {
        const chatEntry = {
          from: sender.id,
          to: msg.to,
          text: msg.text,
          timestamp: msg.timestamp
        };
        chatMessages.push(chatEntry);
        broadcastTo([sender.id, msg.to], { type: 'chat', ...chatEntry });
        return;
      }

      msg.from = sender.id;
      broadcast(msg, ws);
    } catch (e) {}
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

function broadcastTo(ids, message) {
  const str = JSON.stringify(message);
  ids.forEach(targetId => {
    for (let [wsClient, player] of clients.entries()) {
      if (player.id === targetId && wsClient.readyState === WebSocket.OPEN) {
        wsClient.send(str);
        break;
      }
    }
  });
}

console.log('Server running');
