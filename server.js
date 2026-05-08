const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });

const clients = new Map(); // ws -> { id, nick }
const chatMessages = []; // { from, to, text, timestamp }

wss.on('connection', (ws) => {
  const id = Math.random().toString(36).substr(2, 8);
  const player = { id, nick: 'Игрок ' + id };
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

      // Смена ника
      if (msg.type === 'nickname-change') {
        const newNick = msg.nick.trim();
        if (newNick && newNick !== sender.nick) {
          sender.nick = newNick;
          broadcast({ type: 'nickname-changed', id: sender.id, nick: newNick }, ws);
        }
        return;
      }

      // Запрос истории чата
      if (msg.type === 'request-chat-history') {
        const partnerId = msg.partnerId;
        const history = chatMessages.filter(m =>
          (m.from === sender.id && m.to === partnerId) ||
          (m.from === partnerId && m.to === sender.id)
        );
        // Сортируем по времени на всякий случай
        history.sort((a, b) => a.timestamp - b.timestamp);
        ws.send(JSON.stringify({
          type: 'chat-history',
          partnerId: partnerId,
          messages: history
        }));
        return;
      }

      // Сообщение чата — сохраняем в историю и рассылаем
      if (msg.type === 'chat') {
        const chatEntry = {
          from: sender.id,
          to: msg.to,
          text: msg.text,
          timestamp: msg.timestamp
        };
        chatMessages.push(chatEntry);
        // Рассылаем всем участникам диалога (включая отправителя, чтобы синхронизировать)
        broadcastTo([sender.id, msg.to], { type: 'chat', ...chatEntry });
        return;
      }

      // Остальные сообщения (pos) — просто ретранслируем
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

// Рассылка конкретным получателям (по id)
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

console.log('Сервер запущен');
