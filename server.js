const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });

const clients = new Map(); // ws -> { id, nick }

wss.on('connection', (ws) => {
  const id = Math.random().toString(36).substr(2, 8);
  const player = { id, nick: 'Игрок ' + id };
  clients.set(ws, player);

  // Отправляем новому клиенту его id и список всех игроков
  ws.send(JSON.stringify({
    type: 'welcome',
    id: id,
    players: Array.from(clients.values()).map(p => ({ id: p.id, nick: p.nick }))
  }));

  // Сообщаем остальным о новом игроке
  broadcast({ type: 'player-joined', id: id, nick: player.nick }, ws);

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      msg.from = clients.get(ws)?.id;
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

console.log('Сервер запущен');
