require('dotenv').config();
const express   = require('express');
const cors      = require('cors');
const http      = require('http');
const WebSocket = require('ws');

const app    = express();
const server = http.createServer(app);

const {
  VK_CLIENT_ID,
  VK_CLIENT_SECRET,
  FRONTEND_URL = 'http://localhost:5500',
  PORT         = 3000,
  VK_CHANNEL   = 'fasolka',
} = process.env;

const VK_API      = 'https://api.live.vkvideo.ru/v1';
const VK_AUTH_URL = 'https://auth.live.vkvideo.ru/app/oauth2/authorize';
const VK_TOKEN_URL= 'https://auth.live.vkvideo.ru/app/oauth2/token';

/* ════════════════════════════════════════
   CORS — разрешаем только наш фронтенд
════════════════════════════════════════ */
app.use(cors({
  origin: [FRONTEND_URL, 'http://localhost:5500', 'http://127.0.0.1:5500'],
  credentials: true,
}));
app.use(express.json());

/* ════════════════════════════════════════
   СОСТОЯНИЕ СЕРВЕРА
════════════════════════════════════════ */
// Клиентские WebSocket соединения (браузеры)
const clients = new Set();

// Соединение с VK WebSocket
let vkWs         = null;
let vkWsUrl      = null;
let vkRetry      = 0;
let vkRetryTimer = null;

// Сессии пользователей: sessionId → { accessToken, displayName, avatarUrl }
const sessions   = new Map();

/* ════════════════════════════════════════
   ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
════════════════════════════════════════ */
function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

// Отправляем сообщение всем подключённым браузерам
function broadcast(payload) {
  const data = JSON.stringify(payload);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

/* ════════════════════════════════════════
   VK API — получаем WebSocket URL чата
════════════════════════════════════════ */
async function getVkWsUrl(token) {
  const headers = token ? { 'Authorization': `Bearer ${token}` } : {};
  const r = await fetch(`${VK_API}/blog/${VK_CHANNEL}/public_video_stream`, { headers });
  if (!r.ok) throw new Error(`VK API ${r.status}`);
  const data = await r.json();

  log('VK API response keys:', Object.keys(data));

  const d = Array.isArray(data?.data) ? data.data[0] : data;

  const url = d?.chatSettings?.webSocketChannels?.chat
           || d?.webSocketChannels?.chat
           || d?.chatSettings?.chatUrl
           || d?.websocketUrl
           || null;

  if (!url) {
    log('Full VK response:', JSON.stringify(data).slice(0, 800));
    throw new Error('WebSocket URL не найден в ответе VK API');
  }

  return url;
}

/* ════════════════════════════════════════
   VK WEBSOCKET — подключение и переподключение
════════════════════════════════════════ */
async function connectVkWs(token) {
  if (vkRetryTimer) { clearTimeout(vkRetryTimer); vkRetryTimer = null; }
  if (vkWs) { try { vkWs.terminate(); } catch(e) {} vkWs = null; }

  try {
    vkWsUrl = await getVkWsUrl(token);
  } catch(e) {
    log('Не удалось получить WS URL:', e.message);
    broadcast({ type: 'STATUS', status: 'error', message: e.message });
    scheduleVkReconnect(token);
    return;
  }

  // Добавляем токен в URL если есть
  let wsUrl = vkWsUrl;
  if (token) wsUrl += (wsUrl.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(token);

  log('Подключаемся к VK WebSocket:', wsUrl.split('?')[0]);
  broadcast({ type: 'STATUS', status: 'connecting' });

  vkWs = new WebSocket(wsUrl);

  vkWs.on('open', () => {
    log('VK WebSocket подключён');
    vkRetry = 0;
    broadcast({ type: 'STATUS', status: 'connected' });
  });

  vkWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      handleVkMessage(msg);
    } catch(e) {}
  });

  vkWs.on('close', (code, reason) => {
    log(`VK WebSocket закрыт: ${code} ${reason}`);
    broadcast({ type: 'STATUS', status: 'reconnecting' });
    scheduleVkReconnect(token);
  });

  vkWs.on('error', (e) => {
    log('VK WebSocket ошибка:', e.message);
  });
}

function scheduleVkReconnect(token) {
  const delay = Math.min(1000 * Math.pow(2, vkRetry++), 30000);
  log(`Переподключение через ${delay}ms (попытка ${vkRetry})`);
  vkRetryTimer = setTimeout(() => connectVkWs(token), delay);
}

/* ════════════════════════════════════════
   ОБРАБОТКА СООБЩЕНИЙ ОТ VK
════════════════════════════════════════ */
function handleVkMessage(msg) {
  const type = msg.type || msg.event;
  const data = msg.data || msg;

  // История сообщений
  if (type === 'history' && Array.isArray(data)) {
    const messages = data.map(parseVkMessage).filter(Boolean);
    if (messages.length) broadcast({ type: 'HISTORY', messages });
    return;
  }

  // Одиночное сообщение
  if (type === 'message' || type === 'CHAT_MESSAGE' || data?.author) {
    const parsed = parseVkMessage(data);
    if (parsed) broadcast({ type: 'MESSAGE', message: parsed });
    return;
  }

  // Массив сообщений
  if (Array.isArray(msg)) {
    const messages = msg.map(parseVkMessage).filter(Boolean);
    if (messages.length) broadcast({ type: 'HISTORY', messages });
    return;
  }

  if (Array.isArray(msg?.messages)) {
    const messages = msg.messages.map(parseVkMessage).filter(Boolean);
    if (messages.length) broadcast({ type: 'HISTORY', messages });
  }
}

function parseVkMessage(msg) {
  if (!msg) return null;
  const author    = msg.author || msg.user || {};
  const name      = author.displayName || author.nick || author.name || 'Аноним';
  const avatar    = author.avatarUrl   || author.avatar || null;
  const isOwner   = !!(author.isBroadcaster || author.roles?.includes('STREAMER'));
  const id        = msg.id || `${name}:${Date.now()}:${Math.random()}`;

  // Собираем части сообщения
  const parts = msg.data || msg.parts || [];
  let text = '';
  const smiles = [];

  if (Array.isArray(parts) && parts.length) {
    parts.forEach(p => {
      if (p.type === 'text')    text += (p.content?.[0] ?? p.text ?? '');
      else if (p.type === 'smile') {
        text += `:${p.name}:`;
        smiles.push({ name: p.name, url: p.smallUrl || p.url });
      } else if (p.type === 'mention') text += `@${p.displayName || p.nick || ''}`;
      else if (p.type === 'link')      text += p.url;
    });
  } else {
    text = String(msg.text || msg.content || msg.message || '');
  }

  if (!text.trim()) return null;

  return { id, name, avatar, text: text.trim(), smiles, isOwner };
}

/* ════════════════════════════════════════
   OAUTH — авторизация пользователей
════════════════════════════════════════ */

// Редирект на страницу входа VK
app.get('/auth/login', (req, res) => {
  if (!VK_CLIENT_ID) {
    return res.status(500).json({ error: 'VK_CLIENT_ID не задан в .env' });
  }

  const redirectUri = `${req.protocol}://${req.get('host')}/auth/callback`;
  const params = new URLSearchParams({
    response_type: 'code',
    client_id:     VK_CLIENT_ID,
    redirect_uri:  redirectUri,
    scope:         'chat',
    state:         Math.random().toString(36).slice(2),
  });

  res.redirect(`${VK_AUTH_URL}?${params}`);
});

// Callback после авторизации VK
app.get('/auth/callback', async (req, res) => {
  const { code, error } = req.query;

  if (error || !code) {
    return res.redirect(`${FRONTEND_URL}?auth=error`);
  }

  try {
    const redirectUri = `${req.protocol}://${req.get('host')}/auth/callback`;

    // Обмениваем code на токен
    const tokenRes = await fetch(VK_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type:    'authorization_code',
        code,
        redirect_uri:  redirectUri,
        client_id:     VK_CLIENT_ID,
        client_secret: VK_CLIENT_SECRET,
      }),
    });

    if (!tokenRes.ok) throw new Error('Token exchange failed: ' + tokenRes.status);
    const tokenData = await tokenRes.json();
    const accessToken = tokenData.access_token;
    if (!accessToken) throw new Error('No access_token in response');

    // Получаем профиль пользователя
    const profileRes = await fetch(`${VK_API}/user/public`, {
      headers: { 'Authorization': `Bearer ${accessToken}` },
    });
    const profile = profileRes.ok ? await profileRes.json() : {};

    // Создаём сессию
    const sessionId = Math.random().toString(36).slice(2) + Date.now().toString(36);
    sessions.set(sessionId, {
      accessToken,
      displayName: profile.displayName || profile.nick || 'Пользователь',
      avatarUrl:   profile.avatarUrl || null,
      expiresAt:   Date.now() + (tokenData.expires_in || 3600) * 1000,
    });

    log(`Пользователь авторизован: ${sessions.get(sessionId).displayName}`);

    // Переподключаем VK WS с токеном авторизованного пользователя
    connectVkWs(accessToken);

    // Редиректим на фронтенд с session id
    res.redirect(`${FRONTEND_URL}?session=${sessionId}`);

  } catch(e) {
    log('OAuth error:', e.message);
    res.redirect(`${FRONTEND_URL}?auth=error&message=${encodeURIComponent(e.message)}`);
  }
});

// Получение профиля пользователя по session id
app.get('/auth/me', (req, res) => {
  const sessionId = req.headers['x-session-id'] || req.query.session;
  const session   = sessions.get(sessionId);

  if (!session || session.expiresAt < Date.now()) {
    sessions.delete(sessionId);
    return res.status(401).json({ error: 'Сессия истекла' });
  }

  res.json({
    displayName: session.displayName,
    avatarUrl:   session.avatarUrl,
  });
});

// Выход
app.post('/auth/logout', (req, res) => {
  const sessionId = req.headers['x-session-id'];
  sessions.delete(sessionId);
  res.json({ ok: true });
});

/* ════════════════════════════════════════
   REST — отправка сообщения в чат
════════════════════════════════════════ */
app.post('/chat/send', async (req, res) => {
  const sessionId = req.headers['x-session-id'];
  const session   = sessions.get(sessionId);

  if (!session || session.expiresAt < Date.now()) {
    return res.status(401).json({ error: 'Не авторизован' });
  }

  const { text } = req.body;
  if (!text?.trim()) return res.status(400).json({ error: 'Пустое сообщение' });

  try {
    const r = await fetch(`${VK_API}/blog/${VK_CHANNEL}/chat/message`, {
      method:  'POST',
      headers: {
        'Authorization': `Bearer ${session.accessToken}`,
        'Content-Type':  'application/json',
      },
      body: JSON.stringify({
        data: [{ type: 'text', content: [text.trim()] }],
      }),
    });

    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      if (r.status === 401) {
        sessions.delete(sessionId);
        return res.status(401).json({ error: 'Токен истёк, войдите заново' });
      }
      return res.status(r.status).json({ error: err.message || 'Ошибка VK API' });
    }

    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/* ════════════════════════════════════════
   WEBSOCKET СЕРВЕР — для браузеров
════════════════════════════════════════ */
const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  log('Новый клиент:', req.socket.remoteAddress);
  clients.add(ws);

  // Отправляем текущий статус VK соединения
  ws.send(JSON.stringify({
    type:   'STATUS',
    status: vkWs?.readyState === WebSocket.OPEN ? 'connected' : 'connecting',
  }));

  ws.on('close', () => {
    clients.delete(ws);
    log('Клиент отключился. Всего:', clients.size);
  });

  ws.on('error', () => clients.delete(ws));
});

/* ════════════════════════════════════════
   HEALTHCHECK
════════════════════════════════════════ */
app.get('/health', (req, res) => {
  res.json({
    ok:      true,
    channel: VK_CHANNEL,
    clients: clients.size,
    vkWs:    vkWs?.readyState === WebSocket.OPEN ? 'connected' : 'disconnected',
  });
});

/* ════════════════════════════════════════
   ЗАПУСК
════════════════════════════════════════ */
server.listen(PORT, () => {
  log(`Сервер запущен на порту ${PORT}`);
  log(`Канал: ${VK_CHANNEL}`);
  log(`Фронтенд: ${FRONTEND_URL}`);

  if (!VK_CLIENT_ID) {
    log('⚠️  VK_CLIENT_ID не задан — OAuth не будет работать');
    log('   Получи client_id на dev.live.vkvideo.ru');
  }

  // Подключаемся к VK без токена (только чтение)
  connectVkWs(null);
});
