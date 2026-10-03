// SUDHIR RSI PRO TERMINAL backend: Dhan DATA PROVIDER + web-push bridge only. All trading logic lives in the PWA.
const http = require('http');
const fs = require('fs');
const path = require('path');
const express = require('express');
const cors = require('cors');
const webpush = require('web-push');
const { WebSocketServer } = require('ws');
const dhan = require('./dhan');
const { Candles, classify } = require('./candles');
const { MarketFeed, DepthFeed, marketOpen } = require('./feed');
const { generateVapid } = require('./vapid');

const E = process.env, N = (k, d) => (E[k] !== undefined && E[k] !== '' ? Number(E[k]) : d), B = (k, d) => (E[k] === undefined || E[k] === '' ? d : /^(1|true|yes)$/i.test(E[k]));
const CFG = {
  port: N('PORT', 10000), host: E.HOST || '0.0.0.0', cors: E.CORS_ORIGIN || '*', defaultIndex: (E.DEFAULT_INDEX || 'NIFTY').toUpperCase(),
  autoSub: B('AUTO_SUBSCRIBE_INDICES', true), live: B('ENABLE_LIVE_FEED', true), depth20: B('ENABLE_20_DEPTH', true),
  maxClients: N('MAX_CLIENTS', 20), maxSubs: N('MAX_SUBSCRIPTIONS', 5000), maxTicks: N('MAX_TICKS_PER_INSTRUMENT', 3000),
  maxCandles: N('MAX_CANDLES_PER_INSTRUMENT', 2000), tickBuffer: N('TICK_BUFFER_SIZE', 10000),
  tickStale: N('TICK_STALE_MS', 15000), staleAfter: N('STALE_AFTER_MS', 5000), histMs: N('LIVE_HISTORY_REFRESH_MS', 10000),
  tfMs: N('CANDLE_TIMEFRAME_MS', 60000), chainMs: Math.max(3000, N('OPTION_CHAIN_REFRESH_MS', 3200)), depthMax: N('DEPTH_MAX_INSTRUMENTS', 50),
};
const IDX = {
  NIFTY: { id: N('NIFTY_SECURITY_ID', 13), futEx: 'NSE', futSeg: 'NSE_FNO', step: 50 },
  BANKNIFTY: { id: N('BANKNIFTY_SECURITY_ID', 25), futEx: 'NSE', futSeg: 'NSE_FNO', step: 100 },
  FINNIFTY: { id: N('FINNIFTY_SECURITY_ID', 27), futEx: 'NSE', futSeg: 'NSE_FNO', step: 50 },
  MIDCPNIFTY: { id: N('MIDCPNIFTY_SECURITY_ID', 442), futEx: 'NSE', futSeg: 'NSE_FNO', step: 25 },
  SENSEX: { id: N('SENSEX_SECURITY_ID', 51), futEx: 'BSE', futSeg: 'BSE_FNO', step: 100 },
};
const VIX_ID = N('INDIA_VIX_SECURITY_ID', 21);
const idToSym = {}; for (const [s, v] of Object.entries(IDX)) idToSym[v.id] = s;
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

// ---------- per-symbol state ----------
const St = {};
function st(sym) {
  return St[sym] || (St[sym] = { sym, spot: null, prev: null, candles: new Candles(CFG.tfMs, CFG.maxCandles), ticks: [], futId: null, futSym: null, futSeg: null,
    f: { lastVol: null, lastLtp: null, lastSide: 0, bid: 0, ask: 0 }, depth: { bids: [], asks: [], ts: 0 }, chain: null, expiry: null, expiryAt: 0,
    volSource: 'none', dirty: false, depthDirty: false, ready: null, histAt: 0, lastTickAt: 0, chainBusy: false });
}
const vix = { ltp: null, prev: null };
const wanted = () => { const s = new Set([CFG.defaultIndex]); clients.forEach(c => c.sym && s.add(c.sym)); return [...s].filter(x => IDX[x]); };

// ---------- Dhan feeds ----------
const feed = new MarketFeed(CFG.tickStale), depthFeed = new DepthFeed(CFG.tickStale);
feed.on('log', log); depthFeed.on('log', log);
let subCount = 0;
function subscribe(seg, id, mode) { if (feed.subs.get(seg + ':' + id) === mode || subCount >= CFG.maxSubs) return; subCount++; feed.subscribe(seg, id, mode); }

feed.on('packet', p => {
  const now = Date.now();
  if (p.seg === 'IDX_I') {
    if (p.id === VIX_ID) { if (p.ltp) vix.ltp = p.ltp; if (p.type === 'prev') vix.prev = p.prevClose; if (p.type === 'quote' && p.close) vix.prev = p.close; return; }
    const sym = idToSym[p.id]; if (!sym) return; const s = st(sym);
    if (p.type === 'prev') { s.prev = p.prevClose; return; }
    if (!(p.ltp > 0)) return;
    if (p.type === 'quote' && p.close) s.prev = p.close;
    s.spot = p.ltp; s.lastTickAt = now; s.candles.tick(p.ltp, now); s.dirty = true;
    s.ticks.push({ t: now, p: p.ltp }); if (s.ticks.length > CFG.maxTicks) s.ticks.shift();
    return;
  }
  // index future: cumulative volume -> per-candle volume + buy/sell split (order flow)
  for (const s of Object.values(St)) {
    if (s.futId !== p.id || s.futSeg !== p.seg || !(p.type === 'full' || p.type === 'quote')) continue;
    const f = s.f; if (p.depth && p.depth[0]) { f.bid = p.depth[0].bp; f.ask = p.depth[0].ap; }
    if (f.lastVol != null && p.volume > f.lastVol) {
      const side = classify(p.ltp, f.bid, f.ask, f.lastLtp, f.lastSide);
      s.candles.volume(p.volume - f.lastVol, side, now); f.lastSide = side; s.volSource = 'futures-live'; s.dirty = true;
    }
    f.lastVol = p.volume; f.lastLtp = p.ltp;
  }
});
depthFeed.on('depth', m => {
  for (const s of Object.values(St)) if (s.futId === m.id) { (m.side === 'bid' ? (s.depth.bids = m.levels) : (s.depth.asks = m.levels)); s.depth.ts = Date.now(); s.depthDirty = true; }
});

// ---------- per-symbol bootstrap ----------
function ensure(sym) {
  const s = st(sym); if (s.ready) return s.ready;
  s.ready = (async () => {
    const cfg = IDX[sym];
    if (CFG.live) subscribe('IDX_I', cfg.id, 'quote');
    const envFut = E[sym + '_FUT_ID'];
    try {
      const fut = envFut ? { id: envFut, sym: sym + '-FUT(env)' } : await dhan.resolveFuture(sym, cfg.futEx);
      if (fut) { s.futId = Number(fut.id); s.futSym = fut.sym; s.futSeg = cfg.futSeg; log(sym, 'future', s.futSym, s.futId); }
      else log(sym, 'future not found in security master');
    } catch (e) { log(sym, 'future lookup failed:', e.message); }
    if (CFG.live && s.futId) {
      subscribe(s.futSeg, s.futId, 'full');
      if (CFG.depth20 && cfg.futSeg === 'NSE_FNO' && Object.values(St).filter(x => x.futId).length <= CFG.depthMax) depthFeed.subscribe(s.futSeg, s.futId);
    }
    await refreshHistory(sym, true);
  })().catch(e => { log('ensure', sym, e.message); s.ready = null; });
  return s.ready;
}

async function refreshHistory(sym, force) {
  const s = st(sym), cfg = IDX[sym];
  if (!force && Date.now() - s.histAt < (marketOpen() ? CFG.histMs : 60000)) return;
  s.histAt = Date.now();
  try {
    const idx = await dhan.intraday(cfg.id, 'IDX_I', 'INDEX');
    let src = idx.some(x => x.v > 0) ? 'index' : 'none';
    if (s.futId) {
      try {
        const fut = await dhan.intraday(s.futId, s.futSeg, 'FUTIDX'), m = new Map(fut.map(x => [x.t, x.v]));
        if (fut.some(x => x.v > 0)) { idx.forEach(x => { x.v = m.get(x.t) || x.v || 0; }); src = 'futures-history'; }
      } catch (e) { log(sym, 'fut history', e.message); }
    }
    if (!CFG.live) s.candles.arr = [];
    s.candles.merge(idx);
    if (s.volSource !== 'futures-live') s.volSource = src === 'none' ? (s.futId ? 'none (futures history empty)' : 'none (future not found)') : src;
    const last = s.candles.last(); if (!CFG.live && last) { s.spot = last.c; s.dirty = true; }
    if (!s.prev && idx.length) { const d = new Date(idx[idx.length - 1].t * 1000 + 19800e3).toISOString().slice(0, 10); const pc = [...idx].reverse().find(x => new Date(x.t * 1000 + 19800e3).toISOString().slice(0, 10) < d); if (pc) s.prev = pc.c; }
  } catch (e) { log(sym, 'history', e.message); }
}

// ---------- option chain ----------
const leg = x => x ? { id: x.security_id, ltp: x.last_price || 0, oi: x.oi || 0, chg: (x.oi || 0) - (x.previous_oi || 0), vol: x.volume || 0, iv: x.implied_volatility || 0,
  delta: x.greeks ? x.greeks.delta : null, theta: x.greeks ? x.greeks.theta : null, gamma: x.greeks ? x.greeks.gamma : null, vega: x.greeks ? x.greeks.vega : null,
  bid: x.top_bid_price || 0, ask: x.top_ask_price || 0 } : null;
async function pollChain(sym) {
  const s = st(sym), cfg = IDX[sym]; if (s.chainBusy) return; s.chainBusy = true;
  try {
    const today = dhan.istDate();
    if (!s.expiry || s.expiry < today || Date.now() - s.expiryAt > 6 * 3600e3) { const l = await dhan.expiryList(cfg.id); s.expiry = l.find(x => x >= today) || l[0]; s.expiryAt = Date.now(); }
    const d = await dhan.optionChainRaw(cfg.id, s.expiry), spot = d.last_price || s.spot, oc = d.oc || {};
    let rows = Object.keys(oc).map(k => ({ k: parseFloat(k), ce: leg(oc[k].ce), pe: leg(oc[k].pe) })).sort((a, b) => a.k - b.k);
    const below = rows.filter(r => r.k <= spot), above = rows.filter(r => r.k >= spot);
    const max = (arr, f) => arr.reduce((m, r) => (!m || f(r) > f(m) ? r : m), null);
    const sup = max(below, r => r.pe ? r.pe.oi : 0), res = max(above, r => r.ce ? r.ce.oi : 0);
    const totCe = rows.reduce((a, r) => a + (r.ce ? r.ce.oi : 0), 0), totPe = rows.reduce((a, r) => a + (r.pe ? r.pe.oi : 0), 0);
    const atm = Math.round(spot / cfg.step) * cfg.step;
    const near = rows.filter(r => Math.abs(r.k - atm) <= cfg.step * 12);
    s.chain = { sym, expiry: s.expiry, spot, step: cfg.step, atm, ts: Date.now(), pcr: totCe ? +(totPe / totCe).toFixed(2) : null, totCe, totPe,
      support: sup && { k: sup.k, oi: sup.pe.oi, chg: sup.pe.chg, ceOiThere: sup.ce ? sup.ce.oi : 0 },
      resistance: res && { k: res.k, oi: res.ce.oi, chg: res.ce.chg, peOiThere: res.pe ? res.pe.oi : 0 }, rows: near };
    broadcast(sym, { type: 'chain', sym, chain: s.chain });
  } catch (e) { log(sym, 'chain', e.message); } finally { s.chainBusy = false; }
}

// ---------- HTTP + WS ----------
const app = express(); app.use(cors({ origin: CFG.cors === '*' ? true : CFG.cors.split(',') })); app.use(express.json({ limit: '100kb' }));
const server = http.createServer(app);
const clients = new Set();
function broadcast(sym, msg) { const str = JSON.stringify(msg); for (const c of clients) if (c.sym === sym && c.ws.readyState === 1 && c.ws.bufferedAmount < 1e6) c.ws.send(str); }
function idxPayload() { const o = {}; for (const [k, s] of Object.entries(St)) o[k] = { ltp: s.spot, prev: s.prev }; o.VIX = vix; return o; }
function snapshot(sym) {
  const s = st(sym);
  return { type: 'snapshot', sym, candles: s.candles.out(1500), spot: s.spot, prev: s.prev, volSource: s.volSource, futId: s.futId, futSym: s.futSym, tfMs: CFG.tfMs,
    depth: { bids: s.depth.bids, asks: s.depth.asks, ts: s.depth.ts }, chain: s.chain, idx: idxPayload(), serverTime: Date.now() };
}
function status() {
  const now = Date.now(), last = Math.max(0, ...Object.values(St).map(s => s.lastTickAt));
  return { ok: true, dhanConfigured: !!(E.DHAN_CLIENT_ID && E.DHAN_ACCESS_TOKEN), live: CFG.live, marketOpen: marketOpen(),
    feed: { connected: feed.connected, lastTickAgeMs: last ? now - last : null, stale: marketOpen() && (!last || now - last > CFG.staleAfter) },
    depth20: { connected: depthFeed.connected, enabled: CFG.depth20 }, clients: clients.size,
    symbols: Object.fromEntries(Object.values(St).map(s => [s.sym, { futSym: s.futSym, futId: s.futId, volSource: s.volSource, candles: s.candles.arr.length, spot: s.spot, chainAt: s.chain ? s.chain.ts : null }])), time: now };
}
const wsOf = (q) => String(q.symbol || CFG.defaultIndex).toUpperCase();
app.get('/', (q, r) => r.json({ name: 'SUDHIR RSI backend', ok: true }));
app.get('/health', (q, r) => r.json(status()));
app.get('/api/status', (q, r) => r.json(status()));
app.get('/api/snapshot', async (q, r) => { const sym = wsOf(q.query); if (!IDX[sym]) return r.status(400).json({ error: 'unknown symbol' }); await ensure(sym); r.json(snapshot(sym)); });
app.get('/api/candles', async (q, r) => { const sym = wsOf(q.query); if (!IDX[sym]) return r.status(400).json({ error: 'unknown symbol' }); await ensure(sym); const s = st(sym); r.json({ symbol: sym, volSource: s.volSource, candles: s.candles.out(1500) }); });
app.get('/api/chain', async (q, r) => { const sym = wsOf(q.query); if (!IDX[sym]) return r.status(400).json({ error: 'unknown symbol' }); if (!st(sym).chain) await pollChain(sym); r.json(st(sym).chain || { error: 'chain unavailable' }); });
app.get('/api/ticks', (q, r) => { const sym = wsOf(q.query); r.json(st(sym).ticks.slice(-500)); });

// ---- Web Push ----
let vapid = E.VAPID_PUBLIC_KEY && E.VAPID_PRIVATE_KEY ? { publicKey: E.VAPID_PUBLIC_KEY, privateKey: E.VAPID_PRIVATE_KEY } : null;
if (!vapid) { vapid = generateVapid(); log('VAPID keys missing -> generated temporary keys (subscriptions break on restart). Set VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY in env.'); }
webpush.setVapidDetails(E.VAPID_SUBJECT || 'mailto:admin@example.com', vapid.publicKey, vapid.privateKey);
const subFile = path.join(__dirname, 'subs.json'); let subs = []; try { subs = JSON.parse(fs.readFileSync(subFile, 'utf8')); } catch {}
const saveSubs = () => { try { fs.writeFileSync(subFile, JSON.stringify(subs)); } catch {} };
const authed = q => !E.PUSH_SECRET || q.get('x-push-secret') === E.PUSH_SECRET;
app.get('/api/push/key', (q, r) => r.json({ key: vapid.publicKey }));
app.post('/api/push/subscribe', (q, r) => { const sub = q.body; if (!sub || !sub.endpoint) return r.status(400).json({ error: 'bad subscription' }); if (!subs.find(x => x.endpoint === sub.endpoint)) { subs.push(sub); subs = subs.slice(-50); saveSubs(); } r.json({ ok: true, count: subs.length }); });
app.post('/api/push/send', async (q, r) => {
  if (!authed(q)) return r.status(401).json({ error: 'bad push secret' });
  const payload = JSON.stringify({ title: String(q.body.title || 'Alert').slice(0, 100), body: String(q.body.body || '').slice(0, 300), tag: q.body.tag || 'alert' }); let sent = 0;
  await Promise.all(subs.map(x => webpush.sendNotification(x, payload).then(() => sent++).catch(e => { if (e.statusCode === 404 || e.statusCode === 410) subs = subs.filter(y => y.endpoint !== x.endpoint); })));
  saveSubs(); r.json({ ok: true, sent });
});

const wss = new WebSocketServer({ server, path: '/stream' });
wss.on('connection', ws => {
  if (clients.size >= CFG.maxClients) { ws.close(1013, 'max clients'); return; }
  const c = { ws, sym: null, alive: true }; clients.add(c);
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', async raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'sub') { const sym = String(m.sym || '').toUpperCase(); if (!IDX[sym]) return; c.sym = sym; await ensure(sym); if (!st(sym).chain) pollChain(sym); ws.send(JSON.stringify(snapshot(sym))); }
  });
  ws.on('close', () => clients.delete(c)); ws.on('error', () => clients.delete(c));
});

// ---------- loops ----------
setInterval(() => { // throttle tick / depth fan-out
  for (const s of Object.values(St)) {
    if (s.dirty) { s.dirty = false; broadcast(s.sym, { type: 'tick', sym: s.sym, ltp: s.spot, prev: s.prev, candle: s.candles.last(), volSource: s.volSource }); }
    if (s.depthDirty && Date.now() - (s._dsent || 0) > 500) { s.depthDirty = false; s._dsent = Date.now(); broadcast(s.sym, { type: 'depth', sym: s.sym, bids: s.depth.bids, asks: s.depth.asks, ts: s.depth.ts }); }
  }
}, 200);
setInterval(() => { const m = JSON.stringify({ type: 'idx', idx: idxPayload(), feed: status().feed, depth20: { connected: depthFeed.connected }, t: Date.now() }); for (const c of clients) if (c.ws.readyState === 1) c.ws.send(m); }, 1000);
setInterval(() => { for (const c of clients) { if (!c.alive) { c.ws.terminate(); continue; } c.alive = false; try { c.ws.ping(); } catch {} } }, 25000);
setInterval(() => { for (const sym of wanted()) { ensure(sym).then(() => refreshHistory(sym)); } }, Math.min(CFG.histMs, 5000));
setInterval(() => { for (const sym of wanted()) pollChain(sym); }, CFG.chainMs);

server.listen(CFG.port, CFG.host, () => {
  log(`backend on ${CFG.host}:${CFG.port} | Dhan: ${!!(E.DHAN_CLIENT_ID && E.DHAN_ACCESS_TOKEN)} | live=${CFG.live} depth20=${CFG.depth20}`);
  if (CFG.live) {
    feed.start(); if (CFG.depth20) depthFeed.start();
    if (CFG.autoSub) { for (const v of Object.values(IDX)) subscribe('IDX_I', v.id, 'quote'); subscribe('IDX_I', VIX_ID, 'quote'); }
  }
  ensure(CFG.defaultIndex);
});
process.on('unhandledRejection', e => log('unhandledRejection', e && e.message));
