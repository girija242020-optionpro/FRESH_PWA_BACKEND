// Resilient Dhan WebSocket clients: market feed (v2) and 20-level depth.
const WebSocket = require('ws');
const EventEmitter = require('events');
const { parseFeed, parseDepth20 } = require('./parse');

const marketOpen = () => { const d = new Date(Date.now() + 19800e3), m = d.getUTCHours() * 60 + d.getUTCMinutes(), w = d.getUTCDay(); return w > 0 && w < 6 && m >= 540 && m <= 935; };

class BaseFeed extends EventEmitter {
  constructor(name, urlFn, staleMs) {
    super(); this.name = name; this.urlFn = urlFn; this.staleMs = staleMs;
    this.ws = null; this.subs = new Map(); this.retry = 0; this.lastMsg = 0; this.connected = false; this.stopped = false;
    this.dog = setInterval(() => { if (this.connected && marketOpen() && Date.now() - this.lastMsg > this.staleMs) { this.emit('log', this.name + ' stale, reconnecting'); this.ws && this.ws.terminate(); } }, 3000);
  }
  start() {
    const { DHAN_CLIENT_ID: id, DHAN_ACCESS_TOKEN: tk } = process.env;
    if (!id || !tk) { this.emit('log', this.name + ': Dhan credentials missing'); return; }
    this.stopped = false;
    const ws = this.ws = new WebSocket(this.urlFn(tk, id)); ws.binaryType = 'nodebuffer';
    ws.on('open', () => { this.connected = true; this.retry = 0; this.lastMsg = Date.now(); this.emit('log', this.name + ' connected'); this.emit('open'); this._resub(); });
    ws.on('message', (d, isBin) => { this.lastMsg = Date.now(); if (isBin !== false) this._onBin(d); });
    ws.on('close', () => { this.connected = false; this.emit('close'); if (!this.stopped) setTimeout(() => this.start(), Math.min(15000, 1000 * 2 ** this.retry++)); });
    ws.on('error', e => this.emit('log', this.name + ' error: ' + e.message));
  }
  send(o) { if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(o)); }
  stop() { this.stopped = true; clearInterval(this.dog); this.ws && this.ws.close(); }
}

class MarketFeed extends BaseFeed {
  constructor(staleMs) { super('feed', (tk, id) => `wss://api-feed.dhan.co?version=2&token=${tk}&clientId=${id}&authType=2`, staleMs); }
  // mode: 'ticker' (15) | 'quote' (17) | 'full' (21)
  subscribe(seg, id, mode = 'ticker') {
    const k = seg + ':' + id; if (this.subs.get(k) === mode) return; this.subs.set(k, mode);
    if (this.connected) this._send([{ seg, id, mode }]);
  }
  _send(list) {
    const code = { ticker: 15, quote: 17, full: 21 };
    for (const mode of Object.keys(code)) {
      const items = list.filter(x => x.mode === mode); for (let i = 0; i < items.length; i += 100)
        this.send({ RequestCode: code[mode], InstrumentCount: items.slice(i, i + 100).length, InstrumentList: items.slice(i, i + 100).map(x => ({ ExchangeSegment: x.seg, SecurityId: String(x.id) })) });
    }
  }
  _resub() { this._send([...this.subs].map(([k, mode]) => { const [seg, id] = k.split(':'); return { seg, id, mode }; })); }
  _onBin(b) { const p = parseFeed(b); if (p && p.type !== 'unknown') this.emit('packet', p); }
}

class DepthFeed extends BaseFeed {
  constructor(staleMs) { super('depth20', (tk, id) => `wss://depth-api-feed.dhan.co/twentydepth?token=${tk}&clientId=${id}&authType=2`, staleMs * 4); }
  subscribe(seg, id) { const k = seg + ':' + id; if (this.subs.has(k)) return; this.subs.set(k, 1); if (this.connected) this._send([[seg, id]]); }
  _send(list) { this.send({ RequestCode: 23, InstrumentCount: list.length, InstrumentList: list.map(([seg, id]) => ({ ExchangeSegment: seg, SecurityId: String(id) })) }); }
  _resub() { const l = [...this.subs.keys()].map(k => k.split(':')); if (l.length) this._send(l); }
  _onBin(b) { for (const m of parseDepth20(b)) this.emit('depth', m); }
}
module.exports = { MarketFeed, DepthFeed, marketOpen };
