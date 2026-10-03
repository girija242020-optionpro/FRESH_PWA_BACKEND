// Dhan REST helpers + security-master lookup (auto-detects current-month index future for volume)
const BASE = 'https://api.dhan.co/v2';
const MASTER_URL = 'https://images.dhan.co/api-data/api-scrip-master.csv';

const creds = () => ({ id: process.env.DHAN_CLIENT_ID, token: process.env.DHAN_ACCESS_TOKEN });

async function post(path, body) {
  const { id, token } = creds();
  const r = await fetch(BASE + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'access-token': token, 'client-id': id },
    body: JSON.stringify(body),
  });
  const txt = await r.text(); let j; try { j = JSON.parse(txt); } catch { j = { raw: txt }; }
  if (!r.ok) { const e = new Error('Dhan ' + r.status + ' ' + path + ': ' + txt.slice(0, 200)); e.status = r.status; throw e; }
  return j;
}

const istNow = () => new Date(Date.now() + 19800e3);
const fmt = d => d.toISOString().slice(0, 19).replace('T', ' ');
const istDate = () => istNow().toISOString().slice(0, 10);

// Normalise Dhan timestamps -> unix seconds (handles 1980-epoch and IST-shifted variants)
function fixTs(arr) {
  if (!arr.length) return arr;
  let off = 0; const nowS = Date.now() / 1000;
  if (arr[arr.length - 1] < 1.2e9) off += 315532800;
  const last = arr[arr.length - 1] + off;
  if (last - nowS > 3 * 3600) off -= 19800;
  return off ? arr.map(t => t + off) : arr;
}

// 1-minute candles for an instrument, last `days` calendar days (warm-up for RSI/DEMA)
async function intraday(secId, seg, instrument, days = 5) {
  const to = istNow(), from = new Date(to.getTime() - days * 86400e3);
  const j = await post('/charts/intraday', {
    securityId: String(secId), exchangeSegment: seg, instrument, interval: '1',
    fromDate: fmt(from).slice(0, 10) + ' 09:15:00', toDate: fmt(to),
  });
  const ts = fixTs(j.timestamp || []);
  return ts.map((t, i) => ({ t, o: j.open[i], h: j.high[i], l: j.low[i], c: j.close[i], v: j.volume ? (j.volume[i] || 0) : 0 }));
}

async function expiryList(underlyingId, seg = 'IDX_I') {
  const j = await post('/optionchain/expirylist', { UnderlyingScrip: underlyingId, UnderlyingSeg: seg });
  return j.data || [];
}
async function optionChainRaw(underlyingId, expiry, seg = 'IDX_I') {
  return (await post('/optionchain', { UnderlyingScrip: underlyingId, UnderlyingSeg: seg, Expiry: expiry })).data || {};
}

// ---- security master: find nearest-expiry FUTIDX for an index ----
let masterCache = null, masterAt = 0;
async function loadMasterFutures() {
  if (masterCache && Date.now() - masterAt < 6 * 3600e3) return masterCache;
  const res = await fetch(MASTER_URL); if (!res.ok) throw new Error('security master ' + res.status);
  const dec = new TextDecoder(); let buf = '', header = null, ix = {}; const futs = [];
  const pick = (names) => names.map(n => header.indexOf(n)).find(i => i >= 0);
  const onLine = (line) => {
    if (!line) return; const f = line.split(',');
    if (!header) { header = f.map(s => s.trim()); ix = {
      exch: pick(['SEM_EXM_EXCH_ID', 'EXCH_ID']), id: pick(['SEM_SMST_SECURITY_ID', 'SECURITY_ID']),
      inst: pick(['SEM_INSTRUMENT_NAME', 'INSTRUMENT']), sym: pick(['SEM_TRADING_SYMBOL', 'SYMBOL_NAME', 'DISPLAY_NAME']),
      und: pick(['UNDERLYING_SYMBOL']), exp: pick(['SEM_EXPIRY_DATE', 'SM_EXPIRY_DATE']), lot: pick(['SEM_LOT_UNITS', 'LOT_SIZE']) }; return; }
    if (f[ix.inst] !== 'FUTIDX') return;
    futs.push({ exch: f[ix.exch], id: f[ix.id], sym: (f[ix.sym] || '').toUpperCase(), und: ix.und >= 0 ? (f[ix.und] || '').toUpperCase() : '', exp: (f[ix.exp] || '').slice(0, 10), lot: +f[ix.lot] || 0 });
  };
  for await (const chunk of res.body) {
    buf += dec.decode(chunk, { stream: true });
    let i; while ((i = buf.indexOf('\n')) >= 0) { onLine(buf.slice(0, i).replace('\r', '')); buf = buf.slice(i + 1); }
  }
  onLine(buf.replace('\r', ''));
  masterCache = futs; masterAt = Date.now(); return futs;
}
async function resolveFuture(symbol, exchPrefix) {
  const today = istDate();
  const futs = (await loadMasterFutures()).filter(f => (f.und === symbol || f.sym === symbol || f.sym.startsWith(symbol + '-')) && f.exch.startsWith(exchPrefix) && f.exp >= today);
  futs.sort((a, b) => a.exp.localeCompare(b.exp));
  return futs[0] || null;
}

module.exports = { post, intraday, expiryList, optionChainRaw, resolveFuture, istDate, istNow };
