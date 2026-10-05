import express from "express";
import cors from "cors";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";
import { authenticator } from "otplib";
import webpush from "web-push";

const PORT = Number(process.env.PORT || 10000);
const CLIENT_ID = (process.env.DHAN_CLIENT_ID || "").trim();
const PIN = (process.env.DHAN_PIN || "").trim();
const TOTP_SECRET = (process.env.DHAN_TOTP_SECRET || "").replace(/\s+/g, "").toUpperCase();
const STATIC_TOKEN = (process.env.DHAN_ACCESS_TOKEN || "").trim();
const VAPID_PUBLIC_KEY = (process.env.VAPID_PUBLIC_KEY || "").trim();
const VAPID_PRIVATE_KEY = (process.env.VAPID_PRIVATE_KEY || "").trim();
const VAPID_SUBJECT = (process.env.VAPID_SUBJECT || "mailto:alerts@example.com").trim();
if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

const app = express();
app.use(cors({ origin: true }));
app.use(express.json({ limit: "2mb" }));
app.use((req,res,next)=>{res.setHeader("Cache-Control","no-store, no-cache, must-revalidate, proxy-revalidate");res.setHeader("Pragma","no-cache");res.setHeader("Expires","0");next();});

const INDEXES = {
  NIFTY: { securityId: "13", segment: "IDX_I", feedSegment: "IDX_I", name: "NIFTY", step: 50 },
  BANKNIFTY: { securityId: "25", segment: "IDX_I", feedSegment: "IDX_I", name: "BANKNIFTY", step: 100 },
  FINNIFTY: { securityId: "27", segment: "IDX_I", feedSegment: "IDX_I", name: "FINNIFTY", step: 50 },
  MIDCPNIFTY: { securityId: "442", segment: "IDX_I", feedSegment: "IDX_I", name: "MIDCPNIFTY", step: 25 },
  SENSEX: { securityId: "51", segment: "IDX_I", feedSegment: "IDX_I", name: "SENSEX", step: 100 },
};

const state = {
  version: "4.1-RSI-OI-PRO",
  indexKey: process.env.DEFAULT_INDEX || "NIFTY",
  expiry: null,
  spot: null,
  dhanConnected: false,
  depthConnected: false,
  lastTick: null,
  marketStatus: null,
  option: null,
  analytics: null,
  chain: { updatedAt: null, expiry: null, rows: [], atm: null, maxPain: null },
  subscriptions: [],
  server: { startedAt: Date.now(), ticks: 0, packets: 0, reconnects: 0 },
};

const clients = new Set();
const pushSubscriptions = new Map();
const ticks = new Map();
const candles = new Map();
const prevSession = new Map();
const instruments = new Map();
const optionMeta = new Map();
const lastChainRows = new Map();

// RSI OI PRO compatibility layer: lightweight OI history for windowed ΔOI.
const oiSnapshots = [];
const OI_SNAPSHOT_MS = 30_000;
const OI_HISTORY_MS = 24 * 3600_000;
let lastOISnapshotAt = 0;

let dhanWs = null;
let depthWs = null;
let reconnectTimer = null;
let depthReconnectTimer = null;
let chainTimer = null;
let instrumentRefreshTimer = null;
let accessToken = STATIC_TOKEN;
let tokenExpiry = 0;
let authStatus = STATIC_TOKEN ? "STATIC_TOKEN" : "WAITING";
let authLastError = null;
let authLastSuccessAt = null;
let chainBusy = false;
let instrumentCacheLoaded = false;

function now() { return Date.now(); }
function log(...a) { console.log(new Date().toISOString(), ...a); }
function clone(x) { return JSON.parse(JSON.stringify(x)); }
function broadcast(message) {
  const s = JSON.stringify(message);
  for (const ws of clients) if (ws.readyState === WebSocket.OPEN) ws.send(s);
}
function safeNum(v, fallback = null) { const n = Number(v); return Number.isFinite(n) ? n : fallback; }
function key(seg, sec) { return `${seg}:${sec}`; }
function round(v, d = 4) { return v == null || !Number.isFinite(v) ? null : Number(v.toFixed(d)); }

async function getAccessToken() {
  if (STATIC_TOKEN) { authStatus = "STATIC_TOKEN"; authLastError = null; return STATIC_TOKEN; }
  if (!CLIENT_ID || !PIN || !TOTP_SECRET) { authStatus = "CONFIG_ERROR"; throw new Error("Set DHAN_CLIENT_ID, DHAN_PIN and DHAN_TOTP_SECRET"); }
  if (accessToken && now() < tokenExpiry - 60_000) return accessToken;
  const totp = authenticator.generate(TOTP_SECRET);
  const url = `https://auth.dhan.co/app/generateAccessToken?dhanClientId=${encodeURIComponent(CLIENT_ID)}&pin=${encodeURIComponent(PIN)}&totp=${encodeURIComponent(totp)}`;
  const r = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
  });
  const text = await r.text();
  let j; try { j = JSON.parse(text); } catch { throw new Error(`Dhan auth returned non-JSON: ${text.slice(0, 200)}`); }
  if (!r.ok || !j.accessToken) { authStatus = `FAILED_${r.status}`; authLastError = j.errorMessage || j.message || text.slice(0, 200); throw new Error(`Dhan auth failed ${r.status}: ${authLastError}`); }
  accessToken = j.accessToken;
  authStatus = "AUTHENTICATED";
  authLastError = null;
  authLastSuccessAt = now();
  tokenExpiry = j.expiryTime ? new Date(j.expiryTime).getTime() : now() + 23 * 3600_000;
  return accessToken;
}

async function dhanPost(path, body) {
  const token = await getAccessToken();
  const headers = { "Content-Type": "application/json", Accept: "application/json", "access-token": token };
  if (CLIENT_ID) headers["client-id"] = CLIENT_ID;
  const r = await fetch(`https://api.dhan.co/v2${path}`, { method: "POST", headers, body: JSON.stringify(body) });
  const text = await r.text();
  let j; try { j = JSON.parse(text); } catch { j = { raw: text }; }
  if (!r.ok) throw new Error(`Dhan ${r.status}: ${text.slice(0, 400)}`);
  return j;
}

async function dhanGet(path) {
  const token = await getAccessToken();
  const headers = { Accept: "application/json", "access-token": token };
  if (CLIENT_ID) headers["client-id"] = CLIENT_ID;
  const r = await fetch(`https://api.dhan.co/v2${path}`, { headers });
  const text = await r.text();
  let j; try { j = JSON.parse(text); } catch { j = { raw: text }; }
  if (!r.ok) throw new Error(`Dhan ${r.status}: ${text.slice(0, 400)}`);
  return j;
}

function parseCsvLine(line) {
  const out = []; let cur = ""; let q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') { if (q && line[i + 1] === '"') { cur += '"'; i++; } else q = !q; }
    else if (ch === ',' && !q) { out.push(cur); cur = ""; }
    else cur += ch;
  }
  out.push(cur); return out;
}

async function loadInstrumentMaster() {
  const url = "https://images.dhan.co/api-data/api-scrip-master.csv";
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Instrument master ${r.status}`);
  const text = await r.text();
  const lines = text.split(/\r?\n/).filter(Boolean);
  const headers = parseCsvLine(lines[0]).map(x => x.trim());
  const idx = n => headers.indexOf(n);
  const ix = {
    exch: idx("SEM_EXM_EXCH_ID"), seg: idx("SEM_SEGMENT"), id: idx("SEM_SMST_SECURITY_ID"),
    inst: idx("SEM_INSTRUMENT_NAME"), expiry: idx("SEM_EXPIRY_DATE"), strike: idx("SEM_STRIKE_PRICE"),
    opt: idx("SEM_OPTION_TYPE"), trading: idx("SEM_TRADING_SYMBOL"), custom: idx("SEM_CUSTOM_SYMBOL"),
    lot: idx("SEM_LOT_UNITS"), symbol: idx("SM_SYMBOL_NAME"), tick: idx("SEM_TICK_SIZE")
  };
  instruments.clear();
  const segmentMap = { "NSE:I": "IDX_I", "NSE:D": "NSE_FNO", "NSE:E": "NSE_EQ", "BSE:I": "BSE_IDX", "BSE:D": "BSE_FNO", "BSE:E": "BSE_EQ", "MCX:M": "MCX_COMM" };
  for (let i = 1; i < lines.length; i++) {
    const c = parseCsvLine(lines[i]); if (c.length < 8) continue;
    const ex = c[ix.exch], sg = c[ix.seg], es = segmentMap[`${ex}:${sg}`]; if (!es) continue;
    const id = c[ix.id]; if (!id) continue;
    const row = {
      securityId: String(id), exchangeSegment: es, exchange: ex, segmentCode: sg,
      instrument: c[ix.inst] || null, expiry: c[ix.expiry] || null, strike: safeNum(c[ix.strike], 0),
      optionType: c[ix.opt] || null, tradingSymbol: c[ix.trading] || null, customSymbol: c[ix.custom] || null,
      symbol: c[ix.symbol] || null, lotSize: safeNum(c[ix.lot], 1), tickSize: safeNum(c[ix.tick], null)
    };
    instruments.set(key(es, id), row);
  }
  instrumentCacheLoaded = true;
  log(`Loaded ${instruments.size} Dhan instruments`);
}

function underlyingInfo() { return INDEXES[state.indexKey] || INDEXES.NIFTY; }

async function getExpiryList() {
  const x = underlyingInfo();
  const j = await dhanPost("/optionchain/expirylist", { UnderlyingScrip: Number(x.securityId), UnderlyingSeg: x.segment });
  const dates = Array.isArray(j.data) ? j.data : [];
  if (dates.length && (!state.expiry || !dates.includes(state.expiry))) state.expiry = dates[0];
  return dates;
}

async function getOptionChain() {
  const x = underlyingInfo();
  return dhanPost("/optionchain", { UnderlyingScrip: Number(x.securityId), UnderlyingSeg: x.segment, Expiry: state.expiry });
}

function bsTime(expiry) {
  const t = new Date(expiry).getTime() - now();
  return Math.max(t / 86400000 / 365, 1 / (365 * 24 * 60));
}
function normPdf(x) { return Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI); }
function normCdf(x) { return 0.5 * (1 + erf(x / Math.sqrt(2))); }
function erf(x) {
  const sign = x < 0 ? -1 : 1; x = Math.abs(x);
  const a1=0.254829592,a2=-0.284496736,a3=1.421413741,a4=-1.453152027,a5=1.061405429,p=0.3275911;
  const t=1/(1+p*x); const y=1-(((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*Math.exp(-x*x); return sign*y;
}
function hiddenGreeks({ spot, strike, iv, expiry, optionType, rate = 0.06 }) {
  const S = Number(spot), K = Number(strike), sigma = Math.max(Number(iv) / 100, 0.0001), T = bsTime(expiry);
  if (!(S > 0 && K > 0)) return {};
  const isCall = optionType === "CE" || optionType === "CALL";
  const q = 0;
  const d1 = (Math.log(S / K) + (rate - q + sigma*sigma/2)*T) / (sigma*Math.sqrt(T));
  const d2 = d1 - sigma*Math.sqrt(T);
  const pdf = normPdf(d1);
  const gamma = Math.exp(-q*T)*pdf/(S*sigma*Math.sqrt(T));
  const vega = S*Math.exp(-q*T)*pdf*Math.sqrt(T) / 100;
  const theta = (-S*pdf*sigma*Math.exp(-q*T)/(2*Math.sqrt(T)) - (isCall ? rate*K*Math.exp(-rate*T)*normCdf(d2) : -rate*K*Math.exp(-rate*T)*normCdf(-d2))) / 365;
  const delta = isCall ? Math.exp(-q*T)*normCdf(d1) : Math.exp(-q*T)*(normCdf(d1)-1);
  const vanna = -Math.exp(-q*T) * pdf * d2 / sigma / 100;
  const vomma = (vega * d1 * d2) / sigma;
  const charm = isCall
    ? q*Math.exp(-q*T)*normCdf(d1) - Math.exp(-q*T)*pdf*(2*(rate-q)*T-d2*sigma*Math.sqrt(T))/(2*T*sigma*Math.sqrt(T))
    : q*Math.exp(-q*T)*normCdf(-d1) + Math.exp(-q*T)*pdf*(2*(rate-q)*T-d2*sigma*Math.sqrt(T))/(2*T*sigma*Math.sqrt(T));
  const color = -Math.exp(-q*T)*pdf/(2*S*T*sigma*Math.sqrt(T)) * (2*q*T + 1 + ((2*(rate-q)*T-d2*sigma*Math.sqrt(T))*d1)/(sigma*Math.sqrt(T)));
  const speed = -gamma/S * (1 + d1/(sigma*Math.sqrt(T)));
  const zomma = gamma * ((d1*d2 - 1) / sigma);
  return { delta, gamma, vega, theta, vanna, vomma, charm, color, speed, zomma };
}

function optionRow(strike, leg, type, expiry, spot) {
  if (!leg) return null;
  const ltp = safeNum(leg.last_price, 0);
  const oi = safeNum(leg.oi, 0);
  const volume = safeNum(leg.volume, 0);
  const prevOI = safeNum(leg.previous_oi, 0);
  const prevVol = safeNum(leg.previous_volume, 0);
  const iv = safeNum(leg.implied_volatility, 0);
  const g = hiddenGreeks({ spot, strike, iv, expiry, optionType: type });
  const lot = safeNum(instruments.get(key("NSE_FNO", leg.security_id))?.lotSize, 1);
  const gex = (g.gamma || safeNum(leg.greeks?.gamma, 0)) * oi * lot * spot * spot / 1e7;
  return {
    strike, type, securityId: String(leg.security_id), ltp, oi, previousOI: prevOI, changeOI: oi-prevOI,
    volume, previousVolume: prevVol, changeVolume: volume-prevVol, iv,
    bid: safeNum(leg.top_bid_price, 0), ask: safeNum(leg.top_ask_price, 0),
    bidQty: safeNum(leg.top_bid_quantity, 0), askQty: safeNum(leg.top_ask_quantity, 0),
    averagePrice: safeNum(leg.average_price, 0),
    dhanGreeks: leg.greeks || {}, hiddenGreeks: g, gexProxy: round(gex, 3),
    notionalOI: round(oi*lot*strike, 2)
  };
}

function buildAnalytics(rows, spot) {
  const ce = rows.filter(x => x.type === "CE");
  const pe = rows.filter(x => x.type === "PE");
  const sum = (a,k) => a.reduce((s,x)=>s+(Number(x[k])||0),0);
  const callOI=sum(ce,"oi"), putOI=sum(pe,"oi"), callVol=sum(ce,"volume"), putVol=sum(pe,"volume");
  const callChOI=sum(ce,"changeOI"), putChOI=sum(pe,"changeOI");
  const pcr = callOI ? putOI/callOI : null;
  const pcrVol = callVol ? putVol/callVol : null;
  const maxCallWall = ce.reduce((a,b)=>!a||b.oi>a.oi?b:a,null);
  const maxPutWall = pe.reduce((a,b)=>!a||b.oi>a.oi?b:a,null);
  const maxCallChWall = ce.reduce((a,b)=>!a||b.changeOI>a.changeOI?b:a,null);
  const maxPutChWall = pe.reduce((a,b)=>!a||b.changeOI>a.changeOI?b:a,null);
  const atm = rows.reduce((a,b)=>!a||Math.abs(b.strike-spot)<Math.abs(a.strike-spot)?b:a,null)?.strike || null;
  const strikes=[...new Set(rows.map(x=>x.strike))].sort((a,b)=>a-b);
  let minPain=null, minPainVal=Infinity;
  for(const k of strikes){ let pain=0; for(const x of ce) pain += Math.max(0,k-x.strike)*x.oi; for(const x of pe) pain += Math.max(0,x.strike-k)*x.oi; if(pain<minPainVal){minPainVal=pain;minPain=k;} }
  const gexCE=sum(ce,"gexProxy"), gexPE=sum(pe,"gexProxy");
  const dealerProxy = gexCE - gexPE;
  const ivAtm = rows.filter(x=>x.strike===atm).reduce((s,x)=>s+(x.iv||0),0)/(rows.filter(x=>x.strike===atm).length||1);
  const smile = strikes.map(k=>{const r=rows.filter(x=>x.strike===k);return {strike:k,ceIV:r.find(x=>x.type==='CE')?.iv??null,peIV:r.find(x=>x.type==='PE')?.iv??null};});
  return {
    atm, pcr:round(pcr,4), pcrVolume:round(pcrVol,4), callOI, putOI, callChangeOI:callChOI, putChangeOI:putChOI,
    callVolume:callVol, putVolume:putVol, callChangeVolume:sum(ce,"changeVolume"), putChangeVolume:sum(pe,"changeVolume"),
    callOIWall:maxCallWall?.strike??null, putOIWall:maxPutWall?.strike??null,
    callChangeOIWall:maxCallChWall?.strike??null, putChangeOIWall:maxPutChWall?.strike??null,
    maxPain:minPain, maxPainValue:minPainVal===Infinity?null:minPainVal, atmIV:round(ivAtm,3),
    totalGEXProxy:round(dealerProxy,3), callGEXProxy:round(gexCE,3), putGEXProxy:round(gexPE,3),
    dealerHedgePressureProxy: round(-dealerProxy,3), smile,
    interpretation: dealerProxy>0?"positive-gamma proxy / stabilizing hedge tendency":dealerProxy<0?"negative-gamma proxy / amplifying hedge tendency":"neutral gamma proxy"
  };
}

async function refreshChain() {
  if (chainBusy) return; chainBusy = true;
  try {
    if (!state.expiry) await getExpiryList();
    const j = await getOptionChain();
    const spot = safeNum(j?.data?.last_price, state.spot); if (spot) state.spot=spot;
    const oc=j?.data?.oc||{}; const rows=[];
    for(const [ks,row] of Object.entries(oc)){
      const strike=Number(ks); if(!Number.isFinite(strike)) continue;
      const ce=optionRow(strike,row.ce,"CE",state.expiry,state.spot); const pe=optionRow(strike,row.pe,"PE",state.expiry,state.spot);
      if(ce) rows.push(ce); if(pe) rows.push(pe);
      if(ce) optionMeta.set(key("NSE_FNO",ce.securityId),ce); if(pe) optionMeta.set(key("NSE_FNO",pe.securityId),pe);
    }
    state.chain={updatedAt:now(),expiry:state.expiry,rows,atm:buildAnalytics(rows,state.spot).atm,maxPain:buildAnalytics(rows,state.spot).maxPain};
    state.analytics=buildAnalytics(rows,state.spot);
    recordOISnapshot(rows);
    state.option=selectPrimaryOption(rows);
    await syncMarketSubscriptions(rows);
    broadcast({type:"optionChain",chain:clone(state.chain),analytics:clone(state.analytics)});
    broadcast({type:"state",state:publicState()});
  } catch(e){ log("Option chain:",e.message); broadcast({type:"error",message:e.message}); }
  finally { chainBusy=false; }
}

function selectPrimaryOption(rows){
  const side = state.analytics?.pcr != null && state.analytics.pcr < 0.9 ? "CE" : "PE";
  const pool=rows.filter(x=>x.type===side&&x.ltp>0&&x.ltp>=5&&x.ltp<=30);
  const sorted=(pool.length?pool:rows.filter(x=>x.type===side&&x.ltp>0)).sort((a,b)=>Math.abs(a.strike-state.spot)-Math.abs(b.strike-state.spot));
  return sorted[0]||null;
}

async function syncMarketSubscriptions(rows){
  const base=[{ExchangeSegment:underlyingInfo().feedSegment,SecurityId:underlyingInfo().securityId}];
  const near=[...new Map(rows.map(x=>[x.securityId,x])).values()].sort((a,b)=>Math.abs(a.strike-state.spot)-Math.abs(b.strike-state.spot)).slice(0,600);
  const inst=[...base,...near.map(x=>({ExchangeSegment:"NSE_FNO",SecurityId:x.securityId}))];
  state.subscriptions=inst; subscribe(dhanWs,inst,21);
  syncDepthSubscriptions(near.slice(0,20));
}

function subscribe(ws, insts, requestCode=21){
  if(!ws||ws.readyState!==WebSocket.OPEN||!insts?.length)return;
  const unique=[...new Map(insts.map(x=>[key(x.ExchangeSegment,x.SecurityId),x])).values()];
  for(let i=0;i<unique.length;i+=100){const a=unique.slice(i,i+100);try{ws.send(JSON.stringify({RequestCode:requestCode,InstrumentCount:a.length,InstrumentList:a}));}catch{}}
}

function decodeOne(buf){
  if(buf.length<8)return null;
  const dv=new DataView(buf.buffer,buf.byteOffset,buf.byteLength);
  const code=dv.getUint8(0), seg=dv.getUint8(3), sec=dv.getUint32(4,true), len=dv.getUint16(1,true)||buf.length;
  const f=o=>dv.getFloat32(o,true), i=o=>dv.getInt32(o,true), u=o=>dv.getUint32(o,true), i16=o=>dv.getInt16(o,true), u16=o=>dv.getUint16(o,true);
  const base={code,seg,sec,len};
  if(code===2&&buf.length>=16)return {...base,type:"ticker",ltp:f(8),ltt:u(12)};
  if(code===4&&buf.length>=51)return {...base,type:"quote",ltp:f(8),ltq:u16(12),ltt:u(14),avgPrice:f(18),volume:u(22),sellQty:u(26),buyQty:u(30),open:f(34),close:f(38),high:f(42),low:f(46)};
  if(code===5&&buf.length>=12)return {...base,type:"oi",oi:u(8)};
  if(code===6&&buf.length>=16)return {...base,type:"prev",prevClose:f(8),prevOI:u(12)};
  if(code===7)return {...base,type:"marketStatus",statusCode:buf.length>=10?u16(8):null};
  if(code===8&&buf.length>=163){
    const depth=[]; for(let n=0;n<5;n++){const o=63+n*20;depth.push({bidQty:i(o),askQty:i(o+4),bidOrders:i16(o+8),askOrders:i16(o+10),bid:f(o+12),ask:f(o+16)});}
    return {...base,type:"full",ltp:f(8),ltq:u16(12),ltt:u(14),avgPrice:f(18),volume:u(22),sellQty:u(26),buyQty:u(30),oi:u(34),oiDayHigh:u(38),oiDayLow:u(42),open:f(46),close:f(50),high:f(54),low:f(58),depth};
  }
  if(code===50)return {...base,type:"disconnect",disconnectCode:buf.length>=10?u16(8):null};
  return base;
}

function decodeMessage(data){
  const b=Buffer.from(data); const out=[]; let off=0;
  while(off+8<=b.length){
    const len=b.readUInt16LE(off+1)||b.length-off; const end=Math.min(b.length,off+len); const packet=b.subarray(off,end);
    const x=decodeOne(packet); if(x)out.push(x); if(len<=0)break; off=end;
  }
  return out;
}

function updateTick(p){
  if(!p||p.sec==null)return;
  const segName=Object.entries({0:"IDX_I",1:"NSE_EQ",2:"NSE_FNO",3:"NSE_CURRENCY",4:"BSE_EQ",5:"MCX_COMM",7:"BSE_CURRENCY",8:"BSE_FNO"}).find(([,n])=>n===p.seg)?.[1]||p.seg;
  const k=key(segName,p.sec); let t=ticks.get(k)||{securityId:String(p.sec),exchangeSegment:segName,securityIdNum:p.sec};
  Object.assign(t,{updatedAt:now()});
  if(p.ltp!=null)t.ltp=p.ltp; if(p.ltt!=null)t.ltt=p.ltt*1000; if(p.ltq!=null)t.lastTradeQty=p.ltq;
  if(p.avgPrice!=null)t.avgPrice=p.avgPrice; if(p.volume!=null){if(t.sessionStartVolume==null)t.sessionStartVolume=p.volume;t.volume=p.volume;t.changeVolume=p.volume-t.sessionStartVolume;}
  if(p.sellQty!=null)t.sellQuantity=p.sellQty; if(p.buyQty!=null)t.buyQuantity=p.buyQty;
  for(const k0 of ["open","close","high","low","oi","oiDayHigh","oiDayLow","prevClose","prevOI"])if(p[k0]!=null)t[k0]=p[k0];
  if(t.oi!=null){if(t.sessionStartOI==null)t.sessionStartOI=t.oi;t.changeOI=t.oi-t.sessionStartOI;}
  if(p.depth)t.depth=p.depth;
  if(p.type==="prev"){t.prevClose=p.prevClose;t.prevOI=p.prevOI;if(t.oi!=null)t.changeOI=t.oi-p.prevOI;}
  ticks.set(k,t);
  state.server.ticks++;
  if(segName==="IDX_I"&&String(p.sec)===underlyingInfo().securityId){state.spot=p.ltp??state.spot;state.lastTick=now();}
  const meta=optionMeta.get(k); if(meta){Object.assign(meta,{ltp:t.ltp,volume:t.volume,changeVolume:t.changeVolume,oi:t.oi,changeOI:t.changeOI,bid:t.depth?.[0]?.bid??meta.bid,ask:t.depth?.[0]?.ask??meta.ask});}
  updateCandle(k,t);
}

function updateCandle(k,t){
  if(!t.ltp)return; const tf=60_000; const tm=Math.floor((t.ltt||now())/tf)*tf; let arr=candles.get(k)||[]; let c=arr[arr.length-1];
  if(!c||c.time!==tm){c={time:tm,open:t.ltp,high:t.ltp,low:t.ltp,close:t.ltp,volume:t.volume??0};arr=[...arr,c];}
  else {c.high=Math.max(c.high,t.ltp);c.low=Math.min(c.low,t.ltp);c.close=t.ltp;c.volume=t.volume??c.volume;}
  if(arr.length>1000)arr=arr.slice(-1000);candles.set(k,arr);
}

function marketSessionLabel() {
  try {
    const parts = new Intl.DateTimeFormat("en-IN", { timeZone: "Asia/Kolkata", hour: "2-digit", minute: "2-digit", hour12: false }).formatToParts(new Date());
    const h = Number(parts.find(x=>x.type==="hour")?.value || 0), m = Number(parts.find(x=>x.type==="minute")?.value || 0);
    const mins = h * 60 + m;
    if (mins >= 555 && mins <= 930) return "OPEN";
    if (mins >= 540 && mins < 555) return "PRE-MARKET";
    return "CLOSED";
  } catch { return state.marketStatus == null ? "UNKNOWN" : String(state.marketStatus); }
}

function emaSeries(values, length) {
  const out = []; if (!values.length) return out;
  const k = 2 / (length + 1); let e = values[0]; out.push(e);
  for (let i=1;i<values.length;i++) { e = values[i] * k + e * (1-k); out.push(e); }
  return out;
}
function smaSeries(values, length) {
  const out = []; let sum=0;
  for(let i=0;i<values.length;i++){sum+=values[i];if(i>=length)sum-=values[i-length];out.push(i+1>=length?sum/length:null);}
  return out;
}
function wmaSeries(values, length) {
  const out=[]; const den=length*(length+1)/2;
  for(let i=0;i<values.length;i++){if(i+1<length){out.push(null);continue;}let sum=0;for(let j=0;j<length;j++)sum+=values[i-j]*(length-j);out.push(sum/den);} return out;
}
function rsiSeries(values, length=14) {
  const out=Array(values.length).fill(null); if(values.length<=length)return out;
  let gain=0,loss=0; for(let i=1;i<=length;i++){const d=values[i]-values[i-1];gain+=Math.max(d,0);loss+=Math.max(-d,0);}
  const calc=()=>loss===0?100:100-100/(1+gain/loss); out[length]=calc();
  for(let i=length+1;i<values.length;i++){const d=values[i]-values[i-1];gain=(gain*(length-1)+Math.max(d,0))/length;loss=(loss*(length-1)+Math.max(-d,0))/length;out[i]=calc();}
  return out;
}
function indicatorSnapshot() {
  const info=underlyingInfo(); const cs=candles.get(key(info.segment,info.securityId))||[];
  const closes=cs.map(c=>Number(c.close)||0).filter(Number.isFinite); if(!closes.length)return {};
  const rsiArr=rsiSeries(closes,14), rsiValid=rsiArr.filter(v=>v!=null); const rsi=rsiValid.at(-1)??null;
  const rsiSmaArr=smaSeries(rsiValid,9), rsiSma=rsiSmaArr.at(-1)??null;
  const ema24=emaSeries(closes,24), ema=ema24.at(-1)??null; const emaE=emaSeries(ema24,24); const dema=2*ema-(emaE.at(-1)??ema);
  const half=Math.max(2,Math.floor(60/2)), sqrt=Math.max(2,Math.floor(Math.sqrt(60))); const w1=wmaSeries(closes,half), w2=wmaSeries(closes,60); const diff=closes.map((_,i)=>w1[i]!=null&&w2[i]!=null?w1[i]*2-w2[i]:null).filter(v=>v!=null); const hull=diff.length?wmaSeries(diff,sqrt).at(-1):null;
  const vwapDen=cs.reduce((s,c)=>s+(Number(c.volume)||0),0); const vwap=vwapDen?cs.reduce((s,c)=>s+((c.high+c.low+c.close)/3)*(Number(c.volume)||0),0)/vwapDen:null;
  const vols=cs.map(c=>Number(c.volume)||0); const av=smaSeries(vols,20).at(-1)??null;
  return {rsi:round(rsi,2),rsiSma:round(rsiSma,2),ema:round(ema,2),dema:round(dema,2),hull:round(hull,2),vwap:round(vwap,2),volume:vols.at(-1)??0,avgVolume:round(av,2),candles:cs.slice(-160)};
}
function primaryL20() {
  const o=state.option; if(!o) return {bid:0,ask:0,bidQty:0,askQty:0,imbalance:0,securityId:null,depth:[]};
  const t=ticks.get(key("NSE_FNO",String(o.securityId))); const d=t?.depth||[]; let bidQty=0,askQty=0;
  for(const r of d){bidQty+=Number(r.bidQty??r.bid_qty??0)||0;askQty+=Number(r.askQty??r.ask_qty??0)||0;}
  const bid=d[0]?.bid??o.bid??0, ask=d[0]?.ask??o.ask??0;
  return {securityId:String(o.securityId),strike:o.strike,type:o.type,bid,ask,bidQty,askQty,imbalance:(bidQty+askQty)?(bidQty-askQty)/(bidQty+askQty):0,depth:d,updatedAt:t?.updatedAt??null};
}
function recordOISnapshot(rows) {
  const t=now(); if(t-lastOISnapshotAt<OI_SNAPSHOT_MS)return; lastOISnapshotAt=t;
  const near=[...rows].sort((a,b)=>Math.abs(a.strike-(state.spot||0))-Math.abs(b.strike-(state.spot||0))).slice(0,101);
  oiSnapshots.push({ts:t,rows:near.map(r=>({s:Number(r.strike),c:Number(r.oi||0),p:Number(r.type==='PE'?r.oi:0)}))});
  while(oiSnapshots.length && (t-oiSnapshots[0].ts>OI_HISTORY_MS))oiSnapshots.shift();
}
function oiWindowMs(window) { const m=String(window||"5m").toLowerCase(); const map={"5m":5,"10m":10,"15m":15,"30m":30,"1h":60,"2h":120,"3h":180,"1d":1440}; return (map[m]||5)*60_000; }
function oiProfileRows(window="5m", range=30) {
  const current=rowsWithLive().reduce((m,r)=>{const x=m.get(r.strike)||{strike:r.strike,ce:null,pe:null};if(r.type==="CE")x.ce=r;else if(r.type==="PE")x.pe=r;m.set(r.strike,x);return m;},new Map());
  const target=now()-oiWindowMs(window); let base=oiSnapshots[0]; for(const x of oiSnapshots){if(x.ts<=target)base=x;else break;}
  const byStrike=new Map(); if(base) for(const r of base.rows){const x=byStrike.get(r.s)||{ce:0,pe:0}; if(r.c) x.ce=r.c; if(r.p) x.pe=r.p; byStrike.set(r.s,x);}
  const all=[...current.values()].sort((a,b)=>a.strike-b.strike); const atm=state.chain.atm||state.analytics?.atm||state.spot; const nearest=all.sort((a,b)=>Math.abs(a.strike-atm)-Math.abs(b.strike-atm)).slice(0,Math.max(1,range*2+1)).sort((a,b)=>a.strike-b.strike);
  return nearest.map(x=>({strike:x.strike,ceOI:Number(x.ce?.oi||0),peOI:Number(x.pe?.oi||0),ceOIChange:base?Number(x.ce?.oi||0)-Number(byStrike.get(x.strike)?.ce||0):Number(x.ce?.changeOI||0),peOIChange:base?Number(x.pe?.oi||0)-Number(byStrike.get(x.strike)?.pe||0):Number(x.pe?.changeOI||0),ceLtp:x.ce?.ltp??null,peLtp:x.pe?.ltp??null,ceVolume:x.ce?.volume??0,peVolume:x.pe?.volume??0,ceBid:x.ce?.bid??null,ceAsk:x.ce?.ask??null,peBid:x.pe?.bid??null,peAsk:x.pe?.ask??null}));
}
function pwaContext() {
  const ind=indicatorSnapshot(); const oiRows=oiProfileRows("5m",30); const l20=primaryL20();
  const prev=state.lastTick&&ticks.get(key(underlyingInfo().segment,underlyingInfo().securityId))?.prevClose; const change=prev!=null&&state.spot!=null?state.spot-prev:null;
  return {index:state.indexKey,spot:state.spot,prevClose:prev??null,change,expiry:state.expiry,atm:state.chain.atm||state.analytics?.atm||null,market:marketSessionLabel(),marketStatus:state.marketStatus,dhanConnected:state.dhanConnected,depthConnected:state.depthConnected,lastTick:state.lastTick,indicators:ind,candles:ind.candles||[],l20,oiProfile:oiRows,oiWindows:["5m","10m","15m","30m","1h","2h","3h","1d"],analytics:state.analytics,option:state.option,stale:!state.lastTick||(now()-state.lastTick>5000),server:state.server};
}
function publicState(){return {...state,ticks:undefined,pwa:pwaContext()};}
function feedOpen(){return dhanWs&&dhanWs.readyState===WebSocket.OPEN;}

function connectFeed(){
  clearTimeout(reconnectTimer); try{dhanWs?.close();}catch{}
  getAccessToken().then(token=>{
    const url=`wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(token)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`;
    dhanWs=new WebSocket(url);
    dhanWs.on("open",()=>{state.dhanConnected=true;state.server.reconnects++;log("Dhan live feed connected (authenticated)");subscribe(dhanWs,state.subscriptions.length?state.subscriptions:[{ExchangeSegment:underlyingInfo().feedSegment,SecurityId:underlyingInfo().securityId}],21);broadcast({type:"state",state:publicState()});});
    dhanWs.on("message",data=>{for(const p of decodeMessage(data)){state.server.packets++;updateTick(p);if(p.type==="marketStatus")state.marketStatus=p.statusCode;}});
    dhanWs.on("close",(code,reason)=>{state.dhanConnected=false;log(`Dhan feed closed code=${code} reason=${reason?.toString()||""}`);broadcast({type:"state",state:publicState()});reconnectTimer=setTimeout(connectFeed,5000);});
    dhanWs.on("error",e=>log("Dhan feed error",e.message));
  }).catch(e=>{log("Dhan auth failed",e.message);state.dhanConnected=false;broadcast({type:"error",message:e.message});reconnectTimer=setTimeout(connectFeed,10000);});
}

function connectDepth(){
  clearTimeout(depthReconnectTimer); try{depthWs?.close();}catch{}
  getAccessToken().then(token=>{
    depthWs=new WebSocket(`wss://depth-api-feed.dhan.co/twentydepth?token=${encodeURIComponent(token)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`);
    depthWs.on("open",()=>{state.depthConnected=true;syncDepthSubscriptions([...state.chain.rows].sort((a,b)=>Math.abs(a.strike-state.spot)-Math.abs(b.strike-state.spot)).slice(0,20));});
    depthWs.on("message",data=>{for(const p of decodeMessage(data))updateTick(p);});
    depthWs.on("close",()=>{state.depthConnected=false;depthReconnectTimer=setTimeout(connectDepth,7000);});
    depthWs.on("error",e=>log("Depth feed error",e.message));
  }).catch(e=>log("Depth auth",e.message));
}
function syncDepthSubscriptions(rows){
  if(!depthWs||depthWs.readyState!==WebSocket.OPEN)return;
  const inst=rows.slice(0,20).map(x=>({ExchangeSegment:"NSE_FNO",SecurityId:x.securityId}));
  for(let i=0;i<inst.length;i+=50){const a=inst.slice(i,i+50);try{depthWs.send(JSON.stringify({RequestCode:23,InstrumentCount:a.length,InstrumentList:a}));}catch{}}
}

function rowsWithLive(){return state.chain.rows.map(r=>{const t=ticks.get(key("NSE_FNO",r.securityId));return t?{...r,ltp:t.ltp,volume:t.volume,changeVolume:t.changeVolume,oi:t.oi,changeOI:t.changeOI,bid:t.depth?.[0]?.bid??r.bid,ask:t.depth?.[0]?.ask??r.ask,bidQty:t.depth?.[0]?.bidQty??r.bidQty,askQty:t.depth?.[0]?.askQty??r.askQty}:r;});}

app.get("/",(_q,res)=>{
  res.status(200).type("html").send(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="cache-control" content="no-store"><title>Bharati Unique Backend V4</title><style>body{font-family:system-ui,sans-serif;background:#0b1220;color:#eef2ff;padding:24px;line-height:1.5}h1{margin:0 0 8px}a{color:#7dd3fc}.ok{font-weight:700}.card{background:#111b2e;padding:18px;border-radius:14px;max-width:720px}code{background:#1e293b;padding:2px 6px;border-radius:6px}.row{margin:10px 0}.muted{color:#94a3b8}.good{color:#4ade80}.warn{color:#fbbf24}.bad{color:#fb7185}</style></head><body><div class="card"><h1>Bharati Unique Backend V4</h1><div class="good">● ONLINE</div><p>Ultimate frontend-agnostic market data gateway.</p><p>Version: <code>${state.version}</code></p><div class="row">Dhan feed: <strong id="dhan" class="warn">CHECKING...</strong></div><div class="row">Depth feed: <strong id="depth" class="warn">CHECKING...</strong></div><div class="row muted" id="stats">Checking live status...</div><p><a href="/health">Health</a> · <a href="/api/health">API Health JSON</a> · <a href="/api/state">State</a> · <a href="/api/analytics">Analytics</a> · <a href="/api/option-chain">Option Chain</a></p></div><script>async function refresh(){try{const r=await fetch('/api/health',{cache:'no-store'});const j=await r.json();const d=document.getElementById('dhan');const dep=document.getElementById('depth');d.textContent=j.dhanConnected?'CONNECTED':'CONNECTING';d.className=j.dhanConnected?'good':'warn';dep.textContent=j.depthConnected?'CONNECTED':'CONNECTING';dep.className=j.depthConnected?'good':'warn';document.getElementById('stats').textContent='Ticks: '+j.ticks+' · Packets: '+j.packets+' · Subscriptions: '+j.subscriptions+' · Chain rows: '+j.chainRows;}catch(e){document.getElementById('dhan').textContent='UNAVAILABLE';document.getElementById('dhan').className='bad';}}refresh();setInterval(refresh,3000);</script></body></html>`);
});
app.get("/health",(_q,res)=>res.status(200).type("text").send(`OK\nBharati Unique Backend V4\nDhan feed: ${state.dhanConnected ? "CONNECTED" : "CONNECTING"}\nVersion: ${state.version}\n`));
app.get("/api/health",(_q,res)=>res.status(200).json({ok:true,version:state.version,dhanConnected:state.dhanConnected,depthConnected:state.depthConnected,authStatus,authLastSuccessAt,authLastError:authLastError?String(authLastError).slice(0,200):null,ticks:state.server.ticks,packets:state.server.packets,subscriptions:state.subscriptions.length,chainRows:state.chain.rows.length,lastTick:state.lastTick,time:now()}));
app.get("/api/config",(_q,res)=>res.json({ok:true,version:state.version,indexes:INDEXES,vapidPublicKey:VAPID_PUBLIC_KEY,features:["tick","quote","oi","volume","depth5","option-chain","greeks","hidden-greeks","oi-walls","pcr","max-pain","iv-smile","historical","websocket","rsi-oi-pro","oi-window-delta","l20-20","indicators","pwa-context"]}));
app.get("/api/state",(_q,res)=>res.json(publicState()));
app.get("/api/tick",(q,res)=>{const seg=q.query.segment||"NSE_FNO";const sec=String(q.query.securityId||"");const t=ticks.get(key(seg,sec));res.json({ok:!!t,data:t||null});});
app.get("/api/ticks",(_q,res)=>res.json({ok:true,data:[...ticks.values()]}));
app.get("/api/option-chain",(q,res)=>{const window=String(q.query.window||"session");const range=Math.min(Math.max(Number(q.query.range||30),5),50);const rows=window==="session"?rowsWithLive():oiProfileRows(window,range);res.json({ok:true,expiry:state.expiry,spot:state.spot,atm:state.chain.atm,rows,analytics:state.analytics,window,updatedAt:state.chain.updatedAt});});
app.get("/api/oi-profile",(q,res)=>{const window=String(q.query.window||"5m");const range=Math.min(Math.max(Number(q.query.range||30),5),50);const rows=oiProfileRows(window,range);res.json({ok:true,index:state.indexKey,spot:state.spot,atm:state.chain.atm,expiry:state.expiry,window,rows,analytics:state.analytics,updatedAt:state.chain.updatedAt,snapshotAt:oiSnapshots.at(-1)?.ts??null});});
app.get("/api/market-context",(_q,res)=>res.json({ok:true,data:pwaContext()}));
app.get("/api/l20",(q,res)=>{const seg=q.query.segment||"NSE_FNO",sec=String(q.query.securityId||"");if(sec){const t=ticks.get(key(seg,sec));return res.json({ok:true,data:{securityId:sec,depth:t?.depth||[],updatedAt:t?.updatedAt??null}});}res.json({ok:true,data:primaryL20()});});
app.get("/api/analytics",(_q,res)=>res.json({ok:true,spot:state.spot,analytics:state.analytics}));
app.get("/api/history",(q,res)=>{const seg=q.query.segment||"IDX_I",sec=String(q.query.securityId||underlyingInfo().securityId);res.json({ok:true,data:candles.get(key(seg,sec))||[]});});
app.get("/api/instruments",(q,res)=>{const search=String(q.query.search||"").toUpperCase();const limit=Math.min(Number(q.query.limit||100),1000);const arr=[...instruments.values()].filter(x=>!search||JSON.stringify(x).toUpperCase().includes(search)).slice(0,limit);res.json({ok:true,count:arr.length,data:arr});});
app.get("/api/depth",(q,res)=>{const seg=q.query.segment||"NSE_FNO",sec=String(q.query.securityId||"");res.json({ok:true,data:ticks.get(key(seg,sec))?.depth||[]});});
app.post("/api/index",async(req,res)=>{const k=String(req.body?.index||"NIFTY").toUpperCase();if(!INDEXES[k])return res.status(400).json({ok:false,error:`Unknown index. Use ${Object.keys(INDEXES).join(", ")}`});state.indexKey=k;state.expiry=null;state.chain={updatedAt:null,expiry:null,rows:[],atm:null,maxPain:null};state.analytics=null;await refreshChain();broadcast({type:"state",state:publicState()});res.json({ok:true,state:publicState()});});
app.post("/api/expiry",async(req,res)=>{try{const dates=await getExpiryList();res.json({ok:true,dates,selected:state.expiry});}catch(e){res.status(500).json({ok:false,error:e.message});}});
app.post("/api/expiry/select",async(req,res)=>{const x=String(req.body?.expiry||"");if(!x)return res.status(400).json({ok:false,error:"expiry required"});state.expiry=x;await refreshChain();res.json({ok:true,state:publicState()});});
app.post("/api/subscribe",(req,res)=>{const list=Array.isArray(req.body?.instruments)?req.body.instruments:[];const clean=list.map(x=>({ExchangeSegment:String(x.ExchangeSegment||x.exchangeSegment),SecurityId:String(x.SecurityId||x.securityId)})).filter(x=>x.ExchangeSegment&&x.SecurityId);subscribe(dhanWs,clean,21);state.subscriptions=[...new Map([...state.subscriptions,...clean].map(x=>[key(x.ExchangeSegment,x.SecurityId),x])).values()];res.json({ok:true,count:state.subscriptions.length});});
app.post("/api/push/subscribe",(req,res)=>{const s=req.body;if(!s?.endpoint)return res.status(400).json({ok:false,error:"Invalid subscription"});pushSubscriptions.set(s.endpoint,s);res.json({ok:true});});
app.post("/api/push/test",async(_q,res)=>{let sent=0;for(const [k,s] of pushSubscriptions){try{await webpush.sendNotification(s,JSON.stringify({title:"Bharati V4 Test",body:"Live backend push is working."}));sent++;}catch(e){if([404,410].includes(e.statusCode))pushSubscriptions.delete(k);}}res.json({ok:true,devices:pushSubscriptions.size,sent});});

const server=http.createServer(app);const wss=new WebSocketServer({server,path:"/ws"});
wss.on("connection",ws=>{clients.add(ws);ws.send(JSON.stringify({type:"hello",version:state.version,features:["tick","quote","oi","volume","depth","chain","analytics","oi-window-delta","l20","pwa-context"]}));ws.send(JSON.stringify({type:"state",state:publicState()}));ws.on("message",async raw=>{try{const m=JSON.parse(raw.toString());if(m.action==="subscribe"){const idx=String(m.underlying||state.indexKey).toUpperCase();if(INDEXES[idx]&&idx!==state.indexKey){state.indexKey=idx;state.expiry=null;await refreshChain();}if(m.expiry&&String(m.expiry)!==String(state.expiry)){state.expiry=String(m.expiry);await refreshChain();}if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify({type:"pwaContext",data:pwaContext()}));}}catch(e){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify({type:"error",message:String(e.message||e)}));}});ws.on("close",()=>clients.delete(ws));});

async function boot(){
  try{await loadInstrumentMaster();}catch(e){log("Instrument master:",e.message);}
  try{await getExpiryList();await refreshChain();}catch(e){log("Chain startup:",e.message);}
  connectFeed();connectDepth();
  chainTimer=setInterval(refreshChain,3200);
  instrumentRefreshTimer=setInterval(async()=>{try{await loadInstrumentMaster();}catch(e){log("Instrument refresh:",e.message);}},6*3600_000);
}
server.listen(PORT,"0.0.0.0",()=>{log(`Bharati Unique Backend V4 listening on 0.0.0.0:${PORT}`);boot();});
