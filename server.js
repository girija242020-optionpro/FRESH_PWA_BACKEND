
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";
import webpush from "web-push";
import { authenticator } from "otplib";

dotenv.config();

const PORT=Number(process.env.PORT||10000);
const HOST=process.env.HOST||"0.0.0.0";
const CLIENT_ID=process.env.DHAN_CLIENT_ID||"";
const STATIC_TOKEN=process.env.DHAN_ACCESS_TOKEN||"";
const PIN=process.env.DHAN_PIN||"";
const TOTP=process.env.DHAN_TOTP_SECRET||"";
const ORIGIN=process.env.CORS_ORIGIN||"*";
const STALE=Number(process.env.STALE_AFTER_MS||5000);
const MAX_SUB=Number(process.env.MAX_SUBSCRIPTIONS||5000);
const BUFFER=Number(process.env.TICK_BUFFER_SIZE||10000);
let token=STATIC_TOKEN, tokenExpiry=0;

const app=express();
app.use(cors({origin:ORIGIN==="*" ? true : ORIGIN.split(",").map(x=>x.trim())}));
app.use(express.json({limit:"2mb"}));

const S={
  started:Date.now(), live:"DISCONNECTED", depth:"DISCONNECTED",
  lastPacket:0,lastDepth:0,ticks:0,packets:0,reconnects:0,depthReconnects:0,
  subs:new Map(),depthSubs:new Map(),ticks:new Map(),candles:new Map(),depthData:new Map(),clients:new Set()
};

const pushSubs=new Map();
if(process.env.VAPID_PUBLIC_KEY&&process.env.VAPID_PRIVATE_KEY&&process.env.VAPID_SUBJECT)
  webpush.setVapidDetails(process.env.VAPID_SUBJECT,process.env.VAPID_PUBLIC_KEY,process.env.VAPID_PRIVATE_KEY);

function authReady(){return !!(CLIENT_ID&&(token||(PIN&&TOTP)))}
async function getToken(force=false){
  if(STATIC_TOKEN){token=STATIC_TOKEN;return token}
  if(!CLIENT_ID||!PIN||!TOTP) throw Error("Dhan credentials incomplete");
  if(!force&&token&&tokenExpiry>Date.now()+120000)return token;
  const code=authenticator.generate(TOTP);
  const u=`https://auth.dhan.co/app/generateAccessToken?dhanClientId=${encodeURIComponent(CLIENT_ID)}&pin=${encodeURIComponent(PIN)}&totp=${encodeURIComponent(code)}`;
  const r=await fetch(u,{method:"POST"}); const t=await r.text(); let j;
  try{j=JSON.parse(t)}catch{j={raw:t}}
  if(!r.ok||!j.accessToken)throw Error(`Dhan auth ${r.status}: ${JSON.stringify(j)}`);
  token=j.accessToken; tokenExpiry=j.expiryTime?new Date(j.expiryTime).getTime():Date.now()+23*3600000;
  return token;
}
async function dhan(path,opts={}){
  await getToken(false);
  const go=()=>fetch(`https://api.dhan.co/v2${path}`,{...opts,headers:{"Content-Type":"application/json","access-token":token,"client-id":CLIENT_ID,...(opts.headers||{})}});
  let r=await go();
  if(r.status===401||r.status===403){await getToken(true);r=await go()}
  const t=await r.text();let j;try{j=JSON.parse(t)}catch{j={raw:t}}
  if(!r.ok)throw Error(`Dhan ${r.status}: ${JSON.stringify(j)}`);
  return j;
}
function pub(){
  const fresh=!!S.lastPacket&&(Date.now()-S.lastPacket<=STALE);
  return {ok:authReady(),marketLive:fresh,liveStatus:S.live,depthStatus:S.depth,lastPacketAt:S.lastPacket,
    lastDepthAt:S.lastDepth,ticks:S.ticks,packets:S.packets,subscriptions:S.subs.size,
    depthSubscriptions:S.depthSubs.size,pushConfigured:!!(process.env.VAPID_PUBLIC_KEY&&process.env.VAPID_PRIVATE_KEY&&process.env.VAPID_SUBJECT),
    pushSubscriptions:pushSubs.size,uptimeSec:Math.floor((Date.now()-S.started)/1000)};
}
function send(type,data){const m=JSON.stringify({type,ts:Date.now(),data});for(const c of S.clients)if(c.readyState===1)try{c.send(m)}catch{}}
function remember(t){
  const id=String(t.securityId);let a=S.ticks.get(id);if(!a){a=[];S.ticks.set(id,a)}
  a.push(t);if(a.length>BUFFER)a.splice(0,a.length-BUFFER);S.ticks++;
  const bucket=Math.floor(t.ts/60000)*60000;let c=S.candles.get(id);if(!c){c=[];S.candles.set(id,c)}
  let x=c[c.length-1];
  if(!x||x.ts!==bucket){x={ts:bucket,open:t.ltp,high:t.ltp,low:t.ltp,close:t.ltp,volume:t.volume??0,oi:t.oi??null};c.push(x);if(c.length>5000)c.shift()}
  else{x.high=Math.max(x.high,t.ltp);x.low=Math.min(x.low,t.ltp);x.close=t.ltp;x.volume=t.volume??x.volume;x.oi=t.oi??x.oi}
}
function parseFull(buf){
  // Dhan header: byte 0=response code, bytes 1-2 length, byte 3 segment, bytes 4-7 security id.
  if(buf.length<12)return null;
  const code=buf.readUInt8(0),id=String(buf.readInt32LE(4));
  if(code!==8&&code!==2&&code!==4&&code!==6)return null;
  const t={securityId:id,ltp:buf.readFloatLE(8),ts:Date.now()};
  if(code===8&&buf.length>=163){
    t.ltq=buf.readInt16LE(12);t.ltt=buf.readInt32LE(14);t.atp=buf.readFloatLE(18);
    t.volume=buf.readInt32LE(22);t.totalSellQty=buf.readInt32LE(26);t.totalBuyQty=buf.readInt32LE(30);
    t.oi=buf.readInt32LE(34);t.highOi=buf.readInt32LE(38);t.lowOi=buf.readInt32LE(42);
    t.day={open:buf.readFloatLE(46),close:buf.readFloatLE(50),high:buf.readFloatLE(54),low:buf.readFloatLE(58)};
    t.topDepth=[];let o=62;for(let i=0;i<5&&o+20<=buf.length;i++,o+=20)t.topDepth.push({
      bidQty:buf.readInt32LE(o),askQty:buf.readInt32LE(o+4),bidOrders:buf.readInt16LE(o+8),askOrders:buf.readInt16LE(o+10),
      bidPrice:buf.readFloatLE(o+12),askPrice:buf.readFloatLE(o+16)
    });
  }
  return Number.isFinite(t.ltp)?t:null;
}
function parseDepth(buf){
  const out=[];let o=0;
  while(o+12<=buf.length){
    const len=buf.readInt16LE(o);if(len<12||o+len>buf.length)break;
    const code=buf.readUInt8(o),id=String(buf.readInt32LE(o+4));
    if((code===41||code===51)&&len>=332){
      const side=code===41?"bid":"ask",levels=[];let p=o+12;
      for(let i=0;i<20&&p+16<=o+len;i++,p+=16)levels.push({price:buf.readDoubleLE(p),quantity:buf.readUInt32LE(p+8),orders:buf.readUInt32LE(p+12)});
      out.push({securityId:id,side,levels,ts:Date.now()});
    }
    o+=len;
  }
  return out;
}
function liveURL(){return `wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(token)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`}
function depthURL(){return `wss://depth-api-feed.dhan.co/twentydepth?token=${encodeURIComponent(token)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`}
function subKey(x){return `${x.exchangeSegment}:${x.securityId}`}
function sendLiveSubs(){
  if(!liveWS||liveWS.readyState!==1)return;const a=[...S.subs.values()];
  for(let i=0;i<a.length;i+=100){const x=a.slice(i,i+100);liveWS.send(JSON.stringify({RequestCode:21,InstrumentCount:x.length,InstrumentList:x.map(v=>({ExchangeSegment:v.exchangeSegment,SecurityId:String(v.securityId)}))}))}
}
function sendDepthSubs(){
  if(!depthWS||depthWS.readyState!==1)return;const a=[...S.depthSubs.values()];
  for(let i=0;i<a.length;i+=50){const x=a.slice(i,i+50);depthWS.send(JSON.stringify({RequestCode:23,InstrumentCount:x.length,InstrumentList:x.map(v=>({ExchangeSegment:v.exchangeSegment,SecurityId:String(v.securityId)}))}))}
}
let liveWS=null,depthWS=null,liveTimer=null,depthTimer=null;
async function connectLive(){
  if(liveWS||!authReady())return;
  try{await getToken(false)}catch(e){S.live="AUTH_ERROR";send("status",pub());setTimeout(connectLive,10000);return}
  S.live="CONNECTING";send("status",pub());liveWS=new WebSocket(liveURL());
  liveWS.on("open",()=>{S.live="CONNECTED";sendLiveSubs();send("status",pub())});
  liveWS.on("message",raw=>{S.packets++;const t=parseFull(Buffer.from(raw));if(t){S.lastPacket=Date.now();remember(t);send("tick",t)}});
  liveWS.on("close",()=>{liveWS=null;S.live="DISCONNECTED";S.reconnects++;send("status",pub());clearTimeout(liveTimer);liveTimer=setTimeout(connectLive,2000)});
  liveWS.on("error",()=>{S.live="ERROR";try{liveWS.close()}catch{}});
}
async function connectDepth(){
  if(depthWS||!authReady()||!S.depthSubs.size)return;
  try{await getToken(false)}catch{return}
  S.depth="CONNECTING";send("status",pub());depthWS=new WebSocket(depthURL());
  depthWS.on("open",()=>{S.depth="CONNECTED";sendDepthSubs();send("status",pub())});
  depthWS.on("message",raw=>{S.lastDepth=Date.now();for(const d of parseDepth(Buffer.from(raw))){let x=S.depthData.get(d.securityId)||{bid:[],ask:[]};x[d.side]=d.levels;x.updatedAt=Date.now();S.depthData.set(d.securityId,x);send("depth20",{securityId:d.securityId,...x})}});
  depthWS.on("close",()=>{depthWS=null;S.depth="DISCONNECTED";S.depthReconnects++;send("status",pub());clearTimeout(depthTimer);depthTimer=setTimeout(connectDepth,3000)});
  depthWS.on("error",()=>{S.depth="ERROR";try{depthWS.close()}catch{}});
}

app.get("/",(q,r)=>r.json({service:"RSI Sequence ONE Backend",status:"ONLINE",...pub()}));
app.get("/api/health",(q,r)=>r.json({ok:true,...pub()}));
app.get("/api/state",(q,r)=>r.json(pub()));
app.get("/api/ticks",(q,r)=>{const id=String(q.query.securityId||"");const lim=Math.min(Number(q.query.limit||200),BUFFER);r.json({securityId:id,data:(S.ticks.get(id)||[]).slice(-lim)})});
app.get("/api/candles",(q,r)=>{const id=String(q.query.securityId||"");const m=Math.max(1,Number(String(q.query.timeframe||"1m").replace("m",""))||1);const lim=Math.min(Number(q.query.limit||500),5000);const out=[];for(const c of(S.candles.get(id)||[])){const ts=Math.floor(c.ts/(m*60000))*m*60000;let x=out[out.length-1];if(!x||x.ts!==ts){x={ts,open:c.open,high:c.high,low:c.low,close:c.close,volume:c.volume,oi:c.oi};out.push(x)}else{x.high=Math.max(x.high,c.high);x.low=Math.min(x.low,c.low);x.close=c.close;x.volume=c.volume;x.oi=c.oi}}r.json({securityId:id,timeframe:`${m}m`,data:out.slice(-lim)})});
app.get("/api/depth20/:securityId",(q,r)=>r.json({securityId:String(q.params.securityId),data:S.depthData.get(String(q.params.securityId))||null}));
app.post("/api/subscribe",(q,r)=>{
  const a=Array.isArray(q.body?.instruments)?q.body.instruments:[],d=Array.isArray(q.body?.depth20)?q.body.depth20:[];
  if(a.length+d.length>MAX_SUB)return r.status(400).json({error:"Subscription limit exceeded"});
  for(const x of a)if(x?.exchangeSegment&&x?.securityId!=null)S.subs.set(subKey(x),{exchangeSegment:x.exchangeSegment,securityId:String(x.securityId)});
  for(const x of d)if(x?.exchangeSegment&&x?.securityId!=null)S.depthSubs.set(subKey(x),{exchangeSegment:x.exchangeSegment,securityId:String(x.securityId)});
  connectLive();connectDepth();sendLiveSubs();sendDepthSubs();r.json({ok:true,subscriptions:S.subs.size,depthSubscriptions:S.depthSubs.size})
});
app.get("/api/option-chain",async(q,r)=>{try{r.json(await dhan("/optionchain",{method:"POST",body:JSON.stringify({UnderlyingScrip:Number(q.query.underlyingScrip),UnderlyingSeg:String(q.query.underlyingSeg||"IDX_I"),Expiry:String(q.query.expiry||"")})}))}catch(e){r.status(502).json({error:e.message})}});
app.get("/api/expiry-list",async(q,r)=>{try{r.json(await dhan("/optionchain/expirylist",{method:"POST",body:JSON.stringify({UnderlyingScrip:Number(q.query.underlyingScrip),UnderlyingSeg:String(q.query.underlyingSeg||"IDX_I")})}))}catch(e){r.status(502).json({error:e.message})}});

app.get("/api/vapid-public-key",(q,r)=>{if(!process.env.VAPID_PUBLIC_KEY)return r.status(503).json({error:"VAPID not configured"});r.json({publicKey:process.env.VAPID_PUBLIC_KEY})});
app.post("/api/subscribe-push",(q,r)=>{if(!q.body?.endpoint)return r.status(400).json({error:"Invalid subscription"});pushSubs.set(q.body.endpoint,q.body);r.json({ok:true,subscriptions:pushSubs.size})});
async function doPush(body){
  if(!process.env.VAPID_PUBLIC_KEY||!process.env.VAPID_PRIVATE_KEY||!process.env.VAPID_SUBJECT)throw Error("VAPID not configured");
  let sent=0,failed=0;for(const [ep,s] of pushSubs){try{await webpush.sendNotification(s,JSON.stringify(body));sent++}catch(e){failed++;if(e.statusCode===404||e.statusCode===410)pushSubs.delete(ep)}}return{sent,failed,subscriptions:pushSubs.size}
}
app.post("/api/test-push",async(q,r)=>{try{r.json({ok:true,...await doPush({title:"Market Alert Test",body:"Push is working.",signal:"TEST",ts:Date.now()})})}catch(e){r.status(503).json({error:e.message})}});
app.post("/api/push",async(q,r)=>{try{r.json({ok:true,...await doPush({title:q.body?.title||"Market ENTRY",body:q.body?.body||"Signal detected.",side:q.body?.side||null,signal:q.body?.signal||"ENTRY",ts:Date.now()})})}catch(e){r.status(503).json({error:e.message})}});

const server=http.createServer(app),wss=new WebSocketServer({server,path:"/ws"});
wss.on("connection",ws=>{if(S.clients.size>=MAX_CLIENTS){ws.close(1013);return}S.clients.add(ws);ws.send(JSON.stringify({type:"status",ts:Date.now(),data:pub()}));ws.on("close",()=>S.clients.delete(ws));ws.on("error",()=>S.clients.delete(ws))});
server.listen(PORT,HOST,()=>{
  console.log(`RSI Sequence ONE Backend listening on ${HOST}:${PORT}`);
  if(process.env.AUTO_SUBSCRIBE_INDICES!=="false"){
    S.subs.set("IDX_I:"+process.env.NIFTY_SECURITY_ID,{exchangeSegment:"IDX_I",securityId:process.env.NIFTY_SECURITY_ID||"13"});
    S.subs.set("IDX_I:"+process.env.SENSEX_SECURITY_ID,{exchangeSegment:"IDX_I",securityId:process.env.SENSEX_SECURITY_ID||"51"});
  }
  if(process.env.ENABLE_LIVE_FEED!=="false")connectLive();
});
