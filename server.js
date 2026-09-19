import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import webpush from "web-push";

dotenv.config();
const app = express();
const PORT = Number(process.env.PORT || 10000);
const HOST = process.env.HOST || "0.0.0.0";
const ORIGIN = process.env.CORS_ORIGIN || "*";
const subscriptions = new Map();

app.use(cors({ origin: ORIGIN === "*" ? true : ORIGIN.split(",").map(s=>s.trim()) }));
app.use(express.json({limit:"256kb"}));

const pushConfigured = Boolean(process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY && process.env.VAPID_SUBJECT);
if (pushConfigured) {
  webpush.setVapidDetails(process.env.VAPID_SUBJECT, process.env.VAPID_PUBLIC_KEY, process.env.VAPID_PRIVATE_KEY);
}

app.get("/", (req,res)=>res.json({
  service:"PWA Push Alarm Backend",
  status:"ONLINE",
  pushConfigured,
  subscriptions: subscriptions.size
}));
app.get("/api/health",(req,res)=>res.json({ok:true, pushConfigured, subscriptions:subscriptions.size, time:new Date().toISOString()}));
app.get("/api/status",(req,res)=>res.json({ok:true, service:"PWA Push Alarm Backend", pushConfigured, subscriptions:subscriptions.size}));
app.get("/api/vapid-public-key",(req,res)=>{
  if(!process.env.VAPID_PUBLIC_KEY) return res.status(503).json({error:"VAPID public key not configured"});
  res.json({publicKey:process.env.VAPID_PUBLIC_KEY});
});
app.post("/api/subscribe",(req,res)=>{
  if(!req.body?.endpoint) return res.status(400).json({error:"Invalid push subscription"});
  subscriptions.set(req.body.endpoint, req.body);
  res.json({ok:true, subscriptions:subscriptions.size});
});
app.delete("/api/subscribe",(req,res)=>{
  const endpoint = req.body?.endpoint;
  if(endpoint) subscriptions.delete(endpoint);
  res.json({ok:true, subscriptions:subscriptions.size});
});
app.post("/api/test-push", async (req,res)=>{
  if(!pushConfigured) return res.status(503).json({error:"Push not configured"});
  const payload = JSON.stringify({
    title:req.body?.title || "Market Alert Test",
    body:req.body?.body || "Push notification is working.",
    signal:req.body?.signal || "TEST",
    ts:Date.now()
  });
  let sent=0, failed=0;
  for(const [endpoint, sub] of subscriptions) {
    try { await webpush.sendNotification(sub,payload); sent++; }
    catch(e) {
      failed++;
      if(e.statusCode===404 || e.statusCode===410) subscriptions.delete(endpoint);
    }
  }
  res.json({ok:true,sent,failed,subscriptions:subscriptions.size});
});
app.post("/api/push", async (req,res)=>{
  if(!pushConfigured) return res.status(503).json({error:"Push not configured"});
  const payload = JSON.stringify({
    title:req.body?.title || "Market Signal",
    body:req.body?.body || "Signal detected.",
    signal:req.body?.signal || "ALERT",
    side:req.body?.side || null,
    ts:Date.now()
  });
  let sent=0, failed=0;
  for(const [endpoint, sub] of subscriptions) {
    try { await webpush.sendNotification(sub,payload); sent++; }
    catch(e) {
      failed++;
      if(e.statusCode===404 || e.statusCode===410) subscriptions.delete(endpoint);
    }
  }
  res.json({ok:true,sent,failed,subscriptions:subscriptions.size});
});
app.listen(PORT,HOST,()=>console.log(`Push alarm backend listening on ${HOST}:${PORT}`));
