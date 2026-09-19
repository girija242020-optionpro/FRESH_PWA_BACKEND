# Fresh RSI Market Data Backend

This backend is a DhanHQ data gateway only. It keeps Dhan credentials on the server and exposes normalized market data to the PWA.

## Render
Build command:
    npm install
Start command:
    npm start

Set environment variables from `.env.example`.

## Main endpoints
GET  /api/health
GET  /api/state
GET  /api/ticks?securityId=13&limit=200
GET  /api/candles?securityId=13&timeframe=1m&limit=500
GET  /api/instruments?search=NIFTY
GET  /api/option-chain?underlyingScrip=13&underlyingSeg=IDX_I&expiry=YYYY-MM-DD
GET  /api/expiry-list?underlyingScrip=13&underlyingSeg=IDX_I
POST /api/subscribe
WS   /ws

POST /api/subscribe example:
{
  "instruments": [
    {"exchangeSegment":"IDX_I","securityId":"13"},
    {"exchangeSegment":"IDX_I","securityId":"51"}
  ],
  "depth20": [
    {"exchangeSegment":"NSE_FNO","securityId":"12345"}
  ]
}

The server never sends DHAN_ACCESS_TOKEN to the browser.
