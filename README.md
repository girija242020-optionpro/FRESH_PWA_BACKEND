# RSI Sequence — ONE Render Backend

One service handles Dhan authentication, live market feed, 20-level depth,
option-chain/expiry APIs, normalized WebSocket/REST data, and Web Push alarms.

Render:
- Root Directory: blank
- Build: npm install
- Start: npm start

Use either DHAN_ACCESS_TOKEN, or DHAN_CLIENT_ID + DHAN_PIN + DHAN_TOTP_SECRET.
Never put Dhan secrets in the PWA.

The PWA uses only this backend URL.
