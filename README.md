# SUDHIR RSI PRO — FLAT BACKEND

All backend source files are at the backend root. No `lib/`, `scripts/`, or `test/` subfolders are required.

Files:
- `server.js` — Express + WebSocket server
- `dhan.js` — Dhan REST/security-master helpers
- `feed.js` — Dhan market/depth WebSocket feeds
- `parse.js` — binary feed parsers
- `candles.js` — candle/order-flow store
- `vapid.js` — VAPID generator
- `gen-vapid.js` — CLI key generator
- `test.js` — parser/candle/VAPID tests
- `package.json` — Render/Node configuration

Run:
1. `npm install`
2. set `DHAN_CLIENT_ID` and `DHAN_ACCESS_TOKEN`
3. optionally set VAPID/PUSH environment variables
4. `npm test`
5. `npm start`

No trading strategy logic is added to the backend; the PWA remains the strategy/UI layer.
