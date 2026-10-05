# Bharati Unique Backend V4 — Ultimate Market Data Gateway

This backend is intentionally frontend-agnostic. It is a market-data gateway for DhanHQ, exposing a stable REST + WebSocket interface to any frontend.

## Data exposed

- Tick-by-tick LTP/LTT
- Last traded quantity
- Day OHLC
- ATP/VWAP-like average price supplied by Dhan
- Volume + change in volume
- Open Interest + change in OI
- OI day high/low
- Bid/ask totals
- 5-level market depth from FULL packets
- Selected 20 instruments on Dhan 20-level depth connection
- Full option chain for selected expiry
- Dhan Greeks: Delta, Gamma, Vega, Theta
- Dhan IV
- Model-derived hidden Greeks: Vanna, Vomma, Charm, Color, Speed, Zomma
- OI walls / change-OI walls
- PCR / volume PCR
- Max Pain
- ATM and ATM IV
- IV smile snapshot
- Gamma-exposure / dealer-hedge-pressure proxy
- Historical candles
- Dynamic Dhan instrument master
- Push notifications (optional)

## Important distinction

The hidden Greeks and dealer-hedge-pressure values are MODEL-DERIVED. They are not proprietary exchange/dealer positioning data. Dealer hedge pressure is a proxy built from option gamma/OI assumptions and must not be presented as actual dealer inventory.

The backend does not place orders.

## WebSocket

Connect to:

`wss://YOUR-HOST/ws`

Messages include `hello`, `state`, `optionChain`, `tick`, and `error`.

## REST

- `GET /api/health`
- `GET /api/config`
- `GET /api/state`
- `GET /api/ticks`
- `GET /api/tick?segment=NSE_FNO&securityId=...`
- `GET /api/option-chain`
- `GET /api/analytics`
- `GET /api/history?segment=IDX_I&securityId=13`
- `GET /api/depth?segment=NSE_FNO&securityId=...`
- `GET /api/instruments?search=NIFTY`
- `POST /api/index` body `{ "index":"NIFTY" }`
- `POST /api/expiry` 
- `POST /api/expiry/select` body `{ "expiry":"YYYY-MM-DD" }`
- `POST /api/subscribe` body `{ "instruments":[{"ExchangeSegment":"NSE_FNO","SecurityId":"..."}] }`

## Architecture

Dhan -> V4 gateway -> any frontend.

The frontend is not required to know Dhan credentials, TOTP, binary packet format, option-chain rate limits, or WebSocket reconnect logic.


## RSI OI PRO compatibility layer (V4.1)

This upgrade preserves the original V4 gateway and adds an additive interface for the RSI OI PRO PWA. The original source is preserved as `server.v4-original.js`; the deployed `server.js` contains the compatibility additions.

### Added PWA data

- Windowed OI/ΔOI snapshots: 5m, 10m, 15m, 30m, 1h, 2h, 3h, 1d
- ATM ± configurable strike range OI profile
- Live option LTP, OI, ΔOI, volume, bid/ask
- Primary option L20 aggregate and raw depth
- Underlying 1-minute candles
- Backend-calculated RSI(14), RSI SMA(9), EMA(24), DEMA(24), Hull(60), VWAP and volume average
- Market/session status and stale-data state
- `/api/market-context` unified PWA context
- `/api/oi-profile` windowed OI profile
- `/api/l20` depth access
- WebSocket `pwaContext` messages
- WebSocket subscribe handling for index/expiry changes

### PWA endpoints

- `GET /api/market-context`
- `GET /api/oi-profile?window=5m&range=10`
- `GET /api/option-chain?window=5m&range=10`
- `GET /api/l20?segment=NSE_FNO&securityId=...`
- `WS /ws` with `{ "action": "subscribe", "underlying": "NIFTY", "expiry": "YYYY-MM-DD", "depth": 20 }`

The original Dhan authentication, instrument master, option-chain refresh, tick feed, and 20-depth connection remain in the gateway. The PWA never receives Dhan credentials.

### Important

The backend does not invent India VIX. If a VIX instrument/security ID is not configured and subscribed, the PWA should treat VIX as unavailable rather than fabricate a value.
