# RSI OI PRO + Bharati V4 Integration

Deploy this folder as the Render backend. It keeps the existing V4 gateway behavior and adds the RSI OI PRO compatibility layer.

## Data path

Dhan live feed + Dhan option chain + 20-depth -> V4 gateway -> REST/WebSocket -> RSI OI PRO PWA.

## PWA connection

Backend URL:
`https://YOUR-RENDER-HOST`

WebSocket URL:
`wss://YOUR-RENDER-HOST/ws`

The PWA will request:
- `/api/market-context`
- `/api/oi-profile?window=5m&range=10`
- `/api/state`

The PWA also sends a WebSocket subscribe message when the selected index/expiry changes.

## Windowed ΔOI

The compatibility layer keeps lightweight 30-second OI snapshots for up to 24 hours. ΔOI for the selected window is calculated as:

`current OI - OI at the snapshot closest to the start of the selected window`

This is separate from Dhan's day/session change-OI and is intended for the PWA's 5m/10m/15m/30m/1h/2h/3h/1d controls.

## L20

The existing 20-depth feed remains active. `/api/l20` exposes the primary option's depth and aggregate bid/ask quantities. Raw `/api/depth` remains available.

## Safety / trading behavior

The backend is market-data only and does not place orders. The PWA's CALL/PUT state engine remains the decision layer. OI/ΔOI is confirmation, not a guaranteed prediction.
