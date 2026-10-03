// Candle store built from live ticks.
// Spot index gives OHLC; the index future gives volume + buy/sell split (same volume TradingView/Dhan charts show on spot).
class Candles {
  constructor(tfMs = 60000, max = 2000) { this.tf = tfMs; this.max = max; this.arr = []; }
  bucket(ms) { return Math.floor(ms / this.tf) * this.tf; }
  _cur(ms, px) {
    const b = this.bucket(ms); let c = this.arr[this.arr.length - 1];
    if (!c || c.t < b) {
      const o = px != null ? px : (c ? c.c : 0);
      c = { t: b, o, h: o, l: o, c: o, v: 0, bv: 0, sv: 0 };
      this.arr.push(c); if (this.arr.length > this.max) this.arr.shift();
    }
    return c.t === b ? c : null; // late tick for an older bucket is ignored
  }
  tick(px, ms) {
    const c = this._cur(ms, px); if (!c) return null;
    if (px > c.h) c.h = px; if (px < c.l) c.l = px; c.c = px; return c;
  }
  volume(qty, side, ms) { // side: 1 buy, -1 sell, 0 unknown
    const c = this._cur(ms, null); if (!c || !(qty > 0)) return null;
    c.v += qty; if (side > 0) c.bv += qty; else if (side < 0) c.sv += qty; return c;
  }
  // REST history heals closed candles; the current live bucket keeps its live volume / order-flow split.
  merge(rest) { // rest: [{t(sec),o,h,l,c,v}]
    const liveT = this.arr.length ? this.arr[this.arr.length - 1].t : Infinity;
    const map = new Map(this.arr.map(x => [x.t, x]));
    for (const r of rest) {
      const t = r.t * 1000, ex = map.get(t);
      if (ex) {
        if (t === liveT) { if (r.v > ex.v) ex.v = r.v; continue; }
        ex.o = r.o; ex.h = r.h; ex.l = r.l; ex.c = r.c; if (r.v > 0) ex.v = r.v;
      } else map.set(t, { t, o: r.o, h: r.h, l: r.l, c: r.c, v: r.v || 0, bv: 0, sv: 0 });
    }
    this.arr = [...map.values()].sort((a, b) => a.t - b.t).slice(-this.max);
  }
  fmt(c) { return { t: Math.floor(c.t / 1000), o: c.o, h: c.h, l: c.l, c: c.c, v: c.v, bv: c.bv, sv: c.sv }; }
  out(n = 1500) { return this.arr.slice(-n).map(c => this.fmt(c)); }
  last() { const c = this.arr[this.arr.length - 1]; return c ? this.fmt(c) : null; }
}

// Trade-side classification for a futures print: quote-rule first, tick-rule fallback
function classify(ltp, bid, ask, prevLtp, lastSide) {
  if (ask > 0 && ltp >= ask) return 1;
  if (bid > 0 && ltp <= bid) return -1;
  if (prevLtp != null) { if (ltp > prevLtp) return 1; if (ltp < prevLtp) return -1; }
  return lastSide || 0;
}
module.exports = { Candles, classify };
