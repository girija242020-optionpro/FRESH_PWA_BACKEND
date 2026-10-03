// Dhan binary feed parsers (little endian). No dependencies.
const SEG = { 0: 'IDX_I', 1: 'NSE_EQ', 2: 'NSE_FNO', 3: 'NSE_CURRENCY', 4: 'BSE_EQ', 5: 'MCX_COMM', 7: 'BSE_CURRENCY', 8: 'BSE_FNO' };

// Market feed v2: 8-byte header [code u8][len i16][seg u8][secId i32] + payload
function parseFeed(b) {
  if (!b || b.length < 8) return null;
  const code = b.readUInt8(0), seg = SEG[b.readUInt8(3)] || String(b.readUInt8(3)), id = b.readInt32LE(4);
  const p = { code, seg, id };
  try {
    switch (code) {
      case 2: p.type = 'ticker'; p.ltp = b.readFloatLE(8); p.ltt = b.readInt32LE(12); break;
      case 4: p.type = 'quote'; p.ltp = b.readFloatLE(8); p.ltq = b.readInt16LE(12); p.ltt = b.readInt32LE(14);
        p.atp = b.readFloatLE(18); p.volume = b.readInt32LE(22); p.sellQty = b.readInt32LE(26); p.buyQty = b.readInt32LE(30);
        p.open = b.readFloatLE(34); p.close = b.readFloatLE(38); p.high = b.readFloatLE(42); p.low = b.readFloatLE(46); break;
      case 5: p.type = 'oi'; p.oi = b.readInt32LE(8); break;
      case 6: p.type = 'prev'; p.prevClose = b.readFloatLE(8); p.prevOi = b.readInt32LE(12); break;
      case 8: p.type = 'full'; p.ltp = b.readFloatLE(8); p.ltq = b.readInt16LE(12); p.ltt = b.readInt32LE(14);
        p.atp = b.readFloatLE(18); p.volume = b.readInt32LE(22); p.sellQty = b.readInt32LE(26); p.buyQty = b.readInt32LE(30);
        p.oi = b.readInt32LE(34); p.open = b.readFloatLE(46); p.close = b.readFloatLE(50); p.high = b.readFloatLE(54); p.low = b.readFloatLE(58);
        p.depth = [];
        for (let i = 0; i < 5; i++) {
          const o = 62 + i * 20; if (o + 20 > b.length) break;
          p.depth.push({ bq: b.readInt32LE(o), aq: b.readInt32LE(o + 4), bo: b.readInt16LE(o + 8), ao: b.readInt16LE(o + 10), bp: b.readFloatLE(o + 12), ap: b.readFloatLE(o + 16) });
        }
        break;
      case 50: p.type = 'disconnect'; p.reason = b.readInt16LE(8); break;
      default: p.type = 'unknown';
    }
  } catch (e) { return null; }
  return p;
}

// 20-level depth. Messages may be stacked in one frame.
// Header 12 bytes [len i16][code u8: 41 bid / 51 ask][seg u8][secId i32][seq u32], then 20 x {price f64, qty u32, orders u32}
function parseDepth20(b) {
  const out = []; let off = 0;
  while (off + 12 <= b.length) {
    const len = b.readInt16LE(off);
    if (len < 12 || off + len > b.length) break;
    const code = b.readUInt8(off + 2), seg = SEG[b.readUInt8(off + 3)] || '', id = b.readInt32LE(off + 4);
    if (code === 41 || code === 51) {
      const levels = []; let o = off + 12;
      while (o + 16 <= off + len && levels.length < 20) { levels.push([b.readDoubleLE(o), b.readUInt32LE(o + 8), b.readUInt32LE(o + 12)]); o += 16; }
      out.push({ side: code === 41 ? 'bid' : 'ask', seg, id, levels });
    }
    off += len;
  }
  return out;
}
module.exports = { parseFeed, parseDepth20, SEG };
