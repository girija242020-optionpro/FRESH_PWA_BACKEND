const assert = require('assert');
const { parseFeed, parseDepth20 } = require('./parse');
const { Candles, classify } = require('./candles');
const { generateVapid } = require('./vapid');

// full packet (162 bytes)
const b = Buffer.alloc(162); b.writeUInt8(8, 0); b.writeInt16LE(162, 1); b.writeUInt8(2, 3); b.writeInt32LE(35000, 4);
b.writeFloatLE(22620.5, 8); b.writeInt16LE(75, 12); b.writeInt32LE(123456, 22); b.writeInt32LE(1000, 26); b.writeInt32LE(900, 30);
for (let i = 0; i < 5; i++) { const o = 62 + i * 20; b.writeInt32LE(100 + i, o); b.writeInt32LE(200 + i, o + 4); b.writeFloatLE(22620 - i, o + 12); b.writeFloatLE(22620.5 + i, o + 16); }
const p = parseFeed(b); assert.equal(p.type, 'full'); assert.equal(p.seg, 'NSE_FNO'); assert.equal(p.id, 35000);
assert.equal(p.volume, 123456); assert.equal(p.depth.length, 5); assert.equal(p.depth[0].bp, 22620); assert.equal(p.depth[0].ap, 22620.5);
// ticker on index
const t = Buffer.alloc(16); t.writeUInt8(2, 0); t.writeUInt8(0, 3); t.writeInt32LE(13, 4); t.writeFloatLE(22600.25, 8);
assert.equal(parseFeed(t).ltp, 22600.25); assert.equal(parseFeed(t).seg, 'IDX_I');
// depth20: stacked bid+ask
function d20(code) { const m = Buffer.alloc(12 + 320); m.writeInt16LE(332, 0); m.writeUInt8(code, 2); m.writeUInt8(2, 3); m.writeInt32LE(35000, 4);
  for (let i = 0; i < 20; i++) { m.writeDoubleLE(22620 + (code === 41 ? -i : i) * 0.5, 12 + i * 16); m.writeUInt32LE(1000 + i, 20 + i * 16); m.writeUInt32LE(10, 24 + i * 16); } return m; }
const dd = parseDepth20(Buffer.concat([d20(41), d20(51)])); assert.equal(dd.length, 2); assert.equal(dd[0].side, 'bid'); assert.equal(dd[1].levels.length, 20); assert.equal(dd[0].levels[1][1], 1001);
// candles
const c = new Candles(60000, 100); const m0 = Math.floor(Date.now() / 60000) * 60000;
c.tick(100, m0 + 1000); c.tick(105, m0 + 2000); c.tick(98, m0 + 3000); c.volume(50, 1, m0 + 4000); c.volume(30, -1, m0 + 5000);
let l = c.last(); assert.equal(l.o, 100); assert.equal(l.h, 105); assert.equal(l.l, 98); assert.equal(l.c, 98); assert.equal(l.v, 80); assert.equal(l.bv, 50); assert.equal(l.sv, 30);
c.tick(99, m0 + 61000); assert.equal(c.arr.length, 2); assert.equal(c.arr[1].o, 99);
c.merge([{ t: m0 / 1000 - 60, o: 1, h: 2, l: 0.5, c: 1.5, v: 777 }, { t: m0 / 1000, o: 100, h: 105, l: 98, c: 98, v: 70 }]);
assert.equal(c.arr.length, 3); assert.equal(c.arr[0].v, 777); assert.equal(c.arr[1].bv, 50);
assert.equal(classify(10, 9, 10, 9, 0), 1); assert.equal(classify(9, 9, 10, 10, 0), -1); assert.equal(classify(9.5, 9, 10, 9, 0), 1);
const v = generateVapid(); assert.equal(Buffer.from(v.publicKey, 'base64url').length, 65); assert.equal(Buffer.from(v.privateKey, 'base64url').length, 32);
console.log('backend tests OK');
