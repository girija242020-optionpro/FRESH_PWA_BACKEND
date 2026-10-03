// Generates a fresh VAPID key pair (P-256) using only Node's crypto.
const { generateKeyPairSync } = require('crypto');
function generateVapid() {
  const { privateKey } = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const j = privateKey.export({ format: 'jwk' });
  const x = Buffer.from(j.x, 'base64url'), y = Buffer.from(j.y, 'base64url');
  return { publicKey: Buffer.concat([Buffer.from([4]), x, y]).toString('base64url'), privateKey: j.d };
}
module.exports = { generateVapid };
