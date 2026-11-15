'use strict';

// Fixtures for peeroxide-dht's port of `nat-sampler`, the package with which
// `dht-rpc` works out the address other nodes see a node at: sequences of
// samples, and where the sampler has settled after each one.

const fs = require('fs');
const path = require('path');

// The copy of the package that dht-rpc uses.
const dhtRpcDir = path.dirname(require.resolve('dht-rpc/package.json'));
const samplerDir = path.dirname(
  require.resolve('nat-sampler/package.json', { paths: [dhtRpcDir] })
);
const NatSampler = require(samplerDir);
const samplerPkg = require(path.join(samplerDir, 'package.json'));

const HOSTS = ['203.0.113.5', '198.51.100.7', '192.0.2.9'];
const PORTS = [4000, 4001, 5000];
const SEQUENCES = 40;
const STEPS = 40;

// A fixed pseudo-random sequence (MINSTD), so every run writes the same
// fixtures.
let x = 12345;
const next = () => (x = (x * 48271) % 2147483647);

// A sample is written as the index of its host and of its port, "01". Where
// the sampler has settled is written the same way, with "-" for port 0 (a
// host alone) and "--" for nothing.
const index = (host, port) =>
  host === null ? '--' : `${HOSTS.indexOf(host)}${port === 0 ? '-' : PORTS.indexOf(port)}`;

const sequences = [];
for (let s = 0; s < SEQUENCES; s++) {
  // Some sequences keep to one host, some to two ports, and some nearly
  // always repeat one address, so that the sampler settles in every way it
  // can, and the disagreement it tolerates is reached.
  const hosts = s % 3 === 0 ? 1 : 3;
  const ports = s % 2 === 0 ? 2 : 3;
  const biased = s % 4 === 1;

  const sampler = new NatSampler();
  const samples = [];
  const settled = [];
  for (let i = 0; i < STEPS; i++) {
    let h = 0;
    let p = 0;
    if (!biased || next() % 8 === 0) {
      h = next() % hosts;
      p = next() % ports;
    }
    sampler.add(HOSTS[h], PORTS[p]);
    samples.push(index(HOSTS[h], PORTS[p]));
    settled.push(index(sampler.host, sampler.port));
  }
  sequences.push({ samples: samples.join(' '), settled: settled.join(' ') });
}

const outDir = path.resolve(__dirname, '../interop');
const outFile = path.join(outDir, 'nat-sampler-fixtures.json');

fs.mkdirSync(outDir, { recursive: true });

const output = {
  generator: 'generate-nat-sampler-golden.js',
  nat_sampler_version: samplerPkg.version,
  hosts: HOSTS,
  ports: PORTS,
  sequences,
};

fs.writeFileSync(outFile, JSON.stringify(output, null, 2) + '\n');
console.log(`Written ${sequences.length} sequences to ${outFile}`);
