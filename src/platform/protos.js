'use strict';

/** Every API the platform serves or calls, loaded once with the wire options every team's code relies on. */
const fs = require('node:fs');
const path = require('node:path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const root = path.join(__dirname, '..', '..', 'proto');

function protos(directory) {
  return fs.readdirSync(directory, { withFileTypes: true })
    .sort((a, b) => a.name.localeCompare(b.name, 'en'))
    .flatMap((entry) => {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) return protos(file);
      return entry.name.endsWith('.proto') ? [path.relative(root, file)] : [];
    });
}

// Fields keep the names the protobuf files give them, 64-bit integers are strings, every field a message lacks has its
// default, and each oneof has a property that names the member it holds.
const definition = protoLoader.loadSync(protos(root), {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [root],
});

module.exports = grpc.loadPackageDefinition(definition);
