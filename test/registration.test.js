'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const grpc = require('@grpc/grpc-js');
const clients = require('../src/platform/clients');
const { bus } = require('../src/platform/bus');
const { createServer } = require('../src/server/server');

test('every service registers a handler for each of its methods', () => {
  const registered = [];
  const addService = grpc.Server.prototype.addService;
  grpc.Server.prototype.addService = function record(definition, implementation) {
    for (const [name, method] of Object.entries(definition)) {
      const handler = implementation[name] ?? implementation[method.originalName];
      assert.equal(typeof handler, 'function', `${method.path} has no handler`);
      registered.push(method.path);
    }
    return addService.call(this, definition, implementation);
  };
  try {
    const server = createServer();
    server.forceShutdown();
  } finally {
    grpc.Server.prototype.addService = addService;
    bus.close();
    clients.close();
  }
  assert.equal(new Set(registered).size, registered.length);
  assert.ok(registered.length > 0);
});
