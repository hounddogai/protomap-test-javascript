'use strict';

/** Adapt the promise-returning operations of the domains to grpc-js's callback and stream contracts. */
const { toServiceError } = require('./errors');

function unary(action) {
  return (call, callback) => {
    action(call.request).then((reply) => callback(null, reply), (error) => callback(toServiceError(error)));
  };
}

function serverStream(action) {
  return (call) => {
    action(call.request).then((reply) => {
      if (!call.cancelled) call.write(reply);
      call.end();
    }, (error) => call.emit('error', toServiceError(error)));
  };
}

function clientStream(action, empty) {
  return (call, callback) => {
    let reply = empty();
    let chain = Promise.resolve();
    call.on('data', (request) => {
      chain = chain.then(async () => { reply = await action(request); });
    });
    call.on('end', () => {
      chain.then(() => callback(null, reply), (error) => callback(toServiceError(error)));
    });
    call.on('error', (error) => callback(toServiceError(error)));
  };
}

function duplex(action) {
  return (call) => {
    let chain = Promise.resolve();
    call.on('data', (request) => {
      chain = chain.then(async () => {
        const reply = await action(request);
        if (!call.cancelled) call.write(reply);
      });
    });
    call.on('end', () => {
      chain.then(() => call.end(), (error) => call.emit('error', toServiceError(error)));
    });
  };
}

module.exports = { unary, serverStream, clientStream, duplex };
