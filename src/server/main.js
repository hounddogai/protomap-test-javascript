'use strict';

const grpc = require('@grpc/grpc-js');
const clients = require('../platform/clients');
const database = require('../platform/database');
const { bus } = require('../platform/bus');
const { logger } = require('../platform/logger');
const { createServer } = require('./server');

const listen = process.env.GRPC_LISTEN ?? '127.0.0.1:8443';
const server = createServer();

async function close() {
  bus.close();
  clients.close();
  await database.close();
}

server.bindAsync(listen, grpc.ServerCredentials.createInsecure(), (error) => {
  if (error) {
    logger.error('bind_failed', error);
    server.forceShutdown();
    close().finally(() => { process.exitCode = 1; });
    return;
  }
  logger.info('listening', { address: listen });
});

let stopping = false;
function shutdown() {
  if (stopping) return;
  stopping = true;
  const timer = setTimeout(() => server.forceShutdown(), 10_000);
  server.tryShutdown(() => {
    clearTimeout(timer);
    close().catch((error) => {
      logger.error('shutdown_failed', error);
      process.exitCode = 1;
    });
  });
}

process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
