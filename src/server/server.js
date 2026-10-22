'use strict';

/**
 * Compose every Acme Live domain on one authenticated gRPC server.
 */

const grpc = require('@grpc/grpc-js');
const admissions = require('../domains/admissions/index');
const boxoffice = require('../domains/boxoffice/index');
const broadcasts = require('../domains/broadcasts/index');
const concessions = require('../domains/concessions/index');
const fanportal = require('../domains/fanportal/index');
const performers = require('../domains/performers/index');
const seating = require('../domains/seating/index');
const shows = require('../domains/shows/index');
const sponsorships = require('../domains/sponsorships/index');
const ticketing = require('../domains/ticketing/index');
const tours = require('../domains/tours/index');
const venues = require('../domains/venues/index');
const { authenticate } = require('../platform/authentication');
const { bus } = require('../platform/bus');
const clients = require('../platform/clients');
const { logger } = require('../platform/logger');

function createServer() {
  const server = new grpc.Server({
    interceptors: [authenticate(clients.identityApiCredentialValidationClient)],
    'grpc.max_receive_message_length': 8 * 1024 * 1024,
  });
  const dependencies = { clients: clients, bus: bus, logger: logger };
  admissions.register(server, dependencies);
  admissions.startWorkers(dependencies);
  boxoffice.register(server, dependencies);
  boxoffice.startWorkers(dependencies);
  broadcasts.register(server, dependencies);
  broadcasts.startWorkers(dependencies);
  concessions.register(server, dependencies);
  concessions.startWorkers(dependencies);
  fanportal.register(server, dependencies);
  fanportal.startWorkers(dependencies);
  performers.register(server, dependencies);
  performers.startWorkers(dependencies);
  seating.register(server, dependencies);
  seating.startWorkers(dependencies);
  shows.register(server, dependencies);
  shows.startWorkers(dependencies);
  sponsorships.register(server, dependencies);
  sponsorships.startWorkers(dependencies);
  ticketing.register(server, dependencies);
  ticketing.startWorkers(dependencies);
  tours.register(server, dependencies);
  tours.startWorkers(dependencies);
  venues.register(server, dependencies);
  venues.startWorkers(dependencies);
  return server;
}

module.exports = { createServer };
