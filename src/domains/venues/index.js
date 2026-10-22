'use strict';

/**
 * Register the venues team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const venueHistoryHandlers = require('./handlers/venue-history');
const venuePolicyExportHandlers = require('./handlers/venue-policy-export');
const venuePolicySyncHandlers = require('./handlers/venue-policy-sync');
const venueSectionAdminHandlers = require('./handlers/venue-section-admin');
const venueSectionNotificationHandlers = require('./handlers/venue-section-notification');
const venueSectionValidationHandlers = require('./handlers/venue-section-validation');
const store = require('./store');
const { VenueAdminService } = require('./venue-admin.service');
const { VenueNotificationService } = require('./venue-notification.service');
const { VenuePolicyHistoryService } = require('./venue-policy-history.service');
const { VenuePolicyValidationService } = require('./venue-policy-validation.service');
const { createVenuePolicyAdminHandlers } = require('./venue-policy/venue-policy-admin.handlers');
const { createVenuePolicyNotificationHandlers } = require('./venue-policy/venue-policy-notification.handlers');
const { VenueSectionExportService } = require('./venue-section-export.service');
const { VenueSectionQueryService } = require('./venue-section-query.service');
const { createVenueSectionHistoryHandlers } = require('./venue-section/venue-section-history.handlers');
const { createVenueSectionSyncHandlers } = require('./venue-section/venue-section-sync.handlers');
const { VenueService } = require('./venue.service');
const { createVenueExportHandlers } = require('./venue/venue-export.handlers');
const { createVenueQueryHandlers } = require('./venue/venue-query.handlers');
const { createVenueValidationHandlers } = require('./venue/venue-validation.handlers');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.venues.v1.VenueAdminService.service, new VenueAdminService(scope));
  server.addService(acme.venues.v1.VenueExportService.service, createVenueExportHandlers(scope));
  server.addService(acme.venues.v1.VenueHistoryService.service, venueHistoryHandlers);
  server.addService(acme.venues.v1.VenueNotificationService.service, new VenueNotificationService(scope));
  server.addService(acme.venues.v1.VenuePolicyAdminService.service, createVenuePolicyAdminHandlers(scope));
  server.addService(acme.venues.v1.VenuePolicyExportService.service, venuePolicyExportHandlers);
  server.addService(acme.venues.v1.VenuePolicyHistoryService.service, new VenuePolicyHistoryService(scope));
  server.addService(acme.venues.v1.VenuePolicyNotificationService.service, createVenuePolicyNotificationHandlers(scope));
  server.addService(acme.venues.v1.VenuePolicySyncService.service, venuePolicySyncHandlers);
  server.addService(acme.venues.v1.VenuePolicyValidationService.service, new VenuePolicyValidationService(scope));
  server.addService(acme.venues.v1.VenueQueryService.service, createVenueQueryHandlers(scope));
  server.addService(acme.venues.v1.VenueSectionAdminService.service, venueSectionAdminHandlers);
  server.addService(acme.venues.v1.VenueSectionExportService.service, new VenueSectionExportService(scope));
  server.addService(acme.venues.v1.VenueSectionHistoryService.service, createVenueSectionHistoryHandlers(scope));
  server.addService(acme.venues.v1.VenueSectionNotificationService.service, venueSectionNotificationHandlers);
  server.addService(acme.venues.v1.VenueSectionQueryService.service, new VenueSectionQueryService(scope));
  server.addService(acme.venues.v1.VenueSectionSyncService.service, createVenueSectionSyncHandlers(scope));
  server.addService(acme.venues.v1.VenueSectionValidationService.service, venueSectionValidationHandlers);
  server.addService(acme.venues.v1.VenueService.service, new VenueService(scope));
  server.addService(acme.venues.v1.VenueValidationService.service, createVenueValidationHandlers(scope));
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.venues.v1.VenuePolicyNotificationService/NotifyVenuePolicyWatchers:0', (request) => call((done) => clients.venuesVenueNotificationClient.listVenueSubscriptions(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.venues.v1.VenueService/SearchVenues:0', (request) => call((done) => clients.venuesVenueExportClient.listVenueExports(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
