'use strict';

/**
 * Register the fanportal team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const { EventPageQueryService } = require('./event-page-query.service');
const { createEventPageHandlers } = require('./event-page/event-page.handlers');
const { FanSessionQueryService } = require('./fan-session-query.service');
const { createFanSessionHandlers } = require('./fan-session/fan-session.handlers');
const eventPageNotificationHandlers = require('./handlers/event-page-notification');
const fanSessionGatewayHandlers = require('./handlers/fan-session-gateway');
const wishlistNotificationHandlers = require('./handlers/wishlist-notification');
const store = require('./store');
const { WishlistQueryService } = require('./wishlist-query.service');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.fanportal.v1.EventPageNotificationService.service, eventPageNotificationHandlers);
  server.addService(acme.fanportal.v1.EventPageQueryService.service, new EventPageQueryService(scope));
  server.addService(acme.fanportal.v1.EventPageService.service, createEventPageHandlers(scope));
  server.addService(acme.fanportal.v1.FanSessionGatewayService.service, fanSessionGatewayHandlers);
  server.addService(acme.fanportal.v1.FanSessionQueryService.service, new FanSessionQueryService(scope));
  server.addService(acme.fanportal.v1.FanSessionService.service, createFanSessionHandlers(scope));
  server.addService(acme.fanportal.v1.WishlistNotificationService.service, wishlistNotificationHandlers);
  server.addService(acme.fanportal.v1.WishlistQueryService.service, new WishlistQueryService(scope));
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.fanportal.v1.EventPageQueryService/ExportEventPages:0', (request) => call((done) => clients.fanportalWishlistNotificationClient.listWishlistSubscriptions(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.fanportal.v1.EventPageService/CloneEventPage:3', (request) => call((done) => clients.seatingSeatMapWorkflowClient.resumeSeatMapWorkflow(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.fanportal.v1.FanSessionGatewayService/ComposeFanSessionPage:2', (request) => call((done) => clients.metricsMetricSeriesValidationClient.explainMetricSeriesDecision(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.fanportal.v1.FanSessionGatewayService/SubmitFanSessionForm:1', (request) => call((done) => clients.fanportalWishlistNotificationClient.sendWishlistTestNotice(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.fanportal.v1.FanSessionService/DeleteFanSession:2', (request) => call((done) => clients.ledgerLedgerAccountClient.createLedgerAccount(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.fanportal.v1.WishlistNotificationService/SubscribeWishlistUpdates:0', (request) => call((done) => clients.fanportalEventPageNotificationClient.subscribeEventPageUpdates(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
