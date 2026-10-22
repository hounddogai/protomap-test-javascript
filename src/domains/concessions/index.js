'use strict';

/**
 * Register the concessions team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const { ConcessionOrderApprovalService } = require('./concession-order-approval.service');
const { ConcessionOrderQueryService } = require('./concession-order-query.service');
const { ConcessionOrderWorkflowService } = require('./concession-order-workflow.service');
const {
  createConcessionOrderCalculationHandlers,
} = require('./concession-order/concession-order-calculation.handlers');
const { createConcessionOrderReportingHandlers } = require('./concession-order/concession-order-reporting.handlers');
const { ConcessionStandLifecycleService } = require('./concession-stand-lifecycle.service');
const { ConcessionStandValidationService } = require('./concession-stand-validation.service');
const { createConcessionStandApprovalHandlers } = require('./concession-stand/concession-stand-approval.handlers');
const { createConcessionStandQueryHandlers } = require('./concession-stand/concession-stand-query.handlers');
const { createConcessionStandWorkflowHandlers } = require('./concession-stand/concession-stand-workflow.handlers');
const concessionOrderLifecycleHandlers = require('./handlers/concession-order-lifecycle');
const concessionOrderValidationHandlers = require('./handlers/concession-order-validation');
const concessionStandHandlers = require('./handlers/concession-stand');
const concessionStandCalculationHandlers = require('./handlers/concession-stand-calculation');
const menuItemApprovalHandlers = require('./handlers/menu-item-approval');
const menuItemReportingHandlers = require('./handlers/menu-item-reporting');
const { MenuItemCalculationService } = require('./menu-item-calculation.service');
const { MenuItemValidationService } = require('./menu-item-validation.service');
const { createMenuItemLifecycleHandlers } = require('./menu-item/menu-item-lifecycle.handlers');
const { createMenuItemWorkflowHandlers } = require('./menu-item/menu-item-workflow.handlers');
const store = require('./store');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.concessions.v1.ConcessionOrderApprovalService.service, new ConcessionOrderApprovalService(scope));
  server.addService(acme.concessions.v1.ConcessionOrderCalculationService.service, createConcessionOrderCalculationHandlers(scope));
  server.addService(acme.concessions.v1.ConcessionOrderLifecycleService.service, concessionOrderLifecycleHandlers);
  server.addService(acme.concessions.v1.ConcessionOrderQueryService.service, new ConcessionOrderQueryService(scope));
  server.addService(acme.concessions.v1.ConcessionOrderReportingService.service, createConcessionOrderReportingHandlers(scope));
  server.addService(acme.concessions.v1.ConcessionOrderValidationService.service, concessionOrderValidationHandlers);
  server.addService(acme.concessions.v1.ConcessionOrderWorkflowService.service, new ConcessionOrderWorkflowService(scope));
  server.addService(acme.concessions.v1.ConcessionStandApprovalService.service, createConcessionStandApprovalHandlers(scope));
  server.addService(acme.concessions.v1.ConcessionStandCalculationService.service, concessionStandCalculationHandlers);
  server.addService(acme.concessions.v1.ConcessionStandLifecycleService.service, new ConcessionStandLifecycleService(scope));
  server.addService(acme.concessions.v1.ConcessionStandQueryService.service, createConcessionStandQueryHandlers(scope));
  server.addService(acme.concessions.v1.ConcessionStandService.service, concessionStandHandlers);
  server.addService(acme.concessions.v1.ConcessionStandValidationService.service, new ConcessionStandValidationService(scope));
  server.addService(acme.concessions.v1.ConcessionStandWorkflowService.service, createConcessionStandWorkflowHandlers(scope));
  server.addService(acme.concessions.v1.MenuItemApprovalService.service, menuItemApprovalHandlers);
  server.addService(acme.concessions.v1.MenuItemCalculationService.service, new MenuItemCalculationService(scope));
  server.addService(acme.concessions.v1.MenuItemLifecycleService.service, createMenuItemLifecycleHandlers(scope));
  server.addService(acme.concessions.v1.MenuItemReportingService.service, menuItemReportingHandlers);
  server.addService(acme.concessions.v1.MenuItemValidationService.service, new MenuItemValidationService(scope));
  server.addService(acme.concessions.v1.MenuItemWorkflowService.service, createMenuItemWorkflowHandlers(scope));
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.concessions.v1.ConcessionOrderApprovalService/EscalateConcessionOrder:1', (request) => call((done) => clients.broadcastsStreamKeyWorkflowClient.advanceStreamKeyWorkflow(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionOrderCalculationService/GetConcessionOrderRates:0', (request) => call((done) => clients.broadcastsReplayClipCalculationClient.estimateReplayClip(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionOrderLifecycleService/RenewConcessionOrder:2', (request) => call((done) => clients.concessionsMenuItemCalculationClient.calculateMenuItem(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionOrderQueryService/SummarizeConcessionOrders:0', (request) => call((done) => clients.venuesVenueExportClient.listVenueExports(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionOrderReportingService/StreamConcessionOrderMetrics:1', (request) => call((done) => clients.sponsorshipsSponsorshipDealApprovalClient.submitSponsorshipDeal(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionOrderWorkflowService/CancelConcessionOrderWorkflow:0', (request) => call((done) => clients.concessionsConcessionStandLifecycleClient.scheduleConcessionStandExpiry(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionStandApprovalService/GetConcessionStandApprovalStatus:0', (request) => call((done) => clients.venuesVenuePolicyHistoryClient.recordVenuePolicyVersion(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionStandCalculationService/ExplainConcessionStandCalculation:0', (request) => call((done) => clients.sponsorshipsActivationCalculationClient.compareActivationOptions(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionStandLifecycleService/ExpireConcessionStand:0', (request) => call((done) => clients.concessionsConcessionStandValidationClient.checkConcessionStandEligibility(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionStandQueryService/BrowseConcessionStands:0', (request) => call((done) => clients.concessionsConcessionOrderCalculationClient.quoteConcessionOrder(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionStandService/CloneConcessionStand:1', (request) => call((done) => clients.concessionsConcessionOrderValidationClient.previewConcessionOrder(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.ConcessionStandWorkflowService/ResumeConcessionStandWorkflow:1', (request) => call((done) => clients.sponsorshipsSponsorshipDealWorkflowClient.resumeSponsorshipDealWorkflow(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.MenuItemApprovalService/RecallMenuItem:2', (request) => call((done) => clients.concessionsMenuItemValidationClient.validateMenuItem(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.MenuItemCalculationService/QuoteMenuItem:0', (request) => call((done) => clients.concessionsConcessionOrderCalculationClient.estimateConcessionOrder(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.MenuItemLifecycleService/TerminateMenuItem:1', (request) => call((done) => clients.concessionsConcessionOrderQueryClient.summarizeConcessionOrders(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.concessions.v1.MenuItemValidationService/PreviewMenuItem:1', (request) => call((done) => clients.sponsorshipsSponsorCalculationClient.explainSponsorCalculation(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
