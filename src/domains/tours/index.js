'use strict';

/**
 * Register the tours team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const tourBudgetWorkflowHandlers = require('./handlers/tour-budget-workflow');
const tourStopApprovalHandlers = require('./handlers/tour-stop-approval');
const tourWorkflowHandlers = require('./handlers/tour-workflow');
const store = require('./store');
const { TourBudgetApprovalService } = require('./tour-budget-approval.service');
const { createTourBudgetValidationHandlers } = require('./tour-budget/tour-budget-validation.handlers');
const { TourQueryService } = require('./tour-query.service');
const { TourStopQueryService } = require('./tour-stop-query.service');
const { createTourStopWorkflowHandlers } = require('./tour-stop/tour-stop-workflow.handlers');
const { createTourHandlers } = require('./tour/tour.handlers');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.tours.v1.TourBudgetApprovalService.service, new TourBudgetApprovalService(scope));
  server.addService(acme.tours.v1.TourBudgetValidationService.service, createTourBudgetValidationHandlers(scope));
  server.addService(acme.tours.v1.TourBudgetWorkflowService.service, tourBudgetWorkflowHandlers);
  server.addService(acme.tours.v1.TourQueryService.service, new TourQueryService(scope));
  server.addService(acme.tours.v1.TourService.service, createTourHandlers(scope));
  server.addService(acme.tours.v1.TourStopApprovalService.service, tourStopApprovalHandlers);
  server.addService(acme.tours.v1.TourStopQueryService.service, new TourStopQueryService(scope));
  server.addService(acme.tours.v1.TourStopWorkflowService.service, createTourStopWorkflowHandlers(scope));
  server.addService(acme.tours.v1.TourWorkflowService.service, tourWorkflowHandlers);
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.tours.v1.TourBudgetApprovalService/GetTourBudgetApprovalStatus:1', (request) => call((done) => clients.toursTourClient.listTours(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.tours.v1.TourBudgetValidationService/LintTourBudgets:3', (request) => call((done) => clients.venuesVenueQueryClient.browseVenues(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.tours.v1.TourBudgetWorkflowService/ResumeTourBudgetWorkflow:0', (request) => call((done) => clients.venuesVenueExportClient.listVenueExports(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.tours.v1.TourQueryService/SummarizeTours:3', (request) => call((done) => clients.toursTourBudgetValidationClient.lintTourBudgets(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.tours.v1.TourStopApprovalService/DelegateTourStopApproval:1', (request) => call((done) => clients.toursTourStopQueryClient.lookupTourStop(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.tours.v1.TourStopQueryService/FindTourStops:1', (request) => call((done) => clients.venuesVenuePolicySyncClient.syncVenuePolicies(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.tours.v1.TourStopWorkflowService/SignalTourStopWorkflow:1', (request) => call((done) => clients.toursTourStopApprovalClient.rejectTourStop(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
