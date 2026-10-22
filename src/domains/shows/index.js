'use strict';

/**
 * Register the shows team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const lineupCalculationHandlers = require('./handlers/lineup-calculation');
const lineupWorkflowHandlers = require('./handlers/lineup-workflow');
const performanceQueryHandlers = require('./handlers/performance-query');
const showHandlers = require('./handlers/show');
const showApprovalHandlers = require('./handlers/show-approval');
const { LineupLifecycleService } = require('./lineup-lifecycle.service');
const { createLineupApprovalHandlers } = require('./lineup/lineup-approval.handlers');
const { createLineupValidationHandlers } = require('./lineup/lineup-validation.handlers');
const { PerformanceApprovalService } = require('./performance-approval.service');
const { PerformanceValidationService } = require('./performance-validation.service');
const { createPerformanceCalculationHandlers } = require('./performance/performance-calculation.handlers');
const { createPerformanceWorkflowHandlers } = require('./performance/performance-workflow.handlers');
const { ShowCalculationService } = require('./show-calculation.service');
const { ShowValidationService } = require('./show-validation.service');
const { createShowQueryHandlers } = require('./show/show-query.handlers');
const { createShowWorkflowHandlers } = require('./show/show-workflow.handlers');
const store = require('./store');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.shows.v1.LineupApprovalService.service, createLineupApprovalHandlers(scope));
  server.addService(acme.shows.v1.LineupCalculationService.service, lineupCalculationHandlers);
  server.addService(acme.shows.v1.LineupLifecycleService.service, new LineupLifecycleService(scope));
  server.addService(acme.shows.v1.LineupValidationService.service, createLineupValidationHandlers(scope));
  server.addService(acme.shows.v1.LineupWorkflowService.service, lineupWorkflowHandlers);
  server.addService(acme.shows.v1.PerformanceApprovalService.service, new PerformanceApprovalService(scope));
  server.addService(acme.shows.v1.PerformanceCalculationService.service, createPerformanceCalculationHandlers(scope));
  server.addService(acme.shows.v1.PerformanceQueryService.service, performanceQueryHandlers);
  server.addService(acme.shows.v1.PerformanceValidationService.service, new PerformanceValidationService(scope));
  server.addService(acme.shows.v1.PerformanceWorkflowService.service, createPerformanceWorkflowHandlers(scope));
  server.addService(acme.shows.v1.ShowApprovalService.service, showApprovalHandlers);
  server.addService(acme.shows.v1.ShowCalculationService.service, new ShowCalculationService(scope));
  server.addService(acme.shows.v1.ShowQueryService.service, createShowQueryHandlers(scope));
  server.addService(acme.shows.v1.ShowService.service, showHandlers);
  server.addService(acme.shows.v1.ShowValidationService.service, new ShowValidationService(scope));
  server.addService(acme.shows.v1.ShowWorkflowService.service, createShowWorkflowHandlers(scope));
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.shows.v1.LineupApprovalService/CommentLineupApproval:1', (request) => call((done) => clients.licensingSoftwareLicenseClient.createSoftwareLicense(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.LineupCalculationService/EstimateLineup:2', (request) => call((done) => clients.showsShowCalculationClient.simulateShow(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.LineupLifecycleService/ScheduleLineupExpiry:0', (request) => call((done) => clients.seatingSeatHoldReportingClient.listSeatHoldReports(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.LineupValidationService/NormalizeLineup:1', (request) => call((done) => clients.showsShowCalculationClient.explainShowCalculation(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.LineupWorkflowService/ListLineupWorkflows:0', (request) => call((done) => clients.admissionsWristbandApprovalClient.recallWristband(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.PerformanceApprovalService/CommentPerformanceApproval:2', (request) => call((done) => clients.seatingSeatAssignmentApprovalClient.listPendingSeatAssignments(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.PerformanceCalculationService/EstimatePerformance:0', (request) => call((done) => clients.seatingSeatHoldValidationClient.explainSeatHoldDecision(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.PerformanceValidationService/ExplainPerformanceDecision:1', (request) => call((done) => clients.showsShowCalculationClient.explainShowCalculation(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.PerformanceWorkflowService/PausePerformanceWorkflow:0', (request) => call((done) => clients.showsShowApprovalClient.approveShow(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.ShowApprovalService/RejectShow:0', (request) => call((done) => clients.showsPerformanceQueryClient.browsePerformances(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.ShowService/BatchGetShows:0', (request) => call((done) => clients.broadcastsReplayClipCalculationClient.simulateReplayClip(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.shows.v1.ShowValidationService/ScoreShow:0', (request) => call((done) => clients.showsShowCalculationClient.compareShowOptions(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
