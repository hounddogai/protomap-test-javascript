'use strict';

/**
 * Register the seating team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const seatAssignmentApprovalHandlers = require('./handlers/seat-assignment-approval');
const seatAssignmentReportingHandlers = require('./handlers/seat-assignment-reporting');
const seatHoldApprovalHandlers = require('./handlers/seat-hold-approval');
const seatHoldQueryHandlers = require('./handlers/seat-hold-query');
const seatHoldWorkflowHandlers = require('./handlers/seat-hold-workflow');
const seatMapLifecycleHandlers = require('./handlers/seat-map-lifecycle');
const seatMapValidationHandlers = require('./handlers/seat-map-validation');
const { SeatAssignmentCalculationService } = require('./seat-assignment-calculation.service');
const { SeatAssignmentValidationService } = require('./seat-assignment-validation.service');
const { createSeatAssignmentLifecycleHandlers } = require('./seat-assignment/seat-assignment-lifecycle.handlers');
const { createSeatAssignmentWorkflowHandlers } = require('./seat-assignment/seat-assignment-workflow.handlers');
const { SeatHoldCalculationService } = require('./seat-hold-calculation.service');
const { SeatHoldReportingService } = require('./seat-hold-reporting.service');
const { createSeatHoldLifecycleHandlers } = require('./seat-hold/seat-hold-lifecycle.handlers');
const { createSeatHoldValidationHandlers } = require('./seat-hold/seat-hold-validation.handlers');
const { SeatMapApprovalService } = require('./seat-map-approval.service');
const { SeatMapQueryService } = require('./seat-map-query.service');
const { SeatMapWorkflowService } = require('./seat-map-workflow.service');
const { createSeatMapCalculationHandlers } = require('./seat-map/seat-map-calculation.handlers');
const { createSeatMapHandlers } = require('./seat-map/seat-map.handlers');
const store = require('./store');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.seating.v1.SeatAssignmentApprovalService.service, seatAssignmentApprovalHandlers);
  server.addService(acme.seating.v1.SeatAssignmentCalculationService.service, new SeatAssignmentCalculationService(scope));
  server.addService(acme.seating.v1.SeatAssignmentLifecycleService.service, createSeatAssignmentLifecycleHandlers(scope));
  server.addService(acme.seating.v1.SeatAssignmentReportingService.service, seatAssignmentReportingHandlers);
  server.addService(acme.seating.v1.SeatAssignmentValidationService.service, new SeatAssignmentValidationService(scope));
  server.addService(acme.seating.v1.SeatAssignmentWorkflowService.service, createSeatAssignmentWorkflowHandlers(scope));
  server.addService(acme.seating.v1.SeatHoldApprovalService.service, seatHoldApprovalHandlers);
  server.addService(acme.seating.v1.SeatHoldCalculationService.service, new SeatHoldCalculationService(scope));
  server.addService(acme.seating.v1.SeatHoldLifecycleService.service, createSeatHoldLifecycleHandlers(scope));
  server.addService(acme.seating.v1.SeatHoldQueryService.service, seatHoldQueryHandlers);
  server.addService(acme.seating.v1.SeatHoldReportingService.service, new SeatHoldReportingService(scope));
  server.addService(acme.seating.v1.SeatHoldValidationService.service, createSeatHoldValidationHandlers(scope));
  server.addService(acme.seating.v1.SeatHoldWorkflowService.service, seatHoldWorkflowHandlers);
  server.addService(acme.seating.v1.SeatMapApprovalService.service, new SeatMapApprovalService(scope));
  server.addService(acme.seating.v1.SeatMapCalculationService.service, createSeatMapCalculationHandlers(scope));
  server.addService(acme.seating.v1.SeatMapLifecycleService.service, seatMapLifecycleHandlers);
  server.addService(acme.seating.v1.SeatMapQueryService.service, new SeatMapQueryService(scope));
  server.addService(acme.seating.v1.SeatMapService.service, createSeatMapHandlers(scope));
  server.addService(acme.seating.v1.SeatMapValidationService.service, seatMapValidationHandlers);
  server.addService(acme.seating.v1.SeatMapWorkflowService.service, new SeatMapWorkflowService(scope));
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.seating.v1.SeatAssignmentApprovalService/ApproveSeatAssignment:1', (request) => call((done) => clients.identityUserQueryClient.countUsers(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatAssignmentCalculationService/ExplainSeatAssignmentCalculation:2', (request) => call((done) => clients.seatingSeatMapValidationClient.checkSeatMapEligibility(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatAssignmentLifecycleService/SuspendSeatAssignment:1', (request) => call((done) => clients.venuesVenueClient.cloneVenue(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatAssignmentReportingService/StreamSeatAssignmentMetrics:1', (request) => call((done) => clients.seatingSeatHoldValidationClient.checkSeatHoldEligibility(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatAssignmentWorkflowService/StartSeatAssignmentWorkflow:1', (request) => call((done) => clients.seatingSeatHoldValidationClient.previewSeatHold(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatHoldApprovalService/RejectSeatHold:1', (request) => call((done) => clients.seatingSeatMapClient.cloneSeatMap(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatHoldCalculationService/SimulateSeatHold:0', (request) => call((done) => clients.venuesVenueValidationClient.normalizeVenue(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatHoldLifecycleService/ScheduleSeatHoldExpiry:0', (request) => call((done) => clients.seatingSeatAssignmentReportingClient.scheduleSeatAssignmentReport(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatHoldReportingService/FetchSeatHoldReport:0', (request) => call((done) => clients.admissionsAdmissionApprovalClient.approveAdmission(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatHoldValidationService/LintSeatHolds:3', (request) => call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.recalculateBoxOfficeSales(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatMapApprovalService/DelegateSeatMapApproval:0', (request) => call((done) => clients.admissionsGateScanWorkflowClient.signalGateScanWorkflow(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatMapCalculationService/CompareSeatMapOptions:1', (request) => call((done) => clients.sponsorshipsSponsorValidationClient.scoreSponsor(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatMapLifecycleService/ExpireSeatMap:1', (request) => call((done) => clients.seatingSeatAssignmentValidationClient.lintSeatAssignments(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatMapQueryService/FindSeatMaps:0', (request) => call((done) => clients.admissionsWristbandValidationClient.explainWristbandDecision(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatMapValidationService/CheckSeatMapEligibility:0', (request) => call((done) => clients.seatingSeatHoldCalculationClient.compareSeatHoldOptions(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.seating.v1.SeatMapWorkflowService/PauseSeatMapWorkflow:0', (request) => call((done) => clients.ledgerLedgerAccountClient.cloneLedgerAccount(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
