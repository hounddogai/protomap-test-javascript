'use strict';

/**
 * Register the performers team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const performerContractApprovalHandlers = require('./handlers/performer-contract-approval');
const performerQueryHandlers = require('./handlers/performer-query');
const riderApprovalHandlers = require('./handlers/rider-approval');
const { PerformerContractQueryService } = require('./performer-contract-query.service');
const {
  createPerformerContractWorkflowHandlers,
} = require('./performer-contract/performer-contract-workflow.handlers');
const { PerformerService } = require('./performer.service');
const { createPerformerApprovalHandlers } = require('./performer/performer-approval.handlers');
const { createPerformerWorkflowHandlers } = require('./performer/performer-workflow.handlers');
const { RiderValidationService } = require('./rider-validation.service');
const { createRiderWorkflowHandlers } = require('./rider/rider-workflow.handlers');
const store = require('./store');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.performers.v1.PerformerApprovalService.service, createPerformerApprovalHandlers(scope));
  server.addService(acme.performers.v1.PerformerContractApprovalService.service, performerContractApprovalHandlers);
  server.addService(acme.performers.v1.PerformerContractQueryService.service, new PerformerContractQueryService(scope));
  server.addService(acme.performers.v1.PerformerContractWorkflowService.service, createPerformerContractWorkflowHandlers(scope));
  server.addService(acme.performers.v1.PerformerQueryService.service, performerQueryHandlers);
  server.addService(acme.performers.v1.PerformerService.service, new PerformerService(scope));
  server.addService(acme.performers.v1.PerformerWorkflowService.service, createPerformerWorkflowHandlers(scope));
  server.addService(acme.performers.v1.RiderApprovalService.service, riderApprovalHandlers);
  server.addService(acme.performers.v1.RiderValidationService.service, new RiderValidationService(scope));
  server.addService(acme.performers.v1.RiderWorkflowService.service, createRiderWorkflowHandlers(scope));
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.performers.v1.PerformerApprovalService/ApprovePerformer:1', (request) => call((done) => clients.performersPerformerContractQueryClient.summarizePerformerContracts(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.performers.v1.PerformerContractApprovalService/ApprovePerformerContract:2', (request) => call((done) => clients.performersPerformerClient.searchPerformers(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.performers.v1.PerformerContractQueryService/BrowsePerformerContracts:1', (request) => call((done) => clients.performersPerformerQueryClient.countPerformers(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.performers.v1.PerformerContractWorkflowService/ListPerformerContractWorkflows:0', (request) => call((done) => clients.broadcastsBroadcastWorkflowClient.advanceBroadcastWorkflow(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.performers.v1.PerformerQueryService/ExportPerformers:1', (request) => call((done) => clients.performersRiderValidationClient.lintRiders(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.performers.v1.PerformerService/BatchGetPerformers:0', (request) => call((done) => clients.concessionsMenuItemCalculationClient.compareMenuItemOptions(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.performers.v1.PerformerWorkflowService/CancelPerformerWorkflow:1', (request) => call((done) => clients.admissionsWristbandWorkflowClient.listWristbandWorkflows(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.performers.v1.RiderApprovalService/RecallRider:1', (request) => call((done) => clients.performersRiderValidationClient.validateRider(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.performers.v1.RiderValidationService/ValidateRider:0', (request) => call((done) => clients.ledgerLedgerAccountClient.listLedgerAccounts(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
