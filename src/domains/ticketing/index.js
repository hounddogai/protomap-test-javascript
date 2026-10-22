'use strict';

/**
 * Register the ticketing team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const ticketTierApprovalHandlers = require('./handlers/ticket-tier-approval');
const ticketTransferApprovalHandlers = require('./handlers/ticket-transfer-approval');
const ticketWorkflowHandlers = require('./handlers/ticket-workflow');
const store = require('./store');
const { TicketQueryService } = require('./ticket-query.service');
const { TicketTierQueryService } = require('./ticket-tier-query.service');
const { createTicketTierWorkflowHandlers } = require('./ticket-tier/ticket-tier-workflow.handlers');
const { TicketTransferValidationService } = require('./ticket-transfer-validation.service');
const { createTicketTransferWorkflowHandlers } = require('./ticket-transfer/ticket-transfer-workflow.handlers');
const { createTicketHandlers } = require('./ticket/ticket.handlers');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.ticketing.v1.TicketQueryService.service, new TicketQueryService(scope));
  server.addService(acme.ticketing.v1.TicketService.service, createTicketHandlers(scope));
  server.addService(acme.ticketing.v1.TicketTierApprovalService.service, ticketTierApprovalHandlers);
  server.addService(acme.ticketing.v1.TicketTierQueryService.service, new TicketTierQueryService(scope));
  server.addService(acme.ticketing.v1.TicketTierWorkflowService.service, createTicketTierWorkflowHandlers(scope));
  server.addService(acme.ticketing.v1.TicketTransferApprovalService.service, ticketTransferApprovalHandlers);
  server.addService(acme.ticketing.v1.TicketTransferValidationService.service, new TicketTransferValidationService(scope));
  server.addService(acme.ticketing.v1.TicketTransferWorkflowService.service, createTicketTransferWorkflowHandlers(scope));
  server.addService(acme.ticketing.v1.TicketWorkflowService.service, ticketWorkflowHandlers);
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.ticketing.v1.TicketService/BatchGetTickets:0', (request) => call((done) => clients.ticketingTicketTransferValidationClient.explainTicketTransferDecision(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.ticketing.v1.TicketTierApprovalService/CommentTicketTierApproval:0', (request) => call((done) => clients.ticketingTicketTierQueryClient.lookupTicketTier(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.ticketing.v1.TicketTierQueryService/ResolveTicketTiers:0', (request) => call((done) => clients.ticketingTicketTransferValidationClient.validateTicketTransfer(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.ticketing.v1.TicketTransferApprovalService/GetTicketTransferApprovalStatus:0', (request) => call((done) => clients.venuesVenuePolicyValidationClient.previewVenuePolicy(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.ticketing.v1.TicketTransferValidationService/LintTicketTransfers:0', (request) => call((done) => clients.broadcastsStreamKeyCalculationClient.recalculateStreamKeys(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.ticketing.v1.TicketTransferWorkflowService/ResumeTicketTransferWorkflow:0', (request) => call((done) => clients.ticketingTicketTierQueryClient.suggestTicketTiers(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.ticketing.v1.TicketWorkflowService/RetryTicketWorkflow:0', (request) => call((done) => clients.ticketingTicketTransferValidationClient.explainTicketTransferDecision(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
