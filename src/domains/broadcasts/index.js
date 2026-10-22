'use strict';

/**
 * Register the broadcasts team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const { BroadcastApprovalService } = require('./broadcast-approval.service');
const { BroadcastValidationService } = require('./broadcast-validation.service');
const { createBroadcastQueryHandlers } = require('./broadcast/broadcast-query.handlers');
const { createBroadcastWorkflowHandlers } = require('./broadcast/broadcast-workflow.handlers');
const broadcastHandlers = require('./handlers/broadcast');
const replayClipApprovalHandlers = require('./handlers/replay-clip-approval');
const replayClipValidationHandlers = require('./handlers/replay-clip-validation');
const streamKeyCalculationHandlers = require('./handlers/stream-key-calculation');
const streamKeyWorkflowHandlers = require('./handlers/stream-key-workflow');
const { ReplayClipCalculationService } = require('./replay-clip-calculation.service');
const { ReplayClipWorkflowService } = require('./replay-clip-workflow.service');
const { createReplayClipLifecycleHandlers } = require('./replay-clip/replay-clip-lifecycle.handlers');
const store = require('./store');
const { StreamKeyQueryService } = require('./stream-key-query.service');
const { createStreamKeyApprovalHandlers } = require('./stream-key/stream-key-approval.handlers');
const { createStreamKeyValidationHandlers } = require('./stream-key/stream-key-validation.handlers');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.broadcasts.v1.BroadcastApprovalService.service, new BroadcastApprovalService(scope));
  server.addService(acme.broadcasts.v1.BroadcastQueryService.service, createBroadcastQueryHandlers(scope));
  server.addService(acme.broadcasts.v1.BroadcastService.service, broadcastHandlers);
  server.addService(acme.broadcasts.v1.BroadcastValidationService.service, new BroadcastValidationService(scope));
  server.addService(acme.broadcasts.v1.BroadcastWorkflowService.service, createBroadcastWorkflowHandlers(scope));
  server.addService(acme.broadcasts.v1.ReplayClipApprovalService.service, replayClipApprovalHandlers);
  server.addService(acme.broadcasts.v1.ReplayClipCalculationService.service, new ReplayClipCalculationService(scope));
  server.addService(acme.broadcasts.v1.ReplayClipLifecycleService.service, createReplayClipLifecycleHandlers(scope));
  server.addService(acme.broadcasts.v1.ReplayClipValidationService.service, replayClipValidationHandlers);
  server.addService(acme.broadcasts.v1.ReplayClipWorkflowService.service, new ReplayClipWorkflowService(scope));
  server.addService(acme.broadcasts.v1.StreamKeyApprovalService.service, createStreamKeyApprovalHandlers(scope));
  server.addService(acme.broadcasts.v1.StreamKeyCalculationService.service, streamKeyCalculationHandlers);
  server.addService(acme.broadcasts.v1.StreamKeyQueryService.service, new StreamKeyQueryService(scope));
  server.addService(acme.broadcasts.v1.StreamKeyValidationService.service, createStreamKeyValidationHandlers(scope));
  server.addService(acme.broadcasts.v1.StreamKeyWorkflowService.service, streamKeyWorkflowHandlers);
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.broadcasts.v1.BroadcastApprovalService/EscalateBroadcast:1', (request) => call((done) => clients.broadcastsStreamKeyCalculationClient.simulateStreamKey(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.broadcasts.v1.BroadcastService/BatchCreateBroadcasts:0', (request) => call((done) => clients.venuesVenueExportClient.retryVenueExport(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.broadcasts.v1.BroadcastService/WatchBroadcasts:0', (request) => call((done) => clients.broadcastsReplayClipValidationClient.checkReplayClipEligibility(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.broadcasts.v1.BroadcastWorkflowService/SignalBroadcastWorkflow:1', (request) => call((done) => clients.broadcastsReplayClipCalculationClient.recalculateReplayClips(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.broadcasts.v1.ReplayClipCalculationService/CalculateReplayClip:3', (request) => call((done) => clients.broadcastsStreamKeyCalculationClient.recalculateStreamKeys(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.broadcasts.v1.ReplayClipLifecycleService/ExpireReplayClip:1', (request) => call((done) => clients.broadcastsReplayClipValidationClient.previewReplayClip(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.broadcasts.v1.ReplayClipValidationService/PreviewReplayClip:1', (request) => call((done) => clients.broadcastsBroadcastValidationClient.explainBroadcastDecision(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.broadcasts.v1.ReplayClipWorkflowService/StartReplayClipWorkflow:0', (request) => call((done) => clients.admissionsGateScanCalculationClient.estimateGateScan(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.broadcasts.v1.StreamKeyApprovalService/SubmitStreamKey:1', (request) => call((done) => clients.broadcastsReplayClipValidationClient.previewReplayClip(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.broadcasts.v1.StreamKeyQueryService/BrowseStreamKeys:0', (request) => call((done) => clients.broadcastsBroadcastValidationClient.checkBroadcastEligibility(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.broadcasts.v1.StreamKeyValidationService/GetStreamKeyRules:1', (request) => call((done) => clients.broadcastsStreamKeyCalculationClient.estimateStreamKey(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
