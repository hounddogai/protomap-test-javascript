'use strict';

/**
 * Register the sponsorships team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const { ActivationCalculationService } = require('./activation-calculation.service');
const { ActivationWorkflowService } = require('./activation-workflow.service');
const { createActivationLifecycleHandlers } = require('./activation/activation-lifecycle.handlers');
const activationApprovalHandlers = require('./handlers/activation-approval');
const activationValidationHandlers = require('./handlers/activation-validation');
const sponsorCalculationHandlers = require('./handlers/sponsor-calculation');
const sponsorValidationHandlers = require('./handlers/sponsor-validation');
const sponsorshipDealCalculationHandlers = require('./handlers/sponsorship-deal-calculation');
const sponsorshipDealWorkflowHandlers = require('./handlers/sponsorship-deal-workflow');
const { SponsorQueryService } = require('./sponsor-query.service');
const { SponsorWorkflowService } = require('./sponsor-workflow.service');
const { createSponsorApprovalHandlers } = require('./sponsor/sponsor-approval.handlers');
const { createSponsorHandlers } = require('./sponsor/sponsor.handlers');
const { SponsorshipDealQueryService } = require('./sponsorship-deal-query.service');
const { createSponsorshipDealApprovalHandlers } = require('./sponsorship-deal/sponsorship-deal-approval.handlers');
const { createSponsorshipDealValidationHandlers } = require('./sponsorship-deal/sponsorship-deal-validation.handlers');
const store = require('./store');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.sponsorships.v1.ActivationApprovalService.service, activationApprovalHandlers);
  server.addService(acme.sponsorships.v1.ActivationCalculationService.service, new ActivationCalculationService(scope));
  server.addService(acme.sponsorships.v1.ActivationLifecycleService.service, createActivationLifecycleHandlers(scope));
  server.addService(acme.sponsorships.v1.ActivationValidationService.service, activationValidationHandlers);
  server.addService(acme.sponsorships.v1.ActivationWorkflowService.service, new ActivationWorkflowService(scope));
  server.addService(acme.sponsorships.v1.SponsorApprovalService.service, createSponsorApprovalHandlers(scope));
  server.addService(acme.sponsorships.v1.SponsorCalculationService.service, sponsorCalculationHandlers);
  server.addService(acme.sponsorships.v1.SponsorQueryService.service, new SponsorQueryService(scope));
  server.addService(acme.sponsorships.v1.SponsorService.service, createSponsorHandlers(scope));
  server.addService(acme.sponsorships.v1.SponsorValidationService.service, sponsorValidationHandlers);
  server.addService(acme.sponsorships.v1.SponsorWorkflowService.service, new SponsorWorkflowService(scope));
  server.addService(acme.sponsorships.v1.SponsorshipDealApprovalService.service, createSponsorshipDealApprovalHandlers(scope));
  server.addService(acme.sponsorships.v1.SponsorshipDealCalculationService.service, sponsorshipDealCalculationHandlers);
  server.addService(acme.sponsorships.v1.SponsorshipDealQueryService.service, new SponsorshipDealQueryService(scope));
  server.addService(acme.sponsorships.v1.SponsorshipDealValidationService.service, createSponsorshipDealValidationHandlers(scope));
  server.addService(acme.sponsorships.v1.SponsorshipDealWorkflowService.service, sponsorshipDealWorkflowHandlers);
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.sponsorships.v1.ActivationApprovalService/ApproveActivation:0', (request) => call((done) => clients.sponsorshipsSponsorshipDealApprovalClient.approveSponsorshipDeal(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.ActivationCalculationService/CalculateActivation:2', (request) => call((done) => clients.broadcastsReplayClipCalculationClient.calculateReplayClip(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.ActivationLifecycleService/ReinstateActivation:1', (request) => call((done) => clients.sponsorshipsActivationCalculationClient.calculateActivation(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.ActivationValidationService/NormalizeActivation:1', (request) => call((done) => clients.venuesVenuePolicySyncClient.resolveVenuePolicyConflict(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.ActivationWorkflowService/RetryActivationWorkflow:1', (request) => call((done) => clients.boxofficeCashDrawerValidationClient.checkCashDrawerEligibility(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.SponsorApprovalService/EscalateSponsor:1', (request) => call((done) => clients.sponsorshipsSponsorValidationClient.validateSponsor(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.SponsorCalculationService/ExplainSponsorCalculation:0', (request) => call((done) => clients.venuesVenuePolicyValidationClient.normalizeVenuePolicy(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.SponsorQueryService/ResolveSponsors:0', (request) => call((done) => clients.sponsorshipsSponsorshipDealValidationClient.checkSponsorshipDealEligibility(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.SponsorService/ListSponsors:0', (request) => call((done) => clients.featureflagsFeatureFlagQueryClient.summarizeFeatureFlags(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.SponsorValidationService/ValidateSponsor:0', (request) => call((done) => clients.sponsorshipsSponsorshipDealCalculationClient.calculateSponsorshipDeal(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.SponsorWorkflowService/RetrySponsorWorkflow:3', (request) => call((done) => clients.admissionsGateScanValidationClient.normalizeGateScan(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.SponsorshipDealApprovalService/SubmitSponsorshipDeal:0', (request) => call((done) => clients.metricsMetricSeriesQueryClient.browseMetricSerieses(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.SponsorshipDealQueryService/ExportSponsorshipDeals:3', (request) => call((done) => clients.venuesVenueSectionNotificationClient.sendVenueSectionTestNotice(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.SponsorshipDealValidationService/NormalizeSponsorshipDeal:0', (request) => call((done) => clients.sponsorshipsActivationCalculationClient.simulateActivation(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.sponsorships.v1.SponsorshipDealWorkflowService/ListSponsorshipDealWorkflows:0', (request) => call((done) => clients.sponsorshipsSponsorshipDealValidationClient.checkSponsorshipDealEligibility(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
