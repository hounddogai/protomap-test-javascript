'use strict';

/**
 * Register the admissions team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const { AdmissionCalculationService } = require('./admission-calculation.service');
const { AdmissionService } = require('./admission.service');
const { createAdmissionLifecycleHandlers } = require('./admission/admission-lifecycle.handlers');
const { createAdmissionValidationHandlers } = require('./admission/admission-validation.handlers');
const { GateScanApprovalService } = require('./gate-scan-approval.service');
const { GateScanQueryService } = require('./gate-scan-query.service');
const { createGateScanCalculationHandlers } = require('./gate-scan/gate-scan-calculation.handlers');
const { createGateScanValidationHandlers } = require('./gate-scan/gate-scan-validation.handlers');
const admissionApprovalHandlers = require('./handlers/admission-approval');
const admissionQueryHandlers = require('./handlers/admission-query');
const admissionWorkflowHandlers = require('./handlers/admission-workflow');
const gateScanLifecycleHandlers = require('./handlers/gate-scan-lifecycle');
const gateScanWorkflowHandlers = require('./handlers/gate-scan-workflow');
const wristbandLifecycleHandlers = require('./handlers/wristband-lifecycle');
const wristbandWorkflowHandlers = require('./handlers/wristband-workflow');
const store = require('./store');
const { WristbandApprovalService } = require('./wristband-approval.service');
const { WristbandReportingService } = require('./wristband-reporting.service');
const { createWristbandCalculationHandlers } = require('./wristband/wristband-calculation.handlers');
const { createWristbandValidationHandlers } = require('./wristband/wristband-validation.handlers');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.admissions.v1.AdmissionApprovalService.service, admissionApprovalHandlers);
  server.addService(acme.admissions.v1.AdmissionCalculationService.service, new AdmissionCalculationService(scope));
  server.addService(acme.admissions.v1.AdmissionLifecycleService.service, createAdmissionLifecycleHandlers(scope));
  server.addService(acme.admissions.v1.AdmissionQueryService.service, admissionQueryHandlers);
  server.addService(acme.admissions.v1.AdmissionService.service, new AdmissionService(scope));
  server.addService(acme.admissions.v1.AdmissionValidationService.service, createAdmissionValidationHandlers(scope));
  server.addService(acme.admissions.v1.AdmissionWorkflowService.service, admissionWorkflowHandlers);
  server.addService(acme.admissions.v1.GateScanApprovalService.service, new GateScanApprovalService(scope));
  server.addService(acme.admissions.v1.GateScanCalculationService.service, createGateScanCalculationHandlers(scope));
  server.addService(acme.admissions.v1.GateScanLifecycleService.service, gateScanLifecycleHandlers);
  server.addService(acme.admissions.v1.GateScanQueryService.service, new GateScanQueryService(scope));
  server.addService(acme.admissions.v1.GateScanValidationService.service, createGateScanValidationHandlers(scope));
  server.addService(acme.admissions.v1.GateScanWorkflowService.service, gateScanWorkflowHandlers);
  server.addService(acme.admissions.v1.WristbandApprovalService.service, new WristbandApprovalService(scope));
  server.addService(acme.admissions.v1.WristbandCalculationService.service, createWristbandCalculationHandlers(scope));
  server.addService(acme.admissions.v1.WristbandLifecycleService.service, wristbandLifecycleHandlers);
  server.addService(acme.admissions.v1.WristbandReportingService.service, new WristbandReportingService(scope));
  server.addService(acme.admissions.v1.WristbandValidationService.service, createWristbandValidationHandlers(scope));
  server.addService(acme.admissions.v1.WristbandWorkflowService.service, wristbandWorkflowHandlers);
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.admissions.v1.AdmissionApprovalService/RejectAdmission:0', (request) => call((done) => clients.venuesVenuePolicyHistoryClient.compareVenuePolicyVersions(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.AdmissionLifecycleService/TerminateAdmission:1', (request) => call((done) => clients.metricsMetricSeriesClient.searchMetricSerieses(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.AdmissionService/DeleteAdmission:2', (request) => call((done) => clients.sponsorshipsSponsorValidationClient.normalizeSponsor(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.AdmissionValidationService/PreviewAdmission:0', (request) => call((done) => clients.admissionsAdmissionCalculationClient.compareAdmissionOptions(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.AdmissionWorkflowService/RetryAdmissionWorkflow:0', (request) => call((done) => clients.admissionsWristbandReportingClient.scheduleWristbandReport(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.GateScanApprovalService/RecallGateScan:0', (request) => call((done) => clients.admissionsAdmissionLifecycleClient.activateAdmission(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.GateScanCalculationService/ExplainGateScanCalculation:0', (request) => call((done) => clients.admissionsAdmissionCalculationClient.compareAdmissionOptions(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.GateScanLifecycleService/ExpireGateScan:3', (request) => call((done) => clients.searchSearchIndexClient.createSearchIndex(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.GateScanQueryService/FindGateScans:0', (request) => call((done) => clients.admissionsGateScanCalculationClient.recalculateGateScans(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.GateScanValidationService/ExplainGateScanDecision:0', (request) => call((done) => clients.sponsorshipsSponsorshipDealValidationClient.normalizeSponsorshipDeal(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.WristbandApprovalService/ApproveWristband:2', (request) => call((done) => clients.admissionsGateScanApprovalClient.listPendingGateScans(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.WristbandCalculationService/CalculateWristband:1', (request) => call((done) => clients.searchSearchIndexQueryClient.findSearchIndexes(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.WristbandLifecycleService/ReinstateWristband:0', (request) => call((done) => clients.shippingShipmentCalculationClient.recalculateShipments(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.WristbandReportingService/ListWristbandReports:1', (request) => call((done) => clients.patientsPatientQueryClient.findPatients(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.WristbandValidationService/PreviewWristband:0', (request) => call((done) => clients.admissionsAdmissionCalculationClient.explainAdmissionCalculation(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.admissions.v1.WristbandWorkflowService/ResumeWristbandWorkflow:2', (request) => call((done) => clients.admissionsGateScanWorkflowClient.pauseGateScanWorkflow(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
