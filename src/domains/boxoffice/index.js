'use strict';

/**
 * Register the boxoffice team's public gRPC services and workers.
 */

const { deadline, metadataFor } = require('../../platform/credentials');
const { acme } = require('../../platform/protos');
const { call } = require('../../platform/rpc');
const { BoxOfficeSaleApprovalService } = require('./box-office-sale-approval.service');
const { BoxOfficeSaleService } = require('./box-office-sale.service');
const { createBoxOfficeSaleCalculationHandlers } = require('./box-office-sale/box-office-sale-calculation.handlers');
const { createBoxOfficeSaleValidationHandlers } = require('./box-office-sale/box-office-sale-validation.handlers');
const { CashDrawerApprovalService } = require('./cash-drawer-approval.service');
const { CashDrawerQueryService } = require('./cash-drawer-query.service');
const { createCashDrawerCalculationHandlers } = require('./cash-drawer/cash-drawer-calculation.handlers');
const { createCashDrawerValidationHandlers } = require('./cash-drawer/cash-drawer-validation.handlers');
const { GuestListApprovalService } = require('./guest-list-approval.service');
const { GuestListReportingService } = require('./guest-list-reporting.service');
const { createGuestListCalculationHandlers } = require('./guest-list/guest-list-calculation.handlers');
const { createGuestListValidationHandlers } = require('./guest-list/guest-list-validation.handlers');
const boxOfficeSaleQueryHandlers = require('./handlers/box-office-sale-query');
const boxOfficeSaleWorkflowHandlers = require('./handlers/box-office-sale-workflow');
const cashDrawerLifecycleHandlers = require('./handlers/cash-drawer-lifecycle');
const cashDrawerWorkflowHandlers = require('./handlers/cash-drawer-workflow');
const guestListLifecycleHandlers = require('./handlers/guest-list-lifecycle');
const guestListWorkflowHandlers = require('./handlers/guest-list-workflow');
const store = require('./store');

function register(server, dependencies) {
  const scope = { ...dependencies, store };
  server.addService(acme.boxoffice.v1.BoxOfficeSaleApprovalService.service, new BoxOfficeSaleApprovalService(scope));
  server.addService(acme.boxoffice.v1.BoxOfficeSaleCalculationService.service, createBoxOfficeSaleCalculationHandlers(scope));
  server.addService(acme.boxoffice.v1.BoxOfficeSaleQueryService.service, boxOfficeSaleQueryHandlers);
  server.addService(acme.boxoffice.v1.BoxOfficeSaleService.service, new BoxOfficeSaleService(scope));
  server.addService(acme.boxoffice.v1.BoxOfficeSaleValidationService.service, createBoxOfficeSaleValidationHandlers(scope));
  server.addService(acme.boxoffice.v1.BoxOfficeSaleWorkflowService.service, boxOfficeSaleWorkflowHandlers);
  server.addService(acme.boxoffice.v1.CashDrawerApprovalService.service, new CashDrawerApprovalService(scope));
  server.addService(acme.boxoffice.v1.CashDrawerCalculationService.service, createCashDrawerCalculationHandlers(scope));
  server.addService(acme.boxoffice.v1.CashDrawerLifecycleService.service, cashDrawerLifecycleHandlers);
  server.addService(acme.boxoffice.v1.CashDrawerQueryService.service, new CashDrawerQueryService(scope));
  server.addService(acme.boxoffice.v1.CashDrawerValidationService.service, createCashDrawerValidationHandlers(scope));
  server.addService(acme.boxoffice.v1.CashDrawerWorkflowService.service, cashDrawerWorkflowHandlers);
  server.addService(acme.boxoffice.v1.GuestListApprovalService.service, new GuestListApprovalService(scope));
  server.addService(acme.boxoffice.v1.GuestListCalculationService.service, createGuestListCalculationHandlers(scope));
  server.addService(acme.boxoffice.v1.GuestListLifecycleService.service, guestListLifecycleHandlers);
  server.addService(acme.boxoffice.v1.GuestListReportingService.service, new GuestListReportingService(scope));
  server.addService(acme.boxoffice.v1.GuestListValidationService.service, createGuestListValidationHandlers(scope));
  server.addService(acme.boxoffice.v1.GuestListWorkflowService.service, guestListWorkflowHandlers);
}

function startWorkers(dependencies) {
  const { bus, clients } = dependencies;
  bus.consume('acme.boxoffice.v1.BoxOfficeSaleApprovalService/DelegateBoxOfficeSaleApproval:1', (request) => call((done) => clients.venuesVenueExportClient.listVenueExports(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.BoxOfficeSaleCalculationService/QuoteBoxOfficeSale:1', (request) => call((done) => clients.identityUserValidationClient.lintUsers(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.BoxOfficeSaleQueryService/SummarizeBoxOfficeSales:1', (request) => call((done) => clients.concessionsMenuItemValidationClient.validateMenuItem(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.BoxOfficeSaleValidationService/CheckBoxOfficeSaleEligibility:2', (request) => call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.recalculateBoxOfficeSales(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.BoxOfficeSaleWorkflowService/CancelBoxOfficeSaleWorkflow:0', (request) => call((done) => clients.venuesVenueSectionNotificationClient.unsubscribeVenueSectionUpdates(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.CashDrawerApprovalService/ApproveCashDrawer:1', (request) => call((done) => clients.surveysSurveyQueryClient.countSurveys(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.CashDrawerCalculationService/EstimateCashDrawer:0', (request) => call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.explainBoxOfficeSaleCalculation(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.CashDrawerLifecycleService/SuspendCashDrawer:2', (request) => call((done) => clients.venuesVenueHistoryClient.recordVenueVersion(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.CashDrawerQueryService/SummarizeCashDrawers:0', (request) => call((done) => clients.boxofficeGuestListValidationClient.scoreGuestList(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.CashDrawerWorkflowService/PauseCashDrawerWorkflow:2', (request) => call((done) => clients.boxofficeBoxOfficeSaleWorkflowClient.inspectBoxOfficeSaleWorkflow(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.GuestListApprovalService/ListPendingGuestLists:0', (request) => call((done) => clients.concessionsMenuItemApprovalClient.approveMenuItem(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.GuestListCalculationService/RecalculateGuestLists:0', (request) => call((done) => clients.featureflagsFeatureFlagQueryClient.suggestFeatureFlags(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.GuestListLifecycleService/RenewGuestList:0', (request) => call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.explainBoxOfficeSaleCalculation(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
  bus.consume('acme.boxoffice.v1.GuestListReportingService/ScheduleGuestListReport:1', (request) => call((done) => clients.boxofficeBoxOfficeSaleValidationClient.lintBoxOfficeSales(
    request, metadataFor(request.context), { deadline: deadline() }, done)));
}

module.exports = { register, startWorkers };
