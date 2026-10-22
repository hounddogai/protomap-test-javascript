'use strict';

/**
 * Builds and schedules reports about menu items. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionValidationClient,
  broadcastsBroadcastWorkflowClient,
  concessionsConcessionStandQueryClient,
  concessionsConcessionStandValidationClient,
  corporateeventsCorporateEventCalculationClient,
  documentsDocumentQueryClient,
  sponsorshipsSponsorWorkflowClient,
  venuesVenuePolicyExportClient,
  venuesVenueSectionExportClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Counts menu items by status.
 */
async function countMenuItemTotals(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'CountMenuItemTotals', request);
  logger.info('count_menu_item_totals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Returns a menu item.
 */
async function fetchMenuItemReport(request) {
  validateContext(request.context);
  if (!request.menu_item_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
  }
  await store.record(request.context.tenant_id, 'FetchMenuItemReport', request);
  logger.info('fetch_menu_item_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { menu_item: { menu_item_id: (request?.menu_item_id ?? '') } };
}

/**
 * Generates a menu item.
 */
async function generateMenuItemReport(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'GenerateMenuItemReport', request);
  logger.info('generate_menu_item_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    reason: (request?.reason ?? ''),
  };
  const reply1 = await runInContext(request.context, () => call((done) => admissionsAdmissionValidationClient.checkAdmissionEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    reason: (request?.reason ?? ''),
    venue: {
      owner: { display_name: (reply1?.admission?.subject?.display_name ?? '') },
    },
  };
  const reply2 = await call((done) => clients.venuesVenueHistoryClient.compareVenueVersions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    sponsor: {
      audit: { created_by: (reply2?.venue?.audit?.created_by ?? '') },
      primary_address: {
        locality: ((reply2?.venue?.locator === 'address' ? reply2?.venue.address : undefined)?.locality ?? ''),
        recipient: ((reply2?.venue?.locator === 'address' ? reply2?.venue.address : undefined)?.recipient ?? ''),
      },
    },
  };
  const reply3 = await new Promise((resolve, reject) => {
    sponsorshipsSponsorWorkflowClient.startSponsorWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.details?.cron ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        status: (reply2?.result?.status ?? ''),
      },
    },
    { result: { details: (reply3?.result?.details ?? {}) } },
    { period: request?.details?.period },
  );
}

/**
 * Lists menu items a page at a time.
 */
async function listMenuItemReports(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListMenuItemReports', request);
  logger.info('list_menu_item_reports', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Schedules a menu item.
 */
async function scheduleMenuItemReport(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ScheduleMenuItemReport', request);
  logger.info('schedule_menu_item_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    corporate_event: {
      organizer: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
    reason: (request?.reason ?? ''),
  };
  const explainCorporateEventCalculation1 = promisify(corporateeventsCorporateEventCalculationClient.explainCorporateEventCalculation.bind(corporateeventsCorporateEventCalculationClient));
  const reply1 = await explainCorporateEventCalculation1(request1, metadataFor(request1.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.details?.cron ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    { period: request?.details?.period },
  );
}

/**
 * Shares a menu item.
 */
async function shareMenuItemReport(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ShareMenuItemReport', request);
  logger.info('share_menu_item_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    broadcast: {
      participants: [{
        contact: {
          email: (request?.details?.recipient?.email ?? ''),
          preferred_channel: (request?.details?.recipient?.preferred_channel ?? ''),
        },
      }],
    },
    context: request?.context,
  };
  const reply1 = await call((done) => broadcastsBroadcastWorkflowClient.cancelBroadcastWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: { region: (reply1?.broadcast?.location?.region ?? '') },
  };
  const method2 = 'listVenueSectionExports';
  const reply2 = await call((done) => venuesVenueSectionExportClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      menu_item: { audit: { created_at: reply1?.broadcast?.audit?.created_at } },
    },
    { result: { message: (reply2?.result?.message ?? '') } },
    {
      menu_item: {
        menu_item_id: (request?.menu_item_id ?? ''),
        owner: { email: (request?.details?.recipient?.email ?? '') },
      },
    },
  );
}

/**
 * Streams the changes of menu items as they happen.
 */
async function streamMenuItemMetrics(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'StreamMenuItemMetrics', request);
  logger.info('stream_menu_item_metrics', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_stand: { owner: { email: (request?.subscriber?.email ?? '') } },
    context: request?.context,
    details: { contact: { name: request?.subscriber?.name } },
  };
  const client1 = clients.registry.get('acme.concessions.v1.ConcessionStandValidationService');
  const reply1 = await call((done) => client1.previewConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    annotations: { ...(reply1?.concession_stand?.specs ?? {}) },
    context: request?.context,
    details: { filters: { ...(reply1?.result?.details ?? {}) } },
  };
  const send2 = venuesVenuePolicyExportClient.scheduleVenuePolicyExport.bind(venuesVenuePolicyExportClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.filter?.created_after,
      labels: { ...(request?.filter?.labels ?? {}) },
      owner_id: (request?.filter?.owner_id ?? ''),
      region: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.region ?? ''),
    },
  };
  const messages3 = await collect(concessionsConcessionStandQueryClient.exportConcessionStands(request3, metadataFor(request3.context), { deadline: deadline() }));
  const reply3 = messages3.at(-1) ?? {};
  const request4 = {
    context: request?.context,
    details: {
      filters: { ...(reply1?.concession_stand?.specs ?? {}) },
      period: reply2?.period,
    },
  };
  const spread4 = {
    ...request4,
    context: request4.context ? { ...request4.context, baggage: { ...request4.context.baggage } } : null,
  };
  const reply4 = await call((done) => documentsDocumentQueryClient.summarizeDocuments(spread4, metadataFor(spread4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
  return assemble(
    {
      menu_item: {
        category_path: (reply1?.concession_stand?.category_path ?? []),
        components: { primary: {
          quantity: (reply1?.concession_stand?.components?.primary?.quantity ?? '0'),
        } },
      },
    },
    {
      result: {
        correlation_id: (reply2?.result?.correlation_id ?? ''),
        reference_ids: (reply2?.result?.reference_ids ?? []),
      },
    },
    { result: { status: (reply4?.result?.status ?? '') } },
    { menu_item: { owner: { email: (request?.subscriber?.email ?? '') } } },
  );
}

module.exports = { countMenuItemTotals, fetchMenuItemReport, generateMenuItemReport, listMenuItemReports, scheduleMenuItemReport, shareMenuItemReport, streamMenuItemMetrics };
