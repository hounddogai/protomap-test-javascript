'use strict';

/**
 * Answers read-only questions about box office sales, for screens and reports. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionCalculationClient,
  boxofficeBoxOfficeSaleCalculationClient,
  boxofficeBoxOfficeSaleValidationClient,
  boxofficeCashDrawerCalculationClient,
  boxofficeGuestListValidationClient,
  concessionsConcessionOrderCalculationClient,
  concessionsConcessionStandClient,
  concessionsMenuItemValidationClient,
  featureflagsFeatureFlagValidationClient,
  identityUserQueryClient,
  searchSearchIndexValidationClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { upload } = require('../../../platform/streams');
const store = require('../store');

/**
 * Lists box office sales a page at a time.
 */
async function browseBoxOfficeSales(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'BrowseBoxOfficeSales', request);
  logger.info('browse_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    box_office_sale: { billing_address: { region: (request?.filter?.region ?? '') } },
    context: request?.context,
    details: { contact: { email: (request?.context?.actor?.email ?? '') } },
  };
  const reply1 = await new Promise((resolve, reject) => {
    boxofficeBoxOfficeSaleValidationClient.lintBoxOfficeSales(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    context: request?.context,
    details: {
      destination: {
        country_code: (reply1?.box_office_sale?.billing_address?.country_code ?? ''),
      },
    },
  };
  const calculateAdmission2 = promisify(admissionsAdmissionCalculationClient.calculateAdmission.bind(admissionsAdmissionCalculationClient));
  const reply2 = await calculateAdmission2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    {
      box_office_sales: [{
        billing_address: {
          recipient: (reply1?.box_office_sale?.billing_address?.recipient ?? ''),
        },
        metadata: (reply1?.box_office_sale?.metadata ?? {}),
      }],
    },
    {
      result: {
        correlation_id: (reply2?.admission?.correlation_id ?? ''),
        message: (reply2?.result?.message ?? ''),
      },
    },
  );
}

/**
 * Counts box office sales by status.
 */
async function countBoxOfficeSales(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'CountBoxOfficeSales', request);
  logger.info('count_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Streams every matching box office sale.
 */
async function exportBoxOfficeSales(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ExportBoxOfficeSales', request);
  logger.info('export_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.filter?.created_after,
      labels: { ...(request?.filter?.labels ?? {}) },
      owner_id: (request?.filter?.owner_id ?? ''),
      region: (request?.filter?.region ?? ''),
    },
  };
  const reply1 = await call((done) => identityUserQueryClient.countUsers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    box_office_sale: {
      audit: { updated_at: request?.filter?.created_after },
      billing_address: { region: (request?.filter?.region ?? '') },
    },
    context: request?.context,
    details: { locale: (request?.context?.locale ?? '') },
  };
  const method2 = 'normalizeBoxOfficeSale';
  const reply2 = await call((done) => boxofficeBoxOfficeSaleValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    box_office_sale: {
      counterparty: {
        currency_code: (reply2?.box_office_sale?.counterparty?.currency_code ?? ''),
      },
    },
    context: request?.context,
  };
  const client3 = clients.registry.get('acme.boxoffice.v1.BoxOfficeSaleCalculationService');
  const reply3 = await call((done) => client3.estimateBoxOfficeSale(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.destination_uri ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        details: (reply1?.result?.details ?? {}),
      },
    },
    {
      box_office_sale: {
        billing_address: {
          country_code: (reply2?.box_office_sale?.billing_address?.country_code ?? ''),
          lines: (reply2?.box_office_sale?.billing_address?.lines ?? []),
        },
      },
    },
    { box_office_sale: { due_at: reply3?.box_office_sale?.due_at } },
    {
      box_office_sale: { billing_address: { region: (request?.filter?.region ?? '') } },
    },
  );
}

/**
 * Lists box office sales a page at a time.
 */
async function findBoxOfficeSales(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'FindBoxOfficeSales', request);
  logger.info('find_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: { email: (request?.context?.actor?.email ?? '') },
      locale: (request?.context?.locale ?? ''),
    },
    guest_list: {
      billing_address: { region: (request?.filter?.region ?? '') },
      issued_at: request?.filter?.created_after,
    },
  };
  const send1 = boxofficeGuestListValidationClient.validateGuestList.bind(boxofficeGuestListValidationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    feature_flag: {
      audit: {
        created_at: reply1?.guest_list?.audit?.created_at,
        etag: (reply1?.guest_list?.audit?.etag ?? ''),
      },
    },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => featureflagsFeatureFlagValidationClient.previewFeatureFlag(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    {
      box_office_sales: [{
        billing_address: { locality: (reply1?.guest_list?.billing_address?.locality ?? '') },
      }],
    },
    {
      box_office_sales: [{
        audit: { created_at: reply2?.feature_flag?.audit?.created_at },
        counterparty: { display_name: (reply2?.feature_flag?.owner?.display_name ?? '') },
      }],
    },
  );
}

/**
 * Returns a box office sale.
 */
async function lookupBoxOfficeSale(request) {
  validateContext(request.context);
  if (!request.box_office_sale_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'box_office_sale_id is required');
  }
  await store.record(request.context.tenant_id, 'LookupBoxOfficeSale', request);
  logger.info('lookup_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    box_office_sale: {
      audit: { created_at: input1?.context?.deadline },
      box_office_sale_id: (input1?.box_office_sale_id ?? ''),
    },
    context: input1?.context,
  };
  const reply1 = await call((done) => boxofficeBoxOfficeSaleCalculationClient.quoteBoxOfficeSale(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  return assemble(
    { box_office_sale: { total: reply1?.box_office_sale?.total } },
    {
      box_office_sale: { box_office_sale_id: (request?.box_office_sale_id ?? '') },
    },
  );
}

/**
 * Returns a box office sale.
 */
async function peekBoxOfficeSale(request) {
  validateContext(request.context);
  if (!request.box_office_sale_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'box_office_sale_id is required');
  }
  await store.record(request.context.tenant_id, 'PeekBoxOfficeSale', request);
  logger.info('peek_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      owner_id: (request?.box_office_sale_id ?? ''),
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => identityUserQueryClient.findUsers(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    {
      box_office_sale: { box_office_sale_id: (request?.box_office_sale_id ?? '') },
    },
  );
}

/**
 * Returns several box office sales at once.
 */
async function resolveBoxOfficeSales(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ResolveBoxOfficeSales', request);
  logger.info('resolve_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    search_index: {
      audit: { updated_at: request?.context?.deadline },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => searchSearchIndexValidationClient.scoreSearchIndex(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  const request2 = {
    cash_drawer: {
      audit: { created_at: reply1?.search_index?.audit?.created_at },
      contact: { email: (reply1?.search_index?.owner?.email ?? '') },
    },
    context: request?.context,
  };
  const merged2 = Object.assign({}, request2, { context: request2.context });
  const reply2 = await call((done) => boxofficeCashDrawerCalculationClient.explainCashDrawerCalculation(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
  return assemble(
    {
      box_office_sales: [{
        audit: { etag: (reply1?.search_index?.audit?.etag ?? '') },
        billing_address: {
          lines: ((reply1?.search_index?.locator === 'address' ? reply1?.search_index.address : undefined)?.lines ?? []),
        },
      }],
    },
    { result: { details: (reply2?.result?.details ?? {}) } },
  );
}

/**
 * Lists box office sales a page at a time.
 */
async function suggestBoxOfficeSales(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'SuggestBoxOfficeSales', request);
  logger.info('suggest_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_order: {
      billing_address: { region: (request?.filter?.region ?? '') },
      due_at: request?.filter?.created_after,
    },
    context: request?.context,
  };
  const reply1 = await withRetry(() => call((done) => concessionsConcessionOrderCalculationClient.compareConcessionOrderOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    box_office_sales: [{
      billing_address: {
        recipient: (reply1?.concession_order?.billing_address?.recipient ?? ''),
      },
    }],
  };
}

/**
 * Summarizes a box office sale.
 */
async function summarizeBoxOfficeSales(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'SummarizeBoxOfficeSales', request);
  logger.info('summarize_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    reason: (request?.reason ?? ''),
  };
  const task1 = deferred(() => call((done) => boxofficeGuestListValidationClient.scoreGuestList(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  const request2 = {
    context: request?.context,
    menu_item: {
      address: { recipient: (reply1?.guest_list?.billing_address?.recipient ?? '') },
      audit: { created_at: reply1?.guest_list?.audit?.created_at },
    },
  };
  const reply2 = await bus.request('acme.boxoffice.v1.BoxOfficeSaleQueryService/SummarizeBoxOfficeSales:1', request2);
  const request3 = {
    context: request?.context,
    record: {
      address: { lines: [...(reply1?.guest_list?.billing_address?.lines ?? [])] },
      owner: { user_id: (reply2?.menu_item?.owner?.user_id ?? '') },
    },
  };
  const reply3 = await upload(
    (done) => concessionsConcessionStandClient.batchCreateConcessionStands(metadataFor(request3.context), { deadline: deadline() }, done),
    [request3],
  );
  logger.info('local_audit', { value: (request?.details?.cron ?? '') });
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        message: (reply1?.result?.message ?? ''),
      },
    },
    { result: { details: (reply2?.result?.details ?? {}) } },
    { period: request?.details?.period },
  );
}

module.exports = { browseBoxOfficeSales, countBoxOfficeSales, exportBoxOfficeSales, findBoxOfficeSales, lookupBoxOfficeSale, peekBoxOfficeSale, resolveBoxOfficeSales, suggestBoxOfficeSales, summarizeBoxOfficeSales };
