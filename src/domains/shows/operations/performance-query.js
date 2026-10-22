'use strict';

/**
 * Answers read-only questions about performances, for screens and reports. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { auditAuditEventQueryClient, showsLineupValidationClient } = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Lists performances a page at a time.
 */
async function browsePerformances(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'BrowsePerformances', request);
  logger.info('browse_performances', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Streams every matching performance.
 */
async function exportPerformances(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ExportPerformances', request);
  logger.info('export_performances', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { destination: { region: (request?.filter?.region ?? '') } },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => showsLineupValidationClient.scoreLineup(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.filter?.created_after,
      labels: { ...(request?.filter?.labels ?? {}) },
      region: (reply1?.lineup?.billing_address?.region ?? ''),
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => auditAuditEventQueryClient.countAuditEvents(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.destination_uri ?? '') });
  return assemble(
    {
      result: {
        reference_ids: (reply1?.result?.reference_ids ?? []),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      result: {
        message: (reply2?.result?.message ?? ''),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      performance: { location: { region: (request?.filter?.region ?? '') } },
    },
  );
}

/**
 * Lists performances a page at a time.
 */
async function findPerformances(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'FindPerformances', request);
  logger.info('find_performances', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Returns a performance.
 */
async function lookupPerformance(request) {
  validateContext(request.context);
  if (!request.performance_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performance_id is required');
  }
  await store.record(request.context.tenant_id, 'LookupPerformance', request);
  logger.info('lookup_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { performance: { performance_id: (request?.performance_id ?? '') } };
}

/**
 * Returns a performance.
 */
async function peekPerformance(request) {
  validateContext(request.context);
  if (!request.performance_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performance_id is required');
  }
  await store.record(request.context.tenant_id, 'PeekPerformance', request);
  logger.info('peek_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { performance: { performance_id: (request?.performance_id ?? '') } };
}

/**
 * Returns several performances at once.
 */
async function resolvePerformances(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ResolvePerformances', request);
  logger.info('resolve_performances', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return {};
}

module.exports = { browsePerformances, exportPerformances, findPerformances, lookupPerformance, peekPerformance, resolvePerformances };
