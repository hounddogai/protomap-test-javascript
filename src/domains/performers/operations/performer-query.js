'use strict';

/**
 * Answers read-only questions about performers, for screens and reports. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsWristbandValidationClient,
  broadcastsBroadcastClient,
  broadcastsReplayClipCalculationClient,
  concessionsConcessionStandValidationClient,
  documentsDocumentValidationClient,
  notificationsNotificationClient,
  performersRiderValidationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Lists performers a page at a time.
 */
async function browsePerformers(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'BrowsePerformers', request);
  logger.info('browse_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Counts performers by status.
 */
async function countPerformers(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'CountPerformers', request);
  logger.info('count_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    document: { billing_address: { region: (request?.filter?.region ?? '') } },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => documentsDocumentValidationClient.lintDocuments(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    rider: {
      audit: {
        updated_at: reply1?.document?.audit?.updated_at,
        updated_by: (reply1?.document?.audit?.updated_by ?? ''),
      },
      owner: { display_name: (reply1?.document?.counterparty?.display_name ?? '') },
    },
  };
  const reply2 = await withRetry(() => call((done) => performersRiderValidationClient.validateRider(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    { result: { status: (reply1?.outcome?.status ?? '') } },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
  );
}

/**
 * Streams every matching performer.
 */
async function exportPerformers(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ExportPerformers', request);
  logger.info('export_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { labels: { ...(request?.filter?.labels ?? {}) } },
  };
  const task1 = deferred(() => call((done) => notificationsNotificationClient.listNotifications(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  const request2 = {
    context: request?.context,
    details: { locale: (request?.context?.locale ?? '') },
    rider: {
      audit: { created_at: request?.filter?.created_after },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const reply2 = await bus.request('acme.performers.v1.PerformerQueryService/ExportPerformers:1', request2);
  const request3 = {
    context: request?.context,
    rider: {
      audit: {
        updated_at: reply2?.rider?.audit?.updated_at,
        updated_by: (reply2?.rider?.audit?.updated_by ?? ''),
      },
      owner: { email: (reply2?.rider?.owner?.email ?? '') },
    },
  };
  const reply3 = await runInContext(request.context, () => call((done) => performersRiderValidationClient.previewRider(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  const request4 = {
    context: request?.context,
    document: {
      audit: { created_by: (reply3?.rider?.audit?.created_by ?? '') },
      lines: [{
        tax: {
          amount: (reply3?.rider?.action === 'discount' ? reply3?.rider.discount : undefined)?.amount,
        },
      }],
    },
  };
  const reply4 = await call((done) => clients.documentsDocumentValidationClient.previewDocument(request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.destination_uri ?? '') });
  return assemble(
    {
      result: {
        message: (reply1?.result?.message ?? ''),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      performer: {
        audit: { updated_at: reply2?.rider?.audit?.updated_at },
        contact: { email: (reply2?.rider?.owner?.email ?? '') },
      },
    },
    {
      performer: { relationship_manager: { email: (reply3?.rider?.owner?.email ?? '') } },
    },
    {
      performer: {
        primary_address: { postal_code: (reply4?.document?.billing_address?.postal_code ?? '') },
      },
    },
    {
      performer: { primary_address: { region: (request?.filter?.region ?? '') } },
    },
  );
}

/**
 * Lists performers a page at a time.
 */
async function findPerformers(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'FindPerformers', request);
  logger.info('find_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { destination: { region: (request?.filter?.region ?? '') } },
    rider: {
      audit: { created_at: request?.filter?.created_after },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    performersRiderValidationClient.scoreRider(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    context: request?.context,
    replay_clip: {
      audit: { updated_by: (reply1?.rider?.audit?.updated_by ?? '') },
      owner: { user_id: (reply1?.rider?.owner?.user_id ?? '') },
    },
  };
  const simulateReplayClip2 = promisify(broadcastsReplayClipCalculationClient.simulateReplayClip.bind(broadcastsReplayClipCalculationClient));
  const reply2 = await simulateReplayClip2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    {
      performers: [{ audit: { created_at: reply1?.rider?.audit?.created_at } }],
    },
    {
      performers: [{
        primary_address: {
          lines: ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.lines ?? []),
          locality: ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.locality ?? ''),
        },
      }],
    },
  );
}

/**
 * Returns a performer.
 */
async function lookupPerformer(request) {
  validateContext(request.context);
  if (!request.performer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performer_id is required');
  }
  await store.record(request.context.tenant_id, 'LookupPerformer', request);
  logger.info('lookup_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    broadcast: {
      organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
      participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
    },
    context: request?.context,
  };
  const reply1 = await call((done) => broadcastsBroadcastClient.createBroadcast(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { contact: { email: (reply1?.broadcast?.organizer?.email ?? '') } },
    rider: { audit: { updated_by: (reply1?.broadcast?.audit?.updated_by ?? '') } },
  };
  const method2 = 'normalizeRider';
  const reply2 = await call((done) => performersRiderValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    {
      performer: { audit: { created_by: (reply2?.rider?.audit?.created_by ?? '') } },
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
    },
    { performer: { performer_id: (request?.performer_id ?? '') } },
  );
}

/**
 * Returns a performer.
 */
async function peekPerformer(request) {
  validateContext(request.context);
  if (!request.performer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performer_id is required');
  }
  await store.record(request.context.tenant_id, 'PeekPerformer', request);
  logger.info('peek_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { locale: (request?.context?.locale ?? '') },
    rider: {
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const client1 = clients.registry.get('acme.performers.v1.RiderValidationService');
  const reply1 = await call((done) => client1.lintRiders(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    rider: {
      owner: {
        display_name: (reply1?.rider?.owner?.display_name ?? ''),
        user_id: (reply1?.rider?.owner?.user_id ?? ''),
      },
    },
  };
  const send2 = performersRiderValidationClient.previewRider.bind(performersRiderValidationClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  return assemble(
    {
      performer: {
        relationship_manager: { display_name: (reply1?.rider?.owner?.display_name ?? '') },
      },
    },
    {
      performer: { audit: { updated_by: (reply2?.rider?.audit?.updated_by ?? '') } },
    },
    { performer: { performer_id: (request?.performer_id ?? '') } },
  );
}

/**
 * Lists performers a page at a time.
 */
async function suggestPerformers(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'SuggestPerformers', request);
  logger.info('suggest_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_stand: {
      address: { region: (request?.filter?.region ?? '') },
      audit: { created_at: request?.filter?.created_after },
      owner: { user_id: (request?.context?.actor?.user_id ?? '') },
    },
    context: request?.context,
    details: { contact: { email: (request?.context?.actor?.email ?? '') } },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => concessionsConcessionStandValidationClient.validateConcessionStand(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    performers: [{
      audit: { updated_at: reply1?.concession_stand?.audit?.updated_at },
      primary_address: {
        country_code: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.country_code ?? ''),
      },
    }],
  };
}

/**
 * Summarizes a performer.
 */
async function summarizePerformers(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'SummarizePerformers', request);
  logger.info('summarize_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    reason: (input1?.reason ?? ''),
  };
  const reply1 = await call((done) => broadcastsReplayClipCalculationClient.compareReplayClipOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    wristband: {
      address: {
        country_code: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.country_code ?? ''),
      },
      components: { primary: {
        description: (reply1?.replay_clip?.components?.primary?.description ?? ''),
      } },
      dimensions: { width: (reply1?.replay_clip?.dimensions?.width ?? 0) },
      owner: { email: (reply1?.replay_clip?.owner?.email ?? '') },
    },
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => admissionsWristbandValidationClient.lintWristbands(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    rider: { audit: { created_by: (reply1?.replay_clip?.audit?.created_by ?? '') } },
  };
  const copied3 = structuredClone(request3);
  const reply3 = await call((done) => performersRiderValidationClient.validateRider(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.cron ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        details: (reply1?.result?.details ?? {}),
      },
    },
    { result: { details: (reply2?.result?.details ?? {}) } },
    { result: { status: (reply3?.result?.status ?? '') } },
    { period: request?.details?.period },
  );
}

module.exports = { browsePerformers, countPerformers, exportPerformers, findPerformers, lookupPerformer, peekPerformer, suggestPerformers, summarizePerformers };
