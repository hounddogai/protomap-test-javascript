'use strict';

/**
 * Answers read-only questions about admissions, for screens and reports. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionCalculationClient,
  admissionsWristbandCalculationClient,
  admissionsWristbandValidationClient,
  authorizationAccessPolicyQueryClient,
  concessionsConcessionStandCalculationClient,
  identityUserValidationClient,
  searchSearchIndexClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Lists admissions a page at a time.
 */
async function browseAdmissions(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'BrowseAdmissions', request);
  logger.info('browse_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    user: { primary_address: { region: (request?.filter?.region ?? '') } },
  };
  const reply1 = await runInContext(request.context, () => call((done) => identityUserValidationClient.checkUserEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    admissions: [{ correlation_id: (reply1?.result?.correlation_id ?? '') }],
  };
}

/**
 * Counts admissions by status.
 */
async function countAdmissions(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'CountAdmissions', request);
  logger.info('count_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    broadcast: {
      audit: { updated_at: request?.filter?.created_after },
      location: { region: (request?.filter?.region ?? '') },
      organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
      participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
    },
    context: request?.context,
  };
  const reply1 = await call((done) => clients.broadcastsBroadcastValidationClient.normalizeBroadcast(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    result: {
      message: (reply1?.result?.message ?? ''),
      status: (reply1?.result?.status ?? ''),
    },
  };
}

/**
 * Streams every matching admission.
 */
async function exportAdmissions(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ExportAdmissions', request);
  logger.info('export_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    search_index: {
      address: { region: (request?.filter?.region ?? '') },
      audit: { updated_at: request?.filter?.created_after },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    searchSearchIndexClient.createSearchIndex(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    context: request?.context,
    wristband: {
      address: {
        country_code: ((reply1?.search_index?.locator === 'address' ? reply1?.search_index.address : undefined)?.country_code ?? ''),
        locality: ((reply1?.search_index?.locator === 'address' ? reply1?.search_index.address : undefined)?.locality ?? ''),
      },
      owner: { display_name: (reply1?.search_index?.owner?.display_name ?? '') },
    },
  };
  const compareWristbandOptions2 = promisify(admissionsWristbandCalculationClient.compareWristbandOptions.bind(admissionsWristbandCalculationClient));
  const reply2 = await compareWristbandOptions2(request2, metadataFor(request2.context), { deadline: deadline() });
  const request3 = {
    context: request?.context,
    wristband: {
      audit: { updated_by: (reply2?.wristband?.audit?.updated_by ?? '') },
      dimensions: { length: (reply1?.search_index?.dimensions?.length ?? 0) },
    },
  };
  const reply3 = await call((done) => admissionsWristbandValidationClient.validateWristband(request3, metadataFor(request3.context), { deadline: deadline() }, done));
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
        correlation_id: (reply2?.result?.correlation_id ?? ''),
        details: (reply2?.result?.details ?? {}),
      },
    },
    {
      admission: { measurements: [{ unit: (reply3?.wristband?.dimensions?.unit ?? '') }] },
      result: { details: (reply3?.result?.details ?? {}) },
    },
  );
}

/**
 * Returns an admission.
 */
async function lookupAdmission(request) {
  validateContext(request.context);
  if (!request.admission_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
  }
  await store.record(request.context.tenant_id, 'LookupAdmission', request);
  logger.info('lookup_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { admission: { admission_id: (request?.admission_id ?? '') } };
}

/**
 * Returns an admission.
 */
async function peekAdmission(request) {
  validateContext(request.context);
  if (!request.admission_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
  }
  await store.record(request.context.tenant_id, 'PeekAdmission', request);
  logger.info('peek_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const method1 = 'resolveAccessPolicies';
  const reply1 = await call((done) => authorizationAccessPolicyQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    { admission: { admission_id: (request?.admission_id ?? '') } },
  );
}

/**
 * Returns several admissions at once.
 */
async function resolveAdmissions(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ResolveAdmissions', request);
  logger.info('resolve_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return {};
}

/**
 * Lists admissions a page at a time.
 */
async function suggestAdmissions(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'SuggestAdmissions', request);
  logger.info('suggest_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Summarizes an admission.
 */
async function summarizeAdmissions(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'SummarizeAdmissions', request);
  logger.info('summarize_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    admission_id: (request?.admission_id ?? ''),
    context: request?.context,
  };
  const client1 = clients.registry.get('acme.admissions.v1.AdmissionCalculationService');
  const reply1 = await call((done) => client1.compareAdmissionOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    concession_stand: {
      owner: {
        display_name: (reply1?.admission?.subject?.display_name ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
    context: request?.context,
    details: { currency_code: (reply1?.breakdown?.currency_code ?? '') },
    reason: (request?.reason ?? ''),
  };
  const send2 = concessionsConcessionStandCalculationClient.compareConcessionStandOptions.bind(concessionsConcessionStandCalculationClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.cron ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        status: (reply2?.result?.status ?? ''),
      },
    },
    { period: request?.details?.period },
  );
}

module.exports = { browseAdmissions, countAdmissions, exportAdmissions, lookupAdmission, peekAdmission, resolveAdmissions, suggestAdmissions, summarizeAdmissions };
