'use strict';

/**
 * Checks activations against the rules before anything acts on them. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  authorizationAccessPolicyClient,
  broadcastsBroadcastClient,
  broadcastsBroadcastValidationClient,
  broadcastsReplayClipCalculationClient,
  broadcastsStreamKeyQueryClient,
  featureflagsFeatureFlagQueryClient,
  searchSearchIndexValidationClient,
  sponsorshipsActivationCalculationClient,
  sponsorshipsSponsorCalculationClient,
  sponsorshipsSponsorValidationClient,
  sponsorshipsSponsorshipDealCalculationClient,
  venuesVenuePolicySyncClient,
  venuesVenueSectionExportClient,
  venuesVenueSectionSyncClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Checks an activation.
 */
async function checkActivationEligibility(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'CheckActivationEligibility', request);
  logger.info('check_activation_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    broadcast: {
      organizer: {
        display_name: (request?.activation?.assignee?.display_name ?? ''),
        email: (request?.details?.contact?.email ?? ''),
      },
      participants: [{ contact: { name: request?.activation?.requester?.name } }],
    },
    context: request?.context,
    details: { contact: { phone: (request?.activation?.requester?.phone ?? '') } },
  };
  const send1 = broadcastsBroadcastValidationClient.validateBroadcast.bind(broadcastsBroadcastValidationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      contact: {
        email: (request?.activation?.assignee?.email ?? ''),
        name: request?.activation?.requester?.name,
      },
    },
    sponsor: {
      relationship_manager: { display_name: (request?.activation?.assignee?.display_name ?? '') },
    },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => sponsorshipsSponsorValidationClient.previewSponsor(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  const { context: context3, ...body3 } = request;
  const input3 = { ...body3, context: context3 };
  const request3 = {
    activation: {
      audit: { updated_by: (reply1?.broadcast?.audit?.updated_by ?? '') },
      customer: { display_name: (reply1?.broadcast?.organizer?.display_name ?? '') },
    },
    context: input3?.context,
  };
  const reply3 = await call((done) => sponsorshipsActivationCalculationClient.simulateActivation(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      activation: {
        assignee: { display_name: (reply1?.broadcast?.organizer?.display_name ?? '') },
      },
    },
    {
      activation: { requester: { name: reply2?.sponsor?.contact?.name } },
      outcome: { reference_id: (reply2?.outcome?.reference_id ?? '') },
    },
    {
      activation: {
        requester: {
          preferred_channel: (reply3?.activation?.requester?.preferred_channel ?? ''),
        },
      },
      result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
    },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        status: (request?.activation?.status ?? ''),
      },
    },
  );
}

/**
 * Explains an activation.
 */
async function explainActivationDecision(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'ExplainActivationDecision', request);
  logger.info('explain_activation_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.activation?.comments ?? [])) {
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          email: (item?.author_email ?? ''),
          phone: (request?.activation?.requester?.phone ?? ''),
        },
      },
      search_index: { audit: { created_at: item?.created_at } },
    };
    const reply1 = await call((done) => searchSearchIndexValidationClient.normalizeSearchIndex(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    venue_section: {
      children: [{
        children: [{
          audit: {
            created_at: reply1?.search_index?.audit?.created_at,
            updated_by: (reply1?.search_index?.audit?.updated_by ?? ''),
          },
          manager: { user_id: (request?.activation?.assignee?.user_id ?? '') },
        }],
      }],
    },
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => venuesVenueSectionSyncClient.syncVenueSections(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: reply2?.venue_section?.audit?.created_at,
      region: (reply2?.venue_section?.location?.region ?? ''),
    },
  };
  const messages3 = await collect(venuesVenueSectionExportClient.streamVenueSectionExport(request3, metadataFor(request3.context), { deadline: deadline() }));
  const reply3 = messages3.at(-1) ?? {};
  const request4 = {
    context: request?.context,
    filter: {
      created_after: reply2?.venue_section?.audit?.updated_at,
      owner_id: (request?.activation?.sla?.policy_id ?? ''),
      region: (reply2?.venue_section?.location?.region ?? ''),
    },
  };
  const copied4 = structuredClone(request4);
  const reply4 = await call((done) => featureflagsFeatureFlagQueryClient.suggestFeatureFlags(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      activation: { comments: [{ created_at: reply2?.venue_section?.audit?.created_at }] },
    },
    {
      result: {
        correlation_id: (reply4?.result?.correlation_id ?? ''),
        reference_ids: (reply4?.result?.reference_ids ?? []),
      },
    },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        custom_fields: (request?.activation?.custom_fields ?? {}),
      },
    },
  );
}

/**
 * Returns an activation.
 */
async function getActivationRules(request) {
  validateContext(request.context);
  if (!request.activation_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
  }
  await store.record(request.context.tenant_id, 'GetActivationRules', request);
  logger.info('get_activation_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { activation: { activation_id: (request?.activation_id ?? '') } };
}

/**
 * Lints an activation.
 */
async function lintActivations(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'LintActivations', request);
  logger.info('lint_activations', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: { audit: { updated_at: request?.activation?.audit?.updated_at } },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => sponsorshipsSponsorCalculationClient.quoteSponsor(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: reply1?.sponsor?.audit?.updated_at,
      region: (reply1?.sponsor?.primary_address?.region ?? ''),
    },
  };
  const messages2 = await collect(broadcastsStreamKeyQueryClient.exportStreamKeys(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  const request3 = {
    context: request?.context,
    details: {
      contact: { email: (reply1?.sponsor?.relationship_manager?.email ?? '') },
    },
    replay_clip: { owner: { email: (request?.details?.contact?.email ?? '') } },
  };
  const reply3 = await withRetry(() => call((done) => broadcastsReplayClipCalculationClient.explainReplayClipCalculation(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      result: {
        message: (reply3?.result?.message ?? ''),
        reference_ids: (reply3?.result?.reference_ids ?? []),
      },
    },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        requester: request?.activation?.requester,
      },
    },
  );
}

/**
 * Normalizes an activation.
 */
async function normalizeActivation(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'NormalizeActivation', request);
  logger.info('normalize_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.activation?.audit?.updated_at,
      labels: { ...(request?.activation?.custom_fields ?? {}) },
      owner_id: ((request?.activation?.origin === 'customer' ? request?.activation.customer : undefined)?.venue_id ?? ''),
    },
  };
  const task1 = deferred(() => call((done) => authorizationAccessPolicyClient.listAccessPolicies(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  const request2 = {
    context: request?.context,
    venue_policy: {
      audit: {
        created_by: (request?.activation?.audit?.created_by ?? ''),
        etag: (request?.activation?.audit?.etag ?? ''),
        updated_by: (request?.activation?.audit?.updated_by ?? ''),
      },
    },
  };
  const reply2 = await bus.request('acme.sponsorships.v1.ActivationValidationService/NormalizeActivation:1', request2);
  const request3 = {
    context: request?.context,
    replay_clip: {
      audit: { created_by: (reply2?.venue_policy?.audit?.created_by ?? '') },
    },
  };
  const reply3 = await runInContext(request.context, () => call((done) => broadcastsReplayClipCalculationClient.simulateReplayClip(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  const results4 = await Promise.all((request?.activation?.history ?? []).map(async (item) => {
    const request4 = {
      activation: {
        history: [{
          from_status: (item?.from_status ?? ''),
          to_status: (item?.to_status ?? ''),
        }],
      },
      context: request?.context,
    };
    const reply4 = await call((done) => sponsorshipsActivationCalculationClient.calculateActivation(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    return reply4;
  }));
  const reply4 = results4.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    },
    {
      activation: {
        audit: { updated_by: (reply2?.venue_policy?.audit?.updated_by ?? '') },
      },
      result: { status: (reply2?.outcome?.status ?? '') },
    },
    { activation: { tags: (reply3?.replay_clip?.tags ?? []) } },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        assignee: request?.activation?.assignee,
      },
    },
  );
}

/**
 * Previews an activation.
 */
async function previewActivation(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'PreviewActivation', request);
  logger.info('preview_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const settled1 = await Promise.allSettled((request?.activation?.history ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      sponsorship_deal: {
        audit: { created_at: request?.activation?.audit?.created_at },
        due_at: item?.changed_at,
      },
    };
    const reply1 = await call((done) => sponsorshipsSponsorshipDealCalculationClient.recalculateSponsorshipDeals(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const results1 = settled1.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    details: { contact: { name: request?.details?.contact?.name } },
  };
  const reply2 = await call((done) => clients.sponsorshipsSponsorValidationClient.explainSponsorDecision(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const results3 = [];
  for (const item of (request?.activation?.comments ?? [])) {
    const request3 = {
      activation: {
        comments: [{
          author_email: (item?.author_email ?? ''),
          comment_id: (item?.comment_id ?? ''),
          created_at: item?.created_at,
        }],
      },
      context: request?.context,
    };
    const reply3 = await call((done) => sponsorshipsActivationCalculationClient.calculateActivation(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    results3.push(reply3);
  }
  const reply3 = results3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      activation: {
        requester: {
          name: reply2?.sponsor?.name,
          preferred_channel: (reply2?.sponsor?.contact?.preferred_channel ?? ''),
        },
      },
    },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        status: (request?.activation?.status ?? ''),
      },
    },
  );
}

/**
 * Scores an activation.
 */
async function scoreActivation(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'ScoreActivation', request);
  logger.info('score_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const reply1 = await new Promise((resolve, reject) => {
    broadcastsBroadcastClient.batchGetBroadcasts(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    context: request?.context,
    sponsor: {
      audit: { created_by: (request?.activation?.audit?.created_by ?? '') },
      contact: { phone: (request?.activation?.requester?.phone ?? '') },
      primary_address: { country_code: (request?.details?.destination?.country_code ?? '') },
    },
  };
  const validateSponsor2 = promisify(sponsorshipsSponsorValidationClient.validateSponsor.bind(sponsorshipsSponsorValidationClient));
  const reply2 = await validateSponsor2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        details: (reply1?.result?.details ?? {}),
      },
    },
    {
      activation: {
        audit: { updated_at: reply2?.sponsor?.audit?.updated_at },
        requester: { phone: (reply2?.sponsor?.contact?.phone ?? '') },
      },
    },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        priority: (request?.activation?.priority ?? 0),
      },
    },
  );
}

/**
 * Validates an activation.
 */
async function validateActivation(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'ValidateActivation', request);
  logger.info('validate_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    activation: { sla: { policy_id: (request?.activation?.sla?.policy_id ?? '') } },
    context: request?.context,
  };
  const reply1 = await call((done) => sponsorshipsActivationCalculationClient.estimateActivation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { contact: { name: request?.details?.contact?.name } },
  };
  const method2 = 'previewSearchIndex';
  const reply2 = await call((done) => searchSearchIndexValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      activation: { assignee: { email: (reply1?.activation?.requester?.email ?? '') } },
    },
    {
      activation: {
        assignee: { display_name: (reply2?.search_index?.owner?.display_name ?? '') },
        audit: { updated_at: reply2?.search_index?.audit?.updated_at },
      },
    },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        priority: (request?.activation?.priority ?? 0),
      },
    },
  );
}

module.exports = { checkActivationEligibility, explainActivationDecision, getActivationRules, lintActivations, normalizeActivation, previewActivation, scoreActivation, validateActivation };
