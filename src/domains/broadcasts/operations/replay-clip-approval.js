'use strict';

/**
 * Routes replay clips through approval, with escalation and delegation. Each operation validates, records, and calls.
 */

const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  auditAuditEventClient,
  boxofficeBoxOfficeSaleClient,
  broadcastsBroadcastWorkflowClient,
  broadcastsReplayClipCalculationClient,
  broadcastsStreamKeyQueryClient,
  concessionsConcessionStandValidationClient,
  concessionsMenuItemCalculationClient,
  featureflagsFeatureFlagClient,
  featureflagsFeatureFlagValidationClient,
  ledgerLedgerAccountQueryClient,
  showsShowCalculationClient,
  sponsorshipsSponsorshipDealCalculationClient,
  ticketingTicketQueryClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const { collect, upload } = require('../../../platform/streams');
const store = require('../store');

/**
 * Approves a replay clip.
 */
async function approveReplayClip(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'ApproveReplayClip', request);
  logger.info('approve_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsorship_deal: {
      audit: { etag: (request?.replay_clip?.audit?.etag ?? '') },
      billing_address: {
        locality: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.locality ?? ''),
      },
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    sponsorshipsSponsorshipDealCalculationClient.recalculateSponsorshipDeals(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    context: request?.context,
    replay_clip: {
      owner: { display_name: (request?.replay_clip?.owner?.display_name ?? '') },
    },
  };
  const simulateReplayClip2 = promisify(broadcastsReplayClipCalculationClient.simulateReplayClip.bind(broadcastsReplayClipCalculationClient));
  const reply2 = await simulateReplayClip2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      replay_clip: {
        audit: { created_by: (reply1?.sponsorship_deal?.audit?.created_by ?? '') },
      },
    },
    {
      replay_clip: { audit: { created_by: (reply2?.replay_clip?.audit?.created_by ?? '') } },
    },
    {
      replay_clip: {
        owner: request?.replay_clip?.owner,
        replay_clip_id: (request?.replay_clip_id ?? ''),
      },
    },
  );
}

/**
 * Comments a replay clip.
 */
async function commentReplayClipApproval(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'CommentReplayClipApproval', request);
  logger.info('comment_replay_clip_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_1;
  switch (request?.replay_clip?.locator) {
    case 'address':
      selected1_1 = ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.region ?? '');
      break;
    default:
      selected1_1 = '';
  }
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.replay_clip?.audit?.created_at,
      labels: { ...(request?.replay_clip?.specs ?? {}) },
      owner_id: (request?.details?.approver?.user_id ?? ''),
      region: selected1_1,
    },
  };
  const reply1 = await call((done) => ticketingTicketQueryClient.findTickets(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    show: {
      location: {
        locality: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.locality ?? ''),
      },
    },
  };
  const method2 = 'quoteShow';
  const reply2 = await call((done) => showsShowCalculationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    replay_clip: {
      address: { lines: [...(reply2?.show?.location?.lines ?? [])] },
      dimensions: { weight: (request?.replay_clip?.dimensions?.weight ?? 0) },
    },
  };
  const client3 = clients.registry.get('acme.broadcasts.v1.ReplayClipCalculationService');
  const reply3 = await call((done) => client3.explainReplayClipCalculation(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
    },
    {
      replay_clip: { owner: { user_id: (reply2?.show?.organizer?.user_id ?? '') } },
      result: { status: (reply2?.result?.status ?? '') },
    },
    { result: { status: (reply3?.result?.status ?? '') } },
    {
      replay_clip: {
        replay_clip_id: (request?.replay_clip_id ?? ''),
        specs: (request?.replay_clip?.specs ?? {}),
      },
    },
  );
}

/**
 * Delegates a replay clip.
 */
async function delegateReplayClipApproval(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'DelegateReplayClipApproval', request);
  logger.info('delegate_replay_clip_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_stand: {
      dimensions: {
        length: (request?.replay_clip?.dimensions?.length ?? 0),
        width: (request?.replay_clip?.dimensions?.width ?? 0),
      },
      owner: { user_id: (request?.details?.approver?.user_id ?? '') },
    },
    context: request?.context,
  };
  const send1 = concessionsConcessionStandValidationClient.explainConcessionStandDecision.bind(concessionsConcessionStandValidationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.replay_clip?.audit?.updated_at,
      region: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.region ?? ''),
    },
  };
  const messages2 = await collect(ledgerLedgerAccountQueryClient.exportLedgerAccounts(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      replay_clip: { capacity: (reply1?.concession_stand?.capacity ?? '0') },
    },
    {
      replay_clip: {
        code: (request?.replay_clip?.code ?? ''),
        replay_clip_id: (request?.replay_clip_id ?? ''),
      },
    },
  );
}

/**
 * Escalates a replay clip.
 */
async function escalateReplayClip(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'EscalateReplayClip', request);
  logger.info('escalate_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      destination: {
        recipient: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.recipient ?? ''),
        region: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.region ?? ''),
      },
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => concessionsMenuItemCalculationClient.estimateMenuItem(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      region: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.region ?? ''),
    },
  };
  const messages2 = await collect(broadcastsStreamKeyQueryClient.exportStreamKeys(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      replay_clip: {
        components: { primary: { quantity: (reply1?.menu_item?.components?.primary?.quantity ?? '0') } },
        dimensions: { width: (reply1?.menu_item?.dimensions?.width ?? 0) },
      },
    },
    {
      replay_clip: {
        owner: request?.replay_clip?.owner,
        replay_clip_id: (request?.replay_clip_id ?? ''),
      },
    },
  );
}

/**
 * Rejects a replay clip.
 */
async function rejectReplayClip(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'RejectReplayClip', request);
  logger.info('reject_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    subscriber: { email: (request?.details?.approver?.email ?? '') },
  };
  const messages1 = await collect(auditAuditEventClient.watchAuditEvents(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    broadcast: {
      audit: {
        etag: (input2?.replay_clip?.audit?.etag ?? ''),
        updated_by: (input2?.replay_clip?.audit?.updated_by ?? ''),
      },
      location: {
        country_code: ((input2?.replay_clip?.locator === 'address' ? input2?.replay_clip.address : undefined)?.country_code ?? ''),
        region: ((input2?.replay_clip?.locator === 'address' ? input2?.replay_clip.address : undefined)?.region ?? ''),
      },
    },
    context: input2?.context,
  };
  const reply2 = await call((done) => broadcastsBroadcastWorkflowClient.resumeBroadcastWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) },
      replay_clip: { owner: { user_id: (reply2?.broadcast?.organizer?.user_id ?? '') } },
    },
    {
      replay_clip: {
        replay_clip_id: (request?.replay_clip_id ?? ''),
        status: (request?.replay_clip?.status ?? ''),
      },
    },
  );
}

/**
 * Submits a replay clip.
 */
async function submitReplayClip(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'SubmitReplayClip', request);
  logger.info('submit_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_1;
  switch (request?.replay_clip?.locator) {
    case 'address':
      selected1_1 = ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.region ?? '');
      break;
    default:
      selected1_1 = '';
  }
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.replay_clip?.audit?.created_at,
      labels: { ...(request?.replay_clip?.specs ?? {}) },
      owner_id: (request?.details?.approver?.user_id ?? ''),
      region: selected1_1,
    },
  };
  const reply1 = await call((done) => featureflagsFeatureFlagClient.listFeatureFlags(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    feature_flag: {
      owner: {
        display_name: (request?.replay_clip?.owner?.display_name ?? ''),
        email: (request?.details?.approver?.email ?? ''),
      },
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => featureflagsFeatureFlagValidationClient.validateFeatureFlag(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    header: {
      source: (request?.context?.actor?.user_id ?? ''),
      submitted_by: { email: (request?.context?.actor?.email ?? '') },
    },
  };
  const reply3 = await upload(
    (done) => boxofficeBoxOfficeSaleClient.batchCreateBoxOfficeSales(metadataFor(request3.context), { deadline: deadline() }, done),
    [request3],
  );
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { message: (reply1?.result?.message ?? '') } },
    {
      outcome: { message: (reply2?.outcome?.message ?? '') },
      replay_clip: { audit: { created_at: reply2?.feature_flag?.audit?.created_at } },
    },
    {
      replay_clip: {
        capacity: (request?.replay_clip?.capacity ?? '0'),
        replay_clip_id: (request?.replay_clip_id ?? ''),
      },
    },
  );
}

module.exports = { approveReplayClip, commentReplayClipApproval, delegateReplayClipApproval, escalateReplayClip, rejectReplayClip, submitReplayClip };
