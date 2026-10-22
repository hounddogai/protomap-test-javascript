'use strict';

/**
 * Routes stream keys through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { exchange, upload } = require('../../../platform/streams');

/**
 * The handlers of StreamKeyApprovalService, over the dependencies the broadcasts team is given.
 */
function createStreamKeyApprovalHandlers({ store, clients, bus, logger }) {
  /**
   * Approves a stream key.
   */
  async function approveStreamKey(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'ApproveStreamKey', request);
    logger.info('approve_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.stream_key?.audit?.created_at,
        labels: { ...(request?.stream_key?.labels ?? {}) },
      },
    };
    const browseSponsors1 = promisify(clients.sponsorshipsSponsorQueryClient.browseSponsors.bind(clients.sponsorshipsSponsorQueryClient));
    const reply1 = await browseSponsors1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.stream_key?.audit?.updated_at,
        labels: { ...(request?.stream_key?.labels ?? {}) },
        owner_id: (request?.details?.approver?.user_id ?? ''),
      },
    };
    const reply2 = await call((done) => clients.notificationsNotificationQueryClient.findNotifications(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        stream_key: {
          contact: request?.stream_key?.contact,
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Comments a stream key.
   */
  async function commentStreamKeyApproval(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'CommentStreamKeyApproval', request);
    logger.info('comment_stream_key_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        participants: [{
          contact: {
            email: (request?.stream_key?.contact?.email ?? ''),
            phone: (request?.stream_key?.contact?.phone ?? ''),
          },
        }],
      },
      context: request?.context,
    };
    const method1 = 'normalizeBroadcast';
    const reply1 = await call((done) => clients.broadcastsBroadcastValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      replay_clip: {
        address: { recipient: (reply1?.broadcast?.location?.recipient ?? '') },
      },
    };
    const client2 = clients.registry.get('acme.broadcasts.v1.ReplayClipCalculationService');
    const reply2 = await call((done) => client2.recalculateReplayClips(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      change: {
        discount: { amount: request?.stream_key?.available },
        parameters: { ...(request?.annotations ?? {}) },
        scopes: [...(request?.context?.actor?.roles ?? [])],
      },
      context: request?.context,
    };
    const messages3 = await exchange(clients.venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
    const reply3 = messages3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        stream_key: { audit: { etag: (reply1?.broadcast?.audit?.etag ?? '') } },
      },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        stream_key: {
          labels: (request?.stream_key?.labels ?? {}),
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Delegates a stream key.
   */
  async function delegateStreamKeyApproval(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'DelegateStreamKeyApproval', request);
    logger.info('delegate_stream_key_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        participants: [{
          contact: {
            phone: (request?.stream_key?.contact?.phone ?? ''),
            preferred_channel: (request?.stream_key?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const send1 = clients.showsShowClient.createShow.bind(clients.showsShowClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.show?.audit?.updated_at,
        labels: { ...(request?.stream_key?.labels ?? {}) },
        owner_id: (request?.stream_key?.holder?.show_id ?? ''),
        region: (reply1?.show?.location?.region ?? ''),
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.metricsMetricSeriesQueryClient.countMetricSerieses(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      access_policy: {
        owner: { display_name: (reply1?.show?.organizer?.display_name ?? '') },
      },
      context: input3?.context,
    };
    const reply3 = await call((done) => clients.authorizationAccessPolicyValidationClient.normalizeAccessPolicy(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      lineup: {
        audit: {
          etag: (request?.stream_key?.audit?.etag ?? ''),
          updated_by: (reply1?.show?.audit?.updated_by ?? ''),
        },
        billing_address: { recipient: (reply1?.show?.location?.recipient ?? '') },
        card: {
          last4: ((request?.stream_key?.funding === 'card' ? request?.stream_key.card : undefined)?.last4 ?? ''),
        },
      },
    };
    const cloned4 = JSON.parse(JSON.stringify(request4));
    const reply4 = await call((done) => clients.showsLineupCalculationClient.recalculateLineups(cloned4, metadataFor(cloned4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          message: (reply2?.result?.message ?? ''),
        },
      },
      {
        result: { details: (reply3?.result?.details ?? {}) },
        stream_key: {
          audit: { created_by: (reply3?.access_policy?.audit?.created_by ?? '') },
        },
      },
      {
        result: { correlation_id: (reply4?.result?.correlation_id ?? '') },
        stream_key: { audit: { updated_at: reply4?.lineup?.audit?.updated_at } },
      },
      {
        stream_key: {
          labels: (request?.stream_key?.labels ?? {}),
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Escalates a stream key.
   */
  async function escalateStreamKey(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'EscalateStreamKey', request);
    logger.info('escalate_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.stream_key?.holder?.starts_at,
        labels: { ...(request?.stream_key?.labels ?? {}) },
        owner_id: (request?.stream_key?.holder?.show_id ?? ''),
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.broadcastsBroadcastQueryClient.countBroadcasts(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      performance: {
        audit: { created_at: request?.stream_key?.audit?.created_at },
        participants: [{
          contact: {
            email: (request?.stream_key?.contact?.email ?? ''),
            name: request?.stream_key?.contact?.name,
            preferred_channel: (request?.stream_key?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.showsPerformanceCalculationClient.estimatePerformance(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        result: { details: (reply2?.result?.details ?? {}) },
        stream_key: { audit: { updated_by: (reply2?.performance?.audit?.updated_by ?? '') } },
      },
      {
        stream_key: {
          currency_code: (request?.stream_key?.currency_code ?? ''),
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Lists stream keys a page at a time.
   */
  async function listPendingStreamKeys(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListPendingStreamKeys', request);
    logger.info('list_pending_stream_keys', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      header: {
        source: (request?.context?.actor?.user_id ?? ''),
        submitted_by: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const reply1 = await upload(
      (done) => clients.concessionsConcessionStandClient.batchCreateConcessionStands(metadataFor(request1.context), { deadline: deadline() }, done),
      [request1],
    );
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Recalls a stream key.
   */
  async function recallStreamKey(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'RecallStreamKey', request);
    logger.info('recall_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(request?.stream_key?.labels ?? {}) },
        owner_id: ((request?.stream_key?.funding === 'external_account_id' ? request?.stream_key.external_account_id : undefined) ?? ''),
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.showsShowClient.listShows(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        stream_key: {
          balance: request?.stream_key?.balance,
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Submits a stream key.
   */
  async function submitStreamKey(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'SubmitStreamKey', request);
    logger.info('submit_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      guest_list: { audit: { created_by: (request?.stream_key?.audit?.created_by ?? '') } },
    };
    const task1 = deferred(() => call((done) => clients.boxofficeGuestListCalculationClient.estimateGuestList(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      replay_clip: {
        audit: { created_by: (reply1?.guest_list?.audit?.created_by ?? '') },
        owner: { user_id: (request?.details?.approver?.user_id ?? '') },
      },
    };
    const reply2 = await bus.request('acme.broadcasts.v1.StreamKeyApprovalService/SubmitStreamKey:1', request2);
    const request3 = { context: request?.context };
    const reply3 = await runInContext(request.context, () => call((done) => clients.showsPerformanceQueryClient.resolvePerformances(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        result: { message: (reply2?.outcome?.message ?? '') },
      },
      {
        outcome: { status: (reply3?.result?.status ?? '') },
        result: { status: (reply3?.result?.status ?? '') },
      },
      {
        stream_key: {
          opened_at: request?.stream_key?.opened_at,
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  return {
    approveStreamKey: unary(approveStreamKey),
    commentStreamKeyApproval: unary(commentStreamKeyApproval),
    delegateStreamKeyApproval: unary(delegateStreamKeyApproval),
    escalateStreamKey: unary(escalateStreamKey),
    listPendingStreamKeys: unary(listPendingStreamKeys),
    recallStreamKey: unary(recallStreamKey),
    submitStreamKey: unary(submitStreamKey),
  };
}

module.exports = { createStreamKeyApprovalHandlers };
