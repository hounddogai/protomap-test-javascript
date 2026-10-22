'use strict';

/**
 * Routes sponsorship deals through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of SponsorshipDealApprovalService, over the dependencies the sponsorships team is given.
 */
function createSponsorshipDealApprovalHandlers({ store, clients, bus, logger }) {
  /**
   * Approves a sponsorship deal.
   */
  async function approveSponsorshipDeal(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'ApproveSponsorshipDeal', request);
    logger.info('approve_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      cash_drawer: { audit: { created_at: request?.sponsorship_deal?.audit?.created_at } },
      context: request?.context,
      details: { contact: { email: (request?.details?.approver?.email ?? '') } },
    };
    const reply1 = await call((done) => clients.boxofficeCashDrawerCalculationClient.explainCashDrawerCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const method2 = 'batchGetUsers';
    const reply2 = await call((done) => clients.identityUserClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsorship_deal: { audit: { etag: (reply1?.cash_drawer?.audit?.etag ?? '') } },
      },
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        sponsorship_deal: {
          sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
          status: (request?.sponsorship_deal?.status ?? ''),
        },
      },
    );
  }

  /**
   * Comments a sponsorship deal.
   */
  async function commentSponsorshipDealApproval(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'CommentSponsorshipDealApproval', request);
    logger.info('comment_sponsorship_deal_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const client1 = clients.registry.get('acme.sponsorships.v1.SponsorQueryService');
    const reply1 = await call((done) => client1.resolveSponsors(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.sponsorship_deal?.audit?.created_at,
        region: (request?.sponsorship_deal?.billing_address?.region ?? ''),
      },
    };
    const send2 = clients.sponsorshipsSponsorQueryClient.suggestSponsors.bind(clients.sponsorshipsSponsorQueryClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        sponsorship_deal: {
          notes: (request?.sponsorship_deal?.notes ?? ''),
          sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
        },
      },
    );
  }

  /**
   * Delegates a sponsorship deal.
   */
  async function delegateSponsorshipDealApproval(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'DelegateSponsorshipDealApproval', request);
    logger.info('delegate_sponsorship_deal_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      access_policy: { audit: { created_at: request?.sponsorship_deal?.audit?.created_at } },
      context: request?.context,
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.authorizationAccessPolicyClient.cloneAccessPolicy(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        sponsorship_deal: {
          notes: (request?.sponsorship_deal?.notes ?? ''),
          sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a sponsorship deal.
   */
  async function getSponsorshipDealApprovalStatus(request) {
    validateContext(request.context);
    if (!request.sponsorship_deal_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsorship_deal_id is required');
    }
    await store.record(request.context.tenant_id, 'GetSponsorshipDealApprovalStatus', request);
    logger.info('get_sponsorship_deal_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      details: { approver: { email: (input1?.context?.actor?.email ?? '') } },
      replay_clip: {
        audit: { updated_at: input1?.context?.deadline },
        components: { primary: { component_id: (input1?.sponsorship_deal_id ?? '') } },
        owner: { user_id: (input1?.context?.actor?.user_id ?? '') },
      },
    };
    const reply1 = await call((done) => clients.broadcastsReplayClipApprovalClient.commentReplayClipApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    let selected2_1;
    switch (reply1?.replay_clip?.locator) {
      case 'address':
        selected2_1 = ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.locality ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      details: { destination: { locality: selected2_1 } },
      stream_key: {
        audit: {
          created_at: reply1?.replay_clip?.audit?.created_at,
          updated_at: reply1?.replay_clip?.audit?.updated_at,
          updated_by: (reply1?.replay_clip?.audit?.updated_by ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.broadcastsStreamKeyCalculationClient.calculateStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      {
        sponsorship_deal: {
          billing_address: {
            locality: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.locality ?? ''),
            region: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? ''),
          },
        },
      },
      {
        sponsorship_deal: {
          counterparty: { display_name: (reply2?.stream_key?.holder?.display_name ?? '') },
        },
      },
      {
        sponsorship_deal: { sponsorship_deal_id: (request?.sponsorship_deal_id ?? '') },
      },
    );
  }

  /**
   * Lists sponsorship deals a page at a time.
   */
  async function listPendingSponsorshipDeals(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListPendingSponsorshipDeals', request);
    logger.info('list_pending_sponsorship_deals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        primary_address: { region: (request?.filter?.region ?? '') },
        relationship_manager: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.sponsorshipsSponsorApprovalClient.approveSponsor(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { status: (reply1?.result?.status ?? '') } };
  }

  /**
   * Rejects a sponsorship deal.
   */
  async function rejectSponsorshipDeal(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'RejectSponsorshipDeal', request);
    logger.info('reject_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        participants: [{ contact: { email: (request?.details?.approver?.email ?? '') } }],
      },
      context: request?.context,
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.broadcastsBroadcastApprovalClient.rejectBroadcast(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.sponsorship_deal?.audit?.created_at,
        region: (request?.sponsorship_deal?.billing_address?.region ?? ''),
      },
    };
    const reply2 = await withRetry(() => call((done) => clients.sponsorshipsSponsorQueryClient.browseSponsors(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      filter: { region: (reply1?.broadcast?.location?.region ?? '') },
    };
    const messages3 = await collect(clients.sponsorshipsSponsorQueryClient.exportSponsors(request3, metadataFor(request3.context), { deadline: deadline() }));
    const reply3 = messages3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      details: { contact: { email: (reply1?.broadcast?.organizer?.email ?? '') } },
      metric_series: { address: { lines: [...(reply1?.broadcast?.location?.lines ?? [])] } },
    };
    const task4 = deferred(() => call((done) => clients.metricsMetricSeriesValidationClient.validateMetricSeries(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    const reply4 = await task4();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsorship_deal: {
          counterparty: { display_name: (reply1?.broadcast?.organizer?.display_name ?? '') },
        },
      },
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        sponsorship_deal: {
          audit: { updated_by: (reply4?.metric_series?.audit?.updated_by ?? '') },
          billing_address: {
            postal_code: ((reply4?.metric_series?.locator === 'address' ? reply4?.metric_series.address : undefined)?.postal_code ?? ''),
          },
        },
      },
      {
        sponsorship_deal: {
          metadata: (request?.sponsorship_deal?.metadata ?? {}),
          sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
        },
      },
    );
  }

  /**
   * Submits a sponsorship deal.
   */
  async function submitSponsorshipDeal(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'SubmitSponsorshipDeal', request);
    logger.info('submit_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.sponsorship_deal?.billing_address?.region ?? '') },
    };
    const reply1 = await bus.request('acme.sponsorships.v1.SponsorshipDealApprovalService/SubmitSponsorshipDeal:0', request1);
    const request2 = {
      context: request?.context,
      sponsor_id: (request?.sponsorship_deal?.counterparty?.sponsor_id ?? ''),
    };
    const reply2 = await runInContext(request.context, () => call((done) => clients.sponsorshipsSponsorApprovalClient.getSponsorApprovalStatus(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
        sponsorship_deal: {
          billing_address: { country_code: (reply2?.sponsor?.primary_address?.country_code ?? '') },
        },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        sponsorship_deal: { sponsorship_deal_id: (request?.sponsorship_deal_id ?? '') },
      },
    );
  }

  return {
    approveSponsorshipDeal: unary(approveSponsorshipDeal),
    commentSponsorshipDealApproval: unary(commentSponsorshipDealApproval),
    delegateSponsorshipDealApproval: unary(delegateSponsorshipDealApproval),
    getSponsorshipDealApprovalStatus: unary(getSponsorshipDealApprovalStatus),
    listPendingSponsorshipDeals: unary(listPendingSponsorshipDeals),
    rejectSponsorshipDeal: unary(rejectSponsorshipDeal),
    submitSponsorshipDeal: unary(submitSponsorshipDeal),
  };
}

module.exports = { createSponsorshipDealApprovalHandlers };
