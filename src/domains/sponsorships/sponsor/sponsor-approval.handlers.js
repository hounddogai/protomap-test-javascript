'use strict';

/**
 * Routes sponsors through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred } = require('../../../platform/rpc');
const { collect, upload } = require('../../../platform/streams');

/**
 * The handlers of SponsorApprovalService, over the dependencies the sponsorships team is given.
 */
function createSponsorApprovalHandlers({ store, clients, bus, logger }) {
  /**
   * Approves a sponsor.
   */
  async function approveSponsor(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await store.record(request.context.tenant_id, 'ApproveSponsor', request);
    logger.info('approve_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        approver: {
          email: (request?.sponsor?.relationship_manager?.email ?? ''),
          user_id: (request?.sponsor?.relationship_manager?.user_id ?? ''),
        },
      },
      stream_key: {
        audit: { updated_by: (request?.sponsor?.audit?.updated_by ?? '') },
        contact: { name: request?.sponsor?.name },
      },
    };
    const reply1 = await call((done) => clients.broadcastsStreamKeyApprovalClient.recallStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { notify: { name: request?.sponsor?.contact?.name } },
      replay_clip: {
        audit: {
          created_at: request?.sponsor?.audit?.created_at,
          etag: (reply1?.stream_key?.audit?.etag ?? ''),
        },
        owner: { email: (request?.details?.approver?.email ?? '') },
      },
    };
    const method2 = 'cancelReplayClipWorkflow';
    const reply2 = await call((done) => clients.broadcastsReplayClipWorkflowClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      sponsor: {
        audit: { updated_at: request?.sponsor?.audit?.updated_at },
        primary_address: {
          postal_code: ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.postal_code ?? ''),
          region: (request?.sponsor?.primary_address?.region ?? ''),
        },
      },
    };
    const client3 = clients.registry.get('acme.sponsorships.v1.SponsorValidationService');
    const reply3 = await call((done) => client3.explainSponsorDecision(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { period: request?.sponsor?.preferences?.quiet_hours },
      reason: (request?.reason ?? ''),
      sponsor_id: (request?.sponsor?.sponsor_id ?? ''),
    };
    const send4 = clients.sponsorshipsSponsorQueryClient.summarizeSponsors.bind(clients.sponsorshipsSponsorQueryClient);
    const reply4 = await call((done) => send4(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.approver?.display_name ?? '') });
    return assemble(
      { sponsor: { name: reply1?.stream_key?.contact?.name } },
      {
        sponsor: { audit: { etag: (reply2?.replay_clip?.audit?.etag ?? '') } },
      },
      {
        sponsor: {
          relationship_manager: { email: (reply3?.sponsor?.relationship_manager?.email ?? '') },
        },
      },
      {
        result: {
          correlation_id: (reply4?.result?.correlation_id ?? ''),
          message: (reply4?.result?.message ?? ''),
        },
      },
      {
        sponsor: {
          contact: { name: request?.sponsor?.name },
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  /**
   * Comments a sponsor.
   */
  async function commentSponsorApproval(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await store.record(request.context.tenant_id, 'CommentSponsorApproval', request);
    logger.info('comment_sponsor_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: (request?.sponsor?.identity_document === 'passport' ? request?.sponsor.passport : undefined)?.expires_at,
        labels: { ...(request?.sponsor?.attributes ?? {}) },
        owner_id: (request?.details?.approver?.user_id ?? ''),
        region: (request?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.broadcastsStreamKeyWorkflowClient.listStreamKeyWorkflows(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      sponsor: {
        national_id: {
          issuing_country: ((input2?.sponsor?.identity_document === 'passport' ? input2?.sponsor.passport : undefined)?.issuing_country ?? ''),
        },
        primary_address: {
          locality: (input2?.sponsor?.primary_address?.locality ?? ''),
          recipient: (input2?.sponsor?.primary_address?.recipient ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.sponsorshipsSponsorCalculationClient.recalculateSponsors(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      activation: {
        assignee: { email: (request?.details?.approver?.email ?? '') },
        comments: [{ created_at: reply2?.sponsor?.audit?.created_at }],
      },
      context: request?.context,
      details: {
        destination: { locality: (request?.sponsor?.primary_address?.locality ?? '') },
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => clients.sponsorshipsActivationCalculationClient.simulateActivation(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      details: { contact: { email: (reply3?.activation?.assignee?.email ?? '') } },
      sponsorship_deal: { audit: { etag: (reply2?.sponsor?.audit?.etag ?? '') } },
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => clients.sponsorshipsSponsorshipDealValidationClient.validateSponsorshipDeal(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
        sponsor: {
          relationship_manager: {
            display_name: ((reply3?.activation?.origin === 'customer' ? reply3?.activation.customer : undefined)?.display_name ?? ''),
          },
        },
      },
      {
        sponsor: {
          audit: { created_by: (reply4?.sponsorship_deal?.audit?.created_by ?? '') },
        },
      },
      {
        sponsor: {
          name: request?.sponsor?.name,
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  /**
   * Delegates a sponsor.
   */
  async function delegateSponsorApproval(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await store.record(request.context.tenant_id, 'DelegateSponsorApproval', request);
    logger.info('delegate_sponsor_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.sponsor?.primary_address?.region ?? '') },
      subscriber: {
        email: (request?.details?.approver?.email ?? ''),
        name: request?.sponsor?.name,
      },
    };
    const messages1 = await collect(clients.sponsorshipsSponsorClient.watchSponsors(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: {
        approver: { email: (request?.sponsor?.relationship_manager?.email ?? '') },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.broadcastsReplayClipApprovalClient.commentReplayClipApproval(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    let selected3_2;
    switch (reply2?.replay_clip?.locator) {
      case 'address':
        selected3_2 = ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.postal_code ?? '');
        break;
      default:
        selected3_2 = '';
    }
    const request3 = {
      context: request?.context,
      details: {
        approver: { display_name: (reply2?.replay_clip?.owner?.display_name ?? '') },
      },
      replay_clip: {
        address: { postal_code: selected3_2 },
        audit: { updated_by: (reply2?.replay_clip?.audit?.updated_by ?? '') },
        owner: { display_name: (request?.details?.approver?.display_name ?? '') },
      },
    };
    const reply3 = await call((done) => clients.broadcastsReplayClipApprovalClient.submitReplayClip(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsor: {
          primary_address: {
            recipient: ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.recipient ?? ''),
          },
          relationship_manager: { display_name: (reply2?.replay_clip?.owner?.display_name ?? '') },
        },
      },
      {
        result: { correlation_id: (reply3?.result?.correlation_id ?? '') },
        sponsor: { audit: { created_by: (reply3?.replay_clip?.audit?.created_by ?? '') } },
      },
      {
        sponsor: {
          sponsor_id: (request?.sponsor_id ?? ''),
          status: (request?.sponsor?.status ?? ''),
        },
      },
    );
  }

  /**
   * Escalates a sponsor.
   */
  async function escalateSponsor(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await store.record(request.context.tenant_id, 'EscalateSponsor', request);
    logger.info('escalate_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.sponsor?.audit?.created_at,
        region: (request?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const task1 = deferred(() => call((done) => clients.campaignsMarketingCampaignQueryClient.findMarketingCampaigns(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      sponsor: {
        audit: { updated_by: (request?.sponsor?.audit?.updated_by ?? '') },
        contact: {
          email: (request?.sponsor?.contact?.email ?? ''),
          name: request?.sponsor?.contact?.name,
        },
        preferences: {
          marketing_opt_in: (request?.sponsor?.preferences?.marketing_opt_in ?? false),
        },
      },
    };
    const reply2 = await bus.request('acme.sponsorships.v1.SponsorApprovalService/EscalateSponsor:1', request2);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        outcome: { reference_id: (reply2?.outcome?.reference_id ?? '') },
        sponsor: {
          preferences: {
            marketing_opt_in: (reply2?.sponsor?.preferences?.marketing_opt_in ?? false),
          },
        },
      },
      {
        sponsor: {
          name: request?.sponsor?.name,
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a sponsor.
   */
  async function getSponsorApprovalStatus(request) {
    validateContext(request.context);
    if (!request.sponsor_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsor_id is required');
    }
    await store.record(request.context.tenant_id, 'GetSponsorApprovalStatus', request);
    logger.info('get_sponsor_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      record: {
        audit: { updated_at: request?.context?.deadline },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const reply1 = await upload(
      (done) => clients.featureflagsFeatureFlagClient.batchCreateFeatureFlags(metadataFor(request1.context), { deadline: deadline() }, done),
      [request1],
    );
    return { sponsor: { sponsor_id: (request?.sponsor_id ?? '') } };
  }

  /**
   * Lists sponsors a page at a time.
   */
  async function listPendingSponsors(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListPendingSponsors', request);
    logger.info('list_pending_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsorship_deal: {
        billing_address: { region: (request?.filter?.region ?? '') },
        counterparty: { email: (request?.context?.actor?.email ?? '') },
        issued_at: request?.filter?.created_after,
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.sponsorshipsSponsorshipDealCalculationClient.recalculateSponsorshipDeals(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      activation: {
        comments: [{ created_at: reply1?.sponsorship_deal?.audit?.created_at }],
      },
      context: request?.context,
    };
    const reply2 = await call((done) => clients.sponsorshipsActivationCalculationClient.simulateActivation(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        sponsors: [{
          relationship_manager: { email: (reply1?.sponsorship_deal?.counterparty?.email ?? '') },
        }],
      },
      {
        sponsors: [{ contact: { phone: (reply2?.activation?.requester?.phone ?? '') } }],
      },
    );
  }

  /**
   * Recalls a sponsor.
   */
  async function recallSponsor(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await store.record(request.context.tenant_id, 'RecallSponsor', request);
    logger.info('recall_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation: { requester: { phone: (request?.sponsor?.contact?.phone ?? '') } },
      context: request?.context,
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.sponsorshipsActivationCalculationClient.quoteActivation(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          email: (request?.sponsor?.contact?.email ?? ''),
          phone: (reply1?.activation?.requester?.phone ?? ''),
        },
      },
      sponsorship_deal: {
        audit: { updated_by: (request?.sponsor?.audit?.updated_by ?? '') },
        billing_address: { locality: (request?.sponsor?.primary_address?.locality ?? '') },
      },
    };
    const checkSponsorshipDealEligibility2 = promisify(clients.sponsorshipsSponsorshipDealValidationClient.checkSponsorshipDealEligibility.bind(clients.sponsorshipsSponsorshipDealValidationClient));
    const reply2 = await checkSponsorshipDealEligibility2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsor: {
          relationship_manager: { display_name: (reply1?.activation?.assignee?.display_name ?? '') },
        },
      },
      {
        sponsor: {
          contact: { email: (reply2?.sponsorship_deal?.counterparty?.email ?? '') },
        },
      },
      {
        sponsor: {
          contact: { name: request?.sponsor?.name },
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  /**
   * Rejects a sponsor.
   */
  async function rejectSponsor(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await store.record(request.context.tenant_id, 'RejectSponsor', request);
    logger.info('reject_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          email: (request?.sponsor?.relationship_manager?.email ?? ''),
          preferred_channel: (request?.sponsor?.contact?.preferred_channel ?? ''),
        },
      },
      replay_clip: { owner: { email: (request?.details?.approver?.email ?? '') } },
    };
    const reply1 = await call((done) => clients.broadcastsReplayClipWorkflowClient.pauseReplayClipWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.replay_clip?.audit?.updated_at,
        labels: { ...(reply1?.replay_clip?.specs ?? {}) },
        owner_id: (request?.sponsor_id ?? ''),
        region: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? ''),
      },
    };
    const method2 = 'countPerformers';
    const reply2 = await call((done) => clients.performersPerformerQueryClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsor: {
          primary_address: {
            lines: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.lines ?? []),
          },
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        sponsor: {
          sponsor_id: (request?.sponsor_id ?? ''),
          status: (request?.sponsor?.status ?? ''),
        },
      },
    );
  }

  /**
   * Submits a sponsor.
   */
  async function submitSponsor(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await store.record(request.context.tenant_id, 'SubmitSponsor', request);
    logger.info('submit_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: { audit: { updated_at: request?.sponsor?.audit?.updated_at } },
    };
    const client1 = clients.registry.get('acme.venues.v1.VenuePolicySyncService');
    const reply1 = await call((done) => client1.resolveVenuePolicyConflict(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsor: { audit: { created_at: reply1?.venue_policy?.audit?.created_at } },
      },
      {
        sponsor: {
          contact: request?.sponsor?.contact,
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  return {
    approveSponsor: unary(approveSponsor),
    commentSponsorApproval: unary(commentSponsorApproval),
    delegateSponsorApproval: unary(delegateSponsorApproval),
    escalateSponsor: unary(escalateSponsor),
    getSponsorApprovalStatus: unary(getSponsorApprovalStatus),
    listPendingSponsors: unary(listPendingSponsors),
    recallSponsor: unary(recallSponsor),
    rejectSponsor: unary(rejectSponsor),
    submitSponsor: unary(submitSponsor),
  };
}

module.exports = { createSponsorApprovalHandlers };
