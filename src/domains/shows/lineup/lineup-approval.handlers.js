'use strict';

/**
 * Routes lineups through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of LineupApprovalService, over the dependencies the shows team is given.
 */
function createLineupApprovalHandlers({ store, clients, bus, logger }) {
  /**
   * Approves a lineup.
   */
  async function approveLineup(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'ApproveLineup', request);
    logger.info('approve_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      ticket_transfer: { requester: { email: (request?.details?.approver?.email ?? '') } },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.ticketingTicketTransferApprovalClient.approveTicketTransfer(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        recipient: {
          email: (request?.details?.approver?.email ?? ''),
          name: reply1?.ticket_transfer?.requester?.name,
        },
      },
    };
    const reply2 = await withRetry(() => call((done) => clients.venuesVenueNotificationClient.subscribeVenueUpdates(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      { outcome: { status: (reply2?.outcome?.status ?? '') } },
      {
        lineup: {
          issued_at: request?.lineup?.issued_at,
          lineup_id: (request?.lineup_id ?? ''),
        },
      },
    );
  }

  /**
   * Comments a lineup.
   */
  async function commentLineupApproval(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'CommentLineupApproval', request);
    logger.info('comment_lineup_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const task1 = deferred(() => call((done) => clients.authorizationAccessPolicyQueryClient.summarizeAccessPolicies(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      software_license: {
        bank: {
          account_holder: ((request?.lineup?.settlement === 'bank' ? request?.lineup.bank : undefined)?.account_holder ?? ''),
          iban: ((request?.lineup?.settlement === 'bank' ? request?.lineup.bank : undefined)?.iban ?? ''),
        },
        holder: { display_name: (request?.details?.approver?.display_name ?? '') },
      },
    };
    const reply2 = await bus.request('acme.shows.v1.LineupApprovalService/CommentLineupApproval:1', request2);
    logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          details: (reply1?.result?.details ?? {}),
        },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        lineup: {
          lineup_id: (request?.lineup_id ?? ''),
          metadata: (request?.lineup?.metadata ?? {}),
        },
      },
    );
  }

  /**
   * Delegates a lineup.
   */
  async function delegateLineupApproval(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'DelegateLineupApproval', request);
    logger.info('delegate_lineup_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        location: { country_code: (request?.lineup?.billing_address?.country_code ?? '') },
        organizer: {
          email: (request?.details?.approver?.email ?? ''),
          user_id: (request?.details?.approver?.user_id ?? ''),
        },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.showsShowWorkflowClient.resumeShowWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = { context: request?.context };
    const reply2 = await call((done) => clients.showsShowClient.batchGetShows(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const results3 = await Promise.all((request?.lineup?.lines ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        filter: {
          labels: { ...(item?.attributes ?? {}) },
          owner_id: (item?.line_id ?? ''),
          region: (request?.lineup?.billing_address?.region ?? ''),
        },
      };
      const reply3 = await call((done) => clients.showsShowClient.listShows(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.outcome?.message ?? '') } },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { message: (reply2?.result?.message ?? '') },
      },
      {
        lineup: {
          billing_address: request?.lineup?.billing_address,
          lineup_id: (request?.lineup_id ?? ''),
        },
      },
    );
  }

  /**
   * Escalates a lineup.
   */
  async function escalateLineup(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'EscalateLineup', request);
    logger.info('escalate_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_id: (request?.lineup?.counterparty?.venue_id ?? ''),
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.venuesVenueNotificationClient.getVenuePreferences(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        lineup: {
          billing_address: {
            locality: ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.locality ?? ''),
          },
        },
      },
      {
        lineup: {
          lineup_id: (request?.lineup_id ?? ''),
          number: (request?.lineup?.number ?? ''),
        },
      },
    );
  }

  /**
   * Returns a lineup.
   */
  async function getLineupApprovalStatus(request) {
    validateContext(request.context);
    if (!request.lineup_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'lineup_id is required');
    }
    await store.record(request.context.tenant_id, 'GetLineupApprovalStatus', request);
    logger.info('get_lineup_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.lineup_id ?? '') },
    };
    const findVenues1 = promisify(clients.venuesVenueQueryClient.findVenues.bind(clients.venuesVenueQueryClient));
    const reply1 = await findVenues1(request1, metadataFor(request1.context), { deadline: deadline() });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          message: (reply1?.result?.message ?? ''),
        },
      },
      { lineup: { lineup_id: (request?.lineup_id ?? '') } },
    );
  }

  /**
   * Lists lineups a page at a time.
   */
  async function listPendingLineups(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListPendingLineups', request);
    logger.info('list_pending_lineups', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        owner_id: (request?.filter?.owner_id ?? ''),
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const reply1 = await call((done) => clients.venuesVenueSectionHistoryClient.listVenueSectionVersions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        details: (reply1?.result?.details ?? {}),
      },
    };
  }

  /**
   * Rejects a lineup.
   */
  async function rejectLineup(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'RejectLineup', request);
    logger.info('reject_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.lineup?.audit?.updated_at,
        region: (request?.lineup?.billing_address?.region ?? ''),
      },
    };
    const method1 = 'browseShows';
    const reply1 = await call((done) => clients.showsShowQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.result?.status ?? '') } },
      {
        lineup: {
          issued_at: request?.lineup?.issued_at,
          lineup_id: (request?.lineup_id ?? ''),
        },
      },
    );
  }

  /**
   * Submits a lineup.
   */
  async function submitLineup(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'SubmitLineup', request);
    logger.info('submit_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      lineup: {
        audit: { updated_at: request?.lineup?.audit?.updated_at },
        billing_address: { region: (request?.lineup?.billing_address?.region ?? '') },
        counterparty: { venue_id: (request?.lineup?.counterparty?.venue_id ?? '') },
      },
    };
    const client1 = clients.registry.get('acme.shows.v1.LineupLifecycleService');
    const reply1 = await call((done) => client1.scheduleLineupExpiry(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        new_owner: {
          display_name: (reply1?.lineup?.counterparty?.display_name ?? ''),
          user_id: (request?.details?.approver?.user_id ?? ''),
        },
      },
      lineup_id: (request?.lineup_id ?? ''),
    };
    const send2 = clients.showsLineupLifecycleClient.expireLineup.bind(clients.showsLineupLifecycleClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        lineup: { due_at: reply1?.lineup?.due_at },
        outcome: { status: (reply1?.outcome?.status ?? '') },
      },
      {
        lineup: {
          metadata: (reply2?.lineup?.metadata ?? {}),
          number: (reply2?.lineup?.number ?? ''),
        },
      },
      {
        lineup: {
          billing_address: request?.lineup?.billing_address,
          lineup_id: (request?.lineup_id ?? ''),
        },
      },
    );
  }

  return {
    approveLineup: unary(approveLineup),
    commentLineupApproval: unary(commentLineupApproval),
    delegateLineupApproval: unary(delegateLineupApproval),
    escalateLineup: unary(escalateLineup),
    getLineupApprovalStatus: unary(getLineupApprovalStatus),
    listPendingLineups: unary(listPendingLineups),
    rejectLineup: unary(rejectLineup),
    submitLineup: unary(submitLineup),
  };
}

module.exports = { createLineupApprovalHandlers };
