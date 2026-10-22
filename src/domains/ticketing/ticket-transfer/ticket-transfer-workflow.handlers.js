'use strict';

/**
 * Runs the long-lived workflow that takes a ticket transfer from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of TicketTransferWorkflowService, over the dependencies the ticketing team is given.
 */
function createTicketTransferWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a ticket transfer.
   */
  async function advanceTicketTransferWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await store.record(request.context.tenant_id, 'AdvanceTicketTransferWorkflow', request);
    logger.info('advance_ticket_transfer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.ticket_transfer?.origin) {
      case 'customer':
        selected1_1 = ((request?.ticket_transfer?.origin === 'customer' ? request?.ticket_transfer.customer : undefined)?.ticket_tier_id ?? '');
        break;
      default:
        selected1_1 = '';
    }
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { filters: { ...(request?.ticket_transfer?.custom_fields ?? {}) } },
      reason: (request?.reason ?? ''),
      ticket_tier_id: selected1_1,
    };
    const reply1 = await call((done) => clients.ticketingTicketTierQueryClient.summarizeTicketTiers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      sponsorship_deal: {
        audit: {
          created_at: request?.ticket_transfer?.audit?.created_at,
          created_by: (request?.ticket_transfer?.audit?.created_by ?? ''),
          etag: (request?.ticket_transfer?.audit?.etag ?? ''),
          updated_at: request?.ticket_transfer?.audit?.updated_at,
        },
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.sponsorshipsSponsorshipDealApprovalClient.rejectSponsorshipDeal(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        ticket_transfer: {
          audit: { updated_by: (reply2?.sponsorship_deal?.audit?.updated_by ?? '') },
        },
      },
      {
        ticket_transfer: {
          status: (request?.ticket_transfer?.status ?? ''),
          ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
        },
      },
    );
  }

  /**
   * Cancels a ticket transfer.
   */
  async function cancelTicketTransferWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await store.record(request.context.tenant_id, 'CancelTicketTransferWorkflow', request);
    logger.info('cancel_ticket_transfer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      filter: {
        created_after: input1?.ticket_transfer?.sla?.due_at,
        labels: { ...(input1?.details?.parameters ?? {}) },
        owner_id: (input1?.ticket_transfer?.sla?.policy_id ?? ''),
      },
    };
    const reply1 = await call((done) => clients.ticketingTicketQueryClient.countTickets(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    let selected2_2;
    switch (request?.ticket_transfer?.origin) {
      case 'channel':
        selected2_2 = ((request?.ticket_transfer?.origin === 'channel' ? request?.ticket_transfer.channel : undefined)?.address ?? '');
        break;
      default:
        selected2_2 = '';
    }
    const request2 = {
      context: request?.context,
      ticket_transfer: {
        channel: { address: selected2_2 },
        requester: { email: (request?.details?.notify?.email ?? '') },
      },
    };
    const reply2 = await call((done) => clients.ticketingTicketTransferApprovalClient.submitTicketTransfer(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        ticket_transfer: { audit: { etag: (reply2?.ticket_transfer?.audit?.etag ?? '') } },
      },
      {
        ticket_transfer: {
          status: (request?.ticket_transfer?.status ?? ''),
          ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a ticket transfer.
   */
  async function inspectTicketTransferWorkflow(request) {
    validateContext(request.context);
    if (!request.ticket_transfer_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_transfer_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectTicketTransferWorkflow', request);
    logger.info('inspect_ticket_transfer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return {
      ticket_transfer: { ticket_transfer_id: (request?.ticket_transfer_id ?? '') },
    };
  }

  /**
   * Lists ticket transfers a page at a time.
   */
  async function listTicketTransferWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListTicketTransferWorkflows', request);
    logger.info('list_ticket_transfer_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      ticket_tier: { owner: { email: (request?.context?.actor?.email ?? '') } },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.ticketingTicketTierApprovalClient.rejectTicketTier(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      marketing_campaign: {
        audit: {
          etag: (reply1?.ticket_tier?.audit?.etag ?? ''),
          updated_at: reply1?.ticket_tier?.audit?.updated_at,
        },
        organizer: { display_name: (reply1?.ticket_tier?.owner?.display_name ?? '') },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.campaignsMarketingCampaignClient.createMarketingCampaign(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        result: { details: (reply1?.result?.details ?? {}) },
        ticket_transfers: [{
          assignee: { display_name: (reply1?.ticket_tier?.owner?.display_name ?? '') },
        }],
      },
      {
        ticket_transfers: [{
          audit: { created_by: (reply2?.marketing_campaign?.audit?.created_by ?? '') },
        }],
      },
    );
  }

  /**
   * Pauses a ticket transfer.
   */
  async function pauseTicketTransferWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await store.record(request.context.tenant_id, 'PauseTicketTransferWorkflow', request);
    logger.info('pause_ticket_transfer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.ticket_transfer?.audit?.updated_at,
        labels: { ...(request?.ticket_transfer?.custom_fields ?? {}) },
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.ticketingTicketQueryClient.findTickets(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: {
        approver: { user_id: (request?.ticket_transfer?.assignee?.user_id ?? '') },
      },
    };
    const task2 = deferred(() => call((done) => clients.ticketingTicketTierApprovalClient.delegateTicketTierApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          status: (reply2?.outcome?.status ?? ''),
        },
      },
      {
        ticket_transfer: {
          priority: (request?.ticket_transfer?.priority ?? 0),
          ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
        },
      },
    );
  }

  /**
   * Resumes a ticket transfer.
   */
  async function resumeTicketTransferWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await store.record(request.context.tenant_id, 'ResumeTicketTransferWorkflow', request);
    logger.info('resume_ticket_transfer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.ticket_transfer?.audit?.updated_at,
        labels: { ...(request?.ticket_transfer?.custom_fields ?? {}) },
        owner_id: (request?.ticket_transfer?.sla?.policy_id ?? ''),
      },
    };
    const reply1 = await bus.request('acme.ticketing.v1.TicketTransferWorkflowService/ResumeTicketTransferWorkflow:0', request1);
    const request2 = {
      context: request?.context,
      details: { approver: { email: (request?.details?.notify?.email ?? '') } },
      menu_item: {
        audit: { created_at: request?.ticket_transfer?.audit?.created_at },
        owner: { email: (request?.ticket_transfer?.requester?.email ?? '') },
      },
    };
    const reply2 = await runInContext(request.context, () => call((done) => clients.concessionsMenuItemApprovalClient.recallMenuItem(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = { context: request?.context };
    const reply3 = await call((done) => clients.searchSearchIndexQueryClient.resolveSearchIndexes(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = { context: request?.context };
    const reply4 = await new Promise((resolve, reject) => {
      clients.authorizationAccessPolicyQueryClient.resolveAccessPolicies(request4, metadataFor(request4.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        result: { status: (reply2?.outcome?.status ?? '') },
      },
      {
        result: {
          correlation_id: (reply3?.result?.correlation_id ?? ''),
          reference_ids: (reply3?.result?.reference_ids ?? []),
        },
      },
      { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
      {
        ticket_transfer: {
          subject: (request?.ticket_transfer?.subject ?? ''),
          ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
        },
      },
    );
  }

  /**
   * Retrys a ticket transfer.
   */
  async function retryTicketTransferWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await store.record(request.context.tenant_id, 'RetryTicketTransferWorkflow', request);
    logger.info('retry_ticket_transfer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        approver: {
          display_name: (request?.ticket_transfer?.assignee?.display_name ?? ''),
        },
      },
    };
    const commentGateScanApproval1 = promisify(clients.admissionsGateScanApprovalClient.commentGateScanApproval.bind(clients.admissionsGateScanApprovalClient));
    const reply1 = await commentGateScanApproval1(request1, metadataFor(request1.context), { deadline: deadline() });
    const results2 = await Promise.all((request?.ticket_transfer?.comments ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        filter: {
          created_after: item?.created_at,
          labels: { ...(reply1?.result?.details ?? {}) },
          owner_id: (item?.comment_id ?? ''),
        },
      };
      const reply2 = await call((done) => clients.authorizationAccessPolicyQueryClient.findAccessPolicies(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          message: (reply1?.result?.message ?? ''),
          next_steps: (reply1?.outcome?.next_steps ?? []),
        },
      },
      {
        ticket_transfer: {
          priority: (request?.ticket_transfer?.priority ?? 0),
          ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
        },
      },
    );
  }

  /**
   * Starts a ticket transfer.
   */
  async function startTicketTransferWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await store.record(request.context.tenant_id, 'StartTicketTransferWorkflow', request);
    logger.info('start_ticket_transfer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.ticket_transfer?.comments ?? []).map(async (item) => {
      const request1 = {
        box_office_sale: { audit: { created_at: item?.created_at } },
        context: request?.context,
      };
      const reply1 = await call((done) => clients.boxofficeBoxOfficeSaleApprovalClient.submitBoxOfficeSale(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: {
        notify: {
          name: request?.ticket_transfer?.requester?.name,
          phone: (request?.details?.notify?.phone ?? ''),
          preferred_channel: (request?.ticket_transfer?.requester?.preferred_channel ?? ''),
        },
      },
      ticket_tier: {
        audit: { updated_by: (request?.ticket_transfer?.audit?.updated_by ?? '') },
      },
    };
    const reply2 = await call((done) => clients.ticketingTicketTierWorkflowClient.advanceTicketTierWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { period: reply2?.ticket_tier?.effective },
      reason: ((reply2?.ticket_tier?.action === 'block' ? reply2?.ticket_tier.block : undefined)?.reason ?? ''),
    };
    const method3 = 'summarizeTickets';
    const reply3 = await call((done) => clients.ticketingTicketQueryClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            audit: {
              created_by: (request?.ticket_transfer?.audit?.created_by ?? ''),
              updated_at: request?.ticket_transfer?.audit?.updated_at,
            },
          }],
        }],
      },
    };
    const client4 = clients.registry.get('acme.venues.v1.VenueSectionValidationService');
    const reply4 = await call((done) => client4.lintVenueSections(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply2?.result?.message ?? '') } },
      {
        outcome: { status: (reply3?.result?.status ?? '') },
        result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
      },
      {
        result: { correlation_id: (reply4?.result?.correlation_id ?? '') },
        ticket_transfer: { audit: { etag: (reply4?.venue_section?.audit?.etag ?? '') } },
      },
      {
        ticket_transfer: {
          assignee: request?.ticket_transfer?.assignee,
          ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
        },
      },
    );
  }

  return {
    advanceTicketTransferWorkflow: unary(advanceTicketTransferWorkflow),
    cancelTicketTransferWorkflow: unary(cancelTicketTransferWorkflow),
    inspectTicketTransferWorkflow: unary(inspectTicketTransferWorkflow),
    listTicketTransferWorkflows: unary(listTicketTransferWorkflows),
    pauseTicketTransferWorkflow: unary(pauseTicketTransferWorkflow),
    resumeTicketTransferWorkflow: unary(resumeTicketTransferWorkflow),
    retryTicketTransferWorkflow: unary(retryTicketTransferWorkflow),
    startTicketTransferWorkflow: unary(startTicketTransferWorkflow),
  };
}

module.exports = { createTicketTransferWorkflowHandlers };
