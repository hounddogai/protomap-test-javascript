'use strict';

/**
 * Runs the long-lived workflow that takes a ticket tier from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call } = require('../../../platform/rpc');
const { exchange } = require('../../../platform/streams');

/**
 * The handlers of TicketTierWorkflowService, over the dependencies the ticketing team is given.
 */
function createTicketTierWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a ticket tier.
   */
  async function advanceTicketTierWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_tier) {
      await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
    }
    await store.record(request.context.tenant_id, 'AdvanceTicketTierWorkflow', request);
    logger.info('advance_ticket_tier_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { name: request?.details?.notify?.name } },
      ticket_transfer: {
        customer: { display_name: (request?.ticket_tier?.owner?.display_name ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.ticketingTicketTransferValidationClient.explainTicketTransferDecision(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      filter: { created_after: reply1?.ticket_transfer?.sla?.due_at },
    };
    const findTickets2 = promisify(clients.ticketingTicketQueryClient.findTickets.bind(clients.ticketingTicketQueryClient));
    const reply2 = await findTickets2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { ticket_tier: { priority: (reply1?.ticket_transfer?.priority ?? 0) } },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        ticket_tier: {
          parameters: (request?.details?.parameters ?? {}),
          ticket_tier_id: (request?.ticket_tier_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a ticket tier.
   */
  async function inspectTicketTierWorkflow(request) {
    validateContext(request.context);
    if (!request.ticket_tier_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_tier_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectTicketTierWorkflow', request);
    logger.info('inspect_ticket_tier_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.context?.actor?.user_id ?? ''),
      },
    };
    const reply1 = await call((done) => clients.ticketingTicketWorkflowClient.listTicketWorkflows(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          reference_ids: (reply1?.result?.reference_ids ?? []),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { ticket_tier: { ticket_tier_id: (request?.ticket_tier_id ?? '') } },
    );
  }

  /**
   * Lists ticket tiers a page at a time.
   */
  async function listTicketTierWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListTicketTierWorkflows', request);
    logger.info('list_ticket_tier_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { notify: { email: (request?.context?.actor?.email ?? '') } },
      ticket: {
        billing_address: { region: (request?.filter?.region ?? '') },
        issued_at: request?.filter?.created_after,
        metadata: { ...(request?.filter?.labels ?? {}) },
      },
    };
    const method1 = 'startTicketWorkflow';
    const reply1 = await call((done) => clients.ticketingTicketWorkflowClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } };
  }

  /**
   * Pauses a ticket tier.
   */
  async function pauseTicketTierWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_tier) {
      await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
    }
    await store.record(request.context.tenant_id, 'PauseTicketTierWorkflow', request);
    logger.info('pause_ticket_tier_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      cash_drawer: {
        audit: { updated_by: (request?.ticket_tier?.audit?.updated_by ?? '') },
      },
      context: request?.context,
      details: { approver: { email: (request?.details?.notify?.email ?? '') } },
    };
    const client1 = clients.registry.get('acme.boxoffice.v1.CashDrawerApprovalService');
    const reply1 = await call((done) => client1.escalateCashDrawer(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      tour_budget: {
        audit: { updated_by: (request?.ticket_tier?.audit?.updated_by ?? '') },
        bank: {
          iban: ((reply1?.cash_drawer?.funding === 'bank' ? reply1?.cash_drawer.bank : undefined)?.iban ?? ''),
        },
        contact: { email: (reply1?.cash_drawer?.contact?.email ?? '') },
      },
    };
    const send2 = clients.toursTourBudgetApprovalClient.escalateTourBudget.bind(clients.toursTourBudgetApprovalClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        ticket_tier: { audit: { created_by: (reply1?.cash_drawer?.audit?.created_by ?? '') } },
      },
      {
        outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) },
        result: { message: (reply2?.outcome?.message ?? '') },
      },
      {
        ticket_tier: {
          ticket_tier_id: (request?.ticket_tier_id ?? ''),
          version: (request?.ticket_tier?.version ?? '0'),
        },
      },
    );
  }

  /**
   * Resumes a ticket tier.
   */
  async function resumeTicketTierWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_tier) {
      await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
    }
    await store.record(request.context.tenant_id, 'ResumeTicketTierWorkflow', request);
    logger.info('resume_ticket_tier_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: {
        audit: {
          etag: (request?.ticket_tier?.audit?.etag ?? ''),
          updated_at: request?.ticket_tier?.audit?.updated_at,
        },
        contact: {
          email: (request?.ticket_tier?.owner?.email ?? ''),
          phone: (request?.details?.notify?.phone ?? ''),
        },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.broadcastsStreamKeyApprovalClient.commentStreamKeyApproval(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.outcome?.status ?? ''),
        },
      },
      {
        ticket_tier: {
          parameters: (request?.details?.parameters ?? {}),
          ticket_tier_id: (request?.ticket_tier_id ?? ''),
        },
      },
    );
  }

  /**
   * Signals a ticket tier.
   */
  async function signalTicketTierWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_tier) {
      await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
    }
    await store.record(request.context.tenant_id, 'SignalTicketTierWorkflow', request);
    logger.info('signal_ticket_tier_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      ticket_tier_id: (input1?.ticket_tier?.ticket_tier_id ?? ''),
    };
    const reply1 = await call((done) => clients.ticketingTicketTierQueryClient.lookupTicketTier(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      change: {
        children: [{ name: (request?.ticket_tier?.name ?? '') }],
        name: (request?.ticket_tier?.name ?? ''),
        parent_id: (request?.context?.actor?.user_id ?? ''),
      },
      context: request?.context,
    };
    const messages2 = await exchange(clients.venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request2.context), { deadline: deadline() }), [request2]);
    const reply2 = messages2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        ticket_tier: {
          audit: { created_at: reply1?.ticket_tier?.audit?.created_at },
          description: (reply1?.ticket_tier?.description ?? ''),
        },
      },
      {
        ticket_tier: {
          parameters: (request?.details?.parameters ?? {}),
          ticket_tier_id: (request?.ticket_tier_id ?? ''),
        },
      },
    );
  }

  /**
   * Starts a ticket tier.
   */
  async function startTicketTierWorkflow(request) {
    validateContext(request.context);
    if (request.ticket_tier) {
      await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
    }
    await store.record(request.context.tenant_id, 'StartTicketTierWorkflow', request);
    logger.info('start_ticket_tier_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.ticket_tier?.conditions ?? [])) {
      const request1 = {
        context: request?.context,
        ticket_tier: {
          conditions: [{
            attribute: (item?.attribute ?? ''),
            operator: (item?.operator ?? ''),
            value: (item?.value ?? ''),
          }],
        },
      };
      const reply1 = await call((done) => clients.ticketingTicketTierApprovalClient.delegateTicketTierApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: ((reply1?.ticket_tier?.action === 'block' ? reply1?.ticket_tier.block : undefined)?.reason ?? ''),
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.documentsDocumentQueryClient.summarizeDocuments(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: { approver: { email: (request?.details?.notify?.email ?? '') } },
      tour_budget: {
        audit: {
          created_at: request?.ticket_tier?.audit?.created_at,
          created_by: (request?.ticket_tier?.audit?.created_by ?? ''),
        },
        contact: { email: (reply1?.ticket_tier?.owner?.email ?? '') },
      },
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => clients.toursTourBudgetApprovalClient.approveTourBudget(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        outcome: { effective_at: reply3?.outcome?.effective_at },
        result: { status: (reply3?.outcome?.status ?? '') },
      },
      {
        ticket_tier: {
          ticket_tier_id: (request?.ticket_tier_id ?? ''),
          version: (request?.ticket_tier?.version ?? '0'),
        },
      },
    );
  }

  return {
    advanceTicketTierWorkflow: unary(advanceTicketTierWorkflow),
    inspectTicketTierWorkflow: unary(inspectTicketTierWorkflow),
    listTicketTierWorkflows: unary(listTicketTierWorkflows),
    pauseTicketTierWorkflow: unary(pauseTicketTierWorkflow),
    resumeTicketTierWorkflow: unary(resumeTicketTierWorkflow),
    signalTicketTierWorkflow: unary(signalTicketTierWorkflow),
    startTicketTierWorkflow: unary(startTicketTierWorkflow),
  };
}

module.exports = { createTicketTierWorkflowHandlers };
