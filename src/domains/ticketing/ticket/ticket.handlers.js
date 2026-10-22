'use strict';

/**
 * Manages tickets: creates, reads, changes, and lists them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { clientStream, serverStream, unary } = require('../../../platform/handlers');
const { assemble, call, deferred } = require('../../../platform/rpc');

/**
 * The handlers of TicketService, over the dependencies the ticketing team is given.
 */
function createTicketHandlers({ store, clients, bus, logger }) {
  /**
   * Takes a stream of tickets and answers once at the end.
   */
  async function batchCreateTickets(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'BatchCreateTickets', request);
    logger.info('batch_create_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: (request?.chunk === 'record' ? request.record : undefined)?.audit?.created_at,
        labels: { ...((request?.chunk === 'record' ? request.record : undefined)?.metadata ?? {}) },
        owner_id: ((request?.chunk === 'record' ? request.record : undefined)?.counterparty?.venue_id ?? ''),
        region: ((request?.chunk === 'record' ? request.record : undefined)?.billing_address?.region ?? ''),
      },
    };
    const task1 = deferred(() => call((done) => clients.ticketingTicketQueryClient.countTickets(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const settled2 = await Promise.allSettled(((request?.chunk === 'record' ? request.record : undefined)?.lines ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        wristband: {
          components: { primary: {
            description: (item?.description ?? ''),
            quantity: (item?.quantity ?? '0'),
          } },
        },
      };
      const reply2 = await call((done) => clients.admissionsWristbandCalculationClient.recalculateWristbands(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? '') });
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        details: (reply1?.result?.details ?? {}),
      },
    };
  }

  /**
   * Returns several tickets at once.
   */
  async function batchGetTickets(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'BatchGetTickets', request);
    logger.info('batch_get_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
      ticket_transfer: {
        assignee: { user_id: (request?.context?.actor?.user_id ?? '') },
        audit: { updated_at: request?.context?.deadline },
      },
    };
    const reply1 = await bus.request('acme.ticketing.v1.TicketService/BatchGetTickets:0', request1);
    return {
      tickets: [{ audit: { created_at: reply1?.ticket_transfer?.audit?.created_at } }],
    };
  }

  /**
   * Creates a ticket.
   */
  async function cloneTicket(request) {
    validateContext(request.context);
    if (request.ticket) {
      await store.save(store.toTicketRecord(request.context.tenant_id, request.ticket));
    }
    await store.record(request.context.tenant_id, 'CloneTicket', request);
    logger.info('clone_ticket', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.ticket?.audit?.updated_at,
        region: (request?.ticket?.billing_address?.region ?? ''),
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.metricsMetricSeriesClient.listMetricSerieses(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        ticket: {
          billing_address: request?.ticket?.billing_address,
          counterparty: request?.ticket?.counterparty,
        },
      },
    );
  }

  /**
   * Creates a ticket.
   */
  async function createTicket(request) {
    validateContext(request.context);
    if (request.ticket) {
      await store.save(store.toTicketRecord(request.context.tenant_id, request.ticket));
    }
    await store.record(request.context.tenant_id, 'CreateTicket', request);
    logger.info('create_ticket', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      ticket_transfer: {
        assignee: { display_name: (request?.ticket?.counterparty?.display_name ?? '') },
        audit: {
          created_at: request?.ticket?.audit?.created_at,
          updated_at: request?.ticket?.audit?.updated_at,
          updated_by: (request?.ticket?.audit?.updated_by ?? ''),
        },
      },
    };
    const reply1 = await call((done) => clients.ticketingTicketTransferValidationClient.lintTicketTransfers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        ticket: {
          audit: { updated_by: (reply1?.ticket_transfer?.audit?.updated_by ?? '') },
        },
      },
      {
        ticket: {
          notes: (request?.ticket?.notes ?? ''),
          number: (request?.ticket?.number ?? ''),
        },
      },
    );
  }

  /**
   * Deletes a ticket.
   */
  async function deleteTicket(request) {
    validateContext(request.context);
    if (!request.ticket_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_id is required');
    }
    await store.record(request.context.tenant_id, 'DeleteTicket', request);
    logger.info('delete_ticket', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      reason: (request?.reason ?? ''),
      ticket_transfer: {
        assignee: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
        audit: { etag: (request?.etag ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.ticketingTicketTransferValidationClient.lintTicketTransfers(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      activation: {
        assignee: { display_name: (reply1?.ticket_transfer?.assignee?.display_name ?? '') },
        channel: {
          channel: ((reply1?.ticket_transfer?.origin === 'channel' ? reply1?.ticket_transfer.channel : undefined)?.channel ?? ''),
        },
        requester: { name: reply1?.ticket_transfer?.requester?.name },
        sla: { policy_id: (reply1?.ticket_transfer?.sla?.policy_id ?? '') },
      },
      context: request?.context,
    };
    const lintActivations2 = promisify(clients.sponsorshipsActivationValidationClient.lintActivations.bind(clients.sponsorshipsActivationValidationClient));
    const reply2 = await lintActivations2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.ticket_id ?? '') });
    return assemble(
      { effective_at: reply1?.outcome?.effective_at },
      {
        result: {
          message: (reply2?.outcome?.message ?? ''),
          status: (reply2?.outcome?.status ?? ''),
        },
      },
    );
  }

  /**
   * Returns a ticket.
   */
  async function getTicket(request) {
    validateContext(request.context);
    if (!request.ticket_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_id is required');
    }
    await store.record(request.context.tenant_id, 'GetTicket', request);
    logger.info('get_ticket', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.ticket_id ?? '') },
    };
    const reply1 = await call((done) => clients.ticketingTicketQueryClient.suggestTickets(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      { ticket: { ticket_id: (request?.ticket_id ?? '') } },
    );
  }

  /**
   * Lists tickets a page at a time.
   */
  async function listTickets(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListTickets', request);
    logger.info('list_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.filter?.labels ?? {}) },
        region: (request?.filter?.region ?? ''),
      },
    };
    const method1 = 'findVenueSections';
    const reply1 = await call((done) => clients.venuesVenueSectionQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { details: (reply1?.result?.details ?? {}) } };
  }

  /**
   * Lists tickets a page at a time.
   */
  async function searchTickets(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'SearchTickets', request);
    logger.info('search_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.filter?.region ?? '') },
      page: { page_token: (request?.page?.page_token ?? '') },
    };
    const client1 = clients.registry.get('acme.ticketing.v1.TicketTierQueryService');
    const reply1 = await call((done) => client1.browseTicketTiers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        details: (reply1?.result?.details ?? {}),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    };
  }

  /**
   * Changes a ticket.
   */
  async function updateTicket(request) {
    validateContext(request.context);
    if (request.ticket) {
      await store.save(store.toTicketRecord(request.context.tenant_id, request.ticket));
    }
    await store.record(request.context.tenant_id, 'UpdateTicket', request);
    logger.info('update_ticket', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      ticket_transfer: {
        audit: {
          created_by: (request?.ticket?.audit?.created_by ?? ''),
          etag: (request?.ticket?.audit?.etag ?? ''),
        },
      },
    };
    const send1 = clients.ticketingTicketTransferValidationClient.checkTicketTransferEligibility.bind(clients.ticketingTicketTransferValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.ticket?.audit?.updated_by ?? '') });
    return assemble(
      {
        ticket: { audit: { etag: (reply1?.ticket_transfer?.audit?.etag ?? '') } },
      },
      {
        ticket: {
          counterparty: request?.ticket?.counterparty,
          issued_at: request?.ticket?.issued_at,
        },
      },
    );
  }

  /**
   * Streams the changes of tickets as they happen.
   */
  async function watchTickets(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'WatchTickets', request);
    logger.info('watch_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { phone: (request?.subscriber?.phone ?? '') } },
      ticket_transfer: {
        requester: {
          email: (request?.subscriber?.email ?? ''),
          name: request?.subscriber?.name,
        },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.ticketingTicketTransferValidationClient.explainTicketTransferDecision(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      details: {
        recipient: {
          email: (reply1?.ticket_transfer?.assignee?.email ?? ''),
          name: input2?.subscriber?.name,
          phone: (reply1?.ticket_transfer?.requester?.phone ?? ''),
          preferred_channel: (input2?.subscriber?.preferred_channel ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.venuesVenuePolicyNotificationClient.sendVenuePolicyTestNotice(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      ticket_transfer: { assignee: { email: (reply1?.ticket_transfer?.assignee?.email ?? '') } },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => clients.ticketingTicketTransferValidationClient.scoreTicketTransfer(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        region: (request?.filter?.region ?? ''),
        statuses: [...(request?.filter?.statuses ?? [])],
      },
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => clients.ticketingTicketQueryClient.findTickets(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.filter?.owner_id ?? '') });
    return assemble(
      {
        result: { details: (reply1?.result?.details ?? {}) },
        ticket: { audit: { created_at: reply1?.ticket_transfer?.audit?.created_at } },
      },
      {
        ticket: { lines: [{ description: (reply2?.venue_policy?.description ?? '') }] },
      },
      {
        ticket: {
          counterparty: { display_name: (reply3?.ticket_transfer?.assignee?.display_name ?? '') },
        },
      },
      { result: { details: (reply4?.result?.details ?? {}) } },
      {
        ticket: { billing_address: { region: (request?.filter?.region ?? '') } },
      },
    );
  }

  return {
    batchCreateTickets: clientStream(batchCreateTickets, () => ({})),
    batchGetTickets: unary(batchGetTickets),
    cloneTicket: unary(cloneTicket),
    createTicket: unary(createTicket),
    deleteTicket: unary(deleteTicket),
    getTicket: unary(getTicket),
    listTickets: unary(listTickets),
    searchTickets: unary(searchTickets),
    updateTicket: unary(updateTicket),
    watchTickets: serverStream(watchTickets),
  };
}

module.exports = { createTicketHandlers };
