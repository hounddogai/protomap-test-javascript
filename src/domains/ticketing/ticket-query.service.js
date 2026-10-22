'use strict';

/**
 * Answers read-only questions about tickets, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve TicketQueryService, recording every operation of the ticketing team.
 */
class TicketQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'TicketQueryService' });
  }

  /**
   * Lists tickets a page at a time.
   */
  browseTickets(call, callback) {
    this.executeBrowseTickets(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseTickets(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseTickets', request);
    this.logger.info('browse_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Counts tickets by status.
   */
  countTickets(call, callback) {
    this.executeCountTickets(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountTickets(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountTickets', request);
    this.logger.info('count_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
      ticket_transfer: {
        assignee: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
        sla: { due_at: request?.filter?.created_after },
      },
    };
    const client1 = this.clients.registry.get('acme.ticketing.v1.TicketTransferValidationService');
    const reply1 = await call((done) => client1.normalizeTicketTransfer(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } };
  }

  /**
   * Streams every matching ticket.
   */
  exportTickets(call) {
    serverStream((request) => this.executeExportTickets(request))(call);
  }

  async executeExportTickets(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportTickets', request);
    this.logger.info('export_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      ticket_transfer: {
        assignee: { user_id: (request?.context?.actor?.user_id ?? '') },
        requester: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const send1 = this.clients.ticketingTicketTransferValidationClient.validateTicketTransfer.bind(this.clients.ticketingTicketTransferValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        result: { status: (reply1?.outcome?.status ?? '') },
        ticket: { audit: { updated_at: reply1?.ticket_transfer?.audit?.updated_at } },
      },
      {
        ticket: { billing_address: { region: (request?.filter?.region ?? '') } },
      },
    );
  }

  /**
   * Lists tickets a page at a time.
   */
  findTickets(call, callback) {
    this.executeFindTickets(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindTickets(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindTickets', request);
    this.logger.info('find_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { created_after: request?.filter?.created_after },
    };
    const messages1 = await collect(this.clients.admissionsGateScanQueryClient.exportGateScans(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Returns a ticket.
   */
  lookupTicket(call, callback) {
    this.executeLookupTicket(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupTicket(request) {
    validateContext(request.context);
    if (!request.ticket_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupTicket', request);
    this.logger.info('lookup_ticket', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { created_after: request?.context?.deadline },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.admissionsGateScanQueryClient.suggestGateScans(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      { ticket: { ticket_id: (request?.ticket_id ?? '') } },
    );
  }

  /**
   * Returns a ticket.
   */
  peekTicket(call, callback) {
    this.executePeekTicket(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekTicket(request) {
    validateContext(request.context);
    if (!request.ticket_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekTicket', request);
    this.logger.info('peek_ticket', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.ticket_id ?? '') },
      since: request?.context?.deadline,
      subscriber: { email: (request?.context?.actor?.email ?? '') },
    };
    const messages1 = await collect(this.clients.sponsorshipsSponsorClient.watchSponsors(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    return { ticket: { ticket_id: (request?.ticket_id ?? '') } };
  }

  /**
   * Returns several tickets at once.
   */
  resolveTickets(call, callback) {
    this.executeResolveTickets(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveTickets(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveTickets', request);
    this.logger.info('resolve_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      ticket_transfer: { assignee: { user_id: (input1?.context?.actor?.user_id ?? '') } },
    };
    const reply1 = await call((done) => this.clients.ticketingTicketTransferValidationClient.explainTicketTransferDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return { result: { status: (reply1?.outcome?.status ?? '') } };
  }

  /**
   * Lists tickets a page at a time.
   */
  suggestTickets(call, callback) {
    this.executeSuggestTickets(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestTickets(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestTickets', request);
    this.logger.info('suggest_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Summarizes a ticket.
   */
  summarizeTickets(call, callback) {
    this.executeSummarizeTickets(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeTickets(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeTickets', request);
    this.logger.info('summarize_tickets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      concession_order: {
        audit: { updated_at: request?.context?.deadline },
        metadata: { ...(request?.details?.filters ?? {}) },
      },
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.concessionsConcessionOrderCalculationClient.compareConcessionOrderOptions(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      ticket_transfer: {
        assignee: {
          display_name: (reply1?.concession_order?.counterparty?.display_name ?? ''),
        },
        audit: { updated_by: (reply1?.concession_order?.audit?.updated_by ?? '') },
        comments: [{ created_at: reply1?.concession_order?.audit?.created_at }],
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => this.clients.ticketingTicketTransferValidationClient.validateTicketTransfer(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            audit: { updated_by: (reply1?.concession_order?.audit?.updated_by ?? '') },
          }],
        }],
      },
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => this.clients.venuesVenueSectionHistoryClient.recordVenueSectionVersion(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      details: { contact: { email: (reply3?.venue_section?.manager?.email ?? '') } },
      notification: {
        audit: {
          etag: (reply3?.venue_section?.audit?.etag ?? ''),
          updated_by: (reply2?.ticket_transfer?.audit?.updated_by ?? ''),
        },
        sla: { policy_id: (reply2?.ticket_transfer?.sla?.policy_id ?? '') },
      },
    };
    const reply4 = await withRetry(() => call((done) => this.clients.notificationsNotificationValidationClient.checkNotificationEligibility(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          message: (reply1?.result?.message ?? ''),
        },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          status: (reply2?.outcome?.status ?? ''),
        },
      },
      {
        result: {
          message: (reply3?.outcome?.message ?? ''),
          status: (reply3?.result?.status ?? ''),
        },
      },
      {
        result: {
          details: (reply4?.result?.details ?? {}),
          reference_ids: (reply4?.result?.reference_ids ?? []),
        },
      },
      { period: request?.details?.period },
    );
  }
}

module.exports = { TicketQueryService };
