'use strict';

/**
 * Answers read-only questions about ticket tiers, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred } = require('../../platform/rpc');

/**
 * Serve TicketTierQueryService, recording every operation of the ticketing team.
 */
class TicketTierQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'TicketTierQueryService' });
  }

  /**
   * Lists ticket tiers a page at a time.
   */
  browseTicketTiers(call, callback) {
    this.executeBrowseTicketTiers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseTicketTiers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseTicketTiers', request);
    this.logger.info('browse_ticket_tiers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Counts ticket tiers by status.
   */
  countTicketTiers(call, callback) {
    this.executeCountTicketTiers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountTicketTiers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountTicketTiers', request);
    this.logger.info('count_ticket_tiers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      details: { destination: { region: (input1?.filter?.region ?? '') } },
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionOrderCalculationClient.recalculateConcessionOrders(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        details: (reply1?.result?.details ?? {}),
      },
    };
  }

  /**
   * Streams every matching ticket tier.
   */
  exportTicketTiers(call) {
    serverStream((request) => this.executeExportTicketTiers(request))(call);
  }

  async executeExportTicketTiers(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportTicketTiers', request);
    this.logger.info('export_ticket_tiers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
      ticket_transfer: { assignee: { user_id: (request?.context?.actor?.user_id ?? '') } },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.ticketingTicketTransferValidationClient.scoreTicketTransfer(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { name: reply1?.ticket_transfer?.requester?.name } },
      ticket_transfer: { sla: { due_at: reply1?.ticket_transfer?.sla?.due_at } },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => this.clients.ticketingTicketTransferValidationClient.explainTicketTransferDecision(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.filter?.labels ?? {}) },
        owner_id: (request?.filter?.owner_id ?? ''),
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => this.clients.venuesVenueSectionQueryClient.browseVenueSections(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    let selected4_2;
    switch (reply1?.ticket_transfer?.origin) {
      case 'customer':
        selected4_2 = ((reply1?.ticket_transfer?.origin === 'customer' ? reply1?.ticket_transfer.customer : undefined)?.display_name ?? '');
        break;
      default:
        selected4_2 = '';
    }
    const request4 = {
      concession_order: {
        audit: {
          created_by: (reply1?.ticket_transfer?.audit?.created_by ?? ''),
          etag: (reply1?.ticket_transfer?.audit?.etag ?? ''),
          updated_at: reply1?.ticket_transfer?.audit?.updated_at,
        },
        counterparty: { display_name: selected4_2 },
      },
      context: request?.context,
    };
    const reply4 = await call((done) => this.clients.concessionsConcessionOrderCalculationClient.estimateConcessionOrder(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        ticket_tier: {
          ticket_tier_id: ((reply1?.ticket_transfer?.origin === 'customer' ? reply1?.ticket_transfer.customer : undefined)?.ticket_tier_id ?? ''),
        },
      },
      {
        ticket_tier: {
          owner: {
            display_name: ((reply2?.ticket_transfer?.origin === 'customer' ? reply2?.ticket_transfer.customer : undefined)?.display_name ?? ''),
            user_id: (reply2?.ticket_transfer?.assignee?.user_id ?? ''),
          },
        },
      },
      { result: { message: (reply3?.result?.message ?? '') } },
      {
        ticket_tier: { audit: { created_at: reply4?.concession_order?.audit?.created_at } },
      },
    );
  }

  /**
   * Lists ticket tiers a page at a time.
   */
  findTicketTiers(call, callback) {
    this.executeFindTicketTiers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindTicketTiers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindTicketTiers', request);
    this.logger.info('find_ticket_tiers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Returns a ticket tier.
   */
  lookupTicketTier(call, callback) {
    this.executeLookupTicketTier(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupTicketTier(request) {
    validateContext(request.context);
    if (!request.ticket_tier_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_tier_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupTicketTier', request);
    this.logger.info('lookup_ticket_tier', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.ticket_tier_id ?? ''),
      },
    };
    const task1 = deferred(() => call((done) => this.clients.concessionsConcessionStandQueryClient.countConcessionStands(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      { ticket_tier: { ticket_tier_id: (request?.ticket_tier_id ?? '') } },
    );
  }

  /**
   * Returns a ticket tier.
   */
  peekTicketTier(call, callback) {
    this.executePeekTicketTier(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekTicketTier(request) {
    validateContext(request.context);
    if (!request.ticket_tier_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_tier_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekTicketTier', request);
    this.logger.info('peek_ticket_tier', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { ticket_tier: { ticket_tier_id: (request?.ticket_tier_id ?? '') } };
  }

  /**
   * Returns several ticket tiers at once.
   */
  resolveTicketTiers(call, callback) {
    this.executeResolveTicketTiers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveTicketTiers(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveTicketTiers', request);
    this.logger.info('resolve_ticket_tiers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      ticket_transfer: { assignee: { email: (request?.context?.actor?.email ?? '') } },
    };
    const reply1 = await this.bus.request('acme.ticketing.v1.TicketTierQueryService/ResolveTicketTiers:0', request1);
    return {
      ticket_tiers: [{
        owner: {
          display_name: ((reply1?.ticket_transfer?.origin === 'customer' ? reply1?.ticket_transfer.customer : undefined)?.display_name ?? ''),
          email: (reply1?.ticket_transfer?.assignee?.email ?? ''),
        },
      }],
    };
  }

  /**
   * Lists ticket tiers a page at a time.
   */
  suggestTicketTiers(call, callback) {
    this.executeSuggestTicketTiers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestTicketTiers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestTicketTiers', request);
    this.logger.info('suggest_ticket_tiers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      ticket_transfer: { assignee: { email: (request?.context?.actor?.email ?? '') } },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.ticketingTicketTransferValidationClient.explainTicketTransferDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: { status: (reply1?.outcome?.status ?? '') },
      ticket_tiers: [{
        version: ((reply1?.ticket_transfer?.origin === 'customer' ? reply1?.ticket_transfer.customer : undefined)?.version ?? '0'),
      }],
    };
  }

  /**
   * Summarizes a ticket tier.
   */
  summarizeTicketTiers(call, callback) {
    this.executeSummarizeTicketTiers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeTicketTiers(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeTicketTiers', request);
    this.logger.info('summarize_ticket_tiers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const reply1 = await call((done) => this.clients.venuesVenueSectionQueryClient.resolveVenueSections(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          reference_ids: (reply1?.result?.reference_ids ?? []),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { period: request?.details?.period },
    );
  }
}

module.exports = { TicketTierQueryService };
