'use strict';

/**
 * Checks ticket transfers against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve TicketTransferValidationService, recording every operation of the ticketing team.
 */
class TicketTransferValidationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'TicketTransferValidationService' });
  }

  /**
   * Checks a ticket transfer.
   */
  checkTicketTransferEligibility(call, callback) {
    this.executeCheckTicketTransferEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCheckTicketTransferEligibility(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await this.store.save(this.store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await this.store.record(request.context.tenant_id, 'CheckTicketTransferEligibility', request);
    this.logger.info('check_ticket_transfer_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.ticket_transfer?.comments ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        filter: { created_after: item?.created_at },
      };
      const reply1 = await call((done) => this.clients.toursTourQueryClient.browseTours(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      concession_stand: { audit: { etag: (request?.ticket_transfer?.audit?.etag ?? '') } },
      context: request?.context,
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.concessionsConcessionStandCalculationClient.quoteConcessionStand(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      box_office_sale: {
        audit: {
          created_at: input3?.ticket_transfer?.audit?.created_at,
          created_by: (reply2?.concession_stand?.audit?.created_by ?? ''),
        },
        counterparty: {
          display_name: ((input3?.ticket_transfer?.origin === 'customer' ? input3?.ticket_transfer.customer : undefined)?.display_name ?? ''),
        },
      },
      context: input3?.context,
    };
    const reply3 = await call((done) => this.clients.boxofficeBoxOfficeSaleValidationClient.scoreBoxOfficeSale(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { filters: { ...(reply3?.box_office_sale?.metadata ?? {}) } },
      reason: (request?.reason ?? ''),
    };
    const cloned4 = JSON.parse(JSON.stringify(request4));
    const reply4 = await call((done) => this.clients.toursTourQueryClient.summarizeTours(cloned4, metadataFor(cloned4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.contact?.preferred_channel ?? '') });
    return assemble(
      {
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
        ticket_transfer: {
          assignee: { user_id: (reply2?.concession_stand?.owner?.user_id ?? '') },
        },
      },
      {
        ticket_transfer: {
          assignee: {
            display_name: (reply3?.box_office_sale?.counterparty?.display_name ?? ''),
          },
        },
      },
      {
        outcome: { message: (reply4?.result?.message ?? '') },
        result: { reference_ids: (reply4?.result?.reference_ids ?? []) },
      },
      {
        ticket_transfer: {
          subject: (request?.ticket_transfer?.subject ?? ''),
          ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
        },
      },
    );
  }

  /**
   * Explains a ticket transfer.
   */
  explainTicketTransferDecision(call, callback) {
    this.executeExplainTicketTransferDecision(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainTicketTransferDecision(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await this.store.save(this.store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await this.store.record(request.context.tenant_id, 'ExplainTicketTransferDecision', request);
    this.logger.info('explain_ticket_transfer_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      user_id: (request?.ticket_transfer?.assignee?.user_id ?? ''),
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.identityUserQueryClient.lookupUser(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.user?.audit?.created_at,
        owner_id: (reply1?.user?.relationship_manager?.user_id ?? ''),
        region: (reply1?.user?.primary_address?.region ?? ''),
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.notificationsNotificationClient.searchNotifications(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    const results3 = await Promise.all((request?.ticket_transfer?.history ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        filter: { created_after: item?.changed_at },
        subscriber: {
          name: request?.ticket_transfer?.requester?.name,
          preferred_channel: (reply1?.user?.contact?.preferred_channel ?? ''),
        },
      };
      const messages3 = await collect(this.clients.venuesVenuePolicyNotificationClient.streamVenuePolicyNotices(request3, metadataFor(request3.context), { deadline: deadline() }));
      const reply3 = messages3.at(-1) ?? {};
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      details: {
        contact: { preferred_channel: (reply1?.user?.contact?.preferred_channel ?? '') },
      },
      replay_clip: {
        owner: { user_id: (reply1?.user?.relationship_manager?.user_id ?? '') },
      },
    };
    const reply4 = await withRetry(() => call((done) => this.clients.broadcastsReplayClipValidationClient.normalizeReplayClip(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        ticket_transfer: { audit: { created_at: reply1?.user?.audit?.created_at } },
      },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
      },
      { outcome: { message: (reply4?.result?.message ?? '') } },
      {
        ticket_transfer: {
          requester: request?.ticket_transfer?.requester,
          ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a ticket transfer.
   */
  getTicketTransferRules(call, callback) {
    this.executeGetTicketTransferRules(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetTicketTransferRules(request) {
    validateContext(request.context);
    if (!request.ticket_transfer_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_transfer_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetTicketTransferRules', request);
    this.logger.info('get_ticket_transfer_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
    };
    const task1 = deferred(() => call((done) => this.clients.boxofficeBoxOfficeSaleValidationClient.validateBoxOfficeSale(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    return assemble(
      {
        ticket_transfer: { audit: { etag: (reply1?.box_office_sale?.audit?.etag ?? '') } },
      },
      {
        ticket_transfer: { ticket_transfer_id: (request?.ticket_transfer_id ?? '') },
      },
    );
  }

  /**
   * Lints a ticket transfer.
   */
  lintTicketTransfers(call, callback) {
    this.executeLintTicketTransfers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLintTicketTransfers(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await this.store.save(this.store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await this.store.record(request.context.tenant_id, 'LintTicketTransfers', request);
    this.logger.info('lint_ticket_transfers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: {
        audit: { updated_at: request?.ticket_transfer?.audit?.updated_at },
        contact: {
          name: request?.ticket_transfer?.requester?.name,
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await this.bus.request('acme.ticketing.v1.TicketTransferValidationService/LintTicketTransfers:0', request1);
    const request2 = {
      context: request?.context,
      filter: { labels: { ...(reply1?.stream_key?.labels ?? {}) } },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.identityUserQueryClient.browseUsers(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        ticket_transfer: {
          comments: [{ created_at: reply1?.stream_key?.audit?.created_at }],
          requester: { email: (reply1?.stream_key?.contact?.email ?? '') },
        },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        ticket_transfer: {
          subject: (request?.ticket_transfer?.subject ?? ''),
          ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
        },
      },
    );
  }

  /**
   * Normalizes a ticket transfer.
   */
  normalizeTicketTransfer(call, callback) {
    this.executeNormalizeTicketTransfer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeNormalizeTicketTransfer(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await this.store.save(this.store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await this.store.record(request.context.tenant_id, 'NormalizeTicketTransfer', request);
    this.logger.info('normalize_ticket_transfer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.ticket_transfer?.comments ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        details: { author_email: (item?.author_email ?? '') },
        venue: {
          audit: { created_at: item?.created_at },
          owner: {
            display_name: (request?.ticket_transfer?.assignee?.display_name ?? ''),
            email: (request?.details?.contact?.email ?? ''),
          },
        },
      };
      const reply1 = await call((done) => this.clients.venuesVenueHistoryClient.annotateVenueVersion(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    const request2 = { context: request?.context };
    const reply2 = await call((done) => this.clients.toursTourQueryClient.resolveTours(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { details: (reply2?.result?.details ?? {}) },
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
   * Previews a ticket transfer.
   */
  previewTicketTransfer(call, callback) {
    this.executePreviewTicketTransfer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePreviewTicketTransfer(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await this.store.save(this.store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await this.store.record(request.context.tenant_id, 'PreviewTicketTransfer', request);
    this.logger.info('preview_ticket_transfer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      notification: {
        assignee: {
          display_name: (request?.ticket_transfer?.assignee?.display_name ?? ''),
          email: (request?.ticket_transfer?.assignee?.email ?? ''),
        },
        requester: {
          email: (request?.ticket_transfer?.requester?.email ?? ''),
          preferred_channel: (request?.ticket_transfer?.requester?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.notificationsNotificationClient.cloneNotification(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      wristband: {
        audit: { etag: (reply1?.notification?.audit?.etag ?? '') },
        owner: {
          display_name: (request?.ticket_transfer?.assignee?.display_name ?? ''),
        },
      },
    };
    const scoreWristband2 = promisify(this.clients.admissionsWristbandValidationClient.scoreWristband.bind(this.clients.admissionsWristbandValidationClient));
    const reply2 = await scoreWristband2(request2, metadataFor(request2.context), { deadline: deadline() });
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.notification?.sla?.due_at,
        owner_id: ((reply1?.notification?.origin === 'customer' ? reply1?.notification.customer : undefined)?.notification_template_id ?? ''),
        region: ((reply2?.wristband?.locator === 'address' ? reply2?.wristband.address : undefined)?.region ?? ''),
      },
    };
    const messages3 = await collect(this.clients.venuesVenueSectionQueryClient.exportVenueSections(request3, metadataFor(request3.context), { deadline: deadline() }));
    const reply3 = messages3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { message: (reply1?.result?.message ?? '') },
        ticket_transfer: { comments: [{ created_at: reply1?.notification?.audit?.created_at }] },
      },
      {
        ticket_transfer: {
          assignee: { user_id: (reply2?.wristband?.owner?.user_id ?? '') },
          requester: { email: (reply2?.wristband?.owner?.email ?? '') },
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
   * Scores a ticket transfer.
   */
  scoreTicketTransfer(call, callback) {
    this.executeScoreTicketTransfer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScoreTicketTransfer(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await this.store.save(this.store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await this.store.record(request.context.tenant_id, 'ScoreTicketTransfer', request);
    this.logger.info('score_ticket_transfer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_2;
    switch (request?.ticket_transfer?.origin) {
      case 'channel':
        selected1_2 = ((request?.ticket_transfer?.origin === 'channel' ? request?.ticket_transfer.channel : undefined)?.channel ?? '');
        break;
      default:
        selected1_2 = '';
    }
    const request1 = {
      context: request?.context,
      details: {
        destination: { recipient: (request?.details?.destination?.recipient ?? '') },
      },
      seat_assignment: {
        assignee: { email: (request?.ticket_transfer?.assignee?.email ?? '') },
        channel: { channel: selected1_2 },
      },
    };
    const reply1 = await call((done) => this.clients.seatingSeatAssignmentValidationClient.scoreSeatAssignment(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        recipient: {
          email: (request?.ticket_transfer?.requester?.email ?? ''),
          name: reply1?.seat_assignment?.requester?.name,
          preferred_channel: (request?.ticket_transfer?.requester?.preferred_channel ?? ''),
        },
      },
    };
    const method2 = 'sendVenuePolicyTestNotice';
    const reply2 = await call((done) => this.clients.venuesVenuePolicyNotificationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
        ticket_transfer: { sla: { due_at: reply1?.seat_assignment?.sla?.due_at } },
      },
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
        ticket_transfer: { ticket_transfer_id: (request?.ticket_transfer_id ?? '') },
      },
    );
  }

  /**
   * Validates a ticket transfer.
   */
  validateTicketTransfer(call, callback) {
    this.executeValidateTicketTransfer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeValidateTicketTransfer(request) {
    validateContext(request.context);
    if (request.ticket_transfer) {
      await this.store.save(this.store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
    }
    await this.store.record(request.context.tenant_id, 'ValidateTicketTransfer', request);
    this.logger.info('validate_ticket_transfer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.ticket_transfer?.requester?.email ?? '') },
      },
    };
    const client1 = this.clients.registry.get('acme.boxoffice.v1.BoxOfficeSaleValidationService');
    const reply1 = await call((done) => client1.validateBoxOfficeSale(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const results2 = [];
    for (const item of (request?.ticket_transfer?.comments ?? [])) {
      const request2 = {
        context: request?.context,
        details: {
          recipient: {
            email: (item?.author_email ?? ''),
            name: request?.details?.contact?.name,
            phone: (request?.details?.contact?.phone ?? ''),
            preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply2 = await call((done) => this.clients.venuesVenuePolicyNotificationClient.sendVenuePolicyTestNotice(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      results2.push(reply2);
    }
    const reply2 = results2.at(-1) ?? {};
    const results3 = await Promise.all((request?.ticket_transfer?.history ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        filter: {
          created_after: item?.changed_at,
          owner_id: (request?.ticket_transfer?.assignee?.user_id ?? ''),
          region: (reply1?.box_office_sale?.billing_address?.region ?? ''),
        },
      };
      const messages3 = await collect(this.clients.toursTourQueryClient.exportTours(request3, metadataFor(request3.context), { deadline: deadline() }));
      const reply3 = messages3.at(-1) ?? {};
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.outcome?.message ?? '') },
        ticket_transfer: {
          comments: [{ created_at: reply1?.box_office_sale?.audit?.created_at }],
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
}

module.exports = { TicketTransferValidationService };
