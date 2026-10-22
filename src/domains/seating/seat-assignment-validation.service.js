'use strict';

/**
 * Checks seat assignments against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve SeatAssignmentValidationService, recording every operation of the seating team.
 */
class SeatAssignmentValidationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'SeatAssignmentValidationService' });
  }

  /**
   * Checks a seat assignment.
   */
  checkSeatAssignmentEligibility(call, callback) {
    this.executeCheckSeatAssignmentEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCheckSeatAssignmentEligibility(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'CheckSeatAssignmentEligibility', request);
    this.logger.info('check_seat_assignment_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_2;
    switch (request?.seat_assignment?.origin) {
      case 'customer':
        selected1_2 = ((request?.seat_assignment?.origin === 'customer' ? request?.seat_assignment.customer : undefined)?.display_name ?? '');
        break;
      default:
        selected1_2 = '';
    }
    const request1 = {
      access_policy: { owner: { display_name: selected1_2 } },
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (request?.seat_assignment?.requester?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await call((done) => this.clients.authorizationAccessPolicyValidationClient.normalizeAccessPolicy(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) },
        result: { status: (reply1?.outcome?.status ?? '') },
      },
      {
        seat_assignment: {
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
          status: (request?.seat_assignment?.status ?? ''),
        },
      },
    );
  }

  /**
   * Explains a seat assignment.
   */
  explainSeatAssignmentDecision(call, callback) {
    this.executeExplainSeatAssignmentDecision(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainSeatAssignmentDecision(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'ExplainSeatAssignmentDecision', request);
    this.logger.info('explain_seat_assignment_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_assignment: {
        assignee: { user_id: (request?.seat_assignment?.assignee?.user_id ?? '') },
        channel: {
          channel: ((request?.seat_assignment?.origin === 'channel' ? request?.seat_assignment.channel : undefined)?.channel ?? ''),
        },
        comments: [{ created_at: request?.seat_assignment?.audit?.created_at }],
        sla: { breached: (request?.seat_assignment?.sla?.breached ?? false) },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.seatingSeatAssignmentCalculationClient.compareSeatAssignmentOptions(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      gate_scan: {
        subject: { display_name: (reply1?.seat_assignment?.assignee?.display_name ?? '') },
      },
    };
    const simulateGateScan2 = promisify(this.clients.admissionsGateScanCalculationClient.simulateGateScan.bind(this.clients.admissionsGateScanCalculationClient));
    const reply2 = await simulateGateScan2(request2, metadataFor(request2.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_assignment: {
          requester: {
            preferred_channel: (reply1?.seat_assignment?.requester?.preferred_channel ?? ''),
          },
        },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          message: (reply2?.result?.message ?? ''),
        },
      },
      {
        seat_assignment: {
          priority: (request?.seat_assignment?.priority ?? 0),
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a seat assignment.
   */
  getSeatAssignmentRules(call, callback) {
    this.executeGetSeatAssignmentRules(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetSeatAssignmentRules(request) {
    validateContext(request.context);
    if (!request.seat_assignment_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_assignment_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetSeatAssignmentRules', request);
    this.logger.info('get_seat_assignment_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_assignment: {
        assignee: { email: (request?.context?.actor?.email ?? '') },
        seat_assignment_id: (request?.seat_assignment_id ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.seatingSeatAssignmentCalculationClient.calculateSeatAssignment(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
        seat_assignment: { subject: (reply1?.seat_assignment?.subject ?? '') },
      },
      {
        seat_assignment: { seat_assignment_id: (request?.seat_assignment_id ?? '') },
      },
    );
  }

  /**
   * Lints a seat assignment.
   */
  lintSeatAssignments(call, callback) {
    this.executeLintSeatAssignments(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLintSeatAssignments(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'LintSeatAssignments', request);
    this.logger.info('lint_seat_assignments', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const method1 = 'resolveConcessionStands';
    const reply1 = await call((done) => this.clients.concessionsConcessionStandQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const settled2 = await Promise.allSettled((request?.seat_assignment?.comments ?? []).map(async (item) => {
      const request2 = {
        box_office_sale: { audit: { created_at: item?.created_at } },
        context: request?.context,
      };
      const reply2 = await call((done) => this.clients.boxofficeBoxOfficeSaleCalculationClient.compareBoxOfficeSaleOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        seat_assignment: {
          requester: request?.seat_assignment?.requester,
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  /**
   * Normalizes a seat assignment.
   */
  normalizeSeatAssignment(call, callback) {
    this.executeNormalizeSeatAssignment(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeNormalizeSeatAssignment(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'NormalizeSeatAssignment', request);
    this.logger.info('normalize_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.seat_assignment?.comments ?? [])) {
      const request1 = {
        context: request?.context,
        search_index: {
          audit: {
            created_at: item?.created_at,
            etag: (request?.seat_assignment?.audit?.etag ?? ''),
            updated_at: request?.seat_assignment?.audit?.updated_at,
          },
          owner: { email: (item?.author_email ?? '') },
        },
      };
      const reply1 = await call((done) => this.clients.searchSearchIndexClient.createSearchIndex(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const results2 = await Promise.all((request?.seat_assignment?.history ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        seat_assignment: { history: [{ to_status: (item?.to_status ?? '') }] },
      };
      const reply2 = await call((done) => this.clients.seatingSeatAssignmentCalculationClient.simulateSeatAssignment(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      seat_assignment: {
        requester: request?.seat_assignment?.requester,
        seat_assignment_id: (request?.seat_assignment_id ?? ''),
      },
    };
  }

  /**
   * Previews a seat assignment.
   */
  previewSeatAssignment(call, callback) {
    this.executePreviewSeatAssignment(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePreviewSeatAssignment(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'PreviewSeatAssignment', request);
    this.logger.info('preview_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_assignment: {
        audit: { updated_by: (request?.seat_assignment?.audit?.updated_by ?? '') },
        customer: {
          display_name: ((request?.seat_assignment?.origin === 'customer' ? request?.seat_assignment.customer : undefined)?.display_name ?? ''),
        },
      },
    };
    const client1 = this.clients.registry.get('acme.seating.v1.SeatAssignmentCalculationService');
    const reply1 = await call((done) => client1.quoteSeatAssignment(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_assignment: { sla: { breached: (reply1?.seat_assignment?.sla?.breached ?? false) } },
      },
      {
        seat_assignment: {
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
          status: (request?.seat_assignment?.status ?? ''),
        },
      },
    );
  }

  /**
   * Scores a seat assignment.
   */
  scoreSeatAssignment(call, callback) {
    this.executeScoreSeatAssignment(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScoreSeatAssignment(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'ScoreSeatAssignment', request);
    this.logger.info('score_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      user_id: (request?.seat_assignment?.assignee?.user_id ?? ''),
    };
    const send1 = this.clients.identityUserQueryClient.peekUser.bind(this.clients.identityUserQueryClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      subscriber: {
        name: request?.seat_assignment?.requester?.name,
        phone: (reply1?.user?.contact?.phone ?? ''),
      },
    };
    const messages2 = await collect(this.clients.ledgerLedgerAccountClient.watchLedgerAccounts(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        seat_assignment: {
          custom_fields: (request?.seat_assignment?.custom_fields ?? {}),
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  /**
   * Validates a seat assignment.
   */
  validateSeatAssignment(call, callback) {
    this.executeValidateSeatAssignment(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeValidateSeatAssignment(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'ValidateSeatAssignment', request);
    this.logger.info('validate_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      metric_series: { owner: { email: (request?.seat_assignment?.requester?.email ?? '') } },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.metricsMetricSeriesClient.createMetricSeries(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        seat_assignment: {
          custom_fields: (request?.seat_assignment?.custom_fields ?? {}),
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }
}

module.exports = { SeatAssignmentValidationService };
