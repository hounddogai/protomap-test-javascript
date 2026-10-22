'use strict';

/**
 * Checks concession stands against the rules before anything acts on them.
 */

const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve ConcessionStandValidationService, recording every operation of the concessions team.
 */
class ConcessionStandValidationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ConcessionStandValidationService' });
  }

  /**
   * Checks a concession stand.
   */
  checkConcessionStandEligibility(call, callback) {
    this.executeCheckConcessionStandEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCheckConcessionStandEligibility(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await this.store.save(this.store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await this.store.record(request.context.tenant_id, 'CheckConcessionStandEligibility', request);
    this.logger.info('check_concession_stand_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        region: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? ''),
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorshipDealQueryClient.countSponsorshipDeals(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    let selected2_2;
    switch (request?.concession_stand?.locator) {
      case 'address':
        selected2_2 = ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.recipient ?? '');
        break;
      default:
        selected2_2 = '';
    }
    let selected2_4;
    switch (request?.concession_stand?.locator) {
      case 'address':
        selected2_4 = ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.lines ?? []);
        break;
      default:
        selected2_4 = [];
    }
    const request2 = {
      concession_order: {
        audit: { updated_at: request?.concession_stand?.audit?.updated_at },
        billing_address: { recipient: selected2_2 },
        lines: [{
          description: (request?.concession_stand?.components?.primary?.description ?? ''),
        }],
      },
      context: request?.context,
      details: { destination: { lines: selected2_4 } },
    };
    const reply2 = await call((done) => this.clients.concessionsConcessionOrderCalculationClient.recalculateConcessionOrders(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        concession_stand: {
          audit: { created_by: (reply2?.concession_order?.audit?.created_by ?? '') },
        },
      },
      {
        concession_stand: {
          capacity: (request?.concession_stand?.capacity ?? '0'),
          concession_stand_id: (request?.concession_stand_id ?? ''),
        },
      },
    );
  }

  /**
   * Explains a concession stand.
   */
  explainConcessionStandDecision(call, callback) {
    this.executeExplainConcessionStandDecision(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainConcessionStandDecision(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await this.store.save(this.store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await this.store.record(request.context.tenant_id, 'ExplainConcessionStandDecision', request);
    this.logger.info('explain_concession_stand_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: {
        audit: {
          created_at: request?.concession_stand?.audit?.created_at,
          updated_at: request?.concession_stand?.audit?.updated_at,
        },
        lines: [{
          description: (request?.concession_stand?.components?.primary?.description ?? ''),
        }],
      },
      context: request?.context,
    };
    const reply1 = await withRetry(() => call((done) => this.clients.concessionsConcessionOrderCalculationClient.calculateConcessionOrder(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.concession_order?.audit?.created_at,
        owner_id: (request?.concession_stand?.components?.primary?.component_id ?? ''),
        region: (reply1?.concession_order?.billing_address?.region ?? ''),
      },
    };
    const messages2 = await collect(this.clients.sponsorshipsSponsorshipDealQueryClient.exportSponsorshipDeals(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    const request3 = {
      concession_order: {
        audit: { etag: (reply1?.concession_order?.audit?.etag ?? '') },
        billing_address: {
          recipient: (reply1?.concession_order?.billing_address?.recipient ?? ''),
        },
      },
      context: request?.context,
    };
    const task3 = deferred(() => call((done) => this.clients.concessionsConcessionOrderCalculationClient.quoteConcessionOrder(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      {
        concession_stand: { audit: { created_at: reply3?.concession_order?.audit?.created_at } },
        outcome: { message: (reply3?.result?.message ?? '') },
      },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          specs: (request?.concession_stand?.specs ?? {}),
        },
      },
    );
  }

  /**
   * Lints a concession stand.
   */
  lintConcessionStands(call, callback) {
    this.executeLintConcessionStands(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLintConcessionStands(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await this.store.save(this.store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await this.store.record(request.context.tenant_id, 'LintConcessionStands', request);
    this.logger.info('lint_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.concession_stand?.locator) {
      case 'address':
        selected1_1 = ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.lines ?? []);
        break;
      default:
        selected1_1 = [];
    }
    const request1 = {
      concession_order: { billing_address: { lines: selected1_1 } },
      context: request?.context,
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionOrderCalculationClient.calculateConcessionOrder(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          name: (request?.concession_stand?.name ?? ''),
        },
      },
    );
  }

  /**
   * Previews a concession stand.
   */
  previewConcessionStand(call, callback) {
    this.executePreviewConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePreviewConcessionStand(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await this.store.save(this.store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await this.store.record(request.context.tenant_id, 'PreviewConcessionStand', request);
    this.logger.info('preview_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: { audit: { created_at: request?.concession_stand?.audit?.created_at } },
      context: request?.context,
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.concessionsConcessionOrderCalculationClient.calculateConcessionOrder(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          owner: request?.concession_stand?.owner,
        },
      },
    );
  }

  /**
   * Scores a concession stand.
   */
  scoreConcessionStand(call, callback) {
    this.executeScoreConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScoreConcessionStand(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await this.store.save(this.store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await this.store.record(request.context.tenant_id, 'ScoreConcessionStand', request);
    this.logger.info('score_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: {
        counterparty: { display_name: (request?.concession_stand?.owner?.display_name ?? '') },
      },
      context: request?.context,
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionOrderCalculationClient.compareConcessionOrderOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_order: {
        audit: { updated_by: (request?.concession_stand?.audit?.updated_by ?? '') },
      },
      context: request?.context,
    };
    const reply2 = await new Promise((resolve, reject) => {
      this.clients.concessionsConcessionOrderCalculationClient.recalculateConcessionOrders(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request3 = {
      concession_stand: {
        address: { locality: (reply1?.concession_order?.billing_address?.locality ?? '') },
        audit: { updated_at: request?.concession_stand?.audit?.updated_at },
      },
      context: request?.context,
      details: {
        destination: {
          recipient: (request?.details?.destination?.recipient ?? ''),
          region: (request?.details?.destination?.region ?? ''),
        },
      },
    };
    const compareConcessionStandOptions3 = promisify(this.clients.concessionsConcessionStandCalculationClient.compareConcessionStandOptions.bind(this.clients.concessionsConcessionStandCalculationClient));
    const reply3 = await compareConcessionStandOptions3(request3, metadataFor(request3.context), { deadline: deadline() });
    const request4 = {
      context: request?.context,
      filter: { region: (reply2?.concession_order?.billing_address?.region ?? '') },
      subscriber: { email: (reply3?.concession_stand?.owner?.email ?? '') },
    };
    const messages4 = await collect(this.clients.sponsorshipsSponsorClient.watchSponsors(request4, metadataFor(request4.context), { deadline: deadline() }));
    const reply4 = messages4.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: {
          audit: { updated_by: (reply1?.concession_order?.audit?.updated_by ?? '') },
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      { concession_stand: { code: (reply3?.concession_stand?.code ?? '') } },
      {
        concession_stand: {
          capacity: (request?.concession_stand?.capacity ?? '0'),
          concession_stand_id: (request?.concession_stand_id ?? ''),
        },
      },
    );
  }

  /**
   * Validates a concession stand.
   */
  validateConcessionStand(call, callback) {
    this.executeValidateConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeValidateConcessionStand(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await this.store.save(this.store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await this.store.record(request.context.tenant_id, 'ValidateConcessionStand', request);
    this.logger.info('validate_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        address: {
          postal_code: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.postal_code ?? ''),
        },
        audit: { created_by: (request?.concession_stand?.audit?.created_by ?? '') },
        dimensions: { width: (request?.concession_stand?.dimensions?.width ?? 0) },
        owner: { user_id: (request?.concession_stand?.owner?.user_id ?? '') },
      },
      context: request?.context,
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionStandCalculationClient.estimateConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    let selected2_1;
    switch (reply1?.concession_stand?.locator) {
      case 'address':
        selected2_1 = ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.country_code ?? '');
        break;
      default:
        selected2_1 = '';
    }
    let selected2_2;
    switch (request?.concession_stand?.locator) {
      case 'address':
        selected2_2 = ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.lines ?? []);
        break;
      default:
        selected2_2 = [];
    }
    let selected2_3;
    switch (request?.concession_stand?.locator) {
      case 'address':
        selected2_3 = ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.country_code ?? '');
        break;
      default:
        selected2_3 = '';
    }
    let selected2_4;
    switch (request?.concession_stand?.locator) {
      case 'address':
        selected2_4 = ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.locality ?? '');
        break;
      default:
        selected2_4 = '';
    }
    const request2 = {
      concession_order: { billing_address: { country_code: selected2_1 } },
      context: request?.context,
      details: {
        destination: {
          country_code: selected2_3,
          lines: selected2_2,
          locality: selected2_4,
        },
      },
    };
    const reply2 = await call((done) => this.clients.concessionsConcessionOrderCalculationClient.simulateConcessionOrder(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      concession_stand: {
        dimensions: {
          length: (request?.concession_stand?.dimensions?.length ?? 0),
          unit: (reply1?.concession_stand?.dimensions?.unit ?? ''),
        },
        owner: { user_id: (reply1?.concession_stand?.owner?.user_id ?? '') },
      },
      context: request?.context,
      details: {
        destination: {
          postal_code: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.postal_code ?? ''),
        },
      },
    };
    const client3 = this.clients.registry.get('acme.concessions.v1.ConcessionStandCalculationService');
    const reply3 = await call((done) => client3.calculateConcessionStand(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: { capacity: (reply1?.concession_stand?.capacity ?? '0') },
        outcome: { status: (reply1?.result?.status ?? '') },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        concession_stand: { owner: { user_id: (reply3?.concession_stand?.owner?.user_id ?? '') } },
      },
      {
        concession_stand: {
          code: (request?.concession_stand?.code ?? ''),
          concession_stand_id: (request?.concession_stand_id ?? ''),
        },
      },
    );
  }
}

module.exports = { ConcessionStandValidationService };
