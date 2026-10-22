'use strict';

/**
 * Checks menu items against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve MenuItemValidationService, recording every operation of the concessions team.
 */
class MenuItemValidationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'MenuItemValidationService' });
  }

  /**
   * Checks a menu item.
   */
  checkMenuItemEligibility(call, callback) {
    this.executeCheckMenuItemEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCheckMenuItemEligibility(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'CheckMenuItemEligibility', request);
    this.logger.info('check_menu_item_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      activation: {
        requester: {
          email: (input1?.menu_item?.owner?.email ?? ''),
          name: input1?.details?.contact?.name,
        },
      },
      context: input1?.context,
    };
    const reply1 = await call((done) => this.clients.sponsorshipsActivationValidationClient.explainActivationDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.documentsDocumentQueryClient.summarizeDocuments(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      concession_stand: {
        audit: {
          created_at: request?.menu_item?.audit?.created_at,
          updated_by: (request?.menu_item?.audit?.updated_by ?? ''),
        },
      },
      context: request?.context,
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => this.clients.concessionsConcessionStandValidationClient.previewConcessionStand(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: { audit: { updated_at: reply1?.activation?.audit?.updated_at } },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        menu_item: {
          name: (reply3?.concession_stand?.name ?? ''),
          owner: { user_id: (reply3?.concession_stand?.owner?.user_id ?? '') },
        },
      },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          status: (request?.menu_item?.status ?? ''),
        },
      },
    );
  }

  /**
   * Explains a menu item.
   */
  explainMenuItemDecision(call, callback) {
    this.executeExplainMenuItemDecision(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainMenuItemDecision(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'ExplainMenuItemDecision', request);
    this.logger.info('explain_menu_item_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: {
        billing_address: {
          locality: ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.locality ?? ''),
          recipient: ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.recipient ?? ''),
        },
      },
      context: request?.context,
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.concessionsConcessionOrderValidationClient.explainConcessionOrderDecision(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_order: {
        billing_address: {
          recipient: (reply1?.concession_order?.billing_address?.recipient ?? ''),
        },
      },
      context: request?.context,
    };
    const reply2 = await withRetry(() => call((done) => this.clients.concessionsConcessionOrderCalculationClient.quoteConcessionOrder(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: { audit: { updated_at: reply1?.concession_order?.audit?.updated_at } },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      {
        menu_item: {
          audit: { updated_by: (reply2?.concession_order?.audit?.updated_by ?? '') },
        },
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
      },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          status: (request?.menu_item?.status ?? ''),
        },
      },
    );
  }

  /**
   * Returns a menu item.
   */
  getMenuItemRules(call, callback) {
    this.executeGetMenuItemRules(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetMenuItemRules(request) {
    validateContext(request.context);
    if (!request.menu_item_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetMenuItemRules', request);
    this.logger.info('get_menu_item_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { menu_item: { menu_item_id: (request?.menu_item_id ?? '') } };
  }

  /**
   * Previews a menu item.
   */
  previewMenuItem(call, callback) {
    this.executePreviewMenuItem(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePreviewMenuItem(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'PreviewMenuItem', request);
    this.logger.info('preview_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: { audit: { etag: (request?.menu_item?.audit?.etag ?? '') } },
    };
    const task1 = deferred(() => call((done) => this.clients.sponsorshipsSponsorCalculationClient.simulateSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      sponsor: {
        contact: { name: request?.details?.contact?.name },
        national_id: {
          number: ((reply1?.sponsor?.identity_document === 'passport' ? reply1?.sponsor.passport : undefined)?.number ?? ''),
        },
      },
    };
    const reply2 = await this.bus.request('acme.concessions.v1.MenuItemValidationService/PreviewMenuItem:1', request2);
    const request3 = {
      context: request?.context,
      menu_item_id: (request?.menu_item?.menu_item_id ?? ''),
    };
    const reply3 = await runInContext(request.context, () => call((done) => this.clients.concessionsMenuItemCalculationClient.getMenuItemRates(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: { audit: { updated_at: reply1?.sponsor?.audit?.updated_at } },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      { outcome: { status: (reply2?.result?.status ?? '') } },
      {
        menu_item: { dimensions: { weight: (reply3?.menu_item?.dimensions?.weight ?? 0) } },
      },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          specs: (request?.menu_item?.specs ?? {}),
        },
      },
    );
  }

  /**
   * Scores a menu item.
   */
  scoreMenuItem(call, callback) {
    this.executeScoreMenuItem(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScoreMenuItem(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'ScoreMenuItem', request);
    this.logger.info('score_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.menu_item?.audit?.created_at,
        labels: { ...(request?.annotations ?? {}) },
        owner_id: (request?.menu_item?.owner?.user_id ?? ''),
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const messages1 = await collect(this.clients.featureflagsFeatureFlagQueryClient.exportFeatureFlags(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      breakdown: { currency_code: (request?.details?.currency_code ?? '') },
      menu_item: { menu_item_id: (request?.menu_item_id ?? '') },
    };
  }

  /**
   * Validates a menu item.
   */
  validateMenuItem(call, callback) {
    this.executeValidateMenuItem(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeValidateMenuItem(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'ValidateMenuItem', request);
    this.logger.info('validate_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        recipient: {
          phone: (request?.details?.contact?.phone ?? ''),
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await call((done) => this.clients.venuesVenueSectionNotificationClient.sendVenueSectionTestNotice(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_order: {
        billing_address: { postal_code: (reply1?.venue_section?.location?.postal_code ?? '') },
      },
      context: request?.context,
      details: { contact: { email: (request?.details?.contact?.email ?? '') } },
    };
    const reply2 = await new Promise((resolve, reject) => {
      this.clients.concessionsConcessionOrderValidationClient.previewConcessionOrder(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: { audit: { created_at: reply1?.venue_section?.audit?.created_at } },
      },
      {
        outcome: { effective_at: reply2?.outcome?.effective_at },
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
      },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          specs: (request?.menu_item?.specs ?? {}),
        },
      },
    );
  }
}

module.exports = { MenuItemValidationService };
