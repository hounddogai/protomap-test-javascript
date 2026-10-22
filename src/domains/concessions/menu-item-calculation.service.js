'use strict';

/**
 * Computes the figures of a menu item, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve MenuItemCalculationService, recording every operation of the concessions team.
 */
class MenuItemCalculationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'MenuItemCalculationService' });
  }

  /**
   * Calculates a menu item.
   */
  calculateMenuItem(call, callback) {
    this.executeCalculateMenuItem(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCalculateMenuItem(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'CalculateMenuItem', request);
    this.logger.info('calculate_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation: {
        assignee: { display_name: (request?.menu_item?.owner?.display_name ?? '') },
        audit: {
          created_by: (request?.menu_item?.audit?.created_by ?? ''),
          updated_by: (request?.menu_item?.audit?.updated_by ?? ''),
        },
      },
      context: request?.context,
      details: { contact: { email: (request?.menu_item?.owner?.email ?? '') } },
    };
    const client1 = this.clients.registry.get('acme.sponsorships.v1.ActivationValidationService');
    const reply1 = await call((done) => client1.checkActivationEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_stand: {
        address: {
          lines: [...((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.lines ?? [])],
        },
        audit: {
          etag: (request?.menu_item?.audit?.etag ?? ''),
          updated_by: (request?.menu_item?.audit?.updated_by ?? ''),
        },
        dimensions: { width: (request?.menu_item?.dimensions?.width ?? 0) },
      },
      context: request?.context,
    };
    const send2 = this.clients.concessionsConcessionStandValidationClient.checkConcessionStandEligibility.bind(this.clients.concessionsConcessionStandValidationClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      feature_flag: {
        audit: {
          created_at: reply1?.activation?.audit?.created_at,
          created_by: (request?.menu_item?.audit?.created_by ?? ''),
        },
        owner: {
          display_name: (reply1?.activation?.assignee?.display_name ?? ''),
          email: (request?.menu_item?.owner?.email ?? ''),
        },
      },
    };
    const spread3 = {
      ...request3,
      context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
    };
    const reply3 = await call((done) => this.clients.featureflagsFeatureFlagClient.createFeatureFlag(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
    const { context: context4, ...body4 } = request;
    const input4 = { ...body4, context: context4 };
    const request4 = {
      broadcast: {
        participants: [{
          contact: {
            email: (input4?.menu_item?.owner?.email ?? ''),
            name: reply1?.activation?.requester?.name,
          },
        }],
      },
      context: input4?.context,
    };
    const reply4 = await call((done) => this.clients.broadcastsBroadcastValidationClient.normalizeBroadcast(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: {
          owner: {
            display_name: ((reply1?.activation?.origin === 'customer' ? reply1?.activation.customer : undefined)?.display_name ?? ''),
          },
        },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      {
        menu_item: {
          audit: { updated_by: (reply2?.concession_stand?.audit?.updated_by ?? '') },
          owner: reply2?.concession_stand?.owner,
        },
      },
      {
        menu_item: { owner: { user_id: (reply3?.feature_flag?.owner?.user_id ?? '') } },
      },
      { result: { status: (reply4?.result?.status ?? '') } },
      {
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
        menu_item: { menu_item_id: (request?.menu_item_id ?? '') },
      },
    );
  }

  /**
   * Compares a menu item.
   */
  compareMenuItemOptions(call, callback) {
    this.executeCompareMenuItemOptions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCompareMenuItemOptions(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'CompareMenuItemOptions', request);
    this.logger.info('compare_menu_item_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        address: { recipient: (request?.details?.destination?.recipient ?? '') },
      },
      context: request?.context,
      details: {
        destination: {
          recipient: ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.recipient ?? ''),
        },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.concessionsConcessionStandValidationClient.scoreConcessionStand(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { menu_item: { tags: (reply1?.concession_stand?.tags ?? []) } },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          specs: (request?.menu_item?.specs ?? {}),
        },
      },
    );
  }

  /**
   * Estimates a menu item.
   */
  estimateMenuItem(call, callback) {
    this.executeEstimateMenuItem(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEstimateMenuItem(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'EstimateMenuItem', request);
    this.logger.info('estimate_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        address: { lines: [...(request?.details?.destination?.lines ?? [])] },
        audit: { updated_at: request?.menu_item?.audit?.updated_at },
        dimensions: { unit: (request?.menu_item?.dimensions?.unit ?? '') },
      },
      context: request?.context,
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.concessionsConcessionStandValidationClient.checkConcessionStandEligibility(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      activation: {
        assignee: { user_id: (reply1?.concession_stand?.owner?.user_id ?? '') },
      },
      context: request?.context,
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.sponsorshipsActivationValidationClient.validateActivation(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: {
          components: { primary: {
            component_id: (reply1?.concession_stand?.components?.primary?.component_id ?? ''),
          } },
          dimensions: { weight: (reply1?.concession_stand?.dimensions?.weight ?? 0) },
        },
      },
      {
        menu_item: { audit: { created_by: (reply2?.activation?.audit?.created_by ?? '') } },
      },
      {
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
        menu_item: { menu_item_id: (request?.menu_item_id ?? '') },
      },
    );
  }

  /**
   * Explains a menu item.
   */
  explainMenuItemCalculation(call, callback) {
    this.executeExplainMenuItemCalculation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainMenuItemCalculation(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'ExplainMenuItemCalculation', request);
    this.logger.info('explain_menu_item_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        primary_address: {
          postal_code: ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.postal_code ?? ''),
        },
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.sponsorshipsSponsorClient.cloneSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      concession_stand: {
        components: { primary: {
          description: (request?.menu_item?.components?.primary?.description ?? ''),
        } },
      },
      context: request?.context,
    };
    const task2 = deferred(() => call((done) => this.clients.concessionsConcessionStandValidationClient.lintConcessionStands(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: { audit: { etag: (reply1?.sponsor?.audit?.etag ?? '') } },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      {
        menu_item: { category_path: (reply2?.concession_stand?.category_path ?? []) },
      },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          name: (request?.menu_item?.name ?? ''),
        },
      },
    );
  }

  /**
   * Returns a menu item.
   */
  getMenuItemRates(call, callback) {
    this.executeGetMenuItemRates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetMenuItemRates(request) {
    validateContext(request.context);
    if (!request.menu_item_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetMenuItemRates', request);
    this.logger.info('get_menu_item_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { menu_item: { menu_item_id: (request?.menu_item_id ?? '') } };
  }

  /**
   * Quotes a menu item.
   */
  quoteMenuItem(call, callback) {
    this.executeQuoteMenuItem(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeQuoteMenuItem(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'QuoteMenuItem', request);
    this.logger.info('quote_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: { audit: { created_by: (request?.menu_item?.audit?.created_by ?? '') } },
      context: request?.context,
      details: {
        destination: { region: (request?.details?.destination?.region ?? '') },
      },
    };
    const reply1 = await this.bus.request('acme.concessions.v1.MenuItemCalculationService/QuoteMenuItem:0', request1);
    const request2 = {
      concession_stand: { address: { region: (request?.details?.destination?.region ?? '') } },
      context: request?.context,
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.concessionsConcessionStandCalculationClient.simulateConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        breakdown: { currency_code: (reply1?.breakdown?.currency_code ?? '') },
        menu_item: {
          audit: { updated_by: (reply1?.concession_order?.audit?.updated_by ?? '') },
        },
      },
      {
        menu_item: {
          code: (reply2?.concession_stand?.code ?? ''),
          dimensions: { height: (reply2?.concession_stand?.dimensions?.height ?? 0) },
        },
      },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          owner: request?.menu_item?.owner,
        },
      },
    );
  }

  /**
   * Recalculates a menu item.
   */
  recalculateMenuItems(call, callback) {
    this.executeRecalculateMenuItems(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecalculateMenuItems(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'RecalculateMenuItems', request);
    this.logger.info('recalculate_menu_items', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: { address: { lines: [...(request?.details?.destination?.lines ?? [])] } },
      context: request?.context,
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionStandCalculationClient.simulateConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { menu_item: { capacity: (reply1?.concession_stand?.capacity ?? '0') } },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          status: (request?.menu_item?.status ?? ''),
        },
      },
    );
  }

  /**
   * Simulates a menu item.
   */
  simulateMenuItem(call, callback) {
    this.executeSimulateMenuItem(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSimulateMenuItem(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await this.store.save(this.store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await this.store.record(request.context.tenant_id, 'SimulateMenuItem', request);
    this.logger.info('simulate_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        contact: { email: (request?.menu_item?.owner?.email ?? '') },
        relationship_manager: { user_id: (request?.menu_item?.owner?.user_id ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.sponsorshipsSponsorValidationClient.previewSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      filter: {
        region: ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.region ?? ''),
      },
    };
    const listFeatureFlags2 = promisify(this.clients.featureflagsFeatureFlagClient.listFeatureFlags.bind(this.clients.featureflagsFeatureFlagClient));
    const reply2 = await listFeatureFlags2(request2, metadataFor(request2.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: { audit: { updated_by: (reply1?.sponsor?.audit?.updated_by ?? '') } },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          message: (reply2?.result?.message ?? ''),
        },
      },
      {
        menu_item: {
          code: (request?.menu_item?.code ?? ''),
          menu_item_id: (request?.menu_item_id ?? ''),
        },
      },
    );
  }
}

module.exports = { MenuItemCalculationService };
