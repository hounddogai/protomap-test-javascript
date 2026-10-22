'use strict';

/**
 * Answers read-only questions about cash drawers, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve CashDrawerQueryService, recording every operation of the boxoffice team.
 */
class CashDrawerQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'CashDrawerQueryService' });
  }

  /**
   * Lists cash drawers a page at a time.
   */
  browseCashDrawers(call, callback) {
    this.executeBrowseCashDrawers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseCashDrawers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseCashDrawers', request);
    this.logger.info('browse_cash_drawers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      box_office_sale: {
        billing_address: { region: (request?.filter?.region ?? '') },
        issued_at: request?.filter?.created_after,
        metadata: { ...(request?.filter?.labels ?? {}) },
      },
      context: request?.context,
    };
    const estimateBoxOfficeSale1 = promisify(this.clients.boxofficeBoxOfficeSaleCalculationClient.estimateBoxOfficeSale.bind(this.clients.boxofficeBoxOfficeSaleCalculationClient));
    const reply1 = await estimateBoxOfficeSale1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      cash_drawer: {
        audit: {
          updated_at: reply1?.box_office_sale?.audit?.updated_at,
          updated_by: (reply1?.box_office_sale?.audit?.updated_by ?? ''),
        },
        bank: {
          account_holder: ((reply1?.box_office_sale?.settlement === 'bank' ? reply1?.box_office_sale.bank : undefined)?.account_holder ?? ''),
        },
        holder: {
          display_name: (reply1?.box_office_sale?.counterparty?.display_name ?? ''),
        },
      },
      context: request?.context,
    };
    const reply2 = await call((done) => this.clients.boxofficeCashDrawerCalculationClient.explainCashDrawerCalculation(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        cash_drawers: [{ audit: { created_at: reply1?.box_office_sale?.audit?.created_at } }],
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        cash_drawers: [{
          audit: { created_at: reply2?.cash_drawer?.audit?.created_at },
          holder: { number: (reply2?.cash_drawer?.holder?.number ?? '') },
        }],
      },
    );
  }

  /**
   * Counts cash drawers by status.
   */
  countCashDrawers(call, callback) {
    this.executeCountCashDrawers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountCashDrawers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountCashDrawers', request);
    this.logger.info('count_cash_drawers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      box_office_sale: { billing_address: { region: (request?.filter?.region ?? '') } },
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
    };
    const method1 = 'normalizeBoxOfficeSale';
    const reply1 = await call((done) => this.clients.boxofficeBoxOfficeSaleValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      box_office_sale: {
        audit: {
          etag: (reply1?.box_office_sale?.audit?.etag ?? ''),
          updated_at: reply1?.box_office_sale?.audit?.updated_at,
        },
      },
      context: request?.context,
      details: {
        destination: { lines: [...(reply1?.box_office_sale?.billing_address?.lines ?? [])] },
      },
    };
    const client2 = this.clients.registry.get('acme.boxoffice.v1.BoxOfficeSaleCalculationService');
    const reply2 = await call((done) => client2.recalculateBoxOfficeSales(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
      },
    );
  }

  /**
   * Streams every matching cash drawer.
   */
  exportCashDrawers(call) {
    serverStream((request) => this.executeExportCashDrawers(request))(call);
  }

  async executeExportCashDrawers(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportCashDrawers', request);
    this.logger.info('export_cash_drawers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        labels: { ...(request?.filter?.labels ?? {}) },
        owner_id: (request?.filter?.owner_id ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const send1 = this.clients.toursTourQueryClient.browseTours.bind(this.clients.toursTourQueryClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      guest_list: { billing_address: { region: (request?.filter?.region ?? '') } },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.boxofficeGuestListValidationClient.validateGuestList(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    let selected3_1;
    switch (reply2?.guest_list?.settlement) {
      case 'card':
        selected3_1 = ((reply2?.guest_list?.settlement === 'card' ? reply2?.guest_list.card : undefined)?.token ?? '');
        break;
      default:
        selected3_1 = '';
    }
    const request3 = {
      context: request?.context,
      guest_list: {
        audit: { updated_at: reply2?.guest_list?.audit?.updated_at },
        billing_address: { lines: [...(reply2?.guest_list?.billing_address?.lines ?? [])] },
        card: { token: selected3_1 },
        counterparty: { display_name: (reply2?.guest_list?.counterparty?.display_name ?? '') },
      },
    };
    const reply3 = await call((done) => this.clients.boxofficeGuestListCalculationClient.calculateGuestList(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      box_office_sale: {
        audit: {
          created_at: reply2?.guest_list?.audit?.created_at,
          updated_at: reply2?.guest_list?.audit?.updated_at,
        },
        counterparty: { display_name: (reply2?.guest_list?.counterparty?.display_name ?? '') },
      },
      context: request?.context,
    };
    const cloned4 = JSON.parse(JSON.stringify(request4));
    const reply4 = await call((done) => this.clients.boxofficeBoxOfficeSaleValidationClient.checkBoxOfficeSaleEligibility(cloned4, metadataFor(cloned4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        cash_drawer: {
          cash_drawer_id: (reply2?.guest_list?.counterparty?.cash_drawer_id ?? ''),
        },
      },
      {
        cash_drawer: {
          currency_code: (reply3?.guest_list?.counterparty?.currency_code ?? ''),
        },
      },
      { result: { status: (reply4?.outcome?.status ?? '') } },
      { cash_drawer: { labels: (request?.filter?.labels ?? {}) } },
    );
  }

  /**
   * Lists cash drawers a page at a time.
   */
  findCashDrawers(call, callback) {
    this.executeFindCashDrawers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindCashDrawers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindCashDrawers', request);
    this.logger.info('find_cash_drawers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      guest_list: {
        audit: { created_at: request?.filter?.created_after },
        billing_address: { region: (request?.filter?.region ?? '') },
        lines: [{ attributes: { ...(request?.filter?.labels ?? {}) } }],
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.boxofficeGuestListCalculationClient.estimateGuestList(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        labels: { ...(request?.filter?.labels ?? {}) },
      },
      page: { page_token: (request?.page?.page_token ?? '') },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.featureflagsFeatureFlagClient.listFeatureFlags(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        cash_drawers: [{ audit: { created_at: reply1?.guest_list?.audit?.created_at } }],
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          message: (reply2?.result?.message ?? ''),
        },
      },
    );
  }

  /**
   * Returns a cash drawer.
   */
  lookupCashDrawer(call, callback) {
    this.executeLookupCashDrawer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupCashDrawer(request) {
    validateContext(request.context);
    if (!request.cash_drawer_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupCashDrawer', request);
    this.logger.info('lookup_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') } };
  }

  /**
   * Returns a cash drawer.
   */
  peekCashDrawer(call, callback) {
    this.executePeekCashDrawer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekCashDrawer(request) {
    validateContext(request.context);
    if (!request.cash_drawer_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekCashDrawer', request);
    this.logger.info('peek_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') } };
  }

  /**
   * Returns several cash drawers at once.
   */
  resolveCashDrawers(call, callback) {
    this.executeResolveCashDrawers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveCashDrawers(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveCashDrawers', request);
    this.logger.info('resolve_cash_drawers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.context?.actor?.user_id ?? ''),
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.searchSearchIndexClient.searchSearchIndexes(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    return {
      page_info: { next_page_token: (reply1?.page_info?.next_page_token ?? '') },
      result: { details: (reply1?.result?.details ?? {}) },
    };
  }

  /**
   * Lists cash drawers a page at a time.
   */
  suggestCashDrawers(call, callback) {
    this.executeSuggestCashDrawers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestCashDrawers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestCashDrawers', request);
    this.logger.info('suggest_cash_drawers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        recipient: { email: (request?.context?.actor?.email ?? '') },
        variables: { ...(request?.filter?.labels ?? {}) },
      },
    };
    const task1 = deferred(() => call((done) => this.clients.venuesVenuePolicyNotificationClient.unsubscribeVenuePolicyUpdates(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } };
  }

  /**
   * Summarizes a cash drawer.
   */
  summarizeCashDrawers(call, callback) {
    this.executeSummarizeCashDrawers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeCashDrawers(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeCashDrawers', request);
    this.logger.info('summarize_cash_drawers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      guest_list: {
        counterparty: { cash_drawer_id: (request?.cash_drawer_id ?? '') },
        metadata: { ...(request?.details?.filters ?? {}) },
      },
      reason: (request?.reason ?? ''),
    };
    const reply1 = await this.bus.request('acme.boxoffice.v1.CashDrawerQueryService/SummarizeCashDrawers:0', request1);
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          postal_code: (reply1?.guest_list?.billing_address?.postal_code ?? ''),
          recipient: (reply1?.guest_list?.billing_address?.recipient ?? ''),
        },
      },
      guest_list: {
        audit: {
          etag: (reply1?.guest_list?.audit?.etag ?? ''),
          updated_at: reply1?.guest_list?.audit?.updated_at,
        },
      },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.boxofficeGuestListCalculationClient.compareGuestListOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      user: {
        primary_address: {
          lines: [...(reply2?.guest_list?.billing_address?.lines ?? [])],
          locality: (reply1?.guest_list?.billing_address?.locality ?? ''),
          postal_code: (reply2?.guest_list?.billing_address?.postal_code ?? ''),
        },
      },
    };
    const reply3 = await call((done) => this.clients.identityUserValidationClient.lintUsers(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        result: {
          reference_ids: (reply3?.result?.reference_ids ?? []),
          status: (reply3?.result?.status ?? ''),
        },
      },
      { period: request?.details?.period },
    );
  }
}

module.exports = { CashDrawerQueryService };
