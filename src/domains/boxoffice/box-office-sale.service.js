'use strict';

/**
 * Manages box office sales: creates, reads, changes, and lists them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { clientStream, serverStream } = require('../../platform/handlers');
const { assemble, call } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve BoxOfficeSaleService, recording every operation of the boxoffice team.
 */
class BoxOfficeSaleService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'BoxOfficeSaleService' });
  }

  /**
   * Takes a stream of box office sales and answers once at the end.
   */
  batchCreateBoxOfficeSales(call, callback) {
    clientStream((request) => this.executeBatchCreateBoxOfficeSales(request), () => ({}))(call, callback);
  }

  async executeBatchCreateBoxOfficeSales(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'BatchCreateBoxOfficeSales', request);
    this.logger.info('batch_create_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      cash_drawer: {
        bank: {
          account_holder: (((request?.chunk === 'record' ? request.record : undefined)?.settlement === 'bank' ? (request?.chunk === 'record' ? request.record : undefined).bank : undefined)?.account_holder ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.boxofficeCashDrawerValidationClient.normalizeCashDrawer(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          email: ((request?.chunk === 'header' ? request.header : undefined)?.submitted_by?.email ?? ''),
        },
      },
      wristband: {
        address: {
          recipient: ((request?.chunk === 'record' ? request.record : undefined)?.billing_address?.recipient ?? ''),
        },
        audit: { created_by: (reply1?.cash_drawer?.audit?.created_by ?? '') },
      },
    };
    const reply2 = await call((done) => this.clients.admissionsWristbandValidationClient.checkWristbandEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? '') });
    return assemble(
      {
        rejections: [{
          box_office_sale_id: (reply1?.cash_drawer?.holder?.box_office_sale_id ?? ''),
        }],
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
      },
    );
  }

  /**
   * Creates a box office sale.
   */
  cloneBoxOfficeSale(call, callback) {
    this.executeCloneBoxOfficeSale(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCloneBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await this.store.save(this.store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await this.store.record(request.context.tenant_id, 'CloneBoxOfficeSale', request);
    this.logger.info('clone_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.notificationsNotificationClient.batchGetNotifications(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.box_office_sale?.audit?.updated_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (request?.box_office_sale?.box_office_sale_id ?? ''),
        region: (request?.box_office_sale?.billing_address?.region ?? ''),
      },
    };
    const messages2 = await collect(this.clients.boxofficeCashDrawerQueryClient.exportCashDrawers(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        box_office_sale: {
          status: (request?.box_office_sale?.status ?? ''),
          total: request?.box_office_sale?.total,
        },
      },
    );
  }

  /**
   * Creates a box office sale.
   */
  createBoxOfficeSale(call, callback) {
    this.executeCreateBoxOfficeSale(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCreateBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await this.store.save(this.store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await this.store.record(request.context.tenant_id, 'CreateBoxOfficeSale', request);
    this.logger.info('create_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.box_office_sale?.billing_address?.country_code ?? ''),
        },
      },
      guest_list: {
        audit: { created_by: (request?.box_office_sale?.audit?.created_by ?? '') },
      },
    };
    const estimateGuestList1 = promisify(this.clients.boxofficeGuestListCalculationClient.estimateGuestList.bind(this.clients.boxofficeGuestListCalculationClient));
    const reply1 = await estimateGuestList1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      box_office_sale: {
        audit: { created_at: request?.box_office_sale?.audit?.created_at },
        bank: {
          bic: ((request?.box_office_sale?.settlement === 'bank' ? request?.box_office_sale.bank : undefined)?.bic ?? ''),
        },
        billing_address: { recipient: (reply1?.guest_list?.billing_address?.recipient ?? '') },
      },
      context: request?.context,
    };
    const reply2 = await call((done) => this.clients.boxofficeBoxOfficeSaleCalculationClient.calculateBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      cash_drawer: {
        audit: {
          created_by: (request?.box_office_sale?.audit?.created_by ?? ''),
          updated_by: (reply1?.guest_list?.audit?.updated_by ?? ''),
        },
        holder: { display_name: (reply1?.guest_list?.counterparty?.display_name ?? '') },
      },
      context: request?.context,
    };
    const method3 = 'lintCashDrawers';
    const reply3 = await call((done) => this.clients.boxofficeCashDrawerValidationClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      user: { audit: { etag: (reply1?.guest_list?.audit?.etag ?? '') } },
    };
    const client4 = this.clients.registry.get('acme.identity.v1.UserService');
    const reply4 = await call((done) => client4.cloneUser(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        box_office_sale: { audit: { created_by: (reply1?.guest_list?.audit?.created_by ?? '') } },
      },
      {
        box_office_sale: { audit: { updated_at: reply2?.box_office_sale?.audit?.updated_at } },
      },
      {
        box_office_sale: {
          box_office_sale_id: (reply3?.cash_drawer?.holder?.box_office_sale_id ?? ''),
        },
      },
      {
        box_office_sale: {
          number: ((reply4?.user?.identity_document === 'national_id' ? reply4?.user.national_id : undefined)?.number ?? ''),
        },
      },
      {
        box_office_sale: {
          billing_address: request?.box_office_sale?.billing_address,
          status: (request?.box_office_sale?.status ?? ''),
        },
      },
    );
  }

  /**
   * Deletes a box office sale.
   */
  deleteBoxOfficeSale(call, callback) {
    this.executeDeleteBoxOfficeSale(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDeleteBoxOfficeSale(request) {
    validateContext(request.context);
    if (!request.box_office_sale_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'box_office_sale_id is required');
    }
    await this.store.record(request.context.tenant_id, 'DeleteBoxOfficeSale', request);
    this.logger.info('delete_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const send1 = this.clients.notificationsNotificationClient.batchGetNotifications.bind(this.clients.notificationsNotificationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.etag ?? '') });
    return {
      result: {
        reference_ids: (reply1?.result?.reference_ids ?? []),
        status: (reply1?.result?.status ?? ''),
      },
    };
  }

  /**
   * Returns a box office sale.
   */
  getBoxOfficeSale(call, callback) {
    this.executeGetBoxOfficeSale(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetBoxOfficeSale(request) {
    validateContext(request.context);
    if (!request.box_office_sale_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'box_office_sale_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetBoxOfficeSale', request);
    this.logger.info('get_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      cash_drawer: { holder: { box_office_sale_id: (request?.box_office_sale_id ?? '') } },
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.boxofficeCashDrawerValidationClient.previewCashDrawer(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    return assemble(
      {
        box_office_sale: {
          counterparty: { display_name: (reply1?.cash_drawer?.holder?.display_name ?? '') },
        },
      },
      {
        box_office_sale: { box_office_sale_id: (request?.box_office_sale_id ?? '') },
      },
    );
  }

  /**
   * Lists box office sales a page at a time.
   */
  listBoxOfficeSales(call, callback) {
    this.executeListBoxOfficeSales(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListBoxOfficeSales(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListBoxOfficeSales', request);
    this.logger.info('list_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Lists box office sales a page at a time.
   */
  searchBoxOfficeSales(call, callback) {
    this.executeSearchBoxOfficeSales(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSearchBoxOfficeSales(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SearchBoxOfficeSales', request);
    this.logger.info('search_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      filter: { created_after: input1?.filter?.created_after },
    };
    const reply1 = await call((done) => this.clients.auditAuditEventQueryClient.countAuditEvents(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        message: (reply1?.result?.message ?? ''),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    };
  }

  /**
   * Changes a box office sale.
   */
  updateBoxOfficeSale(call, callback) {
    this.executeUpdateBoxOfficeSale(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeUpdateBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await this.store.save(this.store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await this.store.record(request.context.tenant_id, 'UpdateBoxOfficeSale', request);
    this.logger.info('update_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.box_office_sale?.audit?.updated_at,
        labels: { ...(request?.box_office_sale?.metadata ?? {}) },
        region: (request?.box_office_sale?.billing_address?.region ?? ''),
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.boxofficeCashDrawerQueryClient.suggestCashDrawers(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => this.clients.boxofficeBoxOfficeSaleQueryClient.resolveBoxOfficeSales(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.box_office_sale?.audit?.etag ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          message: (reply2?.result?.message ?? ''),
        },
      },
      {
        box_office_sale: {
          billing_address: request?.box_office_sale?.billing_address,
          metadata: (request?.box_office_sale?.metadata ?? {}),
        },
      },
    );
  }

  /**
   * Streams the changes of box office sales as they happen.
   */
  watchBoxOfficeSales(call) {
    serverStream((request) => this.executeWatchBoxOfficeSales(request))(call);
  }

  async executeWatchBoxOfficeSales(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'WatchBoxOfficeSales', request);
    this.logger.info('watch_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.boxofficeBoxOfficeSaleCalculationClient.quoteBoxOfficeSale(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        owner_id: (request?.filter?.owner_id ?? ''),
        region: (reply1?.box_office_sale?.billing_address?.region ?? ''),
      },
      subscriber: { email: (request?.subscriber?.email ?? '') },
    };
    const messages2 = await collect(this.clients.venuesVenueSectionNotificationClient.streamVenueSectionNotices(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
    return assemble(
      {
        box_office_sale: {
          audit: { created_by: (reply1?.box_office_sale?.audit?.created_by ?? '') },
        },
      },
      {
        box_office_sale: { billing_address: { region: (request?.filter?.region ?? '') } },
      },
    );
  }
}

module.exports = { BoxOfficeSaleService };
