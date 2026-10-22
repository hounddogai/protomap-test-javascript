'use strict';

/**
 * Answers read-only questions about event pages, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred } = require('../../platform/rpc');
const { collect, upload } = require('../../platform/streams');

/**
 * Serve EventPageQueryService, recording every operation of the fanportal team.
 */
class EventPageQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'EventPageQueryService' });
  }

  /**
   * Lists event pages a page at a time.
   */
  browseEventPages(call, callback) {
    this.executeBrowseEventPages(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseEventPages(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseEventPages', request);
    this.logger.info('browse_event_pages', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
    };
    const task1 = deferred(() => call((done) => this.clients.fanportalWishlistNotificationClient.sendWishlistTestNotice(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { event_pages: [{ notes: (reply1?.wishlist?.notes ?? '') }] };
  }

  /**
   * Streams every matching event page.
   */
  exportEventPages(call) {
    serverStream((request) => this.executeExportEventPages(request))(call);
  }

  async executeExportEventPages(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportEventPages', request);
    this.logger.info('export_event_pages', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        owner_id: (request?.filter?.owner_id ?? ''),
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const reply1 = await this.bus.request('acme.fanportal.v1.EventPageQueryService/ExportEventPages:0', request1);
    const request2 = {
      context: request?.context,
      filter: {
        owner_id: (request?.filter?.owner_id ?? ''),
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const messages2 = await collect(this.clients.venuesVenueNotificationClient.streamVenueNotices(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      risk_register: {
        children: [{
          children: [{ manager: { email: (request?.context?.actor?.email ?? '') } }],
          location: { region: (request?.filter?.region ?? '') },
        }],
      },
    };
    const reply3 = await runInContext(request.context, () => call((done) => this.clients.riskRiskRegisterClient.updateRiskRegister(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        event_page: {
          billing_address: { postal_code: (reply3?.risk_register?.location?.postal_code ?? '') },
        },
      },
      {
        event_page: { billing_address: { region: (request?.filter?.region ?? '') } },
      },
    );
  }

  /**
   * Lists event pages a page at a time.
   */
  findEventPages(call, callback) {
    this.executeFindEventPages(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindEventPages(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindEventPages', request);
    this.logger.info('find_event_pages', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.filter?.labels ?? {}) },
        owner_id: (request?.filter?.owner_id ?? ''),
        query: (request?.filter?.query ?? ''),
      },
      order_by: (request?.order_by ?? ''),
    };
    const reply1 = await call((done) => this.clients.fanportalWishlistNotificationClient.listWishlistSubscriptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.filter?.region ?? '') });
    return { result: { status: (reply1?.result?.status ?? '') } };
  }

  /**
   * Returns an event page.
   */
  lookupEventPage(call, callback) {
    this.executeLookupEventPage(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupEventPage(request) {
    validateContext(request.context);
    if (!request.event_page_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'event_page_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupEventPage', request);
    this.logger.info('lookup_event_page', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: { owner: { user_id: (request?.context?.actor?.user_id ?? '') } },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.venuesVenuePolicySyncClient.syncVenuePolicies(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    return assemble(
      { result: { status: (reply1?.outcome?.status ?? '') } },
      { event_page: { event_page_id: (request?.event_page_id ?? '') } },
    );
  }

  /**
   * Returns an event page.
   */
  peekEventPage(call, callback) {
    this.executePeekEventPage(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekEventPage(request) {
    validateContext(request.context);
    if (!request.event_page_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'event_page_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekEventPage', request);
    this.logger.info('peek_event_page', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { event_page: { event_page_id: (request?.event_page_id ?? '') } };
  }

  /**
   * Returns several event pages at once.
   */
  resolveEventPages(call, callback) {
    this.executeResolveEventPages(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveEventPages(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveEventPages', request);
    this.logger.info('resolve_event_pages', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      header: {
        source: (request?.context?.actor?.user_id ?? ''),
        submitted_by: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const reply1 = await upload(
      (done) => this.clients.sponsorshipsSponsorClient.batchCreateSponsors(metadataFor(request1.context), { deadline: deadline() }, done),
      [request1],
    );
    return {};
  }

  /**
   * Lists event pages a page at a time.
   */
  suggestEventPages(call, callback) {
    this.executeSuggestEventPages(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestEventPages(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestEventPages', request);
    this.logger.info('suggest_event_pages', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
    };
    const sendWishlistTestNotice1 = promisify(this.clients.fanportalWishlistNotificationClient.sendWishlistTestNotice.bind(this.clients.fanportalWishlistNotificationClient));
    const reply1 = await sendWishlistTestNotice1(request1, metadataFor(request1.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      event_pages: [{ metadata: (reply1?.wishlist?.metadata ?? {}) }],
      result: { status: (reply1?.outcome?.status ?? '') },
    };
  }

  /**
   * Summarizes an event page.
   */
  summarizeEventPages(call, callback) {
    this.executeSummarizeEventPages(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeEventPages(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeEventPages', request);
    this.logger.info('summarize_event_pages', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { approver: { user_id: (request?.context?.actor?.user_id ?? '') } },
    };
    const reply1 = await call((done) => this.clients.boxofficeCashDrawerApprovalClient.escalateCashDrawer(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        notify: {
          email: (reply1?.cash_drawer?.contact?.email ?? ''),
          name: reply1?.cash_drawer?.contact?.name,
          phone: (reply1?.cash_drawer?.contact?.phone ?? ''),
        },
      },
    };
    const method2 = 'scheduleAdmissionExpiry';
    const reply2 = await call((done) => this.clients.admissionsAdmissionLifecycleClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: { recipient: { phone: (reply1?.cash_drawer?.contact?.phone ?? '') } },
    };
    const client3 = this.clients.registry.get('acme.fanportal.v1.WishlistNotificationService');
    const reply3 = await call((done) => client3.notifyWishlistWatchers(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = { context: request?.context };
    const send4 = this.clients.fanportalFanSessionQueryClient.resolveFanSessions.bind(this.clients.fanportalFanSessionQueryClient);
    const reply4 = await call((done) => send4(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.outcome?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      { result: { details: (reply3?.result?.details ?? {}) } },
      {
        result: {
          correlation_id: (reply4?.result?.correlation_id ?? ''),
          details: (reply4?.result?.details ?? {}),
        },
      },
      { period: request?.details?.period },
    );
  }
}

module.exports = { EventPageQueryService };
