'use strict';

/**
 * Builds and schedules reports about guest lists.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect, upload } = require('../../platform/streams');

/**
 * Serve GuestListReportingService, recording every operation of the boxoffice team.
 */
class GuestListReportingService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'GuestListReportingService' });
  }

  /**
   * Counts guest lists by status.
   */
  countGuestListTotals(call, callback) {
    this.executeCountGuestListTotals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountGuestListTotals(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountGuestListTotals', request);
    this.logger.info('count_guest_list_totals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: { address: { region: (request?.filter?.region ?? '') } },
    };
    const client1 = this.clients.registry.get('acme.concessions.v1.MenuItemApprovalService');
    const reply1 = await call((done) => client1.commentMenuItemApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { details: (reply1?.result?.details ?? {}) } };
  }

  /**
   * Deletes a guest list.
   */
  deleteGuestListReport(call, callback) {
    this.executeDeleteGuestListReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDeleteGuestListReport(request) {
    validateContext(request.context);
    if (!request.guest_list_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
    }
    await this.store.record(request.context.tenant_id, 'DeleteGuestListReport', request);
    this.logger.info('delete_guest_list_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.guest_list_id ?? ''),
      },
    };
    const send1 = this.clients.boxofficeBoxOfficeSaleClient.searchBoxOfficeSales.bind(this.clients.boxofficeBoxOfficeSaleClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.etag ?? '') });
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        message: (reply1?.result?.message ?? ''),
      },
    };
  }

  /**
   * Returns a guest list.
   */
  fetchGuestListReport(call, callback) {
    this.executeFetchGuestListReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFetchGuestListReport(request) {
    validateContext(request.context);
    if (!request.guest_list_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
    }
    await this.store.record(request.context.tenant_id, 'FetchGuestListReport', request);
    this.logger.info('fetch_guest_list_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      guest_list: {
        issued_at: request?.context?.deadline,
        lines: [{ attributes: { ...(request?.context?.baggage ?? {}) } }],
      },
      guest_list_id: (request?.guest_list_id ?? ''),
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.boxofficeGuestListCalculationClient.simulateGuestList(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      filter: {
        created_after: reply1?.guest_list?.audit?.updated_at,
        labels: { ...(reply1?.guest_list?.metadata ?? {}) },
        owner_id: (reply1?.guest_list?.guest_list_id ?? ''),
        region: (reply1?.guest_list?.billing_address?.region ?? ''),
      },
    };
    const reply2 = await call((done) => this.clients.boxofficeBoxOfficeSaleClient.searchBoxOfficeSales(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      { guest_list: { notes: (reply1?.guest_list?.notes ?? '') } },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      { guest_list: { guest_list_id: (request?.guest_list_id ?? '') } },
    );
  }

  /**
   * Generates a guest list.
   */
  generateGuestListReport(call, callback) {
    this.executeGenerateGuestListReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGenerateGuestListReport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'GenerateGuestListReport', request);
    this.logger.info('generate_guest_list_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.annotations ?? {}) },
        owner_id: (request?.guest_list_id ?? ''),
      },
      since: request?.context?.deadline,
      subscriber: { email: (request?.context?.actor?.email ?? '') },
    };
    const messages1 = await collect(this.clients.authorizationAccessPolicyClient.watchAccessPolicies(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.venuesVenuePolicyNotificationClient.sendVenuePolicyTestNotice(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      guest_list: {
        audit: {
          etag: (reply2?.venue_policy?.audit?.etag ?? ''),
          updated_by: (reply2?.venue_policy?.audit?.updated_by ?? ''),
        },
      },
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => this.clients.boxofficeGuestListCalculationClient.calculateGuestList(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      details: {
        destination: {
          locality: (reply3?.guest_list?.billing_address?.locality ?? ''),
          postal_code: (reply3?.guest_list?.billing_address?.postal_code ?? ''),
        },
      },
      form: {
        audit: {
          created_at: reply2?.venue_policy?.audit?.created_at,
          created_by: (reply2?.venue_policy?.audit?.created_by ?? ''),
        },
      },
    };
    const merged4 = Object.assign({}, request4, { context: request4.context });
    const reply4 = await call((done) => this.clients.formsFormCalculationClient.compareFormOptions(merged4, metadataFor(merged4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          message: (reply2?.outcome?.message ?? ''),
        },
      },
      {
        result: {
          correlation_id: (reply3?.result?.correlation_id ?? ''),
          reference_ids: (reply3?.result?.reference_ids ?? []),
        },
      },
      {
        result: {
          correlation_id: (reply4?.result?.correlation_id ?? ''),
          reference_ids: (reply4?.result?.reference_ids ?? []),
        },
      },
      { period: request?.details?.period },
    );
  }

  /**
   * Lists guest lists a page at a time.
   */
  listGuestListReports(call, callback) {
    this.executeListGuestListReports(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListGuestListReports(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListGuestListReports', request);
    this.logger.info('list_guest_list_reports', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: { created_at: request?.filter?.created_after },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.venuesVenuePolicyHistoryClient.annotateVenuePolicyVersion(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      header: {
        source: (request?.context?.actor?.user_id ?? ''),
        submitted_by: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const reply2 = await upload(
      (done) => this.clients.ticketingTicketClient.batchCreateTickets(metadataFor(request2.context), { deadline: deadline() }, done),
      [request2],
    );
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      guest_lists: [{
        lines: [{
          tax: {
            amount: (reply1?.venue_policy?.action === 'discount' ? reply1?.venue_policy.discount : undefined)?.amount,
          },
        }],
      }],
      result: { details: (reply1?.result?.details ?? {}) },
    };
  }

  /**
   * Schedules a guest list.
   */
  scheduleGuestListReport(call, callback) {
    this.executeScheduleGuestListReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScheduleGuestListReport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ScheduleGuestListReport', request);
    this.logger.info('schedule_guest_list_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: (request?.reason ?? ''),
      shipment: { audit: { created_at: request?.context?.deadline } },
    };
    const task1 = deferred(() => call((done) => this.clients.shippingShipmentCalculationClient.simulateShipment(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      box_office_sale: {
        audit: { created_by: (reply1?.shipment?.audit?.created_by ?? '') },
        billing_address: {
          lines: [...(reply1?.shipment?.billing_address?.lines ?? [])],
          recipient: (reply1?.shipment?.billing_address?.recipient ?? ''),
        },
        card: {
          network: ((reply1?.shipment?.settlement === 'card' ? reply1?.shipment.card : undefined)?.network ?? ''),
        },
      },
      context: request?.context,
    };
    const reply2 = await this.bus.request('acme.boxoffice.v1.GuestListReportingService/ScheduleGuestListReport:1', request2);
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
      { period: request?.details?.period },
    );
  }
}

module.exports = { GuestListReportingService };
