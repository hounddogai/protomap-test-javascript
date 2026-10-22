'use strict';

/**
 * Routes guest lists through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve GuestListApprovalService, recording every operation of the boxoffice team.
 */
class GuestListApprovalService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'GuestListApprovalService' });
  }

  /**
   * Approves a guest list.
   */
  approveGuestList(call, callback) {
    this.executeApproveGuestList(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeApproveGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await this.store.save(this.store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await this.store.record(request.context.tenant_id, 'ApproveGuestList', request);
    this.logger.info('approve_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.guest_list?.audit?.updated_at,
        region: (request?.guest_list?.billing_address?.region ?? ''),
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.boxofficeGuestListReportingClient.countGuestListTotals(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      details: { recipient: { email: (input2?.details?.approver?.email ?? '') } },
    };
    const reply2 = await call((done) => this.clients.venuesVenueNotificationClient.unsubscribeVenueUpdates(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      {
        guest_list: { audit: { updated_by: (reply2?.venue?.audit?.updated_by ?? '') } },
      },
      {
        guest_list: {
          guest_list_id: (request?.guest_list_id ?? ''),
          status: (request?.guest_list?.status ?? ''),
        },
      },
    );
  }

  /**
   * Delegates a guest list.
   */
  delegateGuestListApproval(call, callback) {
    this.executeDelegateGuestListApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDelegateGuestListApproval(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await this.store.save(this.store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await this.store.record(request.context.tenant_id, 'DelegateGuestListApproval', request);
    this.logger.info('delegate_guest_list_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.guest_list?.audit?.created_at,
        region: (request?.guest_list?.billing_address?.region ?? ''),
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.pipelinesPipelineQueryClient.suggestPipelines(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      cash_drawer: {
        audit: { updated_by: (request?.guest_list?.audit?.updated_by ?? '') },
        bank: {
          iban: ((request?.guest_list?.settlement === 'bank' ? request?.guest_list.bank : undefined)?.iban ?? ''),
        },
        holder: { display_name: (request?.guest_list?.counterparty?.display_name ?? '') },
      },
      context: request?.context,
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => this.clients.boxofficeCashDrawerApprovalClient.rejectCashDrawer(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      cash_drawer: {
        card: {
          last4: ((reply2?.cash_drawer?.funding === 'card' ? reply2?.cash_drawer.card : undefined)?.last4 ?? ''),
        },
      },
      context: request?.context,
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => this.clients.boxofficeCashDrawerValidationClient.checkCashDrawerEligibility(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      { outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) } },
      {
        guest_list: {
          counterparty: { currency_code: (reply3?.cash_drawer?.currency_code ?? '') },
        },
      },
      {
        guest_list: {
          guest_list_id: (request?.guest_list_id ?? ''),
          status: (request?.guest_list?.status ?? ''),
        },
      },
    );
  }

  /**
   * Escalates a guest list.
   */
  escalateGuestList(call, callback) {
    this.executeEscalateGuestList(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEscalateGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await this.store.save(this.store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await this.store.record(request.context.tenant_id, 'EscalateGuestList', request);
    this.logger.info('escalate_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: {
        address: { locality: (request?.guest_list?.billing_address?.locality ?? '') },
        audit: { etag: (request?.guest_list?.audit?.etag ?? '') },
        owner: { email: (request?.details?.approver?.email ?? '') },
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.concessionsMenuItemWorkflowClient.retryMenuItemWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const results2 = [];
    for (const item of (request?.guest_list?.lines ?? [])) {
      const request2 = {
        box_office_sale: { lines: [{ tax: { exemptions: [...(item?.tax?.exemptions ?? [])] } }] },
        context: request?.context,
      };
      const reply2 = await call((done) => this.clients.boxofficeBoxOfficeSaleApprovalClient.delegateBoxOfficeSaleApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      results2.push(reply2);
    }
    const reply2 = results2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        guest_list: {
          guest_list_id: (request?.guest_list_id ?? ''),
          status: (request?.guest_list?.status ?? ''),
        },
      },
    );
  }

  /**
   * Returns a guest list.
   */
  getGuestListApprovalStatus(call, callback) {
    this.executeGetGuestListApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetGuestListApprovalStatus(request) {
    validateContext(request.context);
    if (!request.guest_list_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetGuestListApprovalStatus', request);
    this.logger.info('get_guest_list_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        approver: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
      replay_clip: {
        components: { primary: { component_id: (request?.guest_list_id ?? '') } },
      },
    };
    const task1 = deferred(() => call((done) => this.clients.broadcastsReplayClipApprovalClient.submitReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    return assemble(
      {
        guest_list: { audit: { etag: (reply1?.replay_clip?.audit?.etag ?? '') } },
      },
      { guest_list: { guest_list_id: (request?.guest_list_id ?? '') } },
    );
  }

  /**
   * Lists guest lists a page at a time.
   */
  listPendingGuestLists(call, callback) {
    this.executeListPendingGuestLists(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListPendingGuestLists(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListPendingGuestLists', request);
    this.logger.info('list_pending_guest_lists', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { approver: { email: (request?.context?.actor?.email ?? '') } },
      menu_item: {
        address: { region: (request?.filter?.region ?? '') },
        audit: { created_at: request?.filter?.created_after },
        owner: { user_id: (request?.context?.actor?.user_id ?? '') },
      },
    };
    const reply1 = await this.bus.request('acme.boxoffice.v1.GuestListApprovalService/ListPendingGuestLists:0', request1);
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      guest_lists: [{
        billing_address: {
          lines: ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.lines ?? []),
        },
        counterparty: { display_name: (reply1?.menu_item?.owner?.display_name ?? '') },
      }],
    };
  }

  /**
   * Recalls a guest list.
   */
  recallGuestList(call, callback) {
    this.executeRecallGuestList(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecallGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await this.store.save(this.store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await this.store.record(request.context.tenant_id, 'RecallGuestList', request);
    this.logger.info('recall_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        primary_address: { recipient: (request?.guest_list?.billing_address?.recipient ?? '') },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.sponsorshipsSponsorWorkflowClient.resumeSponsorWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = { context: request?.context };
    const reply2 = await call((done) => this.clients.boxofficeCashDrawerQueryClient.resolveCashDrawers(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.guest_list?.audit?.created_at,
        owner_id: (request?.guest_list?.counterparty?.cash_drawer_id ?? ''),
        region: (reply1?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const reply3 = await new Promise((resolve, reject) => {
      this.clients.sponsorshipsSponsorWorkflowClient.listSponsorWorkflows(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request4 = {
      cash_drawer_id: (request?.guest_list?.counterparty?.cash_drawer_id ?? ''),
      context: request?.context,
    };
    const peekCashDrawer4 = promisify(this.clients.boxofficeCashDrawerQueryClient.peekCashDrawer.bind(this.clients.boxofficeCashDrawerQueryClient));
    const reply4 = await peekCashDrawer4(request4, metadataFor(request4.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        guest_list: {
          billing_address: { postal_code: (reply1?.sponsor?.primary_address?.postal_code ?? '') },
        },
        outcome: { status: (reply1?.outcome?.status ?? '') },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          message: (reply2?.result?.message ?? ''),
        },
      },
      { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
      {
        guest_list: { audit: { updated_by: (reply4?.cash_drawer?.audit?.updated_by ?? '') } },
      },
      {
        guest_list: {
          counterparty: request?.guest_list?.counterparty,
          guest_list_id: (request?.guest_list_id ?? ''),
        },
      },
    );
  }

  /**
   * Rejects a guest list.
   */
  rejectGuestList(call, callback) {
    this.executeRejectGuestList(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRejectGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await this.store.save(this.store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await this.store.record(request.context.tenant_id, 'RejectGuestList', request);
    this.logger.info('reject_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      etag: (request?.guest_list?.audit?.etag ?? ''),
      guest_list_id: (request?.guest_list?.guest_list_id ?? ''),
      reason: (request?.reason ?? ''),
    };
    const reply1 = await call((done) => this.clients.boxofficeGuestListReportingClient.deleteGuestListReport(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const results2 = await Promise.all((request?.guest_list?.lines ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        filter: {
          created_after: request?.guest_list?.audit?.updated_at,
          labels: { ...(item?.attributes ?? {}) },
          owner_id: (item?.line_id ?? ''),
          region: (request?.guest_list?.billing_address?.region ?? ''),
        },
      };
      const reply2 = await call((done) => this.clients.documentsDocumentClient.listDocuments(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        guest_list: {
          guest_list_id: (request?.guest_list_id ?? ''),
          metadata: (request?.guest_list?.metadata ?? {}),
        },
      },
    );
  }

  /**
   * Submits a guest list.
   */
  submitGuestList(call, callback) {
    this.executeSubmitGuestList(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubmitGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await this.store.save(this.store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await this.store.record(request.context.tenant_id, 'SubmitGuestList', request);
    this.logger.info('submit_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      cash_drawer: {
        bank: {
          bic: ((request?.guest_list?.settlement === 'bank' ? request?.guest_list.bank : undefined)?.bic ?? ''),
        },
        holder: { display_name: (request?.details?.approver?.display_name ?? '') },
      },
      context: request?.context,
    };
    const method1 = 'rejectCashDrawer';
    const reply1 = await call((done) => this.clients.boxofficeCashDrawerApprovalClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.cash_drawer?.audit?.created_at,
        labels: { ...(reply1?.cash_drawer?.labels ?? {}) },
        owner_id: (reply1?.cash_drawer?.cash_drawer_id ?? ''),
        region: (request?.guest_list?.billing_address?.region ?? ''),
      },
    };
    const client2 = this.clients.registry.get('acme.boxoffice.v1.BoxOfficeSaleWorkflowService');
    const reply2 = await call((done) => client2.listBoxOfficeSaleWorkflows(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { status: (reply1?.result?.status ?? '') },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        guest_list: {
          guest_list_id: (request?.guest_list_id ?? ''),
          issued_at: request?.guest_list?.issued_at,
        },
      },
    );
  }
}

module.exports = { GuestListApprovalService };
