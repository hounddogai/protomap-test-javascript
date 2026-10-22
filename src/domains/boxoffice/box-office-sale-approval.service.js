'use strict';

/**
 * Routes box office sales through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve BoxOfficeSaleApprovalService, recording every operation of the boxoffice team.
 */
class BoxOfficeSaleApprovalService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'BoxOfficeSaleApprovalService' });
  }

  /**
   * Approves a box office sale.
   */
  approveBoxOfficeSale(call, callback) {
    this.executeApproveBoxOfficeSale(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeApproveBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await this.store.save(this.store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await this.store.record(request.context.tenant_id, 'ApproveBoxOfficeSale', request);
    this.logger.info('approve_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      filter: {
        created_after: input1?.box_office_sale?.due_at,
        labels: { ...(input1?.box_office_sale?.metadata ?? {}) },
        owner_id: (input1?.box_office_sale?.box_office_sale_id ?? ''),
        region: (input1?.box_office_sale?.billing_address?.region ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.shippingShipmentQueryClient.findShipments(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      cash_drawer: { audit: { etag: (request?.box_office_sale?.audit?.etag ?? '') } },
      context: request?.context,
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.boxofficeCashDrawerCalculationClient.recalculateCashDrawers(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        labels: { ...(reply2?.cash_drawer?.labels ?? {}) },
        region: (request?.box_office_sale?.billing_address?.region ?? ''),
      },
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => this.clients.boxofficeBoxOfficeSaleClient.listBoxOfficeSales(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    const results4 = [];
    for (const item of (request?.box_office_sale?.lines ?? [])) {
      const request4 = {
        context: request?.context,
        guest_list: { lines: [{ tax: { jurisdiction: (item?.tax?.jurisdiction ?? '') } }] },
      };
      const reply4 = await call((done) => this.clients.boxofficeGuestListValidationClient.normalizeGuestList(request4, metadataFor(request4.context), { deadline: deadline() }, done));
      results4.push(reply4);
    }
    const reply4 = results4.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        box_office_sale: { audit: { updated_by: (reply2?.cash_drawer?.audit?.updated_by ?? '') } },
      },
      {
        outcome: { message: (reply3?.result?.message ?? '') },
        result: { correlation_id: (reply3?.result?.correlation_id ?? '') },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          total: request?.box_office_sale?.total,
        },
      },
    );
  }

  /**
   * Comments a box office sale.
   */
  commentBoxOfficeSaleApproval(call, callback) {
    this.executeCommentBoxOfficeSaleApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCommentBoxOfficeSaleApproval(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await this.store.save(this.store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await this.store.record(request.context.tenant_id, 'CommentBoxOfficeSaleApproval', request);
    this.logger.info('comment_box_office_sale_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        approver: { display_name: (request?.details?.approver?.display_name ?? '') },
      },
      replay_clip: {
        address: {
          country_code: (request?.box_office_sale?.billing_address?.country_code ?? ''),
          locality: (request?.box_office_sale?.billing_address?.locality ?? ''),
        },
        owner: { user_id: (request?.details?.approver?.user_id ?? '') },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.broadcastsReplayClipApprovalClient.approveReplayClip(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      cash_drawer: {
        audit: {
          created_at: reply1?.replay_clip?.audit?.created_at,
          created_by: (reply1?.replay_clip?.audit?.created_by ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: {
          postal_code: (request?.box_office_sale?.billing_address?.postal_code ?? ''),
        },
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.boxofficeCashDrawerCalculationClient.calculateCashDrawer(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        box_office_sale: {
          lines: [{
            description: (reply1?.replay_clip?.components?.primary?.description ?? ''),
          }],
        },
        outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') },
      },
      {
        box_office_sale: {
          box_office_sale_id: (reply2?.cash_drawer?.holder?.box_office_sale_id ?? ''),
        },
      },
      {
        box_office_sale: {
          metadata: (request?.box_office_sale?.metadata ?? {}),
          total: request?.box_office_sale?.total,
        },
      },
    );
  }

  /**
   * Delegates a box office sale.
   */
  delegateBoxOfficeSaleApproval(call, callback) {
    this.executeDelegateBoxOfficeSaleApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDelegateBoxOfficeSaleApproval(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await this.store.save(this.store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await this.store.record(request.context.tenant_id, 'DelegateBoxOfficeSaleApproval', request);
    this.logger.info('delegate_box_office_sale_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      cash_drawer_id: (request?.box_office_sale?.counterparty?.cash_drawer_id ?? ''),
      context: request?.context,
      details: {
        new_owner: {
          display_name: (request?.box_office_sale?.counterparty?.display_name ?? ''),
          user_id: (request?.details?.approver?.user_id ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => this.clients.boxofficeCashDrawerLifecycleClient.suspendCashDrawer(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      filter: { region: (request?.box_office_sale?.billing_address?.region ?? '') },
    };
    const reply2 = await this.bus.request('acme.boxoffice.v1.BoxOfficeSaleApprovalService/DelegateBoxOfficeSaleApproval:1', request2);
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      {
        box_office_sale: { box_office_sale_id: (request?.box_office_sale_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Escalates a box office sale.
   */
  escalateBoxOfficeSale(call, callback) {
    this.executeEscalateBoxOfficeSale(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEscalateBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await this.store.save(this.store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await this.store.record(request.context.tenant_id, 'EscalateBoxOfficeSale', request);
    this.logger.info('escalate_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        approver: { display_name: (request?.details?.approver?.display_name ?? '') },
      },
      wristband: {
        owner: {
          display_name: (request?.box_office_sale?.counterparty?.display_name ?? ''),
        },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.admissionsWristbandApprovalClient.escalateWristband(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        box_office_sale: {
          billing_address: {
            lines: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.lines ?? []),
          },
        },
        outcome: { effective_at: reply1?.outcome?.effective_at },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          due_at: request?.box_office_sale?.due_at,
        },
      },
    );
  }

  /**
   * Returns a box office sale.
   */
  getBoxOfficeSaleApprovalStatus(call, callback) {
    this.executeGetBoxOfficeSaleApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetBoxOfficeSaleApprovalStatus(request) {
    validateContext(request.context);
    if (!request.box_office_sale_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'box_office_sale_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetBoxOfficeSaleApprovalStatus', request);
    this.logger.info('get_box_office_sale_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
    };
    const reply1 = await call((done) => this.clients.boxofficeGuestListValidationClient.validateGuestList(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      { box_office_sale: { counterparty: reply1?.guest_list?.counterparty } },
      {
        box_office_sale: { box_office_sale_id: (request?.box_office_sale_id ?? '') },
      },
    );
  }

  /**
   * Lists box office sales a page at a time.
   */
  listPendingBoxOfficeSales(call, callback) {
    this.executeListPendingBoxOfficeSales(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListPendingBoxOfficeSales(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListPendingBoxOfficeSales', request);
    this.logger.info('list_pending_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.filter?.labels ?? {}) },
      context: request?.context,
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.venuesVenueExportClient.scheduleVenueExport(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        message: (reply1?.result?.message ?? ''),
      },
    };
  }

  /**
   * Recalls a box office sale.
   */
  recallBoxOfficeSale(call, callback) {
    this.executeRecallBoxOfficeSale(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecallBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await this.store.save(this.store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await this.store.record(request.context.tenant_id, 'RecallBoxOfficeSale', request);
    this.logger.info('recall_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: { owner: { user_id: (request?.details?.approver?.user_id ?? '') } },
    };
    const advanceMenuItemWorkflow1 = promisify(this.clients.concessionsMenuItemWorkflowClient.advanceMenuItemWorkflow.bind(this.clients.concessionsMenuItemWorkflowClient));
    const reply1 = await advanceMenuItemWorkflow1(request1, metadataFor(request1.context), { deadline: deadline() });
    let selected2_2;
    switch (request?.box_office_sale?.settlement) {
      case 'bank':
        selected2_2 = ((request?.box_office_sale?.settlement === 'bank' ? request?.box_office_sale.bank : undefined)?.bic ?? '');
        break;
      default:
        selected2_2 = '';
    }
    const request2 = {
      box_office_sale: {
        bank: { bic: selected2_2 },
        counterparty: { display_name: (reply1?.menu_item?.owner?.display_name ?? '') },
      },
      context: request?.context,
    };
    const reply2 = await call((done) => this.clients.boxofficeBoxOfficeSaleClient.createBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          status: (reply1?.outcome?.status ?? ''),
        },
      },
      {
        box_office_sale: {
          notes: (reply2?.box_office_sale?.notes ?? ''),
          number: (reply2?.box_office_sale?.number ?? ''),
        },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          due_at: request?.box_office_sale?.due_at,
        },
      },
    );
  }

  /**
   * Rejects a box office sale.
   */
  rejectBoxOfficeSale(call, callback) {
    this.executeRejectBoxOfficeSale(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRejectBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await this.store.save(this.store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await this.store.record(request.context.tenant_id, 'RejectBoxOfficeSale', request);
    this.logger.info('reject_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.box_office_sale?.issued_at,
        owner_id: (request?.box_office_sale?.counterparty?.cash_drawer_id ?? ''),
        region: (request?.box_office_sale?.billing_address?.region ?? ''),
      },
    };
    const method1 = 'suggestSearchIndexes';
    const reply1 = await call((done) => this.clients.searchSearchIndexQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const results2 = await Promise.all((request?.box_office_sale?.attachments ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        guest_list: {
          attachments: [{
            checksum: (item?.checksum ?? ''),
            content_type: (item?.content_type ?? ''),
            file_name: (item?.file_name ?? ''),
            uri: (item?.uri ?? ''),
          }],
        },
      };
      const reply2 = await call((done) => this.clients.boxofficeGuestListLifecycleClient.scheduleGuestListExpiry(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      guest_list: {
        bank: {
          iban: ((request?.box_office_sale?.settlement === 'bank' ? request?.box_office_sale.bank : undefined)?.iban ?? ''),
        },
      },
    };
    const client3 = this.clients.registry.get('acme.boxoffice.v1.GuestListValidationService');
    const reply3 = await call((done) => client3.explainGuestListDecision(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        box_office_sale: { metadata: (reply3?.guest_list?.metadata ?? {}) },
        result: { message: (reply3?.result?.message ?? '') },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          total: request?.box_office_sale?.total,
        },
      },
    );
  }

  /**
   * Submits a box office sale.
   */
  submitBoxOfficeSale(call, callback) {
    this.executeSubmitBoxOfficeSale(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubmitBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await this.store.save(this.store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await this.store.record(request.context.tenant_id, 'SubmitBoxOfficeSale', request);
    this.logger.info('submit_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.box_office_sale?.settlement) {
      case 'card':
        selected1_1 = ((request?.box_office_sale?.settlement === 'card' ? request?.box_office_sale.card : undefined)?.holder_name ?? '');
        break;
      default:
        selected1_1 = '';
    }
    const request1 = {
      context: request?.context,
      trip: { card: { holder_name: selected1_1 } },
    };
    const reply1 = await call((done) => this.clients.travelTripClient.cloneTrip(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const results2 = await Promise.all((request?.box_office_sale?.lines ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        filter: {
          labels: { ...(item?.attributes ?? {}) },
          owner_id: (item?.line_id ?? ''),
          region: (request?.box_office_sale?.billing_address?.region ?? ''),
        },
        subscriber: { email: (reply1?.trip?.counterparty?.email ?? '') },
      };
      const messages2 = await collect(this.clients.boxofficeBoxOfficeSaleClient.watchBoxOfficeSales(request2, metadataFor(request2.context), { deadline: deadline() }));
      const reply2 = messages2.at(-1) ?? {};
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        box_office_sale: { audit: { updated_by: (reply1?.trip?.audit?.updated_by ?? '') } },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          metadata: (request?.box_office_sale?.metadata ?? {}),
        },
      },
    );
  }
}

module.exports = { BoxOfficeSaleApprovalService };
