'use strict';

/**
 * Routes cash drawers through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred } = require('../../platform/rpc');
const { upload } = require('../../platform/streams');

/**
 * Serve CashDrawerApprovalService, recording every operation of the boxoffice team.
 */
class CashDrawerApprovalService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'CashDrawerApprovalService' });
  }

  /**
   * Approves a cash drawer.
   */
  approveCashDrawer(call, callback) {
    this.executeApproveCashDrawer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeApproveCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await this.store.save(this.store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await this.store.record(request.context.tenant_id, 'ApproveCashDrawer', request);
    this.logger.info('approve_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { labels: { ...(request?.cash_drawer?.labels ?? {}) } },
    };
    const task1 = deferred(() => call((done) => this.clients.notificationsNotificationQueryClient.countNotifications(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.cash_drawer?.audit?.updated_at,
        labels: { ...(request?.cash_drawer?.labels ?? {}) },
      },
    };
    const reply2 = await this.bus.request('acme.boxoffice.v1.CashDrawerApprovalService/ApproveCashDrawer:1', request2);
    const request3 = {
      context: request?.context,
      tour_budget: {
        bank: {
          account_holder: ((request?.cash_drawer?.funding === 'bank' ? request?.cash_drawer.bank : undefined)?.account_holder ?? ''),
        },
        holder: { display_name: (request?.cash_drawer?.holder?.display_name ?? '') },
      },
    };
    const reply3 = await runInContext(request.context, () => call((done) => this.clients.toursTourBudgetWorkflowClient.resumeTourBudgetWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      {
        outcome: {
          message: (reply2?.result?.message ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        cash_drawer: {
          contact: {
            preferred_channel: (reply3?.tour_budget?.contact?.preferred_channel ?? ''),
          },
        },
        result: { message: (reply3?.result?.message ?? '') },
      },
      {
        cash_drawer: {
          available: request?.cash_drawer?.available,
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
        },
      },
    );
  }

  /**
   * Comments a cash drawer.
   */
  commentCashDrawerApproval(call, callback) {
    this.executeCommentCashDrawerApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCommentCashDrawerApproval(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await this.store.save(this.store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await this.store.record(request.context.tenant_id, 'CommentCashDrawerApproval', request);
    this.logger.info('comment_cash_drawer_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        audit: {
          etag: (request?.cash_drawer?.audit?.etag ?? ''),
          updated_by: (request?.cash_drawer?.audit?.updated_by ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionStandApprovalClient.submitConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        cash_drawer: {
          audit: { updated_by: (reply1?.concession_stand?.audit?.updated_by ?? '') },
        },
      },
      {
        cash_drawer: {
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
          labels: (request?.cash_drawer?.labels ?? {}),
        },
      },
    );
  }

  /**
   * Escalates a cash drawer.
   */
  escalateCashDrawer(call, callback) {
    this.executeEscalateCashDrawer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEscalateCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await this.store.save(this.store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await this.store.record(request.context.tenant_id, 'EscalateCashDrawer', request);
    this.logger.info('escalate_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      lineage_node: {
        children: [{
          children: [{
            audit: {
              created_at: request?.cash_drawer?.audit?.created_at,
              updated_at: request?.cash_drawer?.audit?.updated_at,
            },
          }],
        }],
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.lineageLineageNodeClient.createLineageNode(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      cash_drawer_id: (request?.cash_drawer?.cash_drawer_id ?? ''),
      context: request?.context,
      details: {
        new_owner: { email: (request?.cash_drawer?.contact?.email ?? '') },
        notify_contact: { email: (reply1?.lineage_node?.manager?.email ?? '') },
      },
    };
    const suspendCashDrawer2 = promisify(this.clients.boxofficeCashDrawerLifecycleClient.suspendCashDrawer.bind(this.clients.boxofficeCashDrawerLifecycleClient));
    const reply2 = await suspendCashDrawer2(request2, metadataFor(request2.context), { deadline: deadline() });
    const request3 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.cash_drawer?.labels ?? {}) },
        region: (reply1?.lineage_node?.location?.region ?? ''),
      },
    };
    const reply3 = await call((done) => this.clients.featureflagsFeatureFlagQueryClient.browseFeatureFlags(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        cash_drawer: {
          holder: { display_name: (reply1?.lineage_node?.manager?.display_name ?? '') },
        },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        cash_drawer: {
          currency_code: (reply2?.cash_drawer?.currency_code ?? ''),
          holder: { display_name: (reply2?.cash_drawer?.holder?.display_name ?? '') },
        },
      },
      {
        outcome: {
          message: (reply3?.result?.message ?? ''),
          status: (reply3?.result?.status ?? ''),
        },
      },
      {
        cash_drawer: {
          balance: request?.cash_drawer?.balance,
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
        },
      },
    );
  }

  /**
   * Lists cash drawers a page at a time.
   */
  listPendingCashDrawers(call, callback) {
    this.executeListPendingCashDrawers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListPendingCashDrawers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListPendingCashDrawers', request);
    this.logger.info('list_pending_cash_drawers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Recalls a cash drawer.
   */
  recallCashDrawer(call, callback) {
    this.executeRecallCashDrawer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecallCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await this.store.save(this.store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await this.store.record(request.context.tenant_id, 'RecallCashDrawer', request);
    this.logger.info('recall_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      cash_drawer_id: (request?.cash_drawer_id ?? ''),
      context: request?.context,
      details: {
        new_owner: {
          display_name: (request?.cash_drawer?.holder?.display_name ?? ''),
          email: (request?.cash_drawer?.contact?.email ?? ''),
        },
        notify_contact: {
          phone: (request?.cash_drawer?.contact?.phone ?? ''),
          preferred_channel: (request?.cash_drawer?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const method1 = 'expireCashDrawer';
    const reply1 = await call((done) => this.clients.boxofficeCashDrawerLifecycleClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      box_office_sale: {
        card: {
          last4: ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.last4 ?? ''),
        },
        counterparty: { display_name: (request?.cash_drawer?.holder?.display_name ?? '') },
      },
      context: request?.context,
    };
    const client2 = this.clients.registry.get('acme.boxoffice.v1.BoxOfficeSaleService');
    const reply2 = await call((done) => client2.createBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      cash_drawer: {
        audit: { updated_at: reply1?.cash_drawer?.audit?.updated_at },
        card: {
          last4: ((reply2?.box_office_sale?.settlement === 'card' ? reply2?.box_office_sale.card : undefined)?.last4 ?? ''),
        },
      },
      context: request?.context,
    };
    const send3 = this.clients.boxofficeCashDrawerCalculationClient.explainCashDrawerCalculation.bind(this.clients.boxofficeCashDrawerCalculationClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      concession_stand: {
        audit: { updated_at: request?.cash_drawer?.audit?.updated_at },
        owner: {
          display_name: (request?.cash_drawer?.holder?.display_name ?? ''),
          email: (request?.cash_drawer?.contact?.email ?? ''),
          user_id: (request?.details?.approver?.user_id ?? ''),
        },
      },
      context: request?.context,
    };
    const spread4 = {
      ...request4,
      context: request4.context ? { ...request4.context, baggage: { ...request4.context.baggage } } : null,
    };
    const reply4 = await call((done) => this.clients.concessionsConcessionStandApprovalClient.rejectConcessionStand(spread4, metadataFor(spread4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        cash_drawer: {
          contact: {
            preferred_channel: (reply1?.cash_drawer?.contact?.preferred_channel ?? ''),
          },
        },
      },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        cash_drawer: {
          contact: reply3?.cash_drawer?.contact,
          holder: { display_name: (reply3?.cash_drawer?.holder?.display_name ?? '') },
        },
      },
      {
        cash_drawer: { audit: { etag: (reply4?.concession_stand?.audit?.etag ?? '') } },
      },
      {
        cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Rejects a cash drawer.
   */
  rejectCashDrawer(call, callback) {
    this.executeRejectCashDrawer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRejectCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await this.store.save(this.store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await this.store.record(request.context.tenant_id, 'RejectCashDrawer', request);
    this.logger.info('reject_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      guest_list: {
        card: {
          last4: ((input1?.cash_drawer?.funding === 'card' ? input1?.cash_drawer.card : undefined)?.last4 ?? ''),
          network: ((input1?.cash_drawer?.funding === 'card' ? input1?.cash_drawer.card : undefined)?.network ?? ''),
          token: ((input1?.cash_drawer?.funding === 'card' ? input1?.cash_drawer.card : undefined)?.token ?? ''),
        },
      },
    };
    const reply1 = await call((done) => this.clients.boxofficeGuestListCalculationClient.recalculateGuestLists(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const results2 = await Promise.all((request?.cash_drawer?.limits ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        metric_series: { name: (item?.name ?? '') },
      };
      const reply2 = await call((done) => this.clients.metricsMetricSeriesClient.updateMetricSeries(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    const settled3 = await Promise.allSettled((request?.cash_drawer?.limits ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        metric_series: { name: (item?.name ?? '') },
      };
      const reply3 = await call((done) => this.clients.metricsMetricSeriesValidationClient.previewMetricSeries(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const results3 = settled3.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply3 = results3.at(-1) ?? {};
    const request4 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const cloned4 = JSON.parse(JSON.stringify(request4));
    const reply4 = await call((done) => this.clients.concessionsConcessionOrderReportingClient.scheduleConcessionOrderReport(cloned4, metadataFor(cloned4.context), { deadline: deadline() }, done));
    const request5 = {
      context: request?.context,
      header: {
        source: (request?.cash_drawer?.cash_drawer_id ?? ''),
        submitted_by: request?.cash_drawer?.contact,
      },
    };
    const reply5 = await upload(
      (done) => this.clients.admissionsAdmissionClient.batchCreateAdmissions(metadataFor(request5.context), { deadline: deadline() }, done),
      [request5],
    );
    this.logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        result: {
          details: (reply4?.result?.details ?? {}),
          status: (reply4?.result?.status ?? ''),
        },
      },
      {
        cash_drawer: {
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
          contact: request?.cash_drawer?.contact,
        },
      },
    );
  }

  /**
   * Submits a cash drawer.
   */
  submitCashDrawer(call, callback) {
    this.executeSubmitCashDrawer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubmitCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await this.store.save(this.store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await this.store.record(request.context.tenant_id, 'SubmitCashDrawer', request);
    this.logger.info('submit_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.cash_drawer?.limits ?? [])) {
      const request1 = {
        box_office_sale: { lines: [{ tax: { amount: item?.amount } }] },
        context: request?.context,
      };
      const reply1 = await call((done) => this.clients.boxofficeBoxOfficeSaleApprovalClient.rejectBoxOfficeSale(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          recipient: (reply1?.box_office_sale?.billing_address?.recipient ?? ''),
        },
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => this.clients.boxofficeBoxOfficeSaleCalculationClient.estimateBoxOfficeSale(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const results3 = await Promise.all((request?.cash_drawer?.limits ?? []).map(async (item) => {
      const request3 = {
        box_office_sale: {
          billing_address: {
            country_code: (reply1?.box_office_sale?.billing_address?.country_code ?? ''),
          },
          lines: [{ amount: item?.amount }],
        },
        context: request?.context,
        details: { contact: { email: (request?.cash_drawer?.contact?.email ?? '') } },
      };
      const reply3 = await call((done) => this.clients.boxofficeBoxOfficeSaleValidationClient.validateBoxOfficeSale(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        cash_drawer: {
          holder: {
            display_name: (reply2?.box_office_sale?.counterparty?.display_name ?? ''),
          },
        },
        result: { message: (reply2?.result?.message ?? '') },
      },
      {
        cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }
}

module.exports = { CashDrawerApprovalService };
