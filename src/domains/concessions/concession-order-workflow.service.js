'use strict';

/**
 * Runs the long-lived workflow that takes a concession order from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect, exchange } = require('../../platform/streams');

/**
 * Serve ConcessionOrderWorkflowService, recording every operation of the concessions team.
 */
class ConcessionOrderWorkflowService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ConcessionOrderWorkflowService' });
  }

  /**
   * Advances a concession order.
   */
  advanceConcessionOrderWorkflow(call, callback) {
    this.executeAdvanceConcessionOrderWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeAdvanceConcessionOrderWorkflow(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'AdvanceConcessionOrderWorkflow', request);
    this.logger.info('advance_concession_order_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.concession_order?.due_at,
        owner_id: (request?.concession_order?.counterparty?.menu_item_id ?? ''),
        region: (request?.concession_order?.billing_address?.region ?? ''),
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.accountingChartOfAccountsQueryClient.countChartOfAccountses(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      concession_stand: {
        address: {
          postal_code: (request?.concession_order?.billing_address?.postal_code ?? ''),
        },
        audit: { created_at: request?.concession_order?.audit?.created_at },
        owner: {
          display_name: (request?.concession_order?.counterparty?.display_name ?? ''),
        },
      },
      context: request?.context,
      details: { notify: { phone: (request?.details?.notify?.phone ?? '') } },
    };
    const task2 = deferred(() => call((done) => this.clients.concessionsConcessionStandWorkflowClient.advanceConcessionStandWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        concession_order: {
          billing_address: {
            lines: ((reply2?.concession_stand?.locator === 'address' ? reply2?.concession_stand.address : undefined)?.lines ?? []),
          },
        },
      },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          number: (request?.concession_order?.number ?? ''),
        },
      },
    );
  }

  /**
   * Cancels a concession order.
   */
  cancelConcessionOrderWorkflow(call, callback) {
    this.executeCancelConcessionOrderWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCancelConcessionOrderWorkflow(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'CancelConcessionOrderWorkflow', request);
    this.logger.info('cancel_concession_order_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        address: { lines: [...(request?.concession_order?.billing_address?.lines ?? [])] },
        owner: { email: (request?.details?.notify?.email ?? '') },
      },
      context: request?.context,
    };
    const reply1 = await this.bus.request('acme.concessions.v1.ConcessionOrderWorkflowService/CancelConcessionOrderWorkflow:0', request1);
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.concession_stand?.audit?.created_at,
        region: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.region ?? ''),
      },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.concessionsConcessionStandWorkflowClient.listConcessionStandWorkflows(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.concession_order?.audit?.updated_at,
        owner_id: (request?.concession_order?.counterparty?.menu_item_id ?? ''),
        region: (request?.concession_order?.billing_address?.region ?? ''),
      },
    };
    const reply3 = await call((done) => this.clients.sponsorshipsSponsorApprovalClient.listPendingSponsors(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      { result: { message: (reply2?.result?.message ?? '') } },
      { result: { status: (reply3?.result?.status ?? '') } },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          status: (request?.concession_order?.status ?? ''),
        },
      },
    );
  }

  /**
   * Returns a concession order.
   */
  inspectConcessionOrderWorkflow(call, callback) {
    this.executeInspectConcessionOrderWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeInspectConcessionOrderWorkflow(request) {
    validateContext(request.context);
    if (!request.concession_order_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
    }
    await this.store.record(request.context.tenant_id, 'InspectConcessionOrderWorkflow', request);
    this.logger.info('inspect_concession_order_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue: { owner: { email: (request?.context?.actor?.email ?? '') } },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.venuesVenueExportClient.retryVenueExport(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    return assemble(
      {
        concession_order: {
          counterparty: { display_name: (reply1?.venue?.owner?.display_name ?? '') },
        },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      },
    );
  }

  /**
   * Lists concession orders a page at a time.
   */
  listConcessionOrderWorkflows(call, callback) {
    this.executeListConcessionOrderWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListConcessionOrderWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListConcessionOrderWorkflows', request);
    this.logger.info('list_concession_order_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { statuses: [...(request?.filter?.statuses ?? [])] },
    };
    const messages1 = await collect(this.clients.concessionsConcessionOrderQueryClient.exportConcessionOrders(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Resumes a concession order.
   */
  resumeConcessionOrderWorkflow(call, callback) {
    this.executeResumeConcessionOrderWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResumeConcessionOrderWorkflow(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'ResumeConcessionOrderWorkflow', request);
    this.logger.info('resume_concession_order_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        participants: [{ contact: { phone: (request?.details?.notify?.phone ?? '') } }],
      },
    };
    const explainShowCalculation1 = promisify(this.clients.showsShowCalculationClient.explainShowCalculation.bind(this.clients.showsShowCalculationClient));
    const reply1 = await explainShowCalculation1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      venue_policy: {
        audit: {
          created_at: reply1?.show?.audit?.created_at,
          created_by: (request?.concession_order?.audit?.created_by ?? ''),
          updated_by: (reply1?.show?.audit?.updated_by ?? ''),
        },
      },
    };
    const reply2 = await call((done) => this.clients.venuesVenuePolicyHistoryClient.compareVenuePolicyVersions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: { audit: { etag: (reply1?.show?.audit?.etag ?? '') } },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          due_at: request?.concession_order?.due_at,
        },
      },
    );
  }

  /**
   * Retrys a concession order.
   */
  retryConcessionOrderWorkflow(call, callback) {
    this.executeRetryConcessionOrderWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRetryConcessionOrderWorkflow(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'RetryConcessionOrderWorkflow', request);
    this.logger.info('retry_concession_order_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.concession_order?.settlement) {
      case 'card':
        selected1_1 = ((request?.concession_order?.settlement === 'card' ? request?.concession_order.card : undefined)?.token ?? '');
        break;
      default:
        selected1_1 = '';
    }
    const request1 = {
      concession_order: { card: { token: selected1_1 } },
      context: request?.context,
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionOrderApprovalClient.submitConcessionOrder(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      change: {
        discount: { amount: request?.concession_order?.total },
        parameters: { ...(request?.details?.parameters ?? {}) },
        scopes: [...(request?.concession_order?.billing_address?.lines ?? [])],
      },
      context: request?.context,
    };
    const messages2 = await exchange(this.clients.venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request2.context), { deadline: deadline() }), [request2]);
    const reply2 = messages2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: {
          billing_address: { locality: (reply1?.concession_order?.billing_address?.locality ?? '') },
        },
      },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          counterparty: request?.concession_order?.counterparty,
        },
      },
    );
  }

  /**
   * Signals a concession order.
   */
  signalConcessionOrderWorkflow(call, callback) {
    this.executeSignalConcessionOrderWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSignalConcessionOrderWorkflow(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'SignalConcessionOrderWorkflow', request);
    this.logger.info('signal_concession_order_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.concession_order?.audit?.created_at,
        owner_id: (request?.concession_order?.counterparty?.menu_item_id ?? ''),
        region: (request?.concession_order?.billing_address?.region ?? ''),
      },
    };
    const messages1 = await collect(this.clients.venuesVenueExportClient.streamVenueExport(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      venue: {
        address: {
          locality: (request?.concession_order?.billing_address?.locality ?? ''),
          postal_code: (request?.concession_order?.billing_address?.postal_code ?? ''),
        },
      },
    };
    const client2 = this.clients.registry.get('acme.venues.v1.VenueService');
    const reply2 = await call((done) => client2.createVenue(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: {
          counterparty: { display_name: (reply2?.venue?.owner?.display_name ?? '') },
        },
      },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          total: request?.concession_order?.total,
        },
      },
    );
  }

  /**
   * Starts a concession order.
   */
  startConcessionOrderWorkflow(call, callback) {
    this.executeStartConcessionOrderWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeStartConcessionOrderWorkflow(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'StartConcessionOrderWorkflow', request);
    this.logger.info('start_concession_order_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        recipient: {
          phone: (request?.details?.notify?.phone ?? ''),
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
    };
    const send1 = this.clients.concessionsConcessionOrderReportingClient.shareConcessionOrderReport.bind(this.clients.concessionsConcessionOrderReportingClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        approver: {
          display_name: (request?.concession_order?.counterparty?.display_name ?? ''),
        },
      },
      menu_item: {
        address: {
          recipient: (request?.concession_order?.billing_address?.recipient ?? ''),
        },
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.concessionsMenuItemApprovalClient.escalateMenuItem(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: {
          billing_address: {
            country_code: (reply1?.concession_order?.billing_address?.country_code ?? ''),
          },
        },
      },
      {
        concession_order: {
          lines: [{ quantity: (reply2?.menu_item?.components?.primary?.quantity ?? '0') }],
        },
      },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          number: (request?.concession_order?.number ?? ''),
        },
      },
    );
  }
}

module.exports = { ConcessionOrderWorkflowService };
