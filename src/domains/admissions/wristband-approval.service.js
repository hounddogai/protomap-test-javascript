'use strict';

/**
 * Routes wristbands through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, withRetry } = require('../../platform/rpc');
const { upload } = require('../../platform/streams');

/**
 * Serve WristbandApprovalService, recording every operation of the admissions team.
 */
class WristbandApprovalService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'WristbandApprovalService' });
  }

  /**
   * Approves a wristband.
   */
  approveWristband(call, callback) {
    this.executeApproveWristband(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeApproveWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await this.store.save(this.store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await this.store.record(request.context.tenant_id, 'ApproveWristband', request);
    this.logger.info('approve_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      record: {
        audit: { updated_at: request?.wristband?.audit?.updated_at },
        billing_address: {
          recipient: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.recipient ?? ''),
          region: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.region ?? ''),
        },
      },
    };
    const reply1 = await upload(
      (done) => this.clients.recruitingJobOpeningClient.batchCreateJobOpenings(metadataFor(request1.context), { deadline: deadline() }, done),
      [request1],
    );
    let selected2_1;
    switch (request?.wristband?.locator) {
      case 'address':
        selected2_1 = ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.lines ?? []);
        break;
      default:
        selected2_1 = [];
    }
    const request2 = {
      context: request?.context,
      venue_section: { children: [{ children: [{ location: { lines: selected2_1 } }] }] },
    };
    const reply2 = await call((done) => this.clients.venuesVenueSectionSyncClient.pushVenueSection(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.wristband?.audit?.created_at,
        labels: { ...(reply2?.result?.details ?? {}) },
        owner_id: (request?.wristband?.owner?.user_id ?? ''),
        region: (reply2?.venue_section?.location?.region ?? ''),
      },
    };
    const reply3 = await this.bus.request('acme.admissions.v1.WristbandApprovalService/ApproveWristband:2', request3);
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
        wristband: {
          audit: { created_by: (reply2?.venue_section?.audit?.created_by ?? '') },
        },
      },
      {
        result: {
          message: (reply3?.result?.message ?? ''),
          status: (reply3?.result?.status ?? ''),
        },
      },
      {
        wristband: {
          capacity: (request?.wristband?.capacity ?? '0'),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Comments a wristband.
   */
  commentWristbandApproval(call, callback) {
    this.executeCommentWristbandApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCommentWristbandApproval(request) {
    validateContext(request.context);
    if (request.wristband) {
      await this.store.save(this.store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await this.store.record(request.context.tenant_id, 'CommentWristbandApproval', request);
    this.logger.info('comment_wristband_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { approver: { user_id: (request?.details?.approver?.user_id ?? '') } },
      gate_scan: {
        measurements: [{ unit: (request?.wristband?.dimensions?.unit ?? '') }],
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.admissionsGateScanApprovalClient.rejectGateScan(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      admission: {
        admission_id: (reply1?.gate_scan?.subject?.admission_id ?? ''),
        subject: { kind: (reply1?.gate_scan?.subject?.kind ?? '') },
      },
      context: request?.context,
      details: {
        approver: {
          display_name: (reply1?.gate_scan?.subject?.display_name ?? ''),
          user_id: (request?.details?.approver?.user_id ?? ''),
        },
      },
    };
    const reply2 = await call((done) => this.clients.admissionsAdmissionApprovalClient.submitAdmission(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          message: (reply1?.result?.message ?? ''),
          reference_id: (reply1?.outcome?.reference_id ?? ''),
        },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        wristband: { wristband_id: (request?.wristband_id ?? '') },
      },
    );
  }

  /**
   * Delegates a wristband.
   */
  delegateWristbandApproval(call, callback) {
    this.executeDelegateWristbandApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDelegateWristbandApproval(request) {
    validateContext(request.context);
    if (request.wristband) {
      await this.store.save(this.store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await this.store.record(request.context.tenant_id, 'DelegateWristbandApproval', request);
    this.logger.info('delegate_wristband_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      metric_series: {
        components: { primary: {
          description: (request?.wristband?.components?.primary?.description ?? ''),
        } },
        owner: { user_id: (request?.details?.approver?.user_id ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.metricsMetricSeriesValidationClient.explainMetricSeriesDecision(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      broadcast: {
        location: {
          lines: [...((reply1?.metric_series?.locator === 'address' ? reply1?.metric_series.address : undefined)?.lines ?? [])],
        },
        organizer: { user_id: (request?.wristband?.owner?.user_id ?? '') },
        participants: [{ contact: { email: (reply1?.metric_series?.owner?.email ?? '') } }],
      },
      context: request?.context,
    };
    const approveBroadcast2 = promisify(this.clients.broadcastsBroadcastApprovalClient.approveBroadcast.bind(this.clients.broadcastsBroadcastApprovalClient));
    const reply2 = await approveBroadcast2(request2, metadataFor(request2.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
        wristband: {
          dimensions: { weight: (reply1?.metric_series?.dimensions?.weight ?? 0) },
        },
      },
      {
        wristband: { owner: { user_id: (reply2?.broadcast?.organizer?.user_id ?? '') } },
      },
      {
        wristband: {
          capacity: (request?.wristband?.capacity ?? '0'),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Escalates a wristband.
   */
  escalateWristband(call, callback) {
    this.executeEscalateWristband(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEscalateWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await this.store.save(this.store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await this.store.record(request.context.tenant_id, 'EscalateWristband', request);
    this.logger.info('escalate_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        measurements: [{ unit: (request?.wristband?.dimensions?.unit ?? '') }],
        subject: { display_name: (request?.wristband?.owner?.display_name ?? '') },
      },
      context: request?.context,
      details: { notify: { email: (request?.details?.approver?.email ?? '') } },
    };
    const reply1 = await call((done) => this.clients.admissionsAdmissionWorkflowClient.cancelAdmissionWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      job_opening: {
        audit: {
          created_by: (request?.wristband?.audit?.created_by ?? ''),
          updated_at: request?.wristband?.audit?.updated_at,
        },
        counterparty: { display_name: (request?.wristband?.owner?.display_name ?? '') },
      },
    };
    const method2 = 'cloneJobOpening';
    const reply2 = await call((done) => this.clients.recruitingJobOpeningClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: {
        approver: { display_name: (reply2?.job_opening?.counterparty?.display_name ?? '') },
      },
    };
    const client3 = this.clients.registry.get('acme.admissions.v1.GateScanApprovalService');
    const reply3 = await call((done) => client3.escalateGateScan(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { effective_at: reply1?.outcome?.effective_at },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      { outcome: { status: (reply2?.result?.status ?? '') } },
      {
        outcome: { message: (reply3?.result?.message ?? '') },
        result: { correlation_id: (reply3?.result?.correlation_id ?? '') },
      },
      {
        wristband: {
          name: (request?.wristband?.name ?? ''),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a wristband.
   */
  getWristbandApprovalStatus(call, callback) {
    this.executeGetWristbandApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetWristbandApprovalStatus(request) {
    validateContext(request.context);
    if (!request.wristband_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetWristbandApprovalStatus', request);
    this.logger.info('get_wristband_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
      metric_series: { owner: { user_id: (request?.context?.actor?.user_id ?? '') } },
    };
    const send1 = this.clients.metricsMetricSeriesValidationClient.normalizeMetricSeries.bind(this.clients.metricsMetricSeriesValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        wristband: {
          code: (reply1?.metric_series?.code ?? ''),
          dimensions: { weight: (reply1?.metric_series?.dimensions?.weight ?? 0) },
        },
      },
      { wristband: { wristband_id: (request?.wristband_id ?? '') } },
    );
  }

  /**
   * Lists wristbands a page at a time.
   */
  listPendingWristbands(call, callback) {
    this.executeListPendingWristbands(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListPendingWristbands(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListPendingWristbands', request);
    this.logger.info('list_pending_wristbands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        owner_id: (request?.filter?.owner_id ?? ''),
      },
      page: { page_size: (request?.page?.page_size ?? 0) },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.venuesVenueSectionSyncClient.listVenueSectionConflicts(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } };
  }

  /**
   * Recalls a wristband.
   */
  recallWristband(call, callback) {
    this.executeRecallWristband(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecallWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await this.store.save(this.store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await this.store.record(request.context.tenant_id, 'RecallWristband', request);
    this.logger.info('recall_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      metric_series: {
        owner: { display_name: (input1?.details?.approver?.display_name ?? '') },
      },
    };
    const reply1 = await call((done) => this.clients.metricsMetricSeriesValidationClient.normalizeMetricSeries(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { wristband: { owner: reply1?.metric_series?.owner } },
      {
        wristband: {
          code: (request?.wristband?.code ?? ''),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Rejects a wristband.
   */
  rejectWristband(call, callback) {
    this.executeRejectWristband(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRejectWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await this.store.save(this.store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await this.store.record(request.context.tenant_id, 'RejectWristband', request);
    this.logger.info('reject_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      search_index: { dimensions: { unit: (request?.wristband?.dimensions?.unit ?? '') } },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.searchSearchIndexClient.createSearchIndex(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      sponsor: {
        audit: { created_by: (reply1?.search_index?.audit?.created_by ?? '') },
        relationship_manager: { user_id: (reply1?.search_index?.owner?.user_id ?? '') },
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => this.clients.sponsorshipsSponsorApprovalClient.approveSponsor(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        wristband: {
          dimensions: { height: (reply1?.search_index?.dimensions?.height ?? 0) },
          owner: { display_name: (reply1?.search_index?.owner?.display_name ?? '') },
        },
      },
      {
        wristband: {
          audit: {
            created_at: reply2?.sponsor?.audit?.created_at,
            updated_by: (reply2?.sponsor?.audit?.updated_by ?? ''),
          },
        },
      },
      {
        wristband: {
          name: (request?.wristband?.name ?? ''),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Submits a wristband.
   */
  submitWristband(call, callback) {
    this.executeSubmitWristband(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubmitWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await this.store.save(this.store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await this.store.record(request.context.tenant_id, 'SubmitWristband', request);
    this.logger.info('submit_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      gate_scan: {
        measurements: [{ unit: (request?.wristband?.dimensions?.unit ?? '') }],
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.admissionsGateScanValidationClient.checkGateScanEligibility(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      search_index: { owner: { user_id: (request?.wristband?.owner?.user_id ?? '') } },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.searchSearchIndexClient.updateSearchIndex(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          next_steps: (reply1?.outcome?.next_steps ?? []),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        wristband: {
          owner: { display_name: (reply2?.search_index?.owner?.display_name ?? '') },
        },
      },
      {
        wristband: {
          capacity: (request?.wristband?.capacity ?? '0'),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }
}

module.exports = { WristbandApprovalService };
