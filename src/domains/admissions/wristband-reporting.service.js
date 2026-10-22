'use strict';

/**
 * Builds and schedules reports about wristbands.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve WristbandReportingService, recording every operation of the admissions team.
 */
class WristbandReportingService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'WristbandReportingService' });
  }

  /**
   * Counts wristbands by status.
   */
  countWristbandTotals(call, callback) {
    this.executeCountWristbandTotals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountWristbandTotals(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountWristbandTotals', request);
    this.logger.info('count_wristband_totals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
      venue: {
        address: { region: (request?.filter?.region ?? '') },
        owner: { user_id: (request?.context?.actor?.user_id ?? '') },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.venuesVenueValidationClient.previewVenue(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        message: (reply1?.result?.message ?? ''),
        status: (reply1?.result?.status ?? ''),
      },
    };
  }

  /**
   * Deletes a wristband.
   */
  deleteWristbandReport(call, callback) {
    this.executeDeleteWristbandReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDeleteWristbandReport(request) {
    validateContext(request.context);
    if (!request.wristband_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
    }
    await this.store.record(request.context.tenant_id, 'DeleteWristbandReport', request);
    this.logger.info('delete_wristband_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.wristband_id ?? ''),
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.patientsPatientQueryClient.countPatients(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.etag ?? '') });
    return { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } };
  }

  /**
   * Returns a wristband.
   */
  fetchWristbandReport(call, callback) {
    this.executeFetchWristbandReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFetchWristbandReport(request) {
    validateContext(request.context);
    if (!request.wristband_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
    }
    await this.store.record(request.context.tenant_id, 'FetchWristbandReport', request);
    this.logger.info('fetch_wristband_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { wristband: { wristband_id: (request?.wristband_id ?? '') } };
  }

  /**
   * Generates a wristband.
   */
  generateWristbandReport(call, callback) {
    this.executeGenerateWristbandReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGenerateWristbandReport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'GenerateWristbandReport', request);
    this.logger.info('generate_wristband_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.sponsorshipsActivationApprovalClient.escalateActivation(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      broadcast: {
        organizer: { email: (reply1?.activation?.assignee?.email ?? '') },
        participants: [{ contact: { email: (reply1?.activation?.requester?.email ?? '') } }],
      },
      context: request?.context,
      details: { notify: { name: reply1?.activation?.requester?.name } },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.broadcastsBroadcastWorkflowClient.resumeBroadcastWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      { period: request?.details?.period },
    );
  }

  /**
   * Lists wristbands a page at a time.
   */
  listWristbandReports(call, callback) {
    this.executeListWristbandReports(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListWristbandReports(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListWristbandReports', request);
    this.logger.info('list_wristband_reports', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
      wristband: {
        audit: { updated_at: request?.filter?.created_after },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => this.clients.admissionsWristbandCalculationClient.recalculateWristbands(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      filter: {
        region: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.region ?? ''),
      },
      page: { page_token: (request?.page?.page_token ?? '') },
    };
    const reply2 = await this.bus.request('acme.admissions.v1.WristbandReportingService/ListWristbandReports:1', request2);
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        wristbands: [{ category_path: (reply1?.wristband?.category_path ?? []) }],
      },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
    );
  }

  /**
   * Schedules a wristband.
   */
  scheduleWristbandReport(call, callback) {
    this.executeScheduleWristbandReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScheduleWristbandReport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ScheduleWristbandReport', request);
    this.logger.info('schedule_wristband_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      broadcast: {
        organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
        participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
      },
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.broadcastsBroadcastWorkflowClient.cancelBroadcastWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: { contact: { email: (reply1?.broadcast?.organizer?.email ?? '') } },
      gate_scan: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        subject: { display_name: (reply1?.broadcast?.organizer?.display_name ?? '') },
      },
      reason: (request?.reason ?? ''),
    };
    const reply2 = await call((done) => this.clients.admissionsGateScanValidationClient.normalizeGateScan(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      { result: { correlation_id: (reply2?.gate_scan?.correlation_id ?? '') } },
      { period: request?.details?.period },
    );
  }

  /**
   * Shares a wristband.
   */
  shareWristbandReport(call, callback) {
    this.executeShareWristbandReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeShareWristbandReport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ShareWristbandReport', request);
    this.logger.info('share_wristband_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.admissionsAdmissionQueryClient.resolveAdmissions(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      details: { contact: { phone: (request?.details?.recipient?.phone ?? '') } },
    };
    const explainGateScanDecision2 = promisify(this.clients.admissionsGateScanValidationClient.explainGateScanDecision.bind(this.clients.admissionsGateScanValidationClient));
    const reply2 = await explainGateScanDecision2(request2, metadataFor(request2.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.result?.status ?? '') } },
      {
        outcome: {
          message: (reply2?.outcome?.message ?? ''),
          reference_id: (reply2?.outcome?.reference_id ?? ''),
        },
      },
      {
        wristband: {
          owner: { email: (request?.details?.recipient?.email ?? '') },
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Streams the changes of wristbands as they happen.
   */
  streamWristbandMetrics(call) {
    serverStream((request) => this.executeStreamWristbandMetrics(request))(call);
  }

  async executeStreamWristbandMetrics(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'StreamWristbandMetrics', request);
    this.logger.info('stream_wristband_metrics', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const reply1 = await call((done) => this.clients.authorizationAccessPolicyQueryClient.resolveAccessPolicies(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const method2 = 'resolveDocuments';
    const reply2 = await call((done) => this.clients.documentsDocumentQueryClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      { wristband: { owner: { email: (request?.subscriber?.email ?? '') } } },
    );
  }
}

module.exports = { WristbandReportingService };
