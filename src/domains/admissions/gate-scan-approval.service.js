'use strict';

/**
 * Routes gate scans through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve GateScanApprovalService, recording every operation of the admissions team.
 */
class GateScanApprovalService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'GateScanApprovalService' });
  }

  /**
   * Approves a gate scan.
   */
  approveGateScan(call, callback) {
    this.executeApproveGateScan(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeApproveGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await this.store.save(this.store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await this.store.record(request.context.tenant_id, 'ApproveGateScan', request);
    this.logger.info('approve_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: { subject: { kind: (request?.gate_scan?.subject?.kind ?? '') } },
      context: request?.context,
      details: {
        approver: {
          display_name: (request?.details?.approver?.display_name ?? ''),
          user_id: (request?.details?.approver?.user_id ?? ''),
        },
      },
    };
    const method1 = 'commentAdmissionApproval';
    const reply1 = await call((done) => this.clients.admissionsAdmissionApprovalClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.outcome?.effective_at,
        owner_id: (request?.details?.approver?.user_id ?? ''),
      },
    };
    const client2 = this.clients.registry.get('acme.admissions.v1.GateScanQueryService');
    const reply2 = await call((done) => client2.browseGateScans(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        gate_scan: {
          gate_scan_id: (request?.gate_scan_id ?? ''),
          source: (request?.gate_scan?.source ?? ''),
        },
      },
    );
  }

  /**
   * Comments a gate scan.
   */
  commentGateScanApproval(call, callback) {
    this.executeCommentGateScanApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCommentGateScanApproval(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await this.store.save(this.store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await this.store.record(request.context.tenant_id, 'CommentGateScanApproval', request);
    this.logger.info('comment_gate_scan_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission_id: (request?.gate_scan?.subject?.admission_id ?? ''),
      context: request?.context,
      details: {
        new_owner: {
          display_name: (request?.details?.approver?.display_name ?? ''),
          user_id: (request?.details?.approver?.user_id ?? ''),
        },
      },
    };
    const send1 = this.clients.admissionsAdmissionLifecycleClient.terminateAdmission.bind(this.clients.admissionsAdmissionLifecycleClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_stand: {
        owner: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
      },
      context: request?.context,
      details: {
        approver: { display_name: (reply1?.admission?.subject?.display_name ?? '') },
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.concessionsConcessionStandApprovalClient.submitConcessionStand(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const results3 = await Promise.all((request?.gate_scan?.measurements ?? []).map(async (item) => {
      const request3 = {
        admission: {
          measurements: [{
            metric: (item?.metric ?? ''),
            observed_at: item?.observed_at,
            unit: (item?.unit ?? ''),
            value: (item?.value ?? 0),
          }],
        },
        context: request?.context,
      };
      const reply3 = await call((done) => this.clients.admissionsAdmissionCalculationClient.calculateAdmission(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    const settled4 = await Promise.allSettled((request?.gate_scan?.measurements ?? []).map(async (item) => {
      const request4 = {
        admission: {
          measurements: [{
            metric: (item?.metric ?? ''),
            unit: (item?.unit ?? ''),
            value: (item?.value ?? 0),
          }],
        },
        context: request?.context,
      };
      const reply4 = await call((done) => this.clients.admissionsAdmissionValidationClient.lintAdmissions(request4, metadataFor(request4.context), { deadline: deadline() }, done));
      return reply4;
    }));
    const results4 = settled4.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply4 = results4.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          next_steps: (reply1?.outcome?.next_steps ?? []),
          status: (reply1?.outcome?.status ?? ''),
        },
      },
      { outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) } },
      {
        gate_scan: {
          correlation_id: (request?.gate_scan?.correlation_id ?? ''),
          gate_scan_id: (request?.gate_scan_id ?? ''),
        },
      },
    );
  }

  /**
   * Delegates a gate scan.
   */
  delegateGateScanApproval(call, callback) {
    this.executeDelegateGateScanApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDelegateGateScanApproval(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await this.store.save(this.store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await this.store.record(request.context.tenant_id, 'DelegateGateScanApproval', request);
    this.logger.info('delegate_gate_scan_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      gate_scan_id: (input1?.gate_scan_id ?? ''),
    };
    const reply1 = await call((done) => this.clients.admissionsGateScanQueryClient.lookupGateScan(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      wristband: {
        owner: {
          display_name: (request?.gate_scan?.subject?.display_name ?? ''),
          email: (request?.details?.approver?.email ?? ''),
          user_id: (request?.details?.approver?.user_id ?? ''),
        },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.admissionsWristbandCalculationClient.calculateWristband(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.gate_scan?.occurred_at,
        labels: { ...(reply2?.wristband?.specs ?? {}) },
        owner_id: (reply1?.gate_scan?.subject?.admission_id ?? ''),
        region: ((reply2?.wristband?.locator === 'address' ? reply2?.wristband.address : undefined)?.region ?? ''),
      },
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => this.clients.featureflagsFeatureFlagClient.listFeatureFlags(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { gate_scan: { subject: { kind: (reply1?.gate_scan?.kind ?? '') } } },
      { outcome: { message: (reply2?.result?.message ?? '') } },
      { outcome: { message: (reply3?.result?.message ?? '') } },
      {
        gate_scan: {
          gate_scan_id: (request?.gate_scan_id ?? ''),
          occurred_at: request?.gate_scan?.occurred_at,
        },
      },
    );
  }

  /**
   * Escalates a gate scan.
   */
  escalateGateScan(call, callback) {
    this.executeEscalateGateScan(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEscalateGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await this.store.save(this.store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await this.store.record(request.context.tenant_id, 'EscalateGateScan', request);
    this.logger.info('escalate_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.gate_scan?.measurements ?? [])) {
      const request1 = {
        audit_event: {
          measurements: [{
            observed_at: item?.observed_at,
            value: (item?.value ?? 0),
          }],
        },
        context: request?.context,
      };
      const reply1 = await call((done) => this.clients.auditAuditEventValidationClient.scoreAuditEvent(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const request2 = { context: request?.context };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.ledgerLedgerAccountQueryClient.resolveLedgerAccounts(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { gate_scan: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        gate_scan: {
          gate_scan_id: (request?.gate_scan_id ?? ''),
          source: (request?.gate_scan?.source ?? ''),
        },
      },
    );
  }

  /**
   * Returns a gate scan.
   */
  getGateScanApprovalStatus(call, callback) {
    this.executeGetGateScanApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetGateScanApprovalStatus(request) {
    validateContext(request.context);
    if (!request.gate_scan_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetGateScanApprovalStatus', request);
    this.logger.info('get_gate_scan_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
      user: { user_id: (request?.context?.actor?.user_id ?? '') },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.identityUserValidationClient.validateUser(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      { gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') } },
    );
  }

  /**
   * Lists gate scans a page at a time.
   */
  listPendingGateScans(call, callback) {
    this.executeListPendingGateScans(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListPendingGateScans(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListPendingGateScans', request);
    this.logger.info('list_pending_gate_scans', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        approver: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => this.clients.admissionsAdmissionApprovalClient.escalateAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } };
  }

  /**
   * Recalls a gate scan.
   */
  recallGateScan(call, callback) {
    this.executeRecallGateScan(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecallGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await this.store.save(this.store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await this.store.record(request.context.tenant_id, 'RecallGateScan', request);
    this.logger.info('recall_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission_id: (request?.gate_scan?.subject?.admission_id ?? ''),
      context: request?.context,
      details: {
        new_owner: { display_name: (request?.details?.approver?.display_name ?? '') },
        notify_contact: { email: (request?.details?.approver?.email ?? '') },
      },
    };
    const reply1 = await this.bus.request('acme.admissions.v1.GateScanApprovalService/RecallGateScan:0', request1);
    const request2 = {
      context: request?.context,
      gate_scan_id: (request?.gate_scan?.gate_scan_id ?? ''),
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.admissionsGateScanValidationClient.getGateScanRules(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      ledger_account: {
        contact: { email: (request?.details?.approver?.email ?? '') },
        holder: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
      },
      reason: (request?.reason ?? ''),
    };
    const reply3 = await call((done) => this.clients.ledgerLedgerAccountValidationClient.validateLedgerAccount(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: { labels: { ...(reply3?.ledger_account?.labels ?? {}) } },
    };
    const reply4 = await new Promise((resolve, reject) => {
      this.clients.venuesVenuePolicySyncClient.listVenuePolicyConflicts(request4, metadataFor(request4.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    this.logger.info('local_audit', { value: (request?.details?.approver?.user_id ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      { gate_scan: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      { result: { details: (reply3?.result?.details ?? {}) } },
      {
        result: {
          correlation_id: (reply4?.result?.correlation_id ?? ''),
          reference_ids: (reply4?.result?.reference_ids ?? []),
        },
      },
      {
        gate_scan: {
          gate_scan_id: (request?.gate_scan_id ?? ''),
          subject: request?.gate_scan?.subject,
        },
      },
    );
  }

  /**
   * Rejects a gate scan.
   */
  rejectGateScan(call, callback) {
    this.executeRejectGateScan(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRejectGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await this.store.save(this.store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await this.store.record(request.context.tenant_id, 'RejectGateScan', request);
    this.logger.info('reject_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(request?.gate_scan?.attributes ?? {}) },
        owner_id: (request?.gate_scan?.correlation_id ?? ''),
      },
    };
    const listWristbandReports1 = promisify(this.clients.admissionsWristbandReportingClient.listWristbandReports.bind(this.clients.admissionsWristbandReportingClient));
    const reply1 = await listWristbandReports1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      filter: { created_after: request?.gate_scan?.occurred_at },
    };
    const reply2 = await call((done) => this.clients.featureflagsFeatureFlagClient.listFeatureFlags(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      feature_flag: {
        block: { notify_email: (request?.details?.approver_email ?? '') },
        owner: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
      },
    };
    const method3 = 'cloneFeatureFlag';
    const reply3 = await call((done) => this.clients.featureflagsFeatureFlagClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      { outcome: { status: (reply2?.result?.status ?? '') } },
      {
        outcome: { status: (reply3?.result?.status ?? '') },
        result: { status: (reply3?.result?.status ?? '') },
      },
      {
        gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') },
        result: { correlation_id: (request?.gate_scan?.correlation_id ?? '') },
      },
    );
  }

  /**
   * Submits a gate scan.
   */
  submitGateScan(call, callback) {
    this.executeSubmitGateScan(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubmitGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await this.store.save(this.store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await this.store.record(request.context.tenant_id, 'SubmitGateScan', request);
    this.logger.info('submit_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission_id: (request?.gate_scan?.subject?.admission_id ?? ''),
      context: request?.context,
      details: {
        new_owner: {
          display_name: (request?.gate_scan?.subject?.display_name ?? ''),
          email: (request?.details?.approver?.email ?? ''),
        },
      },
    };
    const client1 = this.clients.registry.get('acme.admissions.v1.AdmissionLifecycleService');
    const reply1 = await call((done) => client1.activateAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') },
        result: { status: (reply1?.result?.status ?? '') },
      },
      {
        gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') },
        result: { correlation_id: (request?.gate_scan?.correlation_id ?? '') },
      },
    );
  }
}

module.exports = { GateScanApprovalService };
