'use strict';

/**
 * Runs the long-lived workflow that takes a sponsor from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve SponsorWorkflowService, recording every operation of the sponsorships team.
 */
class SponsorWorkflowService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'SponsorWorkflowService' });
  }

  /**
   * Advances a sponsor.
   */
  advanceSponsorWorkflow(call, callback) {
    this.executeAdvanceSponsorWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeAdvanceSponsorWorkflow(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await this.store.save(this.store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await this.store.record(request.context.tenant_id, 'AdvanceSponsorWorkflow', request);
    this.logger.info('advance_sponsor_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: { recipient: (request?.sponsor?.primary_address?.recipient ?? '') },
      },
      sponsorship_deal: {
        audit: { created_by: (request?.sponsor?.audit?.created_by ?? '') },
        billing_address: { country_code: (request?.sponsor?.primary_address?.country_code ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.sponsorshipsSponsorshipDealCalculationClient.calculateSponsorshipDeal(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.sponsorship_deal?.audit?.updated_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (reply1?.sponsorship_deal?.counterparty?.sponsor_id ?? ''),
        region: (request?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const countSponsors2 = promisify(this.clients.sponsorshipsSponsorQueryClient.countSponsors.bind(this.clients.sponsorshipsSponsorQueryClient));
    const reply2 = await countSponsors2(request2, metadataFor(request2.context), { deadline: deadline() });
    const request3 = {
      context: request?.context,
      details: {
        approver: {
          display_name: (reply1?.sponsorship_deal?.counterparty?.display_name ?? ''),
        },
      },
      replay_clip: {
        address: { locality: (reply1?.sponsorship_deal?.billing_address?.locality ?? '') },
        audit: { updated_by: (reply1?.sponsorship_deal?.audit?.updated_by ?? '') },
      },
    };
    const reply3 = await call((done) => this.clients.broadcastsReplayClipApprovalClient.delegateReplayClipApproval(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { status: (reply1?.result?.status ?? '') },
        sponsor: {
          primary_address: {
            country_code: (reply1?.sponsorship_deal?.billing_address?.country_code ?? ''),
          },
        },
      },
      { outcome: { message: (reply2?.result?.message ?? '') } },
      { outcome: { reference_id: (reply3?.outcome?.reference_id ?? '') } },
      {
        sponsor: {
          relationship_manager: request?.sponsor?.relationship_manager,
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  /**
   * Cancels a sponsor.
   */
  cancelSponsorWorkflow(call, callback) {
    this.executeCancelSponsorWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCancelSponsorWorkflow(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await this.store.save(this.store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await this.store.record(request.context.tenant_id, 'CancelSponsorWorkflow', request);
    this.logger.info('cancel_sponsor_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsorship_deal: {
        counterparty: {
          display_name: (request?.sponsor?.relationship_manager?.display_name ?? ''),
          email: (request?.sponsor?.relationship_manager?.email ?? ''),
        },
      },
    };
    const method1 = 'calculateSponsorshipDeal';
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorshipDealCalculationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsor: {
          audit: { etag: (reply1?.sponsorship_deal?.audit?.etag ?? '') },
          relationship_manager: {
            display_name: (reply1?.sponsorship_deal?.counterparty?.display_name ?? ''),
          },
        },
      },
      {
        sponsor: {
          name: request?.sponsor?.name,
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a sponsor.
   */
  inspectSponsorWorkflow(call, callback) {
    this.executeInspectSponsorWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeInspectSponsorWorkflow(request) {
    validateContext(request.context);
    if (!request.sponsor_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsor_id is required');
    }
    await this.store.record(request.context.tenant_id, 'InspectSponsorWorkflow', request);
    this.logger.info('inspect_sponsor_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { filters: { ...(request?.context?.baggage ?? {}) } },
    };
    const client1 = this.clients.registry.get('acme.documents.v1.DocumentQueryService');
    const reply1 = await call((done) => client1.summarizeDocuments(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { sponsor: { sponsor_id: (request?.sponsor_id ?? '') } },
    );
  }

  /**
   * Lists sponsors a page at a time.
   */
  listSponsorWorkflows(call, callback) {
    this.executeListSponsorWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListSponsorWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListSponsorWorkflows', request);
    this.logger.info('list_sponsor_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Pauses a sponsor.
   */
  pauseSponsorWorkflow(call, callback) {
    this.executePauseSponsorWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePauseSponsorWorkflow(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await this.store.save(this.store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await this.store.record(request.context.tenant_id, 'PauseSponsorWorkflow', request);
    this.logger.info('pause_sponsor_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        participants: [{
          contact: {
            email: (request?.sponsor?.relationship_manager?.email ?? ''),
            phone: (request?.details?.notify?.phone ?? ''),
            preferred_channel: (request?.sponsor?.contact?.preferred_channel ?? ''),
          },
        }],
      },
      context: request?.context,
    };
    const send1 = this.clients.broadcastsBroadcastWorkflowClient.pauseBroadcastWorkflow.bind(this.clients.broadcastsBroadcastWorkflowClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.broadcast?.audit?.created_at,
        region: (reply1?.broadcast?.location?.region ?? ''),
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.sponsorshipsSponsorQueryClient.suggestSponsors(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { details: (reply1?.result?.details ?? {}) },
        sponsor: { audit: { updated_at: reply1?.broadcast?.audit?.updated_at } },
      },
      { outcome: { message: (reply2?.result?.message ?? '') } },
      {
        sponsor: {
          primary_address: request?.sponsor?.primary_address,
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  /**
   * Resumes a sponsor.
   */
  resumeSponsorWorkflow(call, callback) {
    this.executeResumeSponsorWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResumeSponsorWorkflow(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await this.store.save(this.store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await this.store.record(request.context.tenant_id, 'ResumeSponsorWorkflow', request);
    this.logger.info('resume_sponsor_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      filter: {
        created_after: (input1?.sponsor?.identity_document === 'passport' ? input1?.sponsor.passport : undefined)?.expires_at,
        region: (input1?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.featureflagsFeatureFlagQueryClient.suggestFeatureFlags(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      broadcast: { participants: [{ contact: { name: request?.details?.notify?.name } }] },
      context: request?.context,
      details: {
        notify: {
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.broadcastsBroadcastWorkflowClient.advanceBroadcastWorkflow(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: {
        contact: {
          phone: (request?.details?.notify?.phone ?? ''),
          preferred_channel: (request?.sponsor?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => this.clients.licensingSoftwareLicenseCalculationClient.explainSoftwareLicenseCalculation(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        outcome: { reference_id: (reply2?.outcome?.reference_id ?? '') },
        sponsor: {
          primary_address: { recipient: (reply2?.broadcast?.location?.recipient ?? '') },
        },
      },
      {
        sponsor: { audit: { etag: (reply3?.software_license?.audit?.etag ?? '') } },
      },
      {
        sponsor: {
          name: request?.sponsor?.name,
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  /**
   * Retrys a sponsor.
   */
  retrySponsorWorkflow(call, callback) {
    this.executeRetrySponsorWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRetrySponsorWorkflow(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await this.store.save(this.store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await this.store.record(request.context.tenant_id, 'RetrySponsorWorkflow', request);
    this.logger.info('retry_sponsor_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        contact: { phone: (request?.sponsor?.contact?.phone ?? '') },
        primary_address: { recipient: (request?.sponsor?.primary_address?.recipient ?? '') },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorClient.updateSponsor(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      admission: {
        subject: {
          display_name: (request?.sponsor?.relationship_manager?.display_name ?? ''),
        },
      },
      context: request?.context,
      details: {
        contact: {
          email: (reply1?.sponsor?.relationship_manager?.email ?? ''),
          preferred_channel: (reply1?.sponsor?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.admissionsAdmissionValidationClient.lintAdmissions(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      activation: {
        audit: { created_at: request?.sponsor?.audit?.created_at },
        requester: { email: (reply1?.sponsor?.contact?.email ?? '') },
      },
      context: request?.context,
      details: { contact: { phone: (request?.details?.notify?.phone ?? '') } },
    };
    const task3 = deferred(() => call((done) => this.clients.sponsorshipsActivationValidationClient.checkActivationEligibility(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    const request4 = {
      context: request?.context,
      details: {
        contact: { email: (reply1?.sponsor?.relationship_manager?.email ?? '') },
      },
      gate_scan: {
        subject: {
          display_name: ((reply3?.activation?.origin === 'customer' ? reply3?.activation.customer : undefined)?.display_name ?? ''),
        },
      },
    };
    const reply4 = await this.bus.request('acme.sponsorships.v1.SponsorWorkflowService/RetrySponsorWorkflow:3', request4);
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsor: {
          preferences: { channels: (reply1?.sponsor?.preferences?.channels ?? []) },
        },
      },
      {
        outcome: { reference_id: (reply2?.outcome?.reference_id ?? '') },
        result: { correlation_id: (reply2?.admission?.correlation_id ?? '') },
      },
      { result: { message: (reply3?.result?.message ?? '') } },
      {
        result: {
          reference_ids: (reply4?.result?.reference_ids ?? []),
          status: (reply4?.result?.status ?? ''),
        },
      },
      {
        sponsor: {
          name: request?.sponsor?.name,
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  /**
   * Signals a sponsor.
   */
  signalSponsorWorkflow(call, callback) {
    this.executeSignalSponsorWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSignalSponsorWorkflow(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await this.store.save(this.store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await this.store.record(request.context.tenant_id, 'SignalSponsorWorkflow', request);
    this.logger.info('signal_sponsor_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      form: {
        audit: { updated_by: (request?.sponsor?.audit?.updated_by ?? '') },
        billing_address: { country_code: (request?.sponsor?.primary_address?.country_code ?? '') },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.formsFormCalculationClient.calculateForm(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      sponsor: {
        relationship_manager: {
          display_name: (request?.sponsor?.relationship_manager?.display_name ?? ''),
        },
      },
    };
    const reply2 = await call((done) => this.clients.sponsorshipsSponsorApprovalClient.submitSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsor: {
          primary_address: { recipient: (reply1?.form?.billing_address?.recipient ?? '') },
        },
      },
      {
        sponsor: { audit: { updated_by: (reply2?.sponsor?.audit?.updated_by ?? '') } },
      },
      {
        sponsor: {
          name: request?.sponsor?.name,
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }

  /**
   * Starts a sponsor.
   */
  startSponsorWorkflow(call, callback) {
    this.executeStartSponsorWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeStartSponsorWorkflow(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await this.store.save(this.store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await this.store.record(request.context.tenant_id, 'StartSponsorWorkflow', request);
    this.logger.info('start_sponsor_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      user_id: (request?.sponsor?.relationship_manager?.user_id ?? ''),
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.identityUserQueryClient.lookupUser(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      activation: {
        audit: { updated_at: request?.sponsor?.audit?.updated_at },
        requester: { name: request?.details?.notify?.name },
      },
      context: request?.context,
    };
    const explainActivationDecision2 = promisify(this.clients.sponsorshipsActivationValidationClient.explainActivationDecision.bind(this.clients.sponsorshipsActivationValidationClient));
    const reply2 = await explainActivationDecision2(request2, metadataFor(request2.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsor: {
          contact: {
            name: reply1?.user?.name,
            phone: (reply1?.user?.contact?.phone ?? ''),
          },
        },
      },
      {
        sponsor: { audit: { updated_by: (reply2?.activation?.audit?.updated_by ?? '') } },
      },
      {
        sponsor: {
          relationship_manager: request?.sponsor?.relationship_manager,
          sponsor_id: (request?.sponsor_id ?? ''),
        },
      },
    );
  }
}

module.exports = { SponsorWorkflowService };
