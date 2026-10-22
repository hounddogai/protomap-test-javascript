'use strict';

/**
 * Checks venue policies against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { RpcError, toServiceError } = require('../../platform/errors');

/**
 * Serve VenuePolicyValidationService, recording every operation of the venues team.
 */
class VenuePolicyValidationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'VenuePolicyValidationService' });
  }

  /**
   * Checks a venue policy.
   */
  checkVenuePolicyEligibility(call, callback) {
    this.executeCheckVenuePolicyEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCheckVenuePolicyEligibility(request) {
    validateContext(request.context);
    if (request.venue_policy) {
      await this.store.save(this.store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
    }
    await this.store.record(request.context.tenant_id, 'CheckVenuePolicyEligibility', request);
    this.logger.info('check_venue_policy_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        effective: request?.venue_policy?.effective,
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }

  /**
   * Explains a venue policy.
   */
  explainVenuePolicyDecision(call, callback) {
    this.executeExplainVenuePolicyDecision(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainVenuePolicyDecision(request) {
    validateContext(request.context);
    if (request.venue_policy) {
      await this.store.save(this.store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
    }
    await this.store.record(request.context.tenant_id, 'ExplainVenuePolicyDecision', request);
    this.logger.info('explain_venue_policy_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        priority: (request?.venue_policy?.priority ?? 0),
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }

  /**
   * Returns a venue policy.
   */
  getVenuePolicyRules(call, callback) {
    this.executeGetVenuePolicyRules(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetVenuePolicyRules(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetVenuePolicyRules', request);
    this.logger.info('get_venue_policy_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') } };
  }

  /**
   * Lints a venue policy.
   */
  lintVenuePolicies(call, callback) {
    this.executeLintVenuePolicies(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLintVenuePolicies(request) {
    validateContext(request.context);
    if (request.venue_policy) {
      await this.store.save(this.store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
    }
    await this.store.record(request.context.tenant_id, 'LintVenuePolicies', request);
    this.logger.info('lint_venue_policies', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        status: (request?.venue_policy?.status ?? ''),
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }

  /**
   * Normalizes a venue policy.
   */
  normalizeVenuePolicy(call, callback) {
    this.executeNormalizeVenuePolicy(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeNormalizeVenuePolicy(request) {
    validateContext(request.context);
    if (request.venue_policy) {
      await this.store.save(this.store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
    }
    await this.store.record(request.context.tenant_id, 'NormalizeVenuePolicy', request);
    this.logger.info('normalize_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        name: (request?.venue_policy?.name ?? ''),
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }

  /**
   * Previews a venue policy.
   */
  previewVenuePolicy(call, callback) {
    this.executePreviewVenuePolicy(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePreviewVenuePolicy(request) {
    validateContext(request.context);
    if (request.venue_policy) {
      await this.store.save(this.store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
    }
    await this.store.record(request.context.tenant_id, 'PreviewVenuePolicy', request);
    this.logger.info('preview_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        venue_policy_id: (request?.venue_policy_id ?? ''),
        version: (request?.venue_policy?.version ?? '0'),
      },
    };
  }

  /**
   * Validates a venue policy.
   */
  validateVenuePolicy(call, callback) {
    this.executeValidateVenuePolicy(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeValidateVenuePolicy(request) {
    validateContext(request.context);
    if (request.venue_policy) {
      await this.store.save(this.store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
    }
    await this.store.record(request.context.tenant_id, 'ValidateVenuePolicy', request);
    this.logger.info('validate_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        owner: request?.venue_policy?.owner,
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }
}

module.exports = { VenuePolicyValidationService };
