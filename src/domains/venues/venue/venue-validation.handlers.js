'use strict';

/**
 * Checks venues against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call } = require('../../../platform/rpc');

/**
 * The handlers of VenueValidationService, over the dependencies the venues team is given.
 */
function createVenueValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a venue.
   */
  async function checkVenueEligibility(request) {
    validateContext(request.context);
    if (request.venue) {
      await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await store.record(request.context.tenant_id, 'CheckVenueEligibility', request);
    logger.info('check_venue_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        specs: (request?.venue?.specs ?? {}),
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }

  /**
   * Explains a venue.
   */
  async function explainVenueDecision(request) {
    validateContext(request.context);
    if (request.venue) {
      await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await store.record(request.context.tenant_id, 'ExplainVenueDecision', request);
    logger.info('explain_venue_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        status: (request?.venue?.status ?? ''),
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }

  /**
   * Returns a venue.
   */
  async function getVenueRules(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await store.record(request.context.tenant_id, 'GetVenueRules', request);
    logger.info('get_venue_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { venue: { venue_id: (request?.venue_id ?? '') } };
  }

  /**
   * Lints a venue.
   */
  async function lintVenues(request) {
    validateContext(request.context);
    if (request.venue) {
      await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await store.record(request.context.tenant_id, 'LintVenues', request);
    logger.info('lint_venues', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        status: (request?.venue?.status ?? ''),
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }

  /**
   * Normalizes a venue.
   */
  async function normalizeVenue(request) {
    validateContext(request.context);
    if (request.venue) {
      await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await store.record(request.context.tenant_id, 'NormalizeVenue', request);
    logger.info('normalize_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        status: (request?.venue?.status ?? ''),
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }

  /**
   * Previews a venue.
   */
  async function previewVenue(request) {
    validateContext(request.context);
    if (request.venue) {
      await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await store.record(request.context.tenant_id, 'PreviewVenue', request);
    logger.info('preview_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        code: (request?.venue?.code ?? ''),
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }

  /**
   * Scores a venue.
   */
  async function scoreVenue(request) {
    validateContext(request.context);
    if (request.venue) {
      await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await store.record(request.context.tenant_id, 'ScoreVenue', request);
    logger.info('score_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_id: (request?.venue_id ?? ''),
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.venuesVenueHistoryClient.getVenueVersion(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
        venue: { dimensions: { width: (reply1?.venue?.dimensions?.width ?? 0) } },
      },
      {
        venue: {
          status: (request?.venue?.status ?? ''),
          venue_id: (request?.venue_id ?? ''),
        },
      },
    );
  }

  /**
   * Validates a venue.
   */
  async function validateVenue(request) {
    validateContext(request.context);
    if (request.venue) {
      await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await store.record(request.context.tenant_id, 'ValidateVenue', request);
    logger.info('validate_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        specs: (request?.venue?.specs ?? {}),
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }

  return {
    checkVenueEligibility: unary(checkVenueEligibility),
    explainVenueDecision: unary(explainVenueDecision),
    getVenueRules: unary(getVenueRules),
    lintVenues: unary(lintVenues),
    normalizeVenue: unary(normalizeVenue),
    previewVenue: unary(previewVenue),
    scoreVenue: unary(scoreVenue),
    validateVenue: unary(validateVenue),
  };
}

module.exports = { createVenueValidationHandlers };
