'use strict';

/**
 * Checks venue sections against the rules before anything acts on them. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { venuesVenueValidationClient } = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Checks a venue section.
 */
async function checkVenueSectionEligibility(request) {
  validateContext(request.context);
  if (request.venue_section) {
    await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
  }
  await store.record(request.context.tenant_id, 'CheckVenueSectionEligibility', request);
  logger.info('check_venue_section_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue: {
      address: { postal_code: (request?.venue_section?.location?.postal_code ?? '') },
      audit: { updated_at: request?.venue_section?.audit?.updated_at },
      owner: {
        email: (request?.details?.contact?.email ?? ''),
        user_id: (request?.venue_section?.manager?.user_id ?? ''),
      },
    },
  };
  const reply1 = await withRetry(() => call((done) => venuesVenueValidationClient.lintVenues(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      venue_section: {
        audit: { created_by: (reply1?.venue?.audit?.created_by ?? '') },
        children: [{
          children: [{ audit: { created_at: reply1?.venue?.audit?.created_at } }],
        }],
      },
    },
    {
      venue_section: {
        status: (request?.venue_section?.status ?? ''),
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    },
  );
}

/**
 * Explains a venue section.
 */
async function explainVenueSectionDecision(request) {
  validateContext(request.context);
  if (request.venue_section) {
    await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
  }
  await store.record(request.context.tenant_id, 'ExplainVenueSectionDecision', request);
  logger.info('explain_venue_section_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_section: {
      name: (request?.venue_section?.name ?? ''),
      venue_section_id: (request?.venue_section_id ?? ''),
    },
  };
}

/**
 * Returns a venue section.
 */
async function getVenueSectionRules(request) {
  validateContext(request.context);
  if (!request.venue_section_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
  }
  await store.record(request.context.tenant_id, 'GetVenueSectionRules', request);
  logger.info('get_venue_section_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return {
    venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
  };
}

/**
 * Lints a venue section.
 */
async function lintVenueSections(request) {
  validateContext(request.context);
  if (request.venue_section) {
    await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
  }
  await store.record(request.context.tenant_id, 'LintVenueSections', request);
  logger.info('lint_venue_sections', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_section: {
      parent_id: (request?.venue_section?.parent_id ?? ''),
      venue_section_id: (request?.venue_section_id ?? ''),
    },
  };
}

/**
 * Normalizes a venue section.
 */
async function normalizeVenueSection(request) {
  validateContext(request.context);
  if (request.venue_section) {
    await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
  }
  await store.record(request.context.tenant_id, 'NormalizeVenueSection', request);
  logger.info('normalize_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_section: {
      depth: (request?.venue_section?.depth ?? 0),
      venue_section_id: (request?.venue_section_id ?? ''),
    },
  };
}

/**
 * Previews a venue section.
 */
async function previewVenueSection(request) {
  validateContext(request.context);
  if (request.venue_section) {
    await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
  }
  await store.record(request.context.tenant_id, 'PreviewVenueSection', request);
  logger.info('preview_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_section: {
      status: (request?.venue_section?.status ?? ''),
      venue_section_id: (request?.venue_section_id ?? ''),
    },
  };
}

/**
 * Scores a venue section.
 */
async function scoreVenueSection(request) {
  validateContext(request.context);
  if (request.venue_section) {
    await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
  }
  await store.record(request.context.tenant_id, 'ScoreVenueSection', request);
  logger.info('score_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_section: {
      depth: (request?.venue_section?.depth ?? 0),
      venue_section_id: (request?.venue_section_id ?? ''),
    },
  };
}

/**
 * Validates a venue section.
 */
async function validateVenueSection(request) {
  validateContext(request.context);
  if (request.venue_section) {
    await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
  }
  await store.record(request.context.tenant_id, 'ValidateVenueSection', request);
  logger.info('validate_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_section: {
      parent_id: (request?.venue_section?.parent_id ?? ''),
      venue_section_id: (request?.venue_section_id ?? ''),
    },
  };
}

module.exports = { checkVenueSectionEligibility, explainVenueSectionDecision, getVenueSectionRules, lintVenueSections, normalizeVenueSection, previewVenueSection, scoreVenueSection, validateVenueSection };
