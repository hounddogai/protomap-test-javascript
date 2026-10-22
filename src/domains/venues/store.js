'use strict';

/**
 * The venues team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map venue API values to the database document boundary.
 */
function toVenueRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'venue', record_id: value.venue_id, document: value };
}

/**
 * Map venue policy API values to the database document boundary.
 */
function toVenuePolicyRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'venue_policy', record_id: value.venue_policy_id, document: value };
}

/**
 * Map venue section API values to the database document boundary.
 */
function toVenueSectionRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'venue_section', record_id: value.venue_section_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'venues', action, payload);
}

module.exports = { toVenueRecord, toVenuePolicyRecord, toVenueSectionRecord, save, record };
