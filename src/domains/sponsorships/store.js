'use strict';

/**
 * The sponsorships team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map activation API values to the database document boundary.
 */
function toActivationRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'activation', record_id: value.activation_id, document: value };
}

/**
 * Map sponsor API values to the database document boundary.
 */
function toSponsorRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'sponsor', record_id: value.sponsor_id, document: value };
}

/**
 * Map sponsorship deal API values to the database document boundary.
 */
function toSponsorshipDealRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'sponsorship_deal', record_id: value.sponsorship_deal_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'sponsorships', action, payload);
}

module.exports = { toActivationRecord, toSponsorRecord, toSponsorshipDealRecord, save, record };
