'use strict';

/**
 * The performers team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map performer API values to the database document boundary.
 */
function toPerformerRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'performer', record_id: value.performer_id, document: value };
}

/**
 * Map performer contract API values to the database document boundary.
 */
function toPerformerContractRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'performer_contract', record_id: value.performer_contract_id, document: value };
}

/**
 * Map rider API values to the database document boundary.
 */
function toRiderRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'rider', record_id: value.rider_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'performers', action, payload);
}

module.exports = { toPerformerRecord, toPerformerContractRecord, toRiderRecord, save, record };
