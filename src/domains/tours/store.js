'use strict';

/**
 * The tours team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map tour API values to the database document boundary.
 */
function toTourRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'tour', record_id: value.tour_id, document: value };
}

/**
 * Map tour budget API values to the database document boundary.
 */
function toTourBudgetRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'tour_budget', record_id: value.tour_budget_id, document: value };
}

/**
 * Map tour stop API values to the database document boundary.
 */
function toTourStopRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'tour_stop', record_id: value.tour_stop_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'tours', action, payload);
}

module.exports = { toTourRecord, toTourBudgetRecord, toTourStopRecord, save, record };
