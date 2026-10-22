'use strict';

/**
 * The shows team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map lineup API values to the database document boundary.
 */
function toLineupRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'lineup', record_id: value.lineup_id, document: value };
}

/**
 * Map performance API values to the database document boundary.
 */
function toPerformanceRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'performance', record_id: value.performance_id, document: value };
}

/**
 * Map show API values to the database document boundary.
 */
function toShowRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'show', record_id: value.show_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'shows', action, payload);
}

module.exports = { toLineupRecord, toPerformanceRecord, toShowRecord, save, record };
