'use strict';

/**
 * The seating team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map seat assignment API values to the database document boundary.
 */
function toSeatAssignmentRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'seat_assignment', record_id: value.seat_assignment_id, document: value };
}

/**
 * Map seat hold API values to the database document boundary.
 */
function toSeatHoldRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'seat_hold', record_id: value.seat_hold_id, document: value };
}

/**
 * Map seat map API values to the database document boundary.
 */
function toSeatMapRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'seat_map', record_id: value.seat_map_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'seating', action, payload);
}

module.exports = { toSeatAssignmentRecord, toSeatHoldRecord, toSeatMapRecord, save, record };
