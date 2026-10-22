'use strict';

/**
 * The admissions team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map admission API values to the database document boundary.
 */
function toAdmissionRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'admission', record_id: value.admission_id, document: value };
}

/**
 * Map gate scan API values to the database document boundary.
 */
function toGateScanRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'gate_scan', record_id: value.gate_scan_id, document: value };
}

/**
 * Map wristband API values to the database document boundary.
 */
function toWristbandRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'wristband', record_id: value.wristband_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'admissions', action, payload);
}

module.exports = { toAdmissionRecord, toGateScanRecord, toWristbandRecord, save, record };
