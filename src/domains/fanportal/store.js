'use strict';

/**
 * The fanportal team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map event page API values to the database document boundary.
 */
function toEventPageRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'event_page', record_id: value.event_page_id, document: value };
}

/**
 * Map fan session API values to the database document boundary.
 */
function toFanSessionRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'fan_session', record_id: value.fan_session_id, document: value };
}

/**
 * Map wishlist API values to the database document boundary.
 */
function toWishlistRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'wishlist', record_id: value.wishlist_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'fanportal', action, payload);
}

module.exports = { toEventPageRecord, toFanSessionRecord, toWishlistRecord, save, record };
