'use strict';

/** Tenant context follows the message; the service credential comes from the deployment, never from source control. */
const grpc = require('@grpc/grpc-js');

function metadataFor(context) {
  const metadata = new grpc.Metadata();
  metadata.set('authorization', `Bearer ${process.env.ACME_SERVICE_TOKEN ?? ''}`);
  metadata.set('x-tenant-id', context?.tenant_id || process.env.ACME_TENANT || 'acme');
  return metadata;
}

function deadline(milliseconds = 15_000) {
  return Date.now() + milliseconds;
}

module.exports = { metadataFor, deadline };
