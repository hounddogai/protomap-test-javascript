'use strict';

/**
 * Operator lookups and an escape hatch for incident response.
 */

const grpc = require('@grpc/grpc-js');
const clients = require('../platform/clients');
const { call } = require('../platform/rpc');

const tenant = process.env.ACME_TENANT ?? 'acme';
const metadata = new grpc.Metadata();
metadata.set('authorization', `Bearer ${process.env.ACME_SERVICE_TOKEN ?? ''}`);
metadata.set('x-tenant-id', tenant);
// The program acts with the server's credential, as its service account, which the servers downstream require.
const actor = { user_id: 'livectl', service_account: true };

async function run() {
  switch (process.argv[2]) {
    case 'get-admission-approval-status': {
      const reply = await call((done) => clients.admissionsAdmissionApprovalClient.getAdmissionApprovalStatus(
        { context: { tenant_id: tenant, actor }, admission_id: process.argv[3] ?? '' },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
      process.stdout.write(`${JSON.stringify(reply)}\n`);
      break;
    }
    case 'get-admission-rates': {
      const reply = await call((done) => clients.admissionsAdmissionCalculationClient.getAdmissionRates(
        { context: { tenant_id: tenant, actor }, admission_id: process.argv[3] ?? '' },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
      process.stdout.write(`${JSON.stringify(reply)}\n`);
      break;
    }
    case 'get-admission-lifecycle': {
      const reply = await call((done) => clients.admissionsAdmissionLifecycleClient.getAdmissionLifecycle(
        { context: { tenant_id: tenant, actor }, admission_id: process.argv[3] ?? '' },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
      process.stdout.write(`${JSON.stringify(reply)}\n`);
      break;
    }
    case 'lookup-admission': {
      const reply = await call((done) => clients.admissionsAdmissionQueryClient.lookupAdmission(
        { context: { tenant_id: tenant, actor }, admission_id: process.argv[3] ?? '' },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
      process.stdout.write(`${JSON.stringify(reply)}\n`);
      break;
    }
    case 'peek-admission': {
      const reply = await call((done) => clients.admissionsAdmissionQueryClient.peekAdmission(
        { context: { tenant_id: tenant, actor }, admission_id: process.argv[3] ?? '' },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
      process.stdout.write(`${JSON.stringify(reply)}\n`);
      break;
    }
    case 'get-admission': {
      const reply = await call((done) => clients.admissionsAdmissionClient.getAdmission(
        { context: { tenant_id: tenant, actor }, admission_id: process.argv[3] ?? '' },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
      process.stdout.write(`${JSON.stringify(reply)}\n`);
      break;
    }
    case 'get-admission-rules': {
      const reply = await call((done) => clients.admissionsAdmissionValidationClient.getAdmissionRules(
        { context: { tenant_id: tenant, actor }, admission_id: process.argv[3] ?? '' },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
      process.stdout.write(`${JSON.stringify(reply)}\n`);
      break;
    }
    case 'inspect-admission-workflow': {
      const reply = await call((done) => clients.admissionsAdmissionWorkflowClient.inspectAdmissionWorkflow(
        { context: { tenant_id: tenant, actor }, admission_id: process.argv[3] ?? '' },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
      process.stdout.write(`${JSON.stringify(reply)}\n`);
      break;
    }
    case 'raw': {
      const path = process.argv[3];
      if (!path?.startsWith('/')) throw new Error('Expected an RPC path');
      const payload = Buffer.from(process.argv[4] ?? '', 'base64');
      const client = new grpc.Client(process.env.GRPC_MESH ?? '127.0.0.1:15001', grpc.credentials.createInsecure());
      try {
        const reply = await new Promise((resolve, reject) => {
          client.makeUnaryRequest(path, (value) => value, (value) => value, payload, metadata,
            { deadline: Date.now() + 15_000 }, (error, value) => (error ? reject(error) : resolve(value)));
        });
        process.stdout.write(`${reply.toString('base64')}\n`);
      } finally {
        client.close();
      }
      break;
    }
    default:
      throw new Error('Choose a lookup command or a raw RPC path');
  }
}

run().then(() => clients.close(), (error) => {
  clients.close();
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
