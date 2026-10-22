'use strict';

/**
 * Authenticate every call before its handler runs: the caller's bearer token must be an API credential that identity
 * accepts for the caller's tenant.
 */

const grpc = require('@grpc/grpc-js');
const { deadline, metadataFor } = require('./credentials');
const { toServiceError } = require('./errors');

function authenticate(identity) {
  return (_descriptor, call) => new grpc.ServerInterceptingCall(call, {
    start(next) {
      next({
        onReceiveMetadata(metadata, forward) {
          const authorization = metadata.get('authorization')[0];
          const tenant = metadata.get('x-tenant-id')[0];
          const token = typeof authorization === 'string' && authorization.startsWith('Bearer ')
            ? authorization.slice(7) : '';
          if (typeof tenant !== 'string' || !tenant || !token) {
            call.sendStatus({
              code: grpc.status.UNAUTHENTICATED, details: 'A bearer token and a tenant are required',
              metadata: new grpc.Metadata(),
            });
            return;
          }
          // The lookup is this server's own call, which bears its service credential.
          const request = {
            context: { tenant_id: tenant, actor: { user_id: 'live', service_account: true } },
            api_credential_id: token,
          };
          const options = { deadline: deadline(3_000) };
          identity.checkApiCredentialEligibility(request, metadataFor(request.context), options, (error, reply) => {
            if (error) {
              call.sendStatus(toServiceError(error));
              return;
            }
            if (reply.result?.status !== 'ok' || reply.api_credential?.api_credential_id !== token) {
              call.sendStatus({
                code: grpc.status.PERMISSION_DENIED, details: 'The credential was rejected',
                metadata: new grpc.Metadata(),
              });
              return;
            }
            forward(metadata);
          });
        },
      });
    },
  });
}

module.exports = { authenticate };
