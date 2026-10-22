'use strict';

/** Structured events for the platform's log collector, one JSON object per line. */
class Logger {
  constructor(scope = {}) {
    this.scope = scope;
  }

  child(scope) {
    return new Logger({ ...this.scope, ...scope });
  }

  info(event, fields = {}) {
    process.stdout.write(`${JSON.stringify({ level: 'info', event, ...this.scope, ...fields })}\n`);
  }

  error(event, error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`${JSON.stringify({ level: 'error', event, ...this.scope, error: message })}\n`);
  }
}

module.exports = { Logger, logger: new Logger({ service: 'acme-live' }) };
