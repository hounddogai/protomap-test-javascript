'use strict';

const { close, migrate } = require('../platform/database');

migrate().then(close, async (error) => {
  await close();
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
