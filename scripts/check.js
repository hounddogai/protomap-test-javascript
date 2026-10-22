'use strict';

/** Parse every module, the way the runtime would, without starting a server or opening a database. */
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

function sources(directory) {
  return fs.readdirSync(directory, { withFileTypes: true })
    .sort((a, b) => a.name.localeCompare(b.name, 'en'))
    .flatMap((entry) => {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) return sources(file);
      return entry.name.endsWith('.js') ? [file] : [];
    });
}

const files = ['scripts', 'src', 'test'].flatMap((folder) => sources(folder));
for (const file of files) execFileSync(process.execPath, ['--check', file], { stdio: 'inherit' });
console.info(`Checked ${files.length} modules`);
