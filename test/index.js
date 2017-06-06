const tape = require('tape');
const it = require('tape-promise').default(tape);
const fsLog = require('../fs-log');
const test = require('abstract-log');
const tmpfile = require('tmpfile');
const { promisify } = require('util');
const unlink = promisify(require('fs').unlink);

const common = {
  setup: async (t) => {
    // let dbPath = __dirname + '/../data/test.json';
    let dbPath = tmpfile();
    return fsLog(dbPath);
  },
  teardown: async (t, log) => {
    await unlink(log.dbPath);
  }
};

test(it, common);
