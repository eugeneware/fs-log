const tape = require('tape');
const it = require('tape-promise').default(tape);
const fsLog = require('../fs-log');
const test = require('abstract-log');
const tmpfile = require('tmpfile');
const fs = require('pn/fs');

const common = {
  setup: async (t) => {
    // let dbPath = __dirname + '/../data/test.json';
    let dbPath = tmpfile();
    return fsLog(dbPath);
  },
  teardown: async (t, log) => {
    await fs.unlink(log.dbPath);
  }
};

test(it, common);
