const { Writable, Transform } = require('stream');
const split2 = require('split2');
const fs = require('pn/fs');
const combine = require('stream-combiner2').obj;
const fst = require('fs-tail-stream');

module.exports = (...args) => {
  return new FsLog(...args);
};

class FsLog {
  constructor (dbPath, delim = '\r\n') {
    this.dbPath = dbPath;
    this.delim = delim;
    this.tail = 0;
    this.serialize = stringify(this.delim);
  }

  async open () {
    try {
      let stat = await fs.stat(this.dbPath);
      this.tail = stat.size;
    } catch (err) {
      this.tail = 0;
    }
    this.fd = await fs.open(this.dbPath, 'a+', 0o666);
    this.writeStream = fs.createWriteStream(null, { fd: this.fd, flags: 'a' });
  }

  async close () {
    await fs.close(this.fd);
  }

  async append (data) {
    let buf = await this._writeToPromise(data);
    let pos = this.tail;
    let offset = [pos, buf.length];
    this.tail += buf.length;
    return offset;
  }

  _writeToPromise (data) {
    let ws = this.writeStream;
    let serialize = this.serialize;
    return new Promise((resolve, reject) => {
      ws.once('error', reject);
      let buf = Buffer.from(serialize(data));
      if (!ws.write(buf)) {
        ws.once('drain', complete);
      } else {
        process.nextTick(complete);
      }

      function complete () {
        resolve(buf);
      }
    });
  }

  async get (offset) {
    let key = parseOpts(offset);
    let b = Buffer.alloc(key.length);
    let { buffer: data } = await fs.read(this.fd, b, 0, key.length, key.start);
    if (data.length !== key.length) throw new Error('incorrect read length');
    let obj = JSON.parse(b.toString());
    return obj;
  }

  createReadStream (opts = {}) {
    let start = 0;
    let end;
    let key;

    if (opts.offset) opts.gte = opts.offset;
    if (opts.gt) {
      key = parseOpts(opts.gt);
      start = key.start + key.length + 2;
    } else if (opts.gte) {
      key = parseOpts(opts.gte);
      start = key.start;
    }
    if (opts.lt) {
      key = parseOpts(opts.lt);
      end = key.start - 1;
    } else if (opts.lte) {
      key = parseOpts(opts.lte);
      end = key.start + key.length - 1;
    }
    let readOpts = { start: start, end: end, flags: 'r', tail: opts.tail };

    let frs = fst.createReadStream(this.dbPath, readOpts);

    let rs = combine(
      frs,
      split2(),
      parseLine(readOpts, this.delim)
    );
    rs.close = frs.close.bind(frs);
    frs.once('close', rs.emit.bind(rs, 'close'));

    return rs;
  }

  createWriteStream () {
    let ws = Writable({ objectMode: true });
    ws._write = (data, enc, cb) => {
      this.append(data)
        .then(() => {
          cb();
        })
        .catch(cb);
    };
    return ws;
  }
}

function parseOpts (key) {
  if (typeof key === 'string') {
    let parts = key.split('_');
    return {start: +parts[0], length: +parts[1]};
  } else if (Array.isArray(key) && key.length === 2) {
    return {start: +key[0], length: +key[1]};
  } else if (key && Number.isInteger(key.start) && Number.isInteger(key.length)) {
    return Object.assign({ start: key.start, length: key.length });
  } else {
    throw new TypeError('key must be a string of the form "START_LENGTH", ' +
      'or an array of the form [START, LENGTH], or an objecdt of the form ' +
      '{ start: START, length: LENGTH }, where START AND LENGTH are integers');
  }
}

function parseLine (readOpts, delim) {
  let pos = readOpts.start;
  let ts = Transform({ objectMode: true });
  let delimLength = Buffer.byteLength(delim);
  ts._transform = (data, enc, cb) => {
    try {
      let obj = JSON.parse(data);
      let len = Buffer.byteLength(data) + delimLength;
      ts.push({ offset: [pos, len], value: obj });
      pos += len;
      cb();
    } catch (err) {
      return cb(err);
    }
  };
  return ts;
}

function stringify (delim) {
  return function (data) {
    return Buffer.from(JSON.stringify(data) + delim, 'utf8');
  };
}
