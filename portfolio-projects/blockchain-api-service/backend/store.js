const fs = require('node:fs');
const path = require('node:path');
const { randomUUID, createHash } = require('node:crypto');

const digest = value => createHash('sha256').update(value).digest('hex');

// A single-process store. An exclusive lock prevents accidental concurrent writers.
// Writes replace the complete snapshot only after the temporary file is flushed.
class FileStore {
  constructor(filename) {
    this.filename = path.resolve(filename);
    fs.mkdirSync(path.dirname(this.filename), { recursive: true });
    this.lockfile = `${this.filename}.lock`;
    this.lock = fs.openSync(this.lockfile, 'wx', 0o600);
    try {
      fs.writeFileSync(this.lock, String(process.pid));
      this.data = fs.existsSync(this.filename)
        ? JSON.parse(fs.readFileSync(this.filename, 'utf8'))
        : { version: 1, users: [], keys: [] };
      if (this.data.version !== 1 || !Array.isArray(this.data.users) || !Array.isArray(this.data.keys)) {
        throw new Error('Unsupported or corrupt storage file');
      }
    } catch (error) {
      this.close();
      throw error;
    }
  }

  mutate(change) {
    const next = structuredClone(this.data);
    const result = change(next);
    const temporary = `${this.filename}.${randomUUID()}.tmp`;
    let descriptor;
    try {
      descriptor = fs.openSync(temporary, 'wx', 0o600);
      fs.writeFileSync(descriptor, JSON.stringify(next));
      fs.fsyncSync(descriptor);
      fs.closeSync(descriptor);
      descriptor = undefined;
      fs.renameSync(temporary, this.filename);
      this.data = next;
      return result;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
      if (fs.existsSync(temporary)) fs.unlinkSync(temporary);
    }
  }

  findUser(email) { return this.data.users.find(user => user.email === email); }
  hasUser(id) { return this.data.users.some(user => user.userId === id); }
  addUser(user) {
    return this.mutate(data => {
      if (data.users.some(existing => existing.email === user.email)) return false;
      data.users.push(user);
      return true;
    });
  }
  addKey(key) { return this.mutate(data => data.keys.push(key)); }
  findKey(secret) { return this.data.keys.find(key => key.digest === digest(secret)); }
  listKeys(userId) {
    return this.data.keys.filter(key => key.userId === userId).map(({ digest: hash, userId: owner, ...key }) => key);
  }
  revoke(id, userId) {
    return this.mutate(data => {
      const key = data.keys.find(key => key.id === id && key.userId === userId);
      if (!key) return false;
      key.revoked = true;
      return true;
    });
  }
  recordRequest(id) {
    this.mutate(data => { data.keys.find(key => key.id === id).requests++; });
  }
  close() {
    if (this.lock !== undefined) {
      fs.closeSync(this.lock);
      this.lock = undefined;
      fs.unlinkSync(this.lockfile);
    }
  }
}

module.exports = { FileStore, digest };
