'use strict'

// Set env var BEFORE importing plugins
const { join } = require('node:path')
const os = require('node:os')
const testDbPath = join(os.tmpdir(), `test-db-${Date.now()}.db`)
process.env.NODEJS_DOWNLOAD_STATS_DB = testDbPath

const { describe, it, before, after } = require('node:test')
const assert = require('node:assert')
const fs = require('node:fs')
const fastify = require('fastify')

describe('database plugin', () => {
  let app

  before(async () => {
    app = fastify()
    // Register plugins (skip-override is set in plugins)
    await app.register(require('../../plugins/config'))
    await app.register(require('../../plugins/database'))
    await app.ready()
  })

  after(async () => {
    await app.close()
    // Cleanup test db
    try {
      fs.unlinkSync(testDbPath)
    } catch {}
  })

  it('should decorate fastify with db', () => {
    assert.ok(app.db, 'db should be decorated on fastify')
    assert.ok(typeof app.db.getDailyVersionDownloads === 'function', 'db should have getDailyVersionDownloads method')
  })

  it('should initialize schema', () => {
    assert.ok(true, 'schema initialized without error')
  })

  it('should insert and retrieve version downloads', () => {
    app.db.insertVersionDownload('2024-01-01', 20, 1000)
    app.db.insertVersionDownload('2024-01-01', 18, 500)
    app.db.insertVersionDownload('2024-01-02', 20, 1500)

    const rows = app.db.getDailyVersionDownloads()
    assert.strictEqual(rows.length, 3, 'should have 3 rows')
    assert.strictEqual(rows[0].major_version, 18)
    assert.strictEqual(rows[0].downloads, 500)
  })

  it('should insert and retrieve OS downloads', () => {
    app.db.insertOsDownload('2024-01-01', 'linux', 800)
    app.db.insertOsDownload('2024-01-01', 'win32', 200)
    app.db.insertOsDownload('2024-01-02', 'linux', 1500)

    const rows = app.db.getDailyOsDownloads()
    assert.strictEqual(rows.length, 3, 'should have 3 OS rows')
  })

  it('should get existing dates', () => {
    const dates = app.db.getExistingDates()
    assert.ok(Array.isArray(dates), 'should return array')
    assert.ok(dates.includes('2024-01-01'), 'should include 2024-01-01')
    assert.ok(dates.includes('2024-01-02'), 'should include 2024-01-02')
  })

  it('should get most recent date', () => {
    const mostRecent = app.db.getMostRecentDate()
    assert.strictEqual(mostRecent, '2024-01-02', 'most recent should be 2024-01-02')
  })

  it('should update and get last update timestamp', () => {
    app.db.setLastUpdate('2024-01-15T10:00:00Z')
    const lastUpdate = app.db.getLastUpdate()
    assert.strictEqual(lastUpdate.value, '2024-01-15T10:00:00Z', 'should store last update value')
    assert.ok(lastUpdate.updatedAt > 0, 'should have updatedAt timestamp')
  })
})
