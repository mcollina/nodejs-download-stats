'use strict'

const { describe, it, before, after } = require('node:test')
const assert = require('node:assert')
const { join } = require('node:path')
const os = require('node:os')
const fs = require('node:fs')
const fastify = require('fastify')

describe('metrics route', () => {
  let dbPath
  let app

  before(async () => {
    dbPath = join(os.tmpdir(), `test-metrics-db-${Date.now()}.db`)
    process.env.NODEJS_DOWNLOAD_STATS_DB = dbPath

    // Mock ingestion by setting a recent last_update
    // This prevents the background ingestion from running
    const { Database } = require('../../lib/db')
    const mockDb = new Database(dbPath)
    mockDb.initSchema()
    mockDb.setLastUpdate(new Date().toISOString())
    mockDb.closeDb()

    app = fastify({ logger: false })
    
    // Register plugins manually for testing
    await app.register(require('../../plugins/config'))
    await app.register(require('../../plugins/database'))
    await app.register(require('../../routes/metrics'))
    await app.ready()
  })

  after(async () => {
    await app.close()
    try {
      fs.unlinkSync(dbPath)
    } catch {}
  })

  it('should return data when available', async () => {
    // Insert some test data directly
    app.db.insertVersionDownload('2024-01-01', 20, 1000)
    app.db.insertVersionDownload('2024-01-02', 20, 1500)
    app.db.insertVersionDownload('2024-01-01', 18, 500)
    app.db.insertOsDownload('2024-01-01', 'linux', 800)
    app.db.insertOsDownload('2024-01-02', 'linux', 1200)

    const response = await app.inject({
      method: 'GET',
      url: '/metrics'
    })

    assert.strictEqual(response.statusCode, 200, 'should return 200')
    
    const data = JSON.parse(response.body)
    assert.ok(data.versions, 'should have versions')
    assert.ok(data.operatingSystems, 'should have operatingSystems')
    assert.ok(data.byVersion, 'should have byVersion')
    assert.ok(data.byOs, 'should have byOs')
    
    // Check version data structure
    assert.ok(data.versions['20'], 'should have version 20 data')
    assert.strictEqual(data.versions['20'].length, 2, 'should have 2 dates for v20')
    assert.strictEqual(data.versions['18'].length, 1, 'should have 1 date for v18')
  })

  it('should have correct cache headers', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/metrics'
    })

    assert.strictEqual(response.statusCode, 200)
    const cacheControl = response.headers['cache-control']
    assert.ok(cacheControl.includes('max-age='), 'should have max-age')
    assert.ok(cacheControl.includes('s-maxage='), 'should have s-maxage')
  })

  it('should return empty arrays when no data', async () => {
    // Create fresh db
    const freshDbPath = join(os.tmpdir(), `test-empty-db-${Date.now()}.db`)
    process.env.NODEJS_DOWNLOAD_STATS_DB = freshDbPath
    
    const { Database } = require('../../lib/db')
    const freshDb = new Database(freshDbPath)
    freshDb.initSchema()
    freshDb.setLastUpdate(new Date().toISOString()) // Prevent ingestion
    freshDb.closeDb()

    const freshApp = fastify({ logger: false })
    await freshApp.register(require('../../plugins/config'))
    await freshApp.register(require('../../plugins/database'))
    await freshApp.register(require('../../routes/metrics'))
    await freshApp.ready()

    const response = await freshApp.inject({
      method: 'GET',
      url: '/metrics'
    })

    await freshApp.close()
    try { fs.unlinkSync(freshDbPath) } catch {}

    assert.strictEqual(response.statusCode, 200)
    const data = JSON.parse(response.body)
    assert.deepStrictEqual(data.versions, {})
    assert.deepStrictEqual(data.operatingSystems, {})
  })
})
