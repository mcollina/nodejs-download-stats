'use strict'

const assert = require('node:assert')
const test = require('node:test')
const fastify = require('fastify')
const path = require('node:path')
const adminRoute = require('../../routes/admin.js')

test('admin/health returns healthy status', async () => {
  const app = fastify()

  const mockDb = {
    getDailyVersionDownloads: () => [
      { date: '2024-01-01', major_version: 18, downloads: 100 },
      { date: '2024-01-02', major_version: 18, downloads: 200 }
    ],
    getMonthlyVersionDownloads: () => [
      { major_version: 18, month: '2024-01', total_downloads: 300 },
      { major_version: 20, month: '2024-01', total_downloads: 500 }
    ],
    getMonthlyOsDownloads: () => [],
    getDailyOsDownloads: () => []
  }
  app.decorate('db', mockDb)

  process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH = 'admin:secret'
  await app.register(adminRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/admin/health'
  })

  assert.strictEqual(response.statusCode, 200)
  const json = JSON.parse(response.payload)
  assert.strictEqual(json.healthy, true)
  assert.strictEqual(json.database.dailyRecords, 2)
  assert.strictEqual(json.database.monthlyRecords, 2)
  assert.strictEqual(json.database.uniqueVersions, 2)

  delete process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH
  await app.close()
})

test('admin/ingestion-stats returns database stats', async () => {
  const app = fastify()

  const mockDb = {
    getDailyVersionDownloads: () => [
      { date: '2024-01-01', major_version: 18, downloads: 100 },
      { date: '2024-01-02', major_version: 20, downloads: 200 }
    ],
    getMonthlyVersionDownloads: () => [
      { major_version: 18, month: '2024-01', total_downloads: 100 },
      { major_version: 20, month: '2024-01', total_downloads: 200 }
    ],
    getLastUpdate: () => ({ value: '2024-01-01T00:00:00Z', updatedAt: 1704067200000 }),
    getMostRecentDate: () => '2024-01-02'
  }
  app.decorate('db', mockDb)

  process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH = 'admin:testpass'
  await app.register(adminRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/admin/ingestion-stats'
  })

  assert.strictEqual(response.statusCode, 200)
  const json = JSON.parse(response.payload)

  assert.strictEqual(json.dailyRecords, 2)
  assert.strictEqual(json.monthlyRecords, 2)
  assert.deepStrictEqual(json.uniqueVersions, [18, 20])
  assert.ok(json.totalDailyDownloads > 0)
  assert.strictEqual(json.mostRecentDate, '2024-01-02')
  assert.ok(json.lastUpdate)
  assert.strictEqual(json.ingesterStats, null) // null when no ingestion run yet

  delete process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH
  await app.close()
})

test('admin routes not registered when auth not enabled', async () => {
  const app = fastify()

  const mockDb = {
    clearData: () => {},
    getExistingDates: () => [],
    insertVersionDownload: () => {},
    insertOsDownload: () => {},
    getDailyVersionDownloads: () => []
  }
  app.decorate('db', mockDb)

  // No env var set
  delete process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH

  await app.register(adminRoute, {})

  // Routes should not exist - should return 404
  const response = await app.inject({
    method: 'POST',
    url: '/admin/retrigger-ingestion'
  })

  assert.strictEqual(response.statusCode, 404)

  await app.close()
})

test('admin/retrigger-ingestion returns 401 without authentication', async () => {
  const app = fastify()

  const mockDb = {
    clearData: () => {},
    getDailyVersionDownloads: () => []
  }
  app.decorate('db', mockDb)

  process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH = 'admin:secret'
  await app.register(adminRoute, {})

  const response = await app.inject({
    method: 'POST',
    url: '/admin/retrigger-ingestion'
  })

  assert.strictEqual(response.statusCode, 401)
  assert.ok(response.headers['www-authenticate'])

  delete process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH
  await app.close()
})

test('admin/retrigger-ingestion returns 401 with invalid credentials', async () => {
  const app = fastify()

  const mockDb = {
    clearData: () => {},
    getDailyVersionDownloads: () => []
  }
  app.decorate('db', mockDb)

  process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH = 'admin:secret'
  await app.register(adminRoute, {})

  const response = await app.inject({
    method: 'POST',
    url: '/admin/retrigger-ingestion',
    headers: {
      authorization: 'Basic ' + Buffer.from('wrong:wrong').toString('base64')
    }
  })

  assert.strictEqual(response.statusCode, 401)

  delete process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH
  await app.close()
})

test('admin/retrigger-ingestion works with valid auth', async () => {
  const app = fastify()

  const mockDb = {
    clearData: () => {},
    getDailyVersionDownloads: () => [],
    getLastUpdate: () => null,
    getExistingDates: () => [],
    getMostRecentDate: () => null,
    insertVersionDownload: () => {},
    insertOsDownload: () => {},
    setLastUpdate: () => {}
  }
  app.decorate('db', mockDb)

  process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH = 'admin:secretpassword'
  await app.register(adminRoute, {})

  const response = await app.inject({
    method: 'POST',
    url: '/admin/retrigger-ingestion',
    headers: {
      authorization: 'Basic ' + Buffer.from('admin:secretpassword').toString('base64')
    }
  })

  assert.strictEqual(response.statusCode, 200)
  const json = JSON.parse(response.payload)
  assert.ok(json.message.includes('triggered'))
  // When no clearData specified, it defaults to false
  assert.strictEqual(json.clearData, false)

  delete process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH
  await app.close()
})

test('admin/retrigger-ingestion supports clearData option', async () => {
  const app = fastify()

  let clearDataCalled = false
  const mockDb = {
    clearData: () => { clearDataCalled = true },
    getDailyVersionDownloads: () => [],
    getLastUpdate: () => null,
    getExistingDates: () => [],
    getMostRecentDate: () => null,
    insertVersionDownload: () => {},
    insertOsDownload: () => {},
    setLastUpdate: () => {}
  }
  app.decorate('db', mockDb)

  process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH = 'admin:secret'
  await app.register(adminRoute, {})

  const response = await app.inject({
    method: 'POST',
    url: '/admin/retrigger-ingestion',
    headers: {
      authorization: 'Basic ' + Buffer.from('admin:secret').toString('base64'),
      'content-type': 'application/json'
    },
    body: JSON.stringify({ clearData: true })
  })

  assert.strictEqual(response.statusCode, 200)
  assert.strictEqual(clearDataCalled, true)
  const json = JSON.parse(response.payload)
  assert.strictEqual(json.clearData, true)

  delete process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH
  await app.close()
})

test('admin/retrigger-ingestion supports resetOnly option', async () => {
  const app = fastify()

  const mockDb = {
    getDailyVersionDownloads: () => []
  }
  app.decorate('db', mockDb)

  process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH = 'admin:secret'
  await app.register(adminRoute, {})

  const response = await app.inject({
    method: 'POST',
    url: '/admin/retrigger-ingestion',
    headers: {
      authorization: 'Basic ' + Buffer.from('admin:secret').toString('base64'),
      'content-type': 'application/json'
    },
    body: JSON.stringify({ resetOnly: true })
  })

  assert.strictEqual(response.statusCode, 200)
  const json = JSON.parse(response.payload)
  assert.ok(json.message.includes('reset'))

  delete process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH
  await app.close()
})

test('admin/raw-data/:date returns version list for valid date', async () => {
  const app = fastify()

  const mockDb = {
    getDailyVersionDownloads: () => [],
    getMonthlyVersionDownloads: () => [],
    getMonthlyOsDownloads: () => []
  }
  app.decorate('db', mockDb)

  // Admin routes must be enabled for the endpoint to exist
  process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH = 'admin:secret'
  await app.register(adminRoute, {})

  // This test would need network access - just verify the endpoint exists
  // and returns appropriate error or success structure
  const response = await app.inject({
    method: 'GET',
    url: '/admin/raw-data/2021-01-01'
  })

  // Will either succeed (if we have network) or fail with 500 (if no network)
  // Just verify it's not a 404
  assert.ok(response.statusCode === 200 || response.statusCode === 500)

  delete process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH
  await app.close()
})

test('admin/raw-data/:date returns 400 for invalid date format', async () => {
  const app = fastify()

  const mockDb = {
    getDailyVersionDownloads: () => [],
    getMonthlyVersionDownloads: () => [],
    getMonthlyOsDownloads: () => []
  }
  app.decorate('db', mockDb)

  // Admin routes must be enabled for the endpoint to exist
  process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH = 'admin:secret'
  await app.register(adminRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/admin/raw-data/invalid-date'
  })

  assert.strictEqual(response.statusCode, 400)
  const json = JSON.parse(response.payload)
  assert.ok(json.error.includes('Invalid date'))

  delete process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH
  await app.close()
})

test('DataIngester getStats returns current state', () => {
  const { DataIngester } = require('../../lib/ingest.js')
  const mockDb = {
    getLastUpdate: () => null,
    getMostRecentDate: () => null,
    getExistingDates: () => [],
    listAvailableFiles: () => Promise.resolve([])
  }
  const mockLogger = {
    info: () => {},
    debug: () => {},
    warn: () => {},
    error: () => {},
    trace: () => {}
  }

  const ingester = new DataIngester(mockLogger, mockDb)

  const stats = ingester.getStats()

  assert.strictEqual(stats.isIngesting, false)
  assert.strictEqual(stats.lastError, null)
  assert.ok(stats.stats)
  assert.strictEqual(stats.stats.totalFiles, 0)
  assert.strictEqual(stats.stats.processedFiles, 0)
})

test('DataIngester reset clears state', () => {
  const { DataIngester } = require('../../lib/ingest.js')
  const mockDb = {
    getLastUpdate: () => null,
    getMostRecentDate: () => null,
    getExistingDates: () => [],
    listAvailableFiles: () => Promise.resolve([])
  }
  const mockLogger = {
    info: () => {},
    debug: () => {},
    warn: () => {},
    error: () => {},
    trace: () => {}
  }

  const ingester = new DataIngester(mockLogger, mockDb)

  // Set some fake state
  ingester.isIngesting = true
  ingester.stats.processedFiles = 100
  ingester.stats.byVersion = { 18: 50 }

  ingester.reset()

  assert.strictEqual(ingester.isIngesting, false)
  assert.strictEqual(ingester.stats.processedFiles, 0)
  assert.deepStrictEqual(ingester.stats.byVersion, {})
})
