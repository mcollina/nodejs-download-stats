'use strict'

const { describe, it, before, after } = require('node:test')
const assert = require('node:assert')
const { join } = require('node:path')
const os = require('node:os')
const fs = require('node:fs')
const { MockAgent, setGlobalDispatcher } = require('undici')

const { DataIngester } = require('../../lib/ingest')
const { Database } = require('../../lib/db')

// Simple mock logger
const createMockLogger = () => ({
  info: () => {},
  debug: () => {},
  warn: () => {},
  error: () => {},
  trace: () => {}
})

describe('DataIngester', () => {
  let dbPath
  let db
  let logger
  let mockAgent

  before(async () => {
    dbPath = join(os.tmpdir(), `test-ingest-db-${Date.now()}.db`)
    db = new Database(dbPath)
    db.initSchema()
    logger = createMockLogger()

    mockAgent = new MockAgent()
    mockAgent.disableNetConnect()
    setGlobalDispatcher(mockAgent)
  })

  after(async () => {
    db.closeDb()
    try {
      fs.unlinkSync(dbPath)
    } catch {}
    await mockAgent.close()
  })

  it('should throw if logger is not provided', () => {
    assert.throws(() => {
      new DataIngester(null, db, mockAgent)
    }, /Logger is required/)
  })

  it('should throw if db is not provided', () => {
    assert.throws(() => {
      new DataIngester(logger, null, mockAgent)
    }, /Database is required/)
  })

  it('should create DataIngester instance', () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    assert.ok(ingester, 'should create instance')
    assert.ok(ingester.ingest, 'should have ingest method')
    assert.ok(ingester.ingestWithProgress, 'should have ingestWithProgress method')
  })

  it('should list available files from GCS', async () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    const mockPool = mockAgent.get('https://storage.googleapis.com')
    
    const bucketListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://doc.s3.amazonaws.com/2006-03-01">
  <Contents><Key>nodejs.org-access.log.20240101.json</Key></Contents>
  <Contents><Key>nodejs.org-access.log.20240102.json</Key></Contents>
  <Contents><Key>nodejs.org-access.log.20240103.json</Key></Contents>
</ListBucketResult>`

    mockPool.intercept({
      path: (path) => path.startsWith('/access-logs-summaries-nodejs/'),
      method: 'GET'
    }).reply(200, bucketListing, { 'content-type': 'application/xml' })

    const files = await ingester.listAvailableFiles()

    assert.strictEqual(files.length, 3, 'should list 3 files')
    assert.strictEqual(files[0].date, '2024-01-01', 'should parse first date')
    assert.ok(files[0].url.includes('20240101'), 'should have correct URL')
  })

  it('should filter files since specific date', async () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    const mockPool = mockAgent.get('https://storage.googleapis.com')
    
    mockPool.intercept({
      path: (path) => path.includes('marker=nodejs.org-access.log.20240102'),
      method: 'GET'
    }).reply(200, `<?xml version="1.0"?>
<ListBucketResult>
  <Contents><Key>nodejs.org-access.log.20240103.json</Key></Contents>
</ListBucketResult>`, { 'content-type': 'application/xml' })

    const files = await ingester.listAvailableFiles('2024-01-02')

    assert.ok(files.every(f => f.date > '2024-01-02'), 'should only return dates after marker')
  })

  it('should ingest data with progress callback', async () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    const mockPool = mockAgent.get('https://storage.googleapis.com')
    
    const bucketListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents><Key>nodejs.org-access.log.20240201.json</Key></Contents>
</ListBucketResult>`

    const dayData = JSON.stringify({
      version: { 'v20.19.0': 5000, 'v18.19.0': 3000 },
      os: { linux: 4000, win32: 3000, darwin: 1000 }
    })

    mockPool.intercept({
      path: (path) => path.includes('max-keys=1000') && !path.includes('marker'),
      method: 'GET'
    }).reply(200, bucketListing, { 'content-type': 'application/xml' })

    mockPool.intercept({
      path: (path) => path.endsWith('20240201.json'),
      method: 'GET'
    }).reply(200, dayData, { 'content-type': 'application/json' })

    const progressCalls = []

    await ingester.ingestWithProgress((progress) => {
      progressCalls.push(progress)
    })

    assert.ok(progressCalls.length > 0, 'should call progress callback')
    
    const dates = db.getExistingDates()
    assert.ok(dates.includes('2024-02-01'), 'should have ingested 2024-02-01')

    const versions = db.getDailyVersionDownloads()
    const v20Data = versions.filter(v => v.major_version === 20 && v.date === '2024-02-01')
    assert.strictEqual(v20Data.length, 1, 'should have v20 data')
    assert.strictEqual(v20Data[0].downloads, 5000, 'should have correct download count')
  })
})

describe('DataIngester - skip existing dates', () => {
  let dbPath
  let db
  let logger
  let mockAgent

  before(async () => {
    dbPath = join(os.tmpdir(), `test-ingest-skip-${Date.now()}.db`)
    db = new Database(dbPath)
    db.initSchema()
    logger = createMockLogger()

    mockAgent = new MockAgent()
    mockAgent.disableNetConnect()
    setGlobalDispatcher(mockAgent)

    // Pre-populate with existing data
    db.insertVersionDownload('2024-03-01', 20, 1000)
    db.insertOsDownload('2024-03-01', 'linux', 800)
  })

  after(async () => {
    db.closeDb()
    try { fs.unlinkSync(dbPath) } catch {}
    await mockAgent.close()
  })

  it('should skip existing dates during ingestion', async () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    const mockPool = mockAgent.get('https://storage.googleapis.com')
    
    const bucketListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents><Key>nodejs.org-access.log.20240301.json</Key></Contents>
  <Contents><Key>nodejs.org-access.log.20240302.json</Key></Contents>
</ListBucketResult>`

    const dayData = JSON.stringify({
      version: { 'v20.0.0': 2000 },
      os: { linux: 1500 }
    })

    // Match any GCS list request (may include marker param due to existing data)
    mockPool.intercept({
      path: (path) => path.includes('max-keys=1000'),
      method: 'GET'
    }).reply(200, bucketListing, { 'content-type': 'application/xml' })

    // Only 2024-03-02 should be downloaded (2024-03-01 already exists)
    mockPool.intercept({
      path: (path) => path.includes('20240302.json'),
      method: 'GET'
    }).reply(200, dayData, { 'content-type': 'application/json' })

    await ingester.ingest()

    const dates = db.getExistingDates()
    assert.ok(dates.includes('2024-03-01'), 'should still have original 2024-03-01')
    assert.ok(dates.includes('2024-03-02'), 'should have new 2024-03-02')
  })
})

describe('DataIngester - error handling', () => {
  let dbPath
  let db
  let logger
  let mockAgent

  before(async () => {
    dbPath = join(os.tmpdir(), `test-ingest-error-${Date.now()}.db`)
    db = new Database(dbPath)
    db.initSchema()
    logger = createMockLogger()

    mockAgent = new MockAgent()
    mockAgent.disableNetConnect()
    setGlobalDispatcher(mockAgent)

    // Set old last_update to force ingestion (25 hours ago)
    const twentyFiveHoursAgo = Date.now() - 25 * 60 * 60 * 1000
    db.setLastUpdate(new Date(twentyFiveHoursAgo).toISOString(), twentyFiveHoursAgo)
  })

  after(async () => {
    db.closeDb()
    try { fs.unlinkSync(dbPath) } catch {}
    await mockAgent.close()
  })

  it('should handle GCS fetch errors gracefully', async () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    const mockPool = mockAgent.get('https://storage.googleapis.com')
    
    const bucketListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents><Key>nodejs.org-access.log.20240401.json</Key></Contents>
</ListBucketResult>`

    mockPool.intercept({
      path: (path) => path.includes('max-keys=1000'),
      method: 'GET'
    }).reply(200, bucketListing, { 'content-type': 'application/xml' })

    // Mock a failed fetch
    mockPool.intercept({
      path: (path) => path.includes('20240401.json'),
      method: 'GET'
    }).reply(500, 'Internal Server Error')

    await assert.doesNotReject(async () => {
      await ingester.ingest()
    })
  })
})

describe('DataIngester - version aggregation', () => {
  let dbPath
  let db
  let logger
  let mockAgent

  before(async () => {
    dbPath = join(os.tmpdir(), `test-ingest-aggregation-${Date.now()}.db`)
    db = new Database(dbPath)
    db.initSchema()
    logger = createMockLogger()

    mockAgent = new MockAgent()
    mockAgent.disableNetConnect()
    setGlobalDispatcher(mockAgent)

    // Set old last_update to force ingestion (25 hours ago)
    const twentyFiveHoursAgo = Date.now() - 25 * 60 * 60 * 1000
    db.setLastUpdate(new Date(twentyFiveHoursAgo).toISOString(), twentyFiveHoursAgo)
  })

  after(async () => {
    db.closeDb()
    try { fs.unlinkSync(dbPath) } catch {}
    await mockAgent.close()
  })

  it('should aggregate multiple patch versions by major version', async () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    const mockPool = mockAgent.get('https://storage.googleapis.com')

    const bucketListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents><Key>nodejs.org-access.log.20240601.json</Key></Contents>
</ListBucketResult>`

    // Multiple patch versions for same major - should be aggregated
    const dayData = JSON.stringify({
      version: {
        'v14.15.5': 100000,
        'v14.15.4': 50000,
        'v14.14.0': 30000,
        'v12.20.1': 20000,
        'v12.20.0': 10000
      },
      os: { linux: 210000 }
    })

    mockPool.intercept({
      path: (path) => path.includes('max-keys=1000'),
      method: 'GET'
    }).reply(200, bucketListing, { 'content-type': 'application/xml' })

    mockPool.intercept({
      path: (path) => path.includes('20240601.json'),
      method: 'GET'
    }).reply(200, dayData, { 'content-type': 'application/json' })

    await ingester.ingest()

    const versions = db.getDailyVersionDownloads()

    // Should only have ONE row per major version, not one per patch
    const v14Data = versions.filter(v => v.major_version === 14 && v.date === '2024-06-01')
    const v12Data = versions.filter(v => v.major_version === 12 && v.date === '2024-06-01')

    assert.strictEqual(v14Data.length, 1, 'should have exactly one v14 row (aggregated)')
    assert.strictEqual(v12Data.length, 1, 'should have exactly one v12 row (aggregated)')

    // Downloads should be SUM of all patch versions
    assert.strictEqual(v14Data[0].downloads, 180000, 'v14 should sum to 180000 (100000+50000+30000)')
    assert.strictEqual(v12Data[0].downloads, 30000, 'v12 should sum to 30000 (20000+10000)')
  })
})

describe('DataIngester - version filtering', () => {
  let dbPath
  let db
  let logger
  let mockAgent

  before(async () => {
    dbPath = join(os.tmpdir(), `test-ingest-version-${Date.now()}.db`)
    db = new Database(dbPath)
    db.initSchema()
    logger = createMockLogger()

    mockAgent = new MockAgent()
    mockAgent.disableNetConnect()
    setGlobalDispatcher(mockAgent)

    // Set old last_update to force ingestion (25 hours ago)
    const twentyFiveHoursAgo = Date.now() - 25 * 60 * 60 * 1000
    db.setLastUpdate(new Date(twentyFiveHoursAgo).toISOString(), twentyFiveHoursAgo)
  })

  after(async () => {
    db.closeDb()
    try { fs.unlinkSync(dbPath) } catch {}
    await mockAgent.close()
  })

  it('should skip versions below major 4', async () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    const mockPool = mockAgent.get('https://storage.googleapis.com')
    
    const bucketListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents><Key>nodejs.org-access.log.20240501.json</Key></Contents>
</ListBucketResult>`

    const dayData = JSON.stringify({
      version: { 'v3.0.0': 100, 'v4.0.0': 200, 'v20.0.0': 1000 },
      os: { linux: 1300 }
    })

    // Match any GCS list request
    mockPool.intercept({
      path: (path) => path.includes('max-keys=1000'),
      method: 'GET'
    }).reply(200, bucketListing, { 'content-type': 'application/xml' })

    mockPool.intercept({
      path: (path) => path.includes('20240501.json'),
      method: 'GET'
    }).reply(200, dayData, { 'content-type': 'application/json' })

    await ingester.ingest()

    const versions = db.getDailyVersionDownloads()
    const dates = db.getExistingDates()
    
    const v3Data = versions.filter(v => v.major_version === 3 && v.date === '2024-05-01')
    const v4Data = versions.filter(v => v.major_version === 4 && v.date === '2024-05-01')
    const v20Data = versions.filter(v => v.major_version === 20 && v.date === '2024-05-01')

    assert.strictEqual(v3Data.length, 0, 'should not include version 3')
    assert.strictEqual(v4Data.length, 1, 'should include version 4')
    assert.strictEqual(v20Data.length, 1, 'should include version 20')
  })
})

describe('DataIngester - concurrency', () => {
  let dbPath
  let db
  let logger
  let mockAgent

  before(async () => {
    dbPath = join(os.tmpdir(), `test-ingest-concurrency-${Date.now()}.db`)
    db = new Database(dbPath)
    db.initSchema()
    logger = createMockLogger()
    mockAgent = new MockAgent()
    setGlobalDispatcher(mockAgent)
  })

  after(async () => {
    db.closeDb()
    try { fs.unlinkSync(dbPath) } catch {}
    await mockAgent.close()
  })

  it('should prevent concurrent ingestion', async () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    
    ingester.isIngesting = true

    let called = false
    await ingester.ingestWithProgress(() => { called = true })
    
    assert.strictEqual(called, false, 'should not ingest when already in progress')
  })
})
