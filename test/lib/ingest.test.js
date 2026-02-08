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
      new DataIngester(null, db)
    }, /Logger is required/)
  })

  it('should throw if db is not provided', () => {
    assert.throws(() => {
      new DataIngester(logger, null)
    }, /Database is required/)
  })

  it('should create DataIngester instance', () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    assert.ok(ingester, 'should create instance')
    assert.ok(ingester.ingest, 'should have ingest method')
    assert.ok(ingester.ingestWithProgress, 'should have ingestWithProgress method')
  })

  it('should list available files from GCS', async () => {
    // Pass mockAgent to DataIngester so intercepts work
    const ingester = new DataIngester(logger, db, mockAgent)
    
    const mockPool = mockAgent.get('https://storage.googleapis.com')
    
    const bucketListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://doc.s3.amazonaws.com/2006-03-01">
  <Contents><Key>nodejs.org-access.log.20240101.json</Key></Contents>
  <Contents><Key>nodejs.org-access.log.20240102.json</Key></Contents>
  <Contents><Key>nodejs.org-access.log.20240103.json</Key></Contents>
</ListBucketResult>`

    // Use path function for flexible matching
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
    const mockPool = mockAgent.get('https://storage.googleapis.com')
    
    // Add marker param to the expected URL when resuming from date
    mockPool.intercept({
      path: (path) => path.includes('marker=nodejs.org-access.log.20240102'),
      method: 'GET'
    }).reply(200, `<?xml version="1.0"?>
<ListBucketResult>
  <Contents><Key>nodejs.org-access.log.20240103.json</Key></Contents>
</ListBucketResult>`, { 'content-type': 'application/xml' })

    const ingester = new DataIngester(logger, db, mockAgent)
    const files = await ingester.listAvailableFiles('2024-01-02')

    // Should filter to only files after 2024-01-02
    assert.ok(files.every(f => f.date > '2024-01-02'), 'should only return dates after marker')
  })

  it('should ingest data with progress callback', async () => {
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

    const ingester = new DataIngester(logger, db, mockAgent)
    const progressCalls = []

    await ingester.ingestWithProgress((progress) => {
      progressCalls.push(progress)
    })

    assert.ok(progressCalls.length > 0, 'should call progress callback')
    
    // Verify data was inserted
    const dates = db.getExistingDates()
    assert.ok(dates.includes('2024-02-01'), 'should have ingested 2024-02-01')

    const versions = db.getDailyVersionDownloads()
    const v20Data = versions.filter(v => v.major_version === 20 && v.date === '2024-02-01')
    assert.strictEqual(v20Data.length, 1, 'should have v20 data')
    assert.strictEqual(v20Data[0].downloads, 5000, 'should have correct download count')
  })

  it('should skip existing dates during ingestion', async () => {
    // First, insert some existing data
    db.insertVersionDownload('2024-03-01', 20, 1000)
    db.insertOsDownload('2024-03-01', 'linux', 800)

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

    mockPool.intercept({
      path: (path) => path.includes('max-keys=1000') && !path.includes('marker'),
      method: 'GET'
    }).reply(200, bucketListing, { 'content-type': 'application/xml' })

    // Only the second day should be downloaded (first already exists)
    mockPool.intercept({
      path: (path) => path.includes('20240302.json'),
      method: 'GET'
    }).reply(200, dayData, { 'content-type': 'application/json' })

    const ingester = new DataIngester(logger, db, mockAgent)
    await ingester.ingest()

    const dates = db.getExistingDates()
    assert.ok(dates.includes('2024-03-01'), 'should still have original 2024-03-01')
    assert.ok(dates.includes('2024-03-02'), 'should have new 2024-03-02')
  })

  it('should handle GCS fetch errors gracefully', async () => {
    // Clean out last_update to force re-ingestion
    db.setLastUpdate(new Date(Date.now() - 25 * 60 * 60 * 1000).toISOString())

    const mockPool = mockAgent.get('https://storage.googleapis.com')
    
    const bucketListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents><Key>nodejs.org-access.log.20240401.json</Key></Contents>
</ListBucketResult>`

    mockPool.intercept({
      path: (path) => path.includes('max-keys=1000') && !path.includes('marker'),
      method: 'GET'
    }).reply(200, bucketListing, { 'content-type': 'application/xml' })

    // Mock a failed fetch for the JSON file
    mockPool.intercept({
      path: (path) => path.includes('20240401.json'),
      method: 'GET'
    }).reply(500, 'Internal Server Error')

    const ingester = new DataIngester(logger, db, mockAgent)
    
    // Should not throw even when individual file fails
    await assert.doesNotReject(async () => {
      await ingester.ingest()
    })
  })

  it('should skip versions below major 4', async () => {
    const mockPool = mockAgent.get('https://storage.googleapis.com')
    
    const bucketListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents><Key>nodejs.org-access.log.20240501.json</Key></Contents>
</ListBucketResult>`

    const dayData = JSON.stringify({
      version: { 'v3.0.0': 100, 'v4.0.0': 200, 'v20.0.0': 1000 },
      os: { linux: 1300 }
    })

    mockPool.intercept({
      path: (path) => path.includes('max-keys=1000') && !path.includes('marker'),
      method: 'GET'
    }).reply(200, bucketListing, { 'content-type': 'application/xml' })

    mockPool.intercept({
      path: (path) => path.includes('20240501.json'),
      method: 'GET'
    }).reply(200, dayData, { 'content-type': 'application/json' })

    // Clear last_update to force ingestion
    db.setLastUpdate(new Date(Date.now() - 25 * 60 * 60 * 1000).toISOString())

    const ingester = new DataIngester(logger, db, mockAgent)
    await ingester.ingest()

    const versions = db.getDailyVersionDownloads()
    const v3Data = versions.filter(v => v.major_version === 3)
    const v4Data = versions.filter(v => v.major_version === 4)
    const v20Data = versions.filter(v => v.major_version === 20)

    assert.strictEqual(v3Data.length, 0, 'should not include version 3')
    assert.strictEqual(v4Data.length, 1, 'should include version 4')
    assert.strictEqual(v20Data.length, 1, 'should include version 20')
  })

  it('should prevent concurrent ingestion', async () => {
    const ingester = new DataIngester(logger, db, mockAgent)
    
    // Manually set isIngesting to true
    ingester.isIngesting = true

    // Should skip if already ingesting
    let called = false
    await ingester.ingestWithProgress(() => { called = true })
    
    assert.strictEqual(called, false, 'should not ingest when already in progress')
  })
})
