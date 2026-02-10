'use strict'

const assert = require('node:assert')
const test = require('node:test')
const fastify = require('fastify')
const path = require('node:path')
const fs = require('node:fs')

// Import the root route
const rootRoute = require('../../routes/root.js')

test('root route serves index.html', async () => {
  const app = fastify()

  // Register config and database decorators
  app.decorate('config', {
    NODEJS_DOWNLOAD_STATS_DB: path.join(__dirname, '../fixtures/test-homepage.db')
  })

  // Create a minimal test database
  const { Database } = require('../../lib/db.js')
  const db = new Database(':memory:')
  app.decorate('db', db)

  await app.register(rootRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/'
  })

  assert.strictEqual(response.statusCode, 200)
  assert.ok(response.payload.includes('<title>Node.js Downloads</title>'))
  assert.ok(response.payload.includes('<canvas class="graph" id="downloadsChart">'))
  assert.ok(response.payload.includes('<script src="./count.js">'))

  await app.close()
})

test('root route serves static files (count.js)', async () => {
  const app = fastify()

  app.decorate('config', {
    NODEJS_DOWNLOAD_STATS_DB: path.join(__dirname, '../fixtures/test-homepage.db')
  })

  const { Database } = require('../../lib/db.js')
  const db = new Database(':memory:')
  app.decorate('db', db)

  await app.register(rootRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/count.js'
  })

  assert.strictEqual(response.statusCode, 200)
  // Verify JavaScript content is served
  assert.ok(response.payload.includes('function'))
  assert.ok(response.payload.length > 500)

  await app.close()
})

test('root route serves static files (mvp.css)', async () => {
  const app = fastify()

  app.decorate('config', {
    NODEJS_DOWNLOAD_STATS_DB: path.join(__dirname, '../fixtures/test-homepage.db')
  })

  const { Database } = require('../../lib/db.js')
  const db = new Database(':memory:')
  app.decorate('db', db)

  await app.register(rootRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/mvp.css'
  })

  assert.strictEqual(response.statusCode, 200)
  assert.ok(response.payload.length > 1000) // CSS file should be substantial

  await app.close()
})

test('count.js has valid JavaScript syntax', async () => {
  const countJsPath = path.join(__dirname, '../../public/count.js')
  const content = fs.readFileSync(countJsPath, 'utf-8')

  // Check for balanced parentheses in the key forEach
  assert.ok(!content.includes('el.classList.add(\'hidden\')\n    )'),
    'count.js should not have the syntax error with unclosed forEach')

  // Check that the fixed version exists
  assert.ok(content.includes('el.classList.add(\'hidden\'))'),
    'count.js should have properly closed forEach')

  // Verify valid JavaScript by trying to parse it
  try {
    new Function(content)
  } catch (err) {
    assert.fail(`count.js has JavaScript syntax error: ${err.message}`)
  }
})

test('count.js fetches chart-data from correct endpoint', async () => {
  const countJsPath = path.join(__dirname, '../../public/count.js')
  const content = fs.readFileSync(countJsPath, 'utf-8')

  assert.ok(content.includes("fetch('/chart-data')"),
    'count.js should fetch from /chart-data endpoint')
})
