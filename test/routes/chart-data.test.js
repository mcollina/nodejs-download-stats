'use strict'

const assert = require('node:assert')
const test = require('node:test')
const fastify = require('fastify')
const path = require('node:path')

// Import the chart-data route
const chartDataRoute = require('../../routes/chart-data.js')

test('chart-data returns 503 when no data available', async () => {
  const app = fastify()

  // Mock database with no data
  const mockDb = {
    getMonthlyVersionDownloads: () => [],
    getMonthlyOsDownloads: () => []
  }
  app.decorate('db', mockDb)

  await app.register(chartDataRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/chart-data'
  })

  assert.strictEqual(response.statusCode, 503)
  const json = JSON.parse(response.payload)
  assert.ok(json.error.includes('still loading'))

  await app.close()
})

test('chart-data returns labels from all months', async () => {
  const app = fastify()

  const mockDb = {
    getMonthlyVersionDownloads: () => [
      { major_version: 18, month: '2024-01', total_downloads: 100 },
      { major_version: 20, month: '2024-02', total_downloads: 200 }
    ],
    getMonthlyOsDownloads: () => [
      { os: 'linux', month: '2024-03', total_downloads: 300 }
    ]
  }
  app.decorate('db', mockDb)

  await app.register(chartDataRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/chart-data'
  })

  assert.strictEqual(response.statusCode, 200)
  const json = JSON.parse(response.payload)

  // Labels should include all unique months from both datasets
  assert.deepStrictEqual(json.labels, ['2024-01', '2024-02', '2024-03'])

  await app.close()
})

test('chart-data version chart has datasets for each version plus All', async () => {
  const app = fastify()

  const mockDb = {
    getMonthlyVersionDownloads: () => [
      { major_version: 18, month: '2024-01', total_downloads: 100 },
      { major_version: 18, month: '2024-02', total_downloads: 150 },
      { major_version: 20, month: '2024-01', total_downloads: 200 },
      { major_version: 20, month: '2024-02', total_downloads: 250 }
    ],
    getMonthlyOsDownloads: () => []
  }
  app.decorate('db', mockDb)

  await app.register(chartDataRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/chart-data'
  })

  assert.strictEqual(response.statusCode, 200)
  const json = JSON.parse(response.payload)

  // Should have datasets for version 18, 20, and All
  assert.strictEqual(json.versionChart.datasets.length, 3)

  const labels = json.versionChart.datasets.map(d => d.label)
  assert.ok(labels.includes('18'))
  assert.ok(labels.includes('20'))
  assert.ok(labels.includes('All'))

  await app.close()
})

test('chart-data version datasets have correct data points', async () => {
  const app = fastify()

  const mockDb = {
    getMonthlyVersionDownloads: () => [
      { major_version: 18, month: '2024-01', total_downloads: 100 },
      { major_version: 18, month: '2024-02', total_downloads: 150 },
      { major_version: 20, month: '2024-01', total_downloads: 200 },
      { major_version: 20, month: '2024-02', total_downloads: 250 }
    ],
    getMonthlyOsDownloads: () => []
  }
  app.decorate('db', mockDb)

  await app.register(chartDataRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/chart-data'
  })

  const json = JSON.parse(response.payload)

  // Find version 18 dataset
  const v18 = json.versionChart.datasets.find(d => d.label === '18')
  // Data should be [100, 150] for Jan, Feb
  assert.deepStrictEqual(v18.data, [100, 150])

  // Find version 20 dataset
  const v20 = json.versionChart.datasets.find(d => d.label === '20')
  // Data should be [200, 250] for Jan, Feb
  assert.deepStrictEqual(v20.data, [200, 250])

  // Find All dataset
  const all = json.versionChart.datasets.find(d => d.label === 'All')
  // Data should be [300, 400] (sum of both versions)
  assert.deepStrictEqual(all.data, [300, 400])

  await app.close()
})

test('chart-data All total equals sum of all version datasets', async () => {
  const app = fastify()

  const mockDb = {
    getMonthlyVersionDownloads: () => [
      { major_version: 18, month: '2024-01', total_downloads: 100 },
      { major_version: 18, month: '2024-02', total_downloads: 200 },
      { major_version: 18, month: '2024-03', total_downloads: 300 },
      { major_version: 20, month: '2024-01', total_downloads: 400 },
      { major_version: 20, month: '2024-02', total_downloads: 500 },
      { major_version: 20, month: '2024-03', total_downloads: 600 }
    ],
    getMonthlyOsDownloads: () => []
  }
  app.decorate('db', mockDb)

  await app.register(chartDataRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/chart-data'
  })

  const json = JSON.parse(response.payload)

  const v18 = json.versionChart.datasets.find(d => d.label === '18').data
  const v20 = json.versionChart.datasets.find(d => d.label === '20').data
  const all = json.versionChart.datasets.find(d => d.label === 'All').data

  // Verify All = v18 + v20 for each month
  for (let i = 0; i < all.length; i++) {
    assert.strictEqual(all[i], v18[i] + v20[i], `Month ${i}: All should equal v18 + v20`)
  }

  await app.close()
})

test('chart-data os chart has datasets for each os', async () => {
  const app = fastify()

  const mockDb = {
    getMonthlyVersionDownloads: () => [
      { major_version: 18, month: '2024-01', total_downloads: 100 }
    ],
    getMonthlyOsDownloads: () => [
      { os: 'linux', month: '2024-01', total_downloads: 50 },
      { os: 'win', month: '2024-01', total_downloads: 30 },
      { os: 'osx', month: '2024-01', total_downloads: 20 }
    ]
  }
  app.decorate('db', mockDb)

  await app.register(chartDataRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/chart-data'
  })

  const json = JSON.parse(response.payload)

  // Should have datasets for each OS
  assert.strictEqual(json.osChart.datasets.length, 3)

  const labels = json.osChart.datasets.map(d => d.label)
  assert.ok(labels.includes('linux'))
  assert.ok(labels.includes('win'))
  assert.ok(labels.includes('osx'))

  await app.close()
})

test('chart-data datasets have correct Chart.js properties', async () => {
  const app = fastify()

  const mockDb = {
    getMonthlyVersionDownloads: () => [
      { major_version: 18, month: '2024-01', total_downloads: 100 }
    ],
    getMonthlyOsDownloads: () => []
  }
  app.decorate('db', mockDb)

  await app.register(chartDataRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/chart-data'
  })

  const json = JSON.parse(response.payload)

  // Check version datasets
  for (const dataset of json.versionChart.datasets) {
    assert.ok(Array.isArray(dataset.data), 'Dataset should have data array')
    assert.ok(typeof dataset.label === 'string', 'Dataset should have label')
    assert.strictEqual(dataset.fill, dataset.label === 'All' ? false : true, 'Fill property should be set')
    assert.strictEqual(dataset.showLine, true, 'showLine should be true')
  }

  await app.close()
})

test('chart-data returns CSV data', async () => {
  const app = fastify()

  const mockDb = {
    getMonthlyVersionDownloads: () => [
      { major_version: 18, month: '2024-01', total_downloads: 100 },
      { major_version: 20, month: '2024-01', total_downloads: 200 }
    ],
    getMonthlyOsDownloads: () => [
      { os: 'linux', month: '2024-01', total_downloads: 300 }
    ]
  }
  app.decorate('db', mockDb)

  await app.register(chartDataRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/chart-data'
  })

  const json = JSON.parse(response.payload)

  // Should have CSV string
  assert.ok(typeof json.csv === 'string', 'CSV should be a string')
  assert.ok(json.csv.includes('Month,Version,Operating System,Downloads'), 'CSV should have header')
  assert.ok(json.csv.includes('2024-01,18,,100'), 'CSV should have version data')
  assert.ok(json.csv.includes('2024-01,,linux,300'), 'CSV should have OS data')

  await app.close()
})

test('chart-data returns cache headers', async () => {
  const app = fastify()

  const mockDb = {
    getMonthlyVersionDownloads: () => [
      { major_version: 18, month: '2024-01', total_downloads: 100 }
    ],
    getMonthlyOsDownloads: () => []
  }
  app.decorate('db', mockDb)

  await app.register(chartDataRoute, {})

  const response = await app.inject({
    method: 'GET',
    url: '/chart-data'
  })

  assert.strictEqual(response.statusCode, 200)
  assert.ok(response.headers['cache-control'], 'Should have Cache-Control header')
  assert.ok(response.headers['cache-control'].includes('max-age=3600'), 'Should cache for 1 hour')

  await app.close()
})
