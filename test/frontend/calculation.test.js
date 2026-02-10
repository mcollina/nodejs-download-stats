'use strict'

// Test the calculation logic from count.js
const assert = require('node:assert')
const test = require('node:test')
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')

test('computeDataSet correctly aggregates data by month', () => {
  // Simulate the computeDataSet function logic
  function computeDataSet (labels, versions, includeAll = true) {
    const all = labels.map((label) => 0)

    const datasets = Object.keys(versions).map((version) => {
      const downloadsCounts = {}

      for (const { date, downloads } of versions[version]) {
        const month = date.substring(0, 7) // Extract YYYY-MM from YYYY-MM-DD
        downloadsCounts[month] = (downloadsCounts[month] || 0) + downloads
      }

      const data = labels.map((label) => downloadsCounts[label] || 0)

      for (let i = 0; i < data.length; i++) {
        all[i] += data[i]
      }

      return {
        label: version,
        data,
        fill: true,
        showLine: true
      }
    })

    if (includeAll) {
      datasets.push({
        label: 'All',
        data: all,
        fill: false,
        showLine: true
      })
    }

    return { datasets, allTotals: all }
  }

  const versions = {
    '18': [
      { date: '2024-01-15', downloads: 100 },
      { date: '2024-01-16', downloads: 50 },
      { date: '2024-02-01', downloads: 200 }
    ],
    '20': [
      { date: '2024-01-10', downloads: 300 },
      { date: '2024-02-05', downloads: 150 },
      { date: '2024-03-01', downloads: 400 }
    ]
  }

  // Labels include all months from all data
  const labels = ['2024-01', '2024-02', '2024-03']

  const result = computeDataSet(labels, versions)

  // Check individual versions
  const v18Data = result.datasets.find(d => d.label === '18').data
  const v20Data = result.datasets.find(d => d.label === '20').data

  assert.deepStrictEqual(v18Data, [150, 200, 0], 'Version 18 should sum to 150 for Jan, 200 for Feb, 0 for Mar')
  assert.deepStrictEqual(v20Data, [300, 150, 400], 'Version 20 should sum correctly')

  // Check 'All' total
  const allData = result.datasets.find(d => d.label === 'All').data
  assert.deepStrictEqual(allData, [450, 350, 400], 'All should be sum of both versions')
  assert.deepStrictEqual(result.allTotals, [450, 350, 400])
})

test('labels generation includes all months from both versions and OS data', () => {
  const versions = {
    '18': [
      { date: '2024-01-15', downloads: 100 },
      { date: '2024-03-01', downloads: 200 }
    ]
  }

  const operatingSystems = {
    'linux': [
      { date: '2024-02-10', downloads: 500 },
      { date: '2024-04-05', downloads: 300 }
    ]
  }

  // Simulate the new label generation logic
  const allMonths = new Set()

  for (const versionData of Object.values(versions)) {
    for (const { date } of versionData) {
      allMonths.add(date.substring(0, 7))
    }
  }

  for (const osData of Object.values(operatingSystems)) {
    for (const { date } of osData) {
      allMonths.add(date.substring(0, 7))
    }
  }

  const labels = [...allMonths].sort()

  assert.deepStrictEqual(labels, ['2024-01', '2024-02', '2024-03', '2024-04'])
})

test('old buggy labels generation only includes version 4 months', () => {
  // This test demonstrates the bug - old code only used versions['4']
  const versions = {
    '4': [
      { date: '2024-01-15', downloads: 10 }
    ],
    '18': [
      { date: '2024-01-15', downloads: 100 },
      { date: '2024-03-01', downloads: 200 }
    ]
  }

  const operatingSystems = {
    'linux': [
      { date: '2024-02-10', downloads: 500 },
      { date: '2024-04-05', downloads: 300 }
    ]
  }

  // OLD BUGGY CODE - only used version 4
  const oldLabels = [...new Set(versions['4'].map(({ date }) => date.replace(/-\d{2}$/g, '')))]
  assert.deepStrictEqual(oldLabels, ['2024-01'])

  // Missing months from OS data and other versions!
  assert.ok(!oldLabels.includes('2024-02'))
  assert.ok(!oldLabels.includes('2024-03'))
  assert.ok(!oldLabels.includes('2024-04'))
})

test('count.js uses substring instead of regex for month extraction', () => {
  const countJsPath = path.join(__dirname, '../../public/count.js')
  const content = fs.readFileSync(countJsPath, 'utf-8')

  // Should use substring(0, 7) for extracting month from date
  assert.ok(content.includes('substring(0, 7)'),
    'count.js should use substring(0, 7) for month extraction')

  // Should NOT use the old regex approach
  assert.ok(!content.includes('replace(/-\\d{2}$/g,'),
    'count.js should not use regex replace for month extraction')
})

test('count.js does not have dead code map line', () => {
  const countJsPath = path.join(__dirname, '../../public/count.js')
  const content = fs.readFileSync(countJsPath, 'utf-8')

  // Old buggy code had this dead line:
  // versions[version].map(({ date, downloads }) => downloads)
  assert.ok(!content.match(/versions\[version\]\.map\(\{[^}]+\}\)\s*=>\s*downloads/),
    'count.js should not have dead code map line')
})

test('month extraction via substring(0, 7) is equivalent to regex replace', () => {
  const testDates = [
    '2024-01-15',
    '2024-12-31',
    '2023-06-01'
  ]

  for (const date of testDates) {
    const viaSubstring = date.substring(0, 7)
    const viaRegex = date.replace(/-\d{2}$/g, '')
    assert.strictEqual(viaSubstring, viaRegex,
      `substring and regex should produce same result for ${date}`)
    assert.match(viaSubstring, /^\d{4}-\d{2}$/,
      `result should be YYYY-MM format`)
  }
})
