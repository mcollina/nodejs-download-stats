#!/usr/bin/env node
'use strict'

const undici = require('undici')
const { XMLParser } = require('fast-xml-parser')

const BASE_URL = 'https://storage.googleapis.com/access-logs-summaries-nodejs/'

async function testGCS() {
  const agent = new undici.Agent({ connections: 10 })
  
  try {
    // Try to list files from after the last known date
    const marker = 'nodejs.org-access.log.20241011.json'
    const url = BASE_URL + '?max-keys=1000&marker=' + encodeURIComponent(marker)
    
    console.log('Fetching:', url)
    const response = await undici.request(url, { dispatcher: agent })
    const text = await response.body.text()
    
    console.log('Response status:', response.statusCode)
    
    const parser = new XMLParser({ isArray: (tagName) => tagName === 'Contents' })
    const obj = parser.parse(text)
    
    const contents = (obj.ListBucketResult && obj.ListBucketResult.Contents) || []
    console.log('Number of files found after 2024-10-11:', contents.length)
    
    if (contents.length === 0) {
      console.log('No new files found upstream - data might not be available yet for those dates')
    } else {
      console.log('First few files:')
      contents.slice(0, 5).forEach(c => console.log('  -', c.Key))
      console.log('Last few files:')
      contents.slice(-5).forEach(c => console.log('  -', c.Key))
    }
    
    // Also try fetching one of the recent files to see if it works
    const testUrl = BASE_URL + 'nodejs.org-access.log.20241012.json'
    console.log('\nTrying to fetch a specific file:', testUrl)
    try {
      const testResponse = await undici.request(testUrl, { dispatcher: agent })
      console.log('File 2024-10-12 exists? Status:', testResponse.statusCode)
      if (testResponse.statusCode === 200) {
        const data = await testResponse.body.json()
        console.log('File has version keys:', Object.keys(data.version || {}).slice(0, 5))
      }
    } catch (err) {
      console.log('Error fetching 2024-10-12:', err.message)
    }
    
    // Check what's available from the beginning
    console.log('\n--- Checking total available files ---')
    const listUrl = BASE_URL + '?max-keys=1000'
    const listResponse = await undici.request(listUrl, { dispatcher: agent })
    const listText = await listResponse.body.text()
    const listObj = parser.parse(listText)
    const allContents = (listObj.ListBucketResult && listObj.ListBucketResult.Contents) || []
    console.log('Total files in bucket:', allContents.length)
    
    // Find date range
    const dates = allContents
      .map(c => {
        const match = c.Key.match(/nodejs\.org-access\.log\.(\d{4})(\d{2})(\d{2})\.json/)
        return match ? `${match[1]}-${match[2]}-${match[3]}` : null
      })
      .filter(d => d)
      .sort()
    
    console.log('First date available:', dates[0])
    console.log('Last date available:', dates[dates.length - 1])
    
    agent.close()
  } catch (err) {
    console.error('Error:', err)
    agent.close()
    process.exit(1)
  }
}

testGCS().catch(console.error)
