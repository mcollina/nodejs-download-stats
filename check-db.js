#!/usr/bin/env node
'use strict'

const { DatabaseSync } = require('node:sqlite')
const path = '/tmp/nodejs-download-stats.db'

try {
  const db = new DatabaseSync(path)
  
  // Check last update
  const metaStmt = db.prepare('SELECT value, updated_at FROM metadata WHERE key = ?')
  const meta = metaStmt.get('last_update')
  console.log('Last update metadata:', meta)
  
  if (meta) {
    const date = new Date(parseInt(meta.updated_at))
    console.log('Last update was at:', date.toISOString())
    console.log('Last update value:', meta.value)
  }
  
  // Check most recent data date
  const maxDateStmt = db.prepare('SELECT MAX(date) as max_date FROM version_downloads')
  const maxDate = maxDateStmt.get()
  console.log('Most recent data date:', maxDate)
  
  // Check count of records
  const countStmt = db.prepare('SELECT COUNT(*) as count FROM version_downloads')
  const count = countStmt.get()
  console.log('Total version download records:', count)
  
  // Check distinct dates count
  const dateCountStmt = db.prepare('SELECT COUNT(DISTINCT date) as date_count FROM version_downloads')
  const dateCount = dateCountStmt.get()
  console.log('Distinct dates:', dateCount)
  
  // List first few and last few dates
  const datesStmt = db.prepare('SELECT DISTINCT date FROM version_downloads ORDER BY date')
  const allDates = datesStmt.all()
  console.log('First 5 dates:', allDates.slice(0, 5).map(d => d.date))
  console.log('Last 5 dates:', allDates.slice(-5).map(d => d.date))
  
  db.close()
} catch (err) {
  console.error('Error:', err.message)
  process.exit(1)
}
