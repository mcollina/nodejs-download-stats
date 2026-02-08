'use strict'

const { DatabaseSync } = require('node:sqlite')

class Database {
  constructor (dbPath) {
    this.dbPath = dbPath
    this.db = null
  }

  getDb () {
    if (!this.db) {
      this.db = new DatabaseSync(this.dbPath)
      this.db.exec('PRAGMA journal_mode = WAL')
    }
    return this.db
  }

  initSchema () {
    const db = this.getDb()

    // Create tables for version downloads
    db.exec(`
      CREATE TABLE IF NOT EXISTS version_downloads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        major_version INTEGER NOT NULL,
        downloads INTEGER NOT NULL,
        UNIQUE(date, major_version)
      )
    `)

    // Create tables for OS downloads
    db.exec(`
      CREATE TABLE IF NOT EXISTS os_downloads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        os TEXT NOT NULL,
        downloads INTEGER NOT NULL,
        UNIQUE(date, os)
      )
    `)

    // Create index for faster queries
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_version_date ON version_downloads(date);
      CREATE INDEX IF NOT EXISTS idx_version_major ON version_downloads(major_version);
      CREATE INDEX IF NOT EXISTS idx_os_date ON os_downloads(date);
      CREATE INDEX IF NOT EXISTS idx_os_name ON os_downloads(os);
    `)

    // Create metadata table for tracking last update
    db.exec(`
      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `)
  }

  closeDb () {
    if (this.db) {
      this.db.close()
      this.db = null
    }
  }

  getLastUpdate () {
    const db = this.getDb()
    try {
      const stmt = db.prepare('SELECT value, updated_at FROM metadata WHERE key = ?')
      const row = stmt.get('last_update')
      return row ? { value: row.value, updatedAt: row.updated_at } : null
    } catch (err) {
      return null
    }
  }

  setLastUpdate (value, timestamp) {
    const db = this.getDb()
    const stmt = db.prepare(`
      INSERT INTO metadata (key, value, updated_at) VALUES (?, ?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    `)
    stmt.run('last_update', value, timestamp || Date.now())
  }

  insertVersionDownload (date, majorVersion, downloads) {
    const db = this.getDb()
    const stmt = db.prepare(`
      INSERT INTO version_downloads (date, major_version, downloads) VALUES (?, ?, ?)
      ON CONFLICT(date, major_version) DO UPDATE SET downloads = excluded.downloads
    `)
    stmt.run(date, majorVersion, downloads)
  }

  insertOsDownload (date, os, downloads) {
    const db = this.getDb()
    const stmt = db.prepare(`
      INSERT INTO os_downloads (date, os, downloads) VALUES (?, ?, ?)
      ON CONFLICT(date, os) DO UPDATE SET downloads = excluded.downloads
    `)
    stmt.run(date, os, downloads)
  }

  clearData () {
    const db = this.getDb()
    db.exec('DELETE FROM version_downloads')
    db.exec('DELETE FROM os_downloads')
  }

  getVersionDownloads () {
    const db = this.getDb()
    const stmt = db.prepare(`
      SELECT major_version, date, downloads FROM version_downloads ORDER BY date, major_version
    `)
    return stmt.all()
  }

  getOsDownloads () {
    const db = this.getDb()
    const stmt = db.prepare(`
      SELECT os, date, downloads FROM os_downloads ORDER BY date, os
    `)
    return stmt.all()
  }

  getMonthlyVersionDownloads () {
    const db = this.getDb()
    const stmt = db.prepare(`
      SELECT 
        major_version,
        substr(date, 1, 7) as month,
        SUM(downloads) as total_downloads
      FROM version_downloads
      GROUP BY major_version, month
      ORDER BY month, major_version
    `)
    return stmt.all()
  }

  getMonthlyOsDownloads () {
    const db = this.getDb()
    const stmt = db.prepare(`
      SELECT 
        os,
        substr(date, 1, 7) as month,
        SUM(downloads) as total_downloads
      FROM os_downloads
      GROUP BY os, month
      ORDER BY month, os
    `)
    return stmt.all()
  }

  getDailyVersionDownloads () {
    const db = this.getDb()
    const stmt = db.prepare(`
      SELECT major_version, date, downloads FROM version_downloads ORDER BY date, major_version
    `)
    return stmt.all()
  }

  getDailyOsDownloads () {
    const db = this.getDb()
    const stmt = db.prepare(`
      SELECT os, date, downloads FROM os_downloads ORDER BY date, os
    `)
    return stmt.all()
  }

  getExistingDates () {
    const db = this.getDb()
    const versionStmt = db.prepare('SELECT DISTINCT date FROM version_downloads')
    const osStmt = db.prepare('SELECT DISTINCT date FROM os_downloads')

    const versionDates = new Set(versionStmt.all().map(row => row.date))
    const osDates = new Set(osStmt.all().map(row => row.date))

    // Return dates that exist in both tables (complete data)
    const existingDates = []
    for (const date of versionDates) {
      if (osDates.has(date)) {
        existingDates.push(date)
      }
    }
    return existingDates.sort()
  }

  getMostRecentDate () {
    const db = this.getDb()
    const stmt = db.prepare(`
      SELECT MAX(date) as maxDate FROM (
        SELECT date FROM version_downloads
        UNION
        SELECT date FROM os_downloads
      )
    `)
    const row = stmt.get()
    return row && row.maxDate ? row.maxDate : null
  }
}

module.exports = { Database }
