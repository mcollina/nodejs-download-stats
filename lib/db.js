'use strict'

const { DatabaseSync } = require('node:sqlite')
const { join } = require('node:path')
const os = require('node:os')

const DB_PATH = process.env.NODEJS_DOWNLOAD_STATS_DB || join(os.tmpdir(), 'nodejs-download-stats.db')

let db = null

function getDb () {
  if (!db) {
    db = new DatabaseSync(DB_PATH)
    db.exec('PRAGMA journal_mode = WAL')
  }
  return db
}

function initSchema () {
  const db = getDb()

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

function closeDb () {
  if (db) {
    db.close()
    db = null
  }
}

function getLastUpdate () {
  const db = getDb()
  try {
    const stmt = db.prepare('SELECT value, updated_at FROM metadata WHERE key = ?')
    const row = stmt.get('last_update')
    return row ? { value: row.value, updatedAt: row.updated_at } : null
  } catch (err) {
    return null
  }
}

function setLastUpdate (value) {
  const db = getDb()
  const stmt = db.prepare(`
    INSERT INTO metadata (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
  `)
  stmt.run('last_update', value, Date.now())
}

function insertVersionDownload (date, majorVersion, downloads) {
  const db = getDb()
  const stmt = db.prepare(`
    INSERT INTO version_downloads (date, major_version, downloads) VALUES (?, ?, ?)
    ON CONFLICT(date, major_version) DO UPDATE SET downloads = excluded.downloads
  `)
  stmt.run(date, majorVersion, downloads)
}

function insertOsDownload (date, os, downloads) {
  const db = getDb()
  const stmt = db.prepare(`
    INSERT INTO os_downloads (date, os, downloads) VALUES (?, ?, ?)
    ON CONFLICT(date, os) DO UPDATE SET downloads = excluded.downloads
  `)
  stmt.run(date, os, downloads)
}

function clearData () {
  const db = getDb()
  db.exec('DELETE FROM version_downloads')
  db.exec('DELETE FROM os_downloads')
}

function getVersionDownloads () {
  const db = getDb()
  const stmt = db.prepare(`
    SELECT major_version, date, downloads FROM version_downloads ORDER BY date, major_version
  `)
  return stmt.all()
}

function getOsDownloads () {
  const db = getDb()
  const stmt = db.prepare(`
    SELECT os, date, downloads FROM os_downloads ORDER BY date, os
  `)
  return stmt.all()
}

function getMonthlyVersionDownloads () {
  const db = getDb()
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

function getMonthlyOsDownloads () {
  const db = getDb()
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

function getDailyVersionDownloads () {
  const db = getDb()
  const stmt = db.prepare(`
    SELECT major_version, date, downloads FROM version_downloads ORDER BY date, major_version
  `)
  return stmt.all()
}

function getDailyOsDownloads () {
  const db = getDb()
  const stmt = db.prepare(`
    SELECT os, date, downloads FROM os_downloads ORDER BY date, os
  `)
  return stmt.all()
}

module.exports = {
  initSchema,
  closeDb,
  getLastUpdate,
  setLastUpdate,
  insertVersionDownload,
  insertOsDownload,
  clearData,
  getVersionDownloads,
  getOsDownloads,
  getMonthlyVersionDownloads,
  getMonthlyOsDownloads,
  getDailyVersionDownloads,
  getDailyOsDownloads,
  DB_PATH
}
