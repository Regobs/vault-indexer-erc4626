/**
 * ClickHouse database utilities for the ERC4626 transaction indexer
 * Handles database connections, queries, and data insertion
 */

import { logger } from '../utils/logger.js'
import { executeClickHouse, queryClickHouse } from '../utils/clickhouse.js'
import type { IndexerConfig, DepositRecord, WithdrawalRecord, TransferRecord, CoverageRecord } from '../utils/types.js'

/**
 * Sleep utility function for delays
 * @param ms Milliseconds to sleep
 */
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

/**
 * Database client class for ClickHouse operations
 */
export class DatabaseClient {
  private config: IndexerConfig
  private log = logger.child('database')

  constructor(config: IndexerConfig) {
    this.config = config
  }

  /**
   * Executes a SQL SELECT query against ClickHouse with retry logic
   * @param sql The SQL query to execute
   * @returns Query results as array
   */
  async query(sql: string): Promise<any[]> {
    const startTime = Date.now()
    
    try {
      this.log.verbose('Executing SELECT query', { 
        sqlPreview: sql.substring(0, 100).replace(/\s+/g, ' ') + '...'
      })
      
      const result = await queryClickHouse(this.config, sql)
      
      const duration = Date.now() - startTime
      this.log.db('SELECT', undefined, duration, { rows: result.length })
      
      return result
      
    } catch (error) {
      this.log.error('Database query failed', { 
        sql: sql.substring(0, 200),
        error: error instanceof Error ? error.message : String(error)
      })
      throw error
    }
  }

  /**
   * Formats a hex string to the expected length for ClickHouse unhex()
   * @param hex Hex string without 0x prefix
   * @param expectedLength Expected length in characters
   * @returns Properly formatted hex string
   */
  private formatHex(hex: string, expectedLength: number): string {
    // Remove 0x prefix if present
    const cleanHex = hex.startsWith('0x') ? hex.slice(2) : hex
    
    // Pad with zeros if needed and convert to lowercase
    const paddedHex = cleanHex.padStart(expectedLength, '0').toLowerCase()
    
    if (paddedHex.length !== expectedLength) {
      this.log.warn('Hex string length mismatch', { 
        original: hex, 
        expected: expectedLength, 
        actual: paddedHex.length 
      })
    }
    
    return paddedHex
  }

  /**
   * Executes a SQL INSERT statement to ClickHouse with retry logic
   * @param sql The SQL statement to execute
   * @param attempt Current attempt number (for retry logic)
   * @throws Error if all retry attempts fail
   */
  async insert(sql: string, attempt = 1): Promise<void> {
    const startTime = Date.now()
    
    try {
      this.log.verbose(`Executing SQL query (attempt ${attempt})`, { 
        sqlPreview: sql.substring(0, 100).replace(/\s+/g, ' ') + '...'
      })
      
      await executeClickHouse(this.config, sql)
      
      const duration = Date.now() - startTime
      this.log.db('INSERT', undefined, duration, { attempt })
      
    } catch (error) {
      this.log.warn(`Database insert failed (attempt ${attempt}/3)`, { 
        error: error instanceof Error ? error.message : String(error)
      })
      
      if (attempt < 3) {
        const backoffMs = 300 * attempt
        this.log.verbose(`Retrying in ${backoffMs}ms`)
        await sleep(backoffMs)
        return this.insert(sql, attempt + 1)
      }
      
      this.log.error('Database insert failed after all retries', error)
      throw error
    }
  }


  /**
   * Bulk inserts multiple deposit records into the database with duplicate checking
   * @param records Array of deposit records to insert
   */
  async insertDeposits(records: DepositRecord[]): Promise<void> {
    if (records.length === 0) return
    
    // Check for existing records to avoid duplicates
    const duplicateCheckSql = `
      SELECT hex(vault), hex(tx_hash), hex(sender), hex(owner)
      FROM erc4626_deposits 
      WHERE (vault, tx_hash, sender, owner) IN (
        ${records.map(record => 
          `(unhex('${this.formatHex(record.vault, 40)}'), ` +
          `unhex('${this.formatHex(record.txHash, 64)}'), ` +
          `unhex('${this.formatHex(record.sender, 40)}'), ` +
          `unhex('${this.formatHex(record.owner, 40)}'))`
        ).join(',\n        ')}
      )
    `
    
    const existingRecords = await this.query(duplicateCheckSql)
    const existingKeys = new Set(
      existingRecords.map(row => `${row[0]}:${row[1]}:${row[2]}:${row[3]}`)
    )
    
    // Filter out duplicates
    const newRecords = records.filter(record => {
      const key = `${this.formatHex(record.vault, 40).toUpperCase()}:` +
                  `${this.formatHex(record.txHash, 64).toUpperCase()}:` +
                  `${this.formatHex(record.sender, 40).toUpperCase()}:` +
                  `${this.formatHex(record.owner, 40).toUpperCase()}`
      return !existingKeys.has(key)
    })
    
    if (newRecords.length === 0) {
      this.log.verbose(`All ${records.length} deposits already exist, skipping insert`)
      return
    }
    
    if (newRecords.length < records.length) {
      this.log.verbose(`Filtered out ${records.length - newRecords.length} duplicate deposits, inserting ${newRecords.length} new records`)
    }
    
    const values = newRecords.map(record => 
      `(unhex('${this.formatHex(record.vault, 40)}'),` +
      ` '${record.contractName.replace(/'/g, "\\'")}',` +
      ` unhex('${this.formatHex(record.sender, 40)}'),` +
      ` unhex('${this.formatHex(record.owner, 40)}'),` +
      ` ${record.assets},` +
      ` ${record.shares},` +
      ` ${record.blockNumber},` +
      ` unhex('${this.formatHex(record.txHash, 64)}'),` +
      ` toDateTime(${record.timestamp}))`
    ).join(',\n        ')
    
    const sql = `
      INSERT INTO erc4626_deposits
        (vault, contract_name, sender, owner, assets, shares, block_number, tx_hash, ts)
      VALUES
        ${values}
    `
    await this.insert(sql)
  }


  /**
   * Bulk inserts multiple withdrawal records into the database with duplicate checking
   * @param records Array of withdrawal records to insert
   */
  async insertWithdrawals(records: WithdrawalRecord[]): Promise<void> {
    if (records.length === 0) return
    
    // Check for existing records to avoid duplicates
    const duplicateCheckSql = `
      SELECT hex(vault), hex(tx_hash), hex(sender), hex(receiver), hex(owner)
      FROM erc4626_withdrawals 
      WHERE (vault, tx_hash, sender, receiver, owner) IN (
        ${records.map(record => 
          `(unhex('${this.formatHex(record.vault, 40)}'), ` +
          `unhex('${this.formatHex(record.txHash, 64)}'), ` +
          `unhex('${this.formatHex(record.sender, 40)}'), ` +
          `unhex('${this.formatHex(record.receiver, 40)}'), ` +
          `unhex('${this.formatHex(record.owner, 40)}'))`
        ).join(',\n        ')}
      )
    `
    
    const existingRecords = await this.query(duplicateCheckSql)
    const existingKeys = new Set(
      existingRecords.map(row => `${row[0]}:${row[1]}:${row[2]}:${row[3]}:${row[4]}`)
    )
    
    // Filter out duplicates
    const newRecords = records.filter(record => {
      const key = `${this.formatHex(record.vault, 40).toUpperCase()}:` +
                  `${this.formatHex(record.txHash, 64).toUpperCase()}:` +
                  `${this.formatHex(record.sender, 40).toUpperCase()}:` +
                  `${this.formatHex(record.receiver, 40).toUpperCase()}:` +
                  `${this.formatHex(record.owner, 40).toUpperCase()}`
      return !existingKeys.has(key)
    })
    
    if (newRecords.length === 0) {
      this.log.verbose(`All ${records.length} withdrawals already exist, skipping insert`)
      return
    }
    
    if (newRecords.length < records.length) {
      this.log.verbose(`Filtered out ${records.length - newRecords.length} duplicate withdrawals, inserting ${newRecords.length} new records`)
    }
    
    const values = newRecords.map(record => 
      `(unhex('${this.formatHex(record.vault, 40)}'),` +
      ` '${record.contractName.replace(/'/g, "\\'")}',` +
      ` unhex('${this.formatHex(record.sender, 40)}'),` +
      ` unhex('${this.formatHex(record.receiver, 40)}'),` +
      ` unhex('${this.formatHex(record.owner, 40)}'),` +
      ` ${record.assets},` +
      ` ${record.shares},` +
      ` ${record.blockNumber},` +
      ` unhex('${this.formatHex(record.txHash, 64)}'),` +
      ` toDateTime(${record.timestamp}))`
    ).join(',\n        ')
    
    const sql = `
      INSERT INTO erc4626_withdrawals
        (vault, contract_name, sender, receiver, owner, assets, shares, block_number, tx_hash, ts)
      VALUES
        ${values}
    `
    await this.insert(sql)
  }


  /**
   * Bulk inserts multiple transfer records into the database with duplicate checking
   * @param records Array of transfer records to insert
   */
  async insertTransfers(records: TransferRecord[]): Promise<void> {
    if (records.length === 0) return
    
    // Check for existing records to avoid duplicates
    const duplicateCheckSql = `
      SELECT hex(token), hex(tx_hash), hex(from_addr), hex(to_addr)
      FROM share_transfers 
      WHERE (token, tx_hash, from_addr, to_addr) IN (
        ${records.map(record => 
          `(unhex('${this.formatHex(record.token, 40)}'), ` +
          `unhex('${this.formatHex(record.txHash, 64)}'), ` +
          `unhex('${this.formatHex(record.fromAddr, 40)}'), ` +
          `unhex('${this.formatHex(record.toAddr, 40)}'))`
        ).join(',\n        ')}
      )
    `
    
    const existingRecords = await this.query(duplicateCheckSql)
    const existingKeys = new Set(
      existingRecords.map(row => `${row[0]}:${row[1]}:${row[2]}:${row[3]}`)
    )
    
    // Filter out duplicates
    const newRecords = records.filter(record => {
      const key = `${this.formatHex(record.token, 40).toUpperCase()}:` +
                  `${this.formatHex(record.txHash, 64).toUpperCase()}:` +
                  `${this.formatHex(record.fromAddr, 40).toUpperCase()}:` +
                  `${this.formatHex(record.toAddr, 40).toUpperCase()}`
      return !existingKeys.has(key)
    })
    
    if (newRecords.length === 0) {
      this.log.verbose(`All ${records.length} transfers already exist, skipping insert`)
      return
    }
    
    if (newRecords.length < records.length) {
      this.log.verbose(`Filtered out ${records.length - newRecords.length} duplicate transfers, inserting ${newRecords.length} new records`)
    }
    
    const values = newRecords.map(record => 
      `(unhex('${this.formatHex(record.token, 40)}'),` +
      ` '${record.contractName.replace(/'/g, "\\'")}',` +
      ` unhex('${this.formatHex(record.fromAddr, 40)}'),` +
      ` unhex('${this.formatHex(record.toAddr, 40)}'),` +
      ` ${record.value},` +
      ` ${record.blockNumber},` +
      ` unhex('${this.formatHex(record.txHash, 64)}'),` +
      ` toDateTime(${record.timestamp}))`
    ).join(',\n        ')
    
    const sql = `
      INSERT INTO share_transfers
        (token, contract_name, from_addr, to_addr, value, block_number, tx_hash, ts)
      VALUES
        ${values}
    `
    await this.insert(sql)
  }

  /**
   * Records indexing coverage for a block range with detailed status
   * @param record Coverage record to insert
   */
  async insertCoverage(
    record: CoverageRecord, 
    status: 'completed' | 'failed' | 'partial' = 'completed',
    eventsProcessed: number = 0,
    eventsFailed: number = 0
  ): Promise<void> {
    const sql = `
      INSERT INTO indexer_coverage (address, start_block, end_block, status, events_processed, events_failed, updated_at)
      VALUES (unhex('${this.formatHex(record.address, 40)}'), ${record.startBlock}, ${record.endBlock}, '${status}', ${eventsProcessed}, ${eventsFailed}, now())
    `
    await this.insert(sql)
  }

  /**
   * Tests database connectivity
   * @returns Promise that resolves if connection is successful
   */
  async testConnection(): Promise<void> {
    this.log.verbose('Testing database connection...')
    try {
      await this.insert('SELECT 1')
      this.log.verbose('Database connected')
    } catch (error) {
      this.log.error('Database connection failed', error)
      throw error
    }
  }
}