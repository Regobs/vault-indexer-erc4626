/**
 * Database schema management for the ERC4626 transaction indexer
 * Handles table creation, migrations, and schema validation
 */

import { logger } from '../utils/logger.js'
import { executeClickHouse, queryClickHouse } from '../utils/clickhouse.js'
import type { IndexerConfig } from '../utils/types.js'

/**
 * Database schema manager for ClickHouse
 */
export class SchemaManager {
  private config: IndexerConfig
  private log = logger.child('schema')

  constructor(config: IndexerConfig) {
    this.config = config
  }

  /**
   * Executes a SQL statement against ClickHouse
   */
  private async executeSql(sql: string, description: string): Promise<void> {
    try {
      this.log.verbose(`Executing: ${description}`)
      await executeClickHouse(this.config, sql)
      this.log.verbose(`✓ ${description}`)
    } catch (error) {
      this.log.error(`Failed: ${description}`, error)
      throw error
    }
  }

  /**
   * Creates the database if it doesn't exist
   */
  async createDatabase(): Promise<void> {
    const sql = `CREATE DATABASE IF NOT EXISTS ${this.config.clickhouseDb}`
    await this.executeSql(sql, `Create database '${this.config.clickhouseDb}'`)
  }

  /**
   * Creates all required tables for the indexer
   */
  async createTables(): Promise<void> {
    await this.createDepositsTable()
    await this.createWithdrawalsTable()
    await this.createTransfersTable()
    await this.createCoverageTable()
    await this.createCheckpointsTable()
  }

  /**
   * Creates the ERC4626 deposits table
   */
  private async createDepositsTable(): Promise<void> {
    const sql = `
      CREATE TABLE IF NOT EXISTS erc4626_deposits (
        vault FixedString(20),
        contract_name LowCardinality(String),
        sender FixedString(20), 
        owner FixedString(20),
        assets UInt256,
        shares UInt256,
        block_number UInt64,
        tx_hash FixedString(32),
        ts DateTime,
        created_at DateTime DEFAULT now()
      ) ENGINE = MergeTree() 
      ORDER BY (vault, block_number)
      PARTITION BY toYYYYMM(ts)
    `
    await this.executeSql(sql, 'Create erc4626_deposits table')
  }

  /**
   * Creates the ERC4626 withdrawals table
   */
  private async createWithdrawalsTable(): Promise<void> {
    const sql = `
      CREATE TABLE IF NOT EXISTS erc4626_withdrawals (
        vault FixedString(20),
        contract_name LowCardinality(String),
        sender FixedString(20),
        receiver FixedString(20), 
        owner FixedString(20),
        assets UInt256,
        shares UInt256,
        block_number UInt64,
        tx_hash FixedString(32),
        ts DateTime,
        created_at DateTime DEFAULT now()
      ) ENGINE = MergeTree() 
      ORDER BY (vault, block_number)
      PARTITION BY toYYYYMM(ts)
    `
    await this.executeSql(sql, 'Create erc4626_withdrawals table')
  }

  /**
   * Creates the share transfers table
   */
  private async createTransfersTable(): Promise<void> {
    const sql = `
      CREATE TABLE IF NOT EXISTS share_transfers (
        token FixedString(20),
        contract_name LowCardinality(String),
        from_addr FixedString(20),
        to_addr FixedString(20),
        value UInt256,
        block_number UInt64, 
        tx_hash FixedString(32),
        ts DateTime,
        created_at DateTime DEFAULT now()
      ) ENGINE = MergeTree() 
      ORDER BY (token, block_number)
      PARTITION BY toYYYYMM(ts)
    `
    await this.executeSql(sql, 'Create share_transfers table')
  }

  /**
   * Creates the indexing coverage table
   */
  private async createCoverageTable(): Promise<void> {
    const sql = `
      CREATE TABLE IF NOT EXISTS indexer_coverage (
        address FixedString(20),
        start_block UInt64,
        end_block UInt64,
        status Enum8('completed' = 1, 'failed' = 2, 'partial' = 3),
        events_processed UInt32 DEFAULT 0,
        events_failed UInt32 DEFAULT 0,
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now()
      ) ENGINE = MergeTree() 
      ORDER BY (address, start_block)
    `
    await this.executeSql(sql, 'Create indexer_coverage table')
  }

  /**
   * Creates the checkpoints table for resume functionality
   */
  private async createCheckpointsTable(): Promise<void> {
    const sql = `
      CREATE TABLE IF NOT EXISTS indexer_checkpoints (
        address FixedString(20),
        checkpoint_type Enum8('backfill' = 1, 'realtime' = 2),
        last_processed_block UInt64,
        last_processed_tx_hash FixedString(32),
        processing_state String DEFAULT '',
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now()
      ) ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY (address, checkpoint_type)
    `
    await this.executeSql(sql, 'Create indexer_checkpoints table')
  }

  /**
   * Validates that all required tables exist
   */
  async validateSchema(): Promise<boolean> {
    const requiredTables = [
      'erc4626_deposits',
      'erc4626_withdrawals', 
      'share_transfers',
      'indexer_coverage',
      'indexer_checkpoints'
    ]

    try {
      for (const table of requiredTables) {
        await this.executeSql(`EXISTS TABLE ${table}`, `Check table ${table}`)
      }
      this.log.info('✓ All required tables exist')
      return true
    } catch (error) {
      this.log.error('Schema validation failed', error)
      return false
    }
  }

  /**
   * Migrates existing tables to add missing columns
   */
  async migrateSchema(): Promise<void> {
    this.log.info('Migrating database schema...')
    
    // Check if indexer_coverage needs migration
    try {
      const checkSql = `
        SELECT name 
        FROM system.columns 
        WHERE database = '${this.config.clickhouseDb}' 
          AND table = 'indexer_coverage' 
          AND name IN ('status', 'events_processed', 'events_failed')
      `
      
      const result = await this.executeSqlQuery(checkSql)
      
      if (result.length < 3) {
        this.log.info('Migrating indexer_coverage table...')
        
        // Add missing columns
        const migrations = [
          "ALTER TABLE indexer_coverage ADD COLUMN IF NOT EXISTS status Enum8('completed' = 1, 'failed' = 2, 'partial' = 3) DEFAULT 'completed'",
          "ALTER TABLE indexer_coverage ADD COLUMN IF NOT EXISTS events_processed UInt32 DEFAULT 0",  
          "ALTER TABLE indexer_coverage ADD COLUMN IF NOT EXISTS events_failed UInt32 DEFAULT 0"
        ]
        
        for (const migration of migrations) {
          await this.executeSql(migration, 'Add missing column')
        }
        
        this.log.info('✓ indexer_coverage migration completed')
      }
      
    } catch (error) {
      this.log.warn('Migration check failed, proceeding with table creation', error)
    }
  }
  private async executeSqlQuery(sql: string): Promise<any[]> {
    return queryClickHouse(this.config, sql)
  }

  /**
   * Initializes the complete database schema
   */
  async initializeSchema(): Promise<void> {
    this.log.info('Initializing database schema...')
    
    await this.createDatabase()
    await this.createTables()
    await this.migrateSchema()
    
    const isValid = await this.validateSchema()
    if (!isValid) {
      throw new Error('Schema validation failed after creation')
    }
    
    this.log.info('✓ Database schema initialized successfully')
  }

  /**
   * Drops all indexer tables (use with caution)
   */
  async dropTables(): Promise<void> {
    const tables = [
      'erc4626_deposits',
      'erc4626_withdrawals',
      'share_transfers', 
      'indexer_coverage',
      'indexer_checkpoints'
    ]

    this.log.warn('Dropping all indexer tables...')
    
    for (const table of tables) {
      try {
        await this.executeSql(`DROP TABLE IF EXISTS ${table}`, `Drop table ${table}`)
      } catch (error) {
        this.log.warn(`Failed to drop table ${table}`, error)
      }
    }
    
    this.log.warn('✓ All indexer tables dropped')
  }
}