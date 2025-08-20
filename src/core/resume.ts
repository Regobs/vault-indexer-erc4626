/**
 * Resume functionality for the ERC4626 transaction indexer
 * Handles checkpoint tracking, gap detection, and automatic resume logic
 */

import { logger } from '../utils/logger.js'
import { queryClickHouse, executeClickHouse } from '../utils/clickhouse.js'
import type { IndexerConfig } from '../utils/types.js'

export interface CheckpointData {
  address: string
  checkpointType: 'backfill' | 'realtime'
  lastProcessedBlock: bigint
  lastProcessedTxHash: string
  processingState: string
  updatedAt: string
}

export interface CoverageGap {
  startBlock: bigint
  endBlock: bigint
  reason: 'missing' | 'failed' | 'partial'
}

export interface ResumeAnalysis {
  lastProcessedBlock: bigint | null
  hasGaps: boolean
  gaps: CoverageGap[]
  totalMissingBlocks: bigint
  recommendedStartBlock: bigint | null
}

/**
 * Resume manager handles checkpoint persistence and gap analysis
 */
export class ResumeManager {
  private config: IndexerConfig
  private log = logger.child('resume')

  constructor(config: IndexerConfig) {
    this.config = config
  }

  /**
   * Executes a SQL query against ClickHouse
   */
  private async executeQuery(sql: string): Promise<any[]> {
    try {
      return await queryClickHouse(this.config, sql)
    } catch (error) {
      this.log.error('Database query failed', { sql: sql.substring(0, 100), error })
      throw error
    }
  }

  /**
   * Saves a checkpoint for the current processing state
   */
  async saveCheckpoint(
    checkpointType: 'backfill' | 'realtime',
    lastProcessedBlock: bigint,
    lastProcessedTxHash: string = '',
    processingState: string = ''
  ): Promise<void> {
    const sql = `
      INSERT INTO indexer_checkpoints 
        (address, checkpoint_type, last_processed_block, last_processed_tx_hash, processing_state, updated_at)
      VALUES 
        (unhex('${this.config.vault.slice(2)}'), '${checkpointType}', ${lastProcessedBlock}, unhex('${lastProcessedTxHash}'), '${processingState}', now())
    `
    
    try {
      await this.executeInsert(sql)
      this.log.verbose('Checkpoint saved', { 
        checkpointType, 
        lastProcessedBlock: lastProcessedBlock.toString(),
        processingState 
      })
    } catch (error) {
      this.log.error('Failed to save checkpoint', error)
      throw error
    }
  }

  /**
   * Executes an INSERT statement (doesn't return JSON)
   */
  private async executeInsert(sql: string): Promise<void> {
    try {
      await executeClickHouse(this.config, sql)
    } catch (error) {
      this.log.error('Database insert failed', { sql: sql.substring(0, 100), error })
      throw error
    }
  }

  /**
   * Retrieves the latest checkpoint for the vault
   */
  async getLatestCheckpoint(checkpointType: 'backfill' | 'realtime'): Promise<CheckpointData | null> {
    const sql = `
      SELECT 
        hex(address) as address,
        checkpoint_type,
        last_processed_block,
        hex(last_processed_tx_hash) as last_processed_tx_hash,
        processing_state,
        toString(updated_at) as updated_at
      FROM indexer_checkpoints 
      WHERE address = unhex('${this.config.vault.slice(2)}')
        AND checkpoint_type = '${checkpointType}'
      ORDER BY updated_at DESC 
      LIMIT 1
    `

    try {
      const result = await this.executeQuery(sql)
      
      if (result.length === 0) {
        return null
      }

      const [address, type, block, txHash, state, updatedAt] = result[0]
      
      return {
        address,
        checkpointType: type,
        lastProcessedBlock: BigInt(block),
        lastProcessedTxHash: txHash,
        processingState: state,
        updatedAt
      }
    } catch (error) {
      this.log.error('Failed to get latest checkpoint', error)
      return null
    }
  }

  /**
   * Analyzes coverage to identify gaps and determine resume strategy
   */
  async analyzeCoverage(deploymentBlock: bigint, latestBlock: bigint): Promise<ResumeAnalysis> {
    this.log.verbose('Analyzing coverage for resume', { 
      deploymentBlock: deploymentBlock.toString(), 
      latestBlock: latestBlock.toString() 
    })

    // Get all coverage records sorted by block range
    const sql = `
      SELECT 
        start_block,
        end_block,
        status,
        events_processed + events_failed as total_events
      FROM indexer_coverage 
      WHERE address = unhex('${this.config.vault.slice(2)}')
      ORDER BY start_block ASC
    `

    try {
      const coverageData = await this.executeQuery(sql)
      
      if (coverageData.length === 0) {
        // No coverage data - fresh start
        return {
          lastProcessedBlock: null,
          hasGaps: true,
          gaps: [{ 
            startBlock: deploymentBlock, 
            endBlock: latestBlock, 
            reason: 'missing' 
          }],
          totalMissingBlocks: latestBlock - deploymentBlock + 1n,
          recommendedStartBlock: deploymentBlock
        }
      }

      // Analyze gaps in coverage
      const gaps: CoverageGap[] = []
      let lastEndBlock = deploymentBlock - 1n
      let highestProcessedBlock = 0n

      for (const [startBlock, endBlock, status] of coverageData) {
        const start = BigInt(startBlock)
        const end = BigInt(endBlock)
        
        // Check for gap before this range
        if (start > lastEndBlock + 1n) {
          gaps.push({
            startBlock: lastEndBlock + 1n,
            endBlock: start - 1n,
            reason: 'missing'
          })
        }

        // Check if this range had failures or was partial
        if (status === 'failed') {
          gaps.push({
            startBlock: start,
            endBlock: end,
            reason: 'failed'
          })
        } else if (status === 'partial') {
          gaps.push({
            startBlock: start,
            endBlock: end,
            reason: 'partial'
          })
        } else {
          // Successfully completed range
          highestProcessedBlock = end > highestProcessedBlock ? end : highestProcessedBlock
        }

        lastEndBlock = end
      }

      // Check for gap after the last range to latest block
      if (lastEndBlock < latestBlock) {
        gaps.push({
          startBlock: lastEndBlock + 1n,
          endBlock: latestBlock,
          reason: 'missing'
        })
      }

      const totalMissingBlocks = gaps.reduce(
        (total, gap) => total + (gap.endBlock - gap.startBlock + 1n), 
        0n
      )

      // Determine recommended start block
      let recommendedStartBlock: bigint | null = null
      if (gaps.length > 0) {
        // Start from the earliest gap
        recommendedStartBlock = gaps[0].startBlock
      } else if (highestProcessedBlock < latestBlock) {
        // No gaps but need to continue from last processed
        recommendedStartBlock = highestProcessedBlock + 1n
      }

      this.log.info('Coverage analysis complete', {
        totalRanges: coverageData.length,
        highestProcessedBlock: highestProcessedBlock.toString(),
        gapCount: gaps.length,
        totalMissingBlocks: totalMissingBlocks.toString(),
        recommendedStartBlock: recommendedStartBlock?.toString() || 'none'
      })

      return {
        lastProcessedBlock: highestProcessedBlock > 0n ? highestProcessedBlock : null,
        hasGaps: gaps.length > 0,
        gaps,
        totalMissingBlocks,
        recommendedStartBlock
      }

    } catch (error) {
      this.log.error('Failed to analyze coverage', error)
      throw error
    }
  }

  /**
   * Determines the optimal resume strategy
   */
  async getResumeStrategy(deploymentBlock: bigint, latestBlock: bigint): Promise<{
    shouldResume: boolean
    startBlock: bigint
    strategy: 'fresh_start' | 'resume_from_checkpoint' | 'fill_gaps'
    description: string
    gaps?: CoverageGap[]
  }> {
    const checkpoint = await this.getLatestCheckpoint('backfill')
    const analysis = await this.analyzeCoverage(deploymentBlock, latestBlock)

    // Fresh start if no previous data
    if (!checkpoint && !analysis.lastProcessedBlock) {
      return {
        shouldResume: false,
        startBlock: deploymentBlock,
        strategy: 'fresh_start',
        description: 'No previous indexing data found. Starting fresh from deployment block.'
      }
    }

    // Resume from checkpoint if available and no gaps
    if (checkpoint && !analysis.hasGaps) {
      const nextBlock = checkpoint.lastProcessedBlock + 1n
      return {
        shouldResume: true,
        startBlock: nextBlock <= latestBlock ? nextBlock : latestBlock,
        strategy: 'resume_from_checkpoint',
        description: `Resuming from checkpoint at block ${checkpoint.lastProcessedBlock.toString()}`
      }
    }

    // Fill gaps strategy if coverage has holes
    if (analysis.hasGaps && analysis.recommendedStartBlock) {
      return {
        shouldResume: true,
        startBlock: analysis.recommendedStartBlock,
        strategy: 'fill_gaps',
        description: `Found ${analysis.gaps.length} gaps in coverage. Starting gap filling from block ${analysis.recommendedStartBlock.toString()}`,
        gaps: analysis.gaps
      }
    }

    // Fallback to continuing from last processed
    const startBlock = analysis.lastProcessedBlock 
      ? analysis.lastProcessedBlock + 1n 
      : deploymentBlock

    return {
      shouldResume: true,
      startBlock,
      strategy: 'resume_from_checkpoint',
      description: `Continuing from last processed block ${analysis.lastProcessedBlock?.toString() || 'unknown'}`
    }
  }

  /**
   * Creates a processing plan for gap filling
   */
  async createGapFillingPlan(gaps: CoverageGap[]): Promise<Array<{
    fromBlock: bigint
    toBlock: bigint
    priority: number
    reason: string
  }>> {
    const plan = gaps.map((gap, index) => ({
      fromBlock: gap.startBlock,
      toBlock: gap.endBlock,
      priority: gap.reason === 'failed' ? 1 : gap.reason === 'partial' ? 2 : 3,
      reason: `Gap ${index + 1}: ${gap.reason} (${gap.endBlock - gap.startBlock + 1n} blocks)`
    }))

    // Sort by priority (failed gaps first, then partial, then missing)
    plan.sort((a, b) => a.priority - b.priority)

    this.log.info('Gap filling plan created', {
      totalGaps: plan.length,
      totalBlocks: plan.reduce((sum, item) => sum + (item.toBlock - item.fromBlock + 1n), 0n).toString()
    })

    return plan
  }
}