/**
 * Main indexer class for the ERC4626 transaction indexer
 * Orchestrates the indexing process with chunked backfill capabilities
 */

import { BlockchainClient } from './blockchain.js'
import { DatabaseClient } from '../storage/database.js'
import { EventProcessor } from './events.js'
import { ResumeManager, type CoverageGap } from './resume.js'
import { SchemaManager } from '../storage/schema.js'
import { logger } from '../utils/logger.js'
import type { IndexerConfig } from '../utils/types.js'

/**
 * Sleep utility function for delays
 * @param ms Milliseconds to sleep
 */
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

/**
 * Main indexer class that coordinates all indexing operations
 */
export class Indexer {
  private blockchain: BlockchainClient
  private database: DatabaseClient
  private eventProcessor: EventProcessor
  private resumeManager: ResumeManager
  private schemaManager: SchemaManager
  private config: IndexerConfig
  private log = logger.child('indexer')
  private processingRanges: Set<string> = new Set()
  private contractNames: Map<string, string> = new Map()

  constructor(config: IndexerConfig) {
    this.config = config
    this.blockchain = new BlockchainClient(config)
    this.database = new DatabaseClient(config)
    this.eventProcessor = new EventProcessor(this.blockchain, this.database, config, this.getContractName.bind(this))
    this.resumeManager = new ResumeManager(config)
    this.schemaManager = new SchemaManager(config)
    
    this.log.verbose('Indexer initialized', {
      vault: config.vault,
      chain: config.chain,
      chunkBlocks: Number(config.performance.chunkBlocks)
    })
  }

  /**
   * Processes a single block range and logs the results
   * @param fromBlock Starting block number (inclusive)
   * @param toBlock Ending block number (inclusive)
   */
  async processRange(fromBlock: bigint, toBlock: bigint): Promise<{
    fromBlock: bigint
    toBlock: bigint
    results: {
      deposits: { processed: number; failed: number }
      withdrawals: { processed: number; failed: number }
      transfers: { processed: number; failed: number }
      success: boolean
      partialSuccess: boolean
    }
    duration: number
  }> {
    const rangeKey = `${fromBlock}-${toBlock}`
    
    // Check if this range is already being processed
    if (this.processingRanges.has(rangeKey)) {
      this.log.warn('Range already being processed, skipping duplicate', { fromBlock, toBlock })
      throw new Error(`Range ${fromBlock}-${toBlock} is already being processed`)
    }
    
    // Mark range as being processed
    this.processingRanges.add(rangeKey)
    
    const startTime = Date.now()
    
    try {
      const results = await this.eventProcessor.processBlockRange(fromBlock, toBlock)
      const duration = Date.now() - startTime
      
      if (results.success) {
        this.log.verbose('Range completed successfully', {
          fromBlock,
          toBlock,
          deposits: results.deposits.processed,
          withdrawals: results.withdrawals.processed,
          transfers: results.transfers.processed,
          duration
        })
      } else if (results.partialSuccess) {
        this.log.warn('Range completed with partial failures', {
          fromBlock,
          toBlock,
          processed: {
            deposits: results.deposits.processed,
            withdrawals: results.withdrawals.processed,
            transfers: results.transfers.processed
          },
          failed: {
            deposits: results.deposits.failed,
            withdrawals: results.withdrawals.failed,
            transfers: results.transfers.failed
          },
          duration
        })
      } else {
        this.log.error('Range processing failed completely', {
          fromBlock,
          toBlock,
          duration
        })
      }

      return { fromBlock, toBlock, results, duration }

    } catch (error) {
      this.log.error(`Range processing failed: ${fromBlock} → ${toBlock}`, error)
      throw error
    } finally {
      // Always remove from processing set when done
      this.processingRanges.delete(rangeKey)
    }
  }

  /**
   * Performs chunked backfill indexing from a starting block to ending block
   * Processes blocks in configurable chunks with parallel processing support
   * 
   * @param fromBlock Optional starting block (defaults to contract deployment block)
   * @param toBlock Optional ending block (defaults to latest block)
   * @param maxConcurrentChunks Maximum number of chunks to process in parallel (default: 1) keep it as one, rpc call rate limit error when set more than 1
   */
  async backfill(fromBlock?: bigint, toBlock?: bigint, maxConcurrentChunks: number = 1): Promise<void> {
    const backfillStart = Date.now()
    
    const start = fromBlock ?? await this.blockchain.findDeploymentBlock(this.config.vault)
    const head = toBlock ?? await this.blockchain.getBlockNumber()
    const totalBlocks = head - start + 1n
    
    this.log.info('Starting backfill')
    this.log.verbose('Backfill configuration', {
      fromBlock: start,
      toBlock: head,
      totalBlocks: Number(totalBlocks),
      chunkSize: Number(this.config.performance.chunkBlocks),
      maxConcurrentChunks
    })
    
    // Create array of chunk ranges
    const chunks: Array<{ from: bigint; to: bigint }> = []
    let from = start
    while (from <= head) {
      const to = from + this.config.performance.chunkBlocks - 1n > head 
        ? head 
        : from + this.config.performance.chunkBlocks - 1n
      chunks.push({ from, to })
      from = to + 1n
    }
    
    let completedChunks = 0
    let totalProcessed = { deposits: 0, withdrawals: 0, transfers: 0 }
    let totalFailed = { deposits: 0, withdrawals: 0, transfers: 0 }
    
    // Process chunks in parallel batches
    for (let i = 0; i < chunks.length; i += maxConcurrentChunks) {
      const batch = chunks.slice(i, i + maxConcurrentChunks)
      
      this.log.verbose(`Processing batch ${Math.floor(i / maxConcurrentChunks) + 1}/${Math.ceil(chunks.length / maxConcurrentChunks)}`, {
        chunks: batch.map(c => `${c.from} → ${c.to}`)
      })
      
      // Process batch in parallel with Promise.allSettled for partial failure handling
      const batchStart = Date.now()
      const batchResults = await Promise.allSettled(
        batch.map(chunk => this.processRange(chunk.from, chunk.to))
      )
      const batchDuration = Date.now() - batchStart
      
      // Analyze batch results
      let batchSuccesses = 0
      let batchFailures = 0
      
      batchResults.forEach((result, index) => {
        const chunk = batch[index]
        
        if (result.status === 'fulfilled') {
          batchSuccesses++
          const data = result.value
          totalProcessed.deposits += data.results.deposits.processed
          totalProcessed.withdrawals += data.results.withdrawals.processed
          totalProcessed.transfers += data.results.transfers.processed
          totalFailed.deposits += data.results.deposits.failed
          totalFailed.withdrawals += data.results.withdrawals.failed
          totalFailed.transfers += data.results.transfers.failed
        } else {
          batchFailures++
          this.log.error(`Batch chunk failed: ${chunk.from} → ${chunk.to}`, {
            error: result.reason
          })
        }
      })
      
      completedChunks += batchSuccesses
      const progress = (completedChunks / chunks.length) * 100
      const avgTimePerBatch = (Date.now() - backfillStart) / Math.ceil((i + maxConcurrentChunks) / maxConcurrentChunks)
      const remainingBatches = Math.ceil((chunks.length - (i + maxConcurrentChunks)) / maxConcurrentChunks)
      const estimatedTimeRemaining = remainingBatches * avgTimePerBatch
      
      this.log.progress('Backfill progress', completedChunks, chunks.length)
      this.log.verbose('Batch details', {
        batchDuration: `${batchDuration}ms`,
        batchSuccesses,
        batchFailures,
        progressPercent: `${progress.toFixed(1)}%`,
        totalProcessed: totalProcessed.deposits + totalProcessed.withdrawals + totalProcessed.transfers,
        totalFailed: totalFailed.deposits + totalFailed.withdrawals + totalFailed.transfers,
        estimatedRemaining: estimatedTimeRemaining > 0 ? `${(estimatedTimeRemaining / 60000).toFixed(1)}min` : 'N/A'
      })
      
      // Clear timestamp cache periodically to manage memory
      if (Math.floor(i / maxConcurrentChunks) % 10 === 0) {
        const cacheStats = this.blockchain.getCacheStats()
        this.log.verbose('Memory management check', {
          cacheSize: cacheStats.size,
          memoryUsage: `${(cacheStats.memoryUsage.heapUsed / 1024 / 1024).toFixed(1)}MB`,
          batchesProcessed: Math.floor(i / maxConcurrentChunks) + 1
        })
        
        // Clear cache if memory usage is high
        if (cacheStats.memoryUsage.heapUsed > cacheStats.memoryThresholdMB * 1024 * 1024 * 0.8) {
          this.blockchain.clearTimestampCache()
          this.log.verbose('Cleared timestamp cache due to memory pressure')
        }
      }
      
      // Add delay between batches to respect rate limits
      if (i + maxConcurrentChunks < chunks.length) {
        await sleep(this.config.performance.sleepMs)
      }
      
      // Stop processing if too many failures
      if (batchFailures > 0 && batchFailures >= batchSuccesses) {
        this.log.warn('High failure rate detected, consider reducing concurrency or checking configuration')
      }
    }
    
    const totalDuration = Date.now() - backfillStart
    const totalEventsProcessed = totalProcessed.deposits + totalProcessed.withdrawals + totalProcessed.transfers
    const totalEventsFailed = totalFailed.deposits + totalFailed.withdrawals + totalFailed.transfers
    
    if (completedChunks === chunks.length && totalEventsFailed === 0) {
      this.log.info('Backfill completed successfully')
      this.log.verbose('Backfill summary', {
        totalChunks: chunks.length,
        completedChunks,
        totalBlocks: Number(totalBlocks),
        totalDuration: `${(totalDuration / 60000).toFixed(2)}min`,
        avgChunkTime: `${(totalDuration / completedChunks).toFixed(0)}ms`,
        blocksPerSecond: (Number(totalBlocks) / (totalDuration / 1000)).toFixed(2),
        eventsProcessed: totalEventsProcessed,
        concurrency: maxConcurrentChunks
      })
    } else {
      this.log.warn('Backfill completed with issues', {
        failedChunks: chunks.length - completedChunks,
        eventsProcessed: totalEventsProcessed,
        eventsFailed: totalEventsFailed,
        successRate: `${((completedChunks / chunks.length) * 100).toFixed(1)}%`
      })
    }
  }

  /**
   * Processes events for a specific block range without chunking
   * Useful for processing smaller ranges or real-time updates
   * 
   * @param fromBlock Starting block number
   * @param toBlock Ending block number
   */
  async indexRange(fromBlock: bigint, toBlock: bigint): Promise<void> {
    this.log.verbose(`Starting range indexing`, { fromBlock, toBlock })
    
    const startTime = Date.now()
    await this.processRange(fromBlock, toBlock)
    const totalTime = Date.now() - startTime
    
    this.log.verbose(`Range indexing completed`, { fromBlock, toBlock, duration: `${totalTime}ms` })
  }

  /**
   * Gets contract name with caching
   * @param address Contract address
   * @returns Contract name
   */
  async getContractName(address: string): Promise<string> {
    if (this.contractNames.has(address)) {
      return this.contractNames.get(address)!
    }
    
    const name = await this.blockchain.getContractName(address as any)
    this.contractNames.set(address, name)
    return name
  }

  /**
   * Initializes database schema and tests connections
   * @returns Promise that resolves if all connections are successful
   */
  async initialize(): Promise<void> {
    this.log.info('Initializing indexer...')
    
    try {
      // Initialize database schema
      await this.schemaManager.initializeSchema()
      
      // Test blockchain connection
      const blockNumber = await this.blockchain.getBlockNumber()
      this.log.verbose('Blockchain connection successful', { currentBlock: blockNumber.toString() })
      
      // Test database connection
      await this.database.testConnection()
      
      // Pre-cache vault contract name
      const vaultName = await this.getContractName(this.config.vault)
      this.log.info('Vault contract name cached', { vault: this.config.vault, name: vaultName })
      
      
      this.log.info('✓ Indexer initialization complete')
    } catch (error) {
      this.log.error('Indexer initialization failed', error)
      throw error
    }
  }

  /**
   * Tests connectivity to both blockchain and database
   * @returns Promise that resolves if all connections are successful
   */
  async testConnections(): Promise<void> {
    this.log.verbose('Testing connections...')
    
    try {
      // Test blockchain connection
      const blockNumber = await this.blockchain.getBlockNumber()
      this.log.verbose('Blockchain connection successful', { currentBlock: blockNumber.toString() })
      
      // Test database connection
      await this.database.testConnection()
      
      this.log.verbose('All connections verified')
    } catch (error) {
      this.log.error('Connection test failed', error)
      throw error
    }
  }

  /**
   * Smart backfill with automatic resume functionality
   * Analyzes existing coverage and resumes from the optimal point
   */
  async smartBackfill(fromBlock?: bigint, toBlock?: bigint, maxConcurrentChunks: number = 1): Promise<void> {
    this.log.info('Starting smart backfill with resume capability')
    
    const deploymentBlock = fromBlock ?? await this.blockchain.findDeploymentBlock(this.config.vault)
    const latestBlock = toBlock ?? await this.blockchain.getBlockNumber()
    
    // Get resume strategy
    const strategy = await this.resumeManager.getResumeStrategy(deploymentBlock, latestBlock)
    
    this.log.info('Resume strategy determined', {
      strategy: strategy.strategy,
      description: strategy.description,
      startBlock: strategy.startBlock.toString(),
      shouldResume: strategy.shouldResume
    })

    if (strategy.strategy === 'fill_gaps' && strategy.gaps) {
      await this.fillGaps(strategy.gaps, maxConcurrentChunks)
    } else {
      // Regular backfill from determined start block
      await this.backfill(strategy.startBlock, latestBlock, maxConcurrentChunks)
    }
    
    // Save final checkpoint
    await this.resumeManager.saveCheckpoint('backfill', latestBlock, '', 'completed')
    this.log.info('✓ Smart backfill completed, checkpoint saved')
  }

  /**
   * Fills coverage gaps in priority order
   */
  async fillGaps(gaps: CoverageGap[], maxConcurrentChunks: number = 1): Promise<void> {
    this.log.info('Starting gap filling process', { totalGaps: gaps.length })
    
    const plan = await this.resumeManager.createGapFillingPlan(gaps)
    
    for (let i = 0; i < plan.length; i++) {
      const gap = plan[i]
      this.log.info(`Filling gap ${i + 1}/${plan.length}: ${gap.reason}`)
      
      try {
        await this.backfill(gap.fromBlock, gap.toBlock, maxConcurrentChunks)
        this.log.success(`✓ Gap ${i + 1} filled successfully`)
      } catch (error) {
        this.log.error(`Gap ${i + 1} filling failed`, error)
        // Continue with next gap instead of failing completely
      }
    }
    
    this.log.info('✓ Gap filling process completed')
  }

  /**
   * Gets current indexer status and statistics
   * @returns Status information about the indexer
   */
  async getStatus(): Promise<{
    currentBlock: bigint
    vaultAddress: string
    cacheSize: number
    lastCheckpoint: any
    coverageAnalysis: any
    config: IndexerConfig
  }> {
    const currentBlock = await this.blockchain.getBlockNumber()
    const deploymentBlock = await this.blockchain.findDeploymentBlock(this.config.vault)
    
    const [lastCheckpoint, coverageAnalysis] = await Promise.all([
      this.resumeManager.getLatestCheckpoint('backfill'),
      this.resumeManager.analyzeCoverage(deploymentBlock, currentBlock)
    ])

    return {
      currentBlock,
      vaultAddress: this.config.vault,
      cacheSize: this.blockchain.getCacheSize(),
      lastCheckpoint,
      coverageAnalysis,
      config: this.config
    }
  }

  /**
   * Gracefully shuts down the indexer
   * Clears caches and performs cleanup
   */
  async shutdown(): Promise<void> {
    this.log.verbose('Shutting down indexer')
    this.blockchain.destroy()
    this.log.verbose('Indexer shutdown complete')
  }
}