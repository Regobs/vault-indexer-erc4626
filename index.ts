/**
 * ERC4626 Vault Transaction Indexer - Entry Point
 * 
 * Command line interface for the ERC4626 transaction indexer.
 * Indexes vault deposit, withdrawal, and share transfer events
 * to ClickHouse database for analytics and monitoring purposes.
 * 
 */

import { loadConfig, validateConfig } from './src/utils/config.js'
import { Indexer } from './src/core/indexer.js'
import { logger, configureLoggerFromEnv } from './src/utils/logger.js'

/**
 * Main application entry point
 * Parses command line arguments and starts the indexing process
 */
async function main(): Promise<void> {
  try {
    // Configure logger from environment variables
    configureLoggerFromEnv()
    
    // Load and validate configuration from environment
    const config = loadConfig()
    validateConfig(config)
    
    logger.info('ERC4626 Transaction Indexer Starting')
    logger.verbose('Configuration loaded', {
      vault: config.vault,
      chain: config.chain,
      database: config.clickhouseDb
    })
    
    // Parse command line arguments for block range
    const [, , blockA, blockB] = process.argv
    const fromBlock = blockA ? BigInt(blockA) : undefined
    const toBlock = blockB ? BigInt(blockB) : undefined
    
    logger.verbose('Block range configuration', {
      fromBlock: fromBlock?.toString() || 'auto-detect',
      toBlock: toBlock?.toString() || 'latest'
    })
    
    // Initialize indexer with schema setup
    const indexer = new Indexer(config)
    await indexer.initialize()
    
    // Check if this is a resume operation (no CLI args provided)
    const shouldUseSmartResume = !fromBlock && !toBlock
    
    if (shouldUseSmartResume) {
      logger.info('No block range specified - using smart resume functionality')
      await indexer.smartBackfill()
    } else {
      logger.info('Block range specified - performing manual backfill', {
        fromBlock: fromBlock?.toString() || 'auto-detect',
        toBlock: toBlock?.toString() || 'latest'
      })
      await indexer.backfill(fromBlock, toBlock)
    }
    
    // Clean shutdown
    await indexer.shutdown()
    logger.info('Indexing completed successfully')
    
  } catch (error) {
    logger.error('Indexing failed', error)
    process.exit(1)
  }
}

/**
 * Handle uncaught errors and signals gracefully
 */
process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception', error)
  process.exit(1)
})

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection', { reason, promise })
  process.exit(1)
})

process.on('SIGINT', () => {
  logger.info('Received SIGINT, shutting down gracefully')
  process.exit(0)
})

process.on('SIGTERM', () => {
  logger.info('Received SIGTERM, shutting down gracefully')
  process.exit(0)
})

// Start the application
main()