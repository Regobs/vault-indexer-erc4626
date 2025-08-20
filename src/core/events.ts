/**
 * Event processing utilities for the ERC4626 transaction indexer
 * Handles parsing and processing of deposit, withdrawal, and transfer events
 */

import type { Hex } from 'viem'
import { BlockchainClient } from './blockchain.js'
import { DatabaseClient } from '../storage/database.js'
import { logger } from '../utils/logger.js'
import type { 
  IndexerConfig, 
  DepositEventArgs, 
  WithdrawalEventArgs, 
  TransferEventArgs,
  DepositRecord,
  WithdrawalRecord,
  TransferRecord,
  CoverageRecord
} from '../utils/types.js'

/**
 * Event processor class that handles the extraction and storage of blockchain events
 */
export class EventProcessor {
  private blockchain: BlockchainClient
  private database: DatabaseClient
  private config: IndexerConfig
  private log = logger.child('events')
  private getContractName: (address: string) => Promise<string>

  constructor(
    blockchain: BlockchainClient,
    database: DatabaseClient,
    config: IndexerConfig,
    getContractName: (address: string) => Promise<string>
  ) {
    this.blockchain = blockchain
    this.database = database
    this.config = config
    this.getContractName = getContractName
  }

  /**
   * Validates event arguments for deposits
   */
  private validateDepositArgs(args: any): args is DepositEventArgs {
    return args && 
           typeof args.caller === 'string' && 
           typeof args.owner === 'string' && 
           typeof args.assets === 'bigint' && 
           typeof args.shares === 'bigint'
  }

  /**
   * Validates event arguments for withdrawals
   */
  private validateWithdrawalArgs(args: any): args is WithdrawalEventArgs {
    return args && 
           typeof args.caller === 'string' && 
           typeof args.receiver === 'string' && 
           typeof args.owner === 'string' && 
           typeof args.assets === 'bigint' && 
           typeof args.shares === 'bigint'
  }

  /**
   * Validates event arguments for transfers
   */
  private validateTransferArgs(args: any): args is TransferEventArgs {
    return args && 
           typeof args.from === 'string' && 
           typeof args.to === 'string' && 
           typeof args.value === 'bigint'
  }

  /**
   * Safely processes a single deposit event with error handling
   * Returns the processed record or null if processing failed
   */
  private async processDepositEvent(log: any): Promise<DepositRecord | null> {
    try {
      if (!log.args || !log.blockNumber || !log.transactionHash) {
        this.log.warn('Invalid deposit log structure', { 
          blockNumber: log.blockNumber?.toString(), 
          txHash: log.transactionHash 
        })
        return null
      }

      if (!this.validateDepositArgs(log.args)) {
        this.log.warn('Invalid deposit event arguments', { 
          blockNumber: log.blockNumber.toString(), 
          txHash: log.transactionHash,
          args: log.args
        })
        return null
      }

      const { caller, owner, assets, shares } = log.args as DepositEventArgs
      
      let timestamp: bigint
      try {
        timestamp = await this.blockchain.getBlockTimestamp(log.blockNumber)
      } catch (error) {
        this.log.error('Failed to get timestamp, cannot process event', { 
          blockNumber: log.blockNumber.toString(),
          error: error instanceof Error ? error.message : String(error)
        })
        return null
      }

      let contractName: string
      try {
        contractName = await this.getContractName(this.config.vault)
      } catch (error) {
        this.log.warn('Failed to get contract name, using default', { 
          vault: this.config.vault,
          error: error instanceof Error ? error.message : String(error)
        })
        contractName = 'Unknown Vault'
      }
      
      const record: DepositRecord = {
        vault: this.config.vault.slice(2),
        contractName,
        sender: caller.slice(2),
        owner: owner.slice(2),
        assets: assets.toString(),
        shares: shares.toString(),
        blockNumber: log.blockNumber.toString(),
        txHash: (log.transactionHash as Hex).slice(2),
        timestamp: timestamp.toString()
      }
      
      return record

    } catch (error) {
      this.log.warn('Failed to process deposit event', { 
        blockNumber: log.blockNumber?.toString(), 
        txHash: log.transactionHash,
        error: error instanceof Error ? error.message : String(error)
      })
      return null
    }
  }

  /**
   * Safely processes a single withdrawal event with error handling
   * Returns the processed record or null if processing failed
   */
  private async processWithdrawalEvent(log: any): Promise<WithdrawalRecord | null> {
    try {
      if (!log.args || !log.blockNumber || !log.transactionHash) {
        this.log.warn('Invalid withdrawal log structure', { 
          blockNumber: log.blockNumber?.toString(), 
          txHash: log.transactionHash 
        })
        return null
      }

      if (!this.validateWithdrawalArgs(log.args)) {
        this.log.warn('Invalid withdrawal event arguments', { 
          blockNumber: log.blockNumber.toString(), 
          txHash: log.transactionHash,
          args: log.args
        })
        return null
      }

      const { caller, receiver, owner, assets, shares } = log.args as WithdrawalEventArgs
      
      let timestamp: bigint
      try {
        timestamp = await this.blockchain.getBlockTimestamp(log.blockNumber)
      } catch (error) {
        this.log.error('Failed to get timestamp, cannot process event', { 
          blockNumber: log.blockNumber.toString(),
          error: error instanceof Error ? error.message : String(error)
        })
        return null
      }

      let contractName: string
      try {
        contractName = await this.getContractName(this.config.vault)
      } catch (error) {
        this.log.warn('Failed to get contract name, using default', { 
          vault: this.config.vault,
          error: error instanceof Error ? error.message : String(error)
        })
        contractName = 'Unknown Vault'
      }
      
      const record: WithdrawalRecord = {
        vault: this.config.vault.slice(2),
        contractName,
        sender: caller.slice(2),
        receiver: receiver.slice(2),
        owner: owner.slice(2),
        assets: assets.toString(),
        shares: shares.toString(),
        blockNumber: log.blockNumber.toString(),
        txHash: (log.transactionHash as Hex).slice(2),
        timestamp: timestamp.toString()
      }
      
      return record

    } catch (error) {
      this.log.warn('Failed to process withdrawal event', { 
        blockNumber: log.blockNumber?.toString(), 
        txHash: log.transactionHash,
        error: error instanceof Error ? error.message : String(error)
      })
      return null
    }
  }

  /**
   * Safely processes a single transfer event with error handling
   * Returns the processed record or null if processing failed
   */
  private async processTransferEvent(log: any): Promise<TransferRecord | null> {
    try {
      if (!log.args || !log.blockNumber || !log.transactionHash || !log.address) {
        this.log.warn('Invalid transfer log structure', { 
          blockNumber: log.blockNumber?.toString(), 
          txHash: log.transactionHash,
          address: log.address
        })
        return null
      }

      if (!this.validateTransferArgs(log.args)) {
        this.log.warn('Invalid transfer event arguments', { 
          blockNumber: log.blockNumber.toString(), 
          txHash: log.transactionHash,
          address: log.address,
          args: log.args
        })
        return null
      }

      const { from, to, value } = log.args as TransferEventArgs
      
      let timestamp: bigint
      try {
        timestamp = await this.blockchain.getBlockTimestamp(log.blockNumber)
      } catch (error) {
        this.log.error('Failed to get timestamp, cannot process event', { 
          blockNumber: log.blockNumber.toString(),
          error: error instanceof Error ? error.message : String(error)
        })
        return null
      }

      let contractName: string
      try {
        contractName = await this.getContractName(log.address)
      } catch (error) {
        this.log.warn('Failed to get contract name, using default', { 
          token: log.address,
          error: error instanceof Error ? error.message : String(error)
        })
        contractName = 'Unknown Token'
      }
      
      const record: TransferRecord = {
        token: log.address.slice(2),
        contractName,
        fromAddr: from.slice(2),
        toAddr: to.slice(2),
        value: value.toString(),
        blockNumber: log.blockNumber.toString(),
        txHash: (log.transactionHash as Hex).slice(2),
        timestamp: timestamp.toString()
      }
      
      return record

    } catch (error) {
      this.log.warn('Failed to process transfer event', { 
        blockNumber: log.blockNumber?.toString(), 
        txHash: log.transactionHash,
        address: log.address,
        error: error instanceof Error ? error.message : String(error)
      })
      return null
    }
  }




  /**
   * Optimized method that fetches all event types in a single getLogs call
   * and processes them based on event signatures for maximum performance
   */
  private async processAllEventsOptimized(fromBlock: bigint, toBlock: bigint): Promise<{
    deposits: { processed: number; failed: number }
    withdrawals: { processed: number; failed: number }
    transfers: { processed: number; failed: number }
    success: boolean
  }> {
    const startTime = Date.now()
    
    // Prepare addresses - only vault contract (no separate share token)
    const addresses = [this.config.vault]

    try {
      // Single getLogs call for all events from all relevant addresses
      this.log.verbose(`Fetching all events with single call`, { fromBlock, toBlock, addresses: addresses.length })
      
      const allLogs = await this.blockchain.getLogsSafe({ 
        address: addresses, 
        event: undefined, // No event filter - get all events
        fromBlock, 
        toBlock 
      })

      this.log.verbose(`Retrieved ${allLogs.length} total logs, categorizing by event type`)

      // Categorize logs by event signature
      const depositLogs: any[] = []
      const withdrawalLogs: any[] = []
      const transferLogs: any[] = []

      for (const log of allLogs) {
        if (!log.topics || !log.topics[0]) continue

        const eventSignature = log.topics[0]
        
        // Match event signatures (these are keccak256 hashes of event signatures)
        if (eventSignature === '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7') {
          // Deposit(address indexed caller, address indexed owner, uint256 assets, uint256 shares)
          try {
            // Parse the log with deposit event ABI
            const parsedLog = {
              ...log,
              args: {
                caller: `0x${log.topics[1].slice(26)}` as Hex,
                owner: `0x${log.topics[2].slice(26)}` as Hex,
                assets: BigInt(log.data.slice(0, 66)),
                shares: BigInt('0x' + log.data.slice(66, 130))
              }
            }
            depositLogs.push(parsedLog)
          } catch (error) {
            this.log.warn('Failed to parse deposit log', { blockNumber: log.blockNumber, error })
          }
        } else if (eventSignature === '0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db') {
          // Withdraw(address indexed caller, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)
          try {
            const parsedLog = {
              ...log,
              args: {
                caller: `0x${log.topics[1].slice(26)}` as Hex,
                receiver: `0x${log.topics[2].slice(26)}` as Hex,
                owner: `0x${log.topics[3].slice(26)}` as Hex,
                assets: BigInt(log.data.slice(0, 66)),
                shares: BigInt('0x' + log.data.slice(66, 130))
              }
            }
            withdrawalLogs.push(parsedLog)
          } catch (error) {
            this.log.warn('Failed to parse withdrawal log', { blockNumber: log.blockNumber, error })
          }
        } else if (eventSignature === '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') {
          // Transfer(address indexed from, address indexed to, uint256 value)
          try {
            const parsedLog = {
              ...log,
              args: {
                from: `0x${log.topics[1].slice(26)}` as Hex,
                to: `0x${log.topics[2].slice(26)}` as Hex,
                value: BigInt(log.data)
              }
            }
            transferLogs.push(parsedLog)
          } catch (error) {
            this.log.warn('Failed to parse transfer log', { blockNumber: log.blockNumber, error })
          }
        }
      }

      this.log.verbose('Event categorization complete', {
        deposits: depositLogs.length,
        withdrawals: withdrawalLogs.length,
        transfers: transferLogs.length,
        total: allLogs.length,
        processingTime: `${Date.now() - startTime}ms`
      })

      // Process each event type
      const [depositResults, withdrawalResults, transferResults] = await Promise.allSettled([
        this.processDepositLogs(depositLogs),
        this.processWithdrawalLogs(withdrawalLogs),
        this.processTransferLogs(transferLogs)
      ])

      // Extract results
      const deposits = depositResults.status === 'fulfilled' ? depositResults.value : { processed: 0, failed: 0 }
      const withdrawals = withdrawalResults.status === 'fulfilled' ? withdrawalResults.value : { processed: 0, failed: 0 }
      const transfers = transferResults.status === 'fulfilled' ? transferResults.value : { processed: 0, failed: 0 }

      // Log any processing failures
      if (depositResults.status === 'rejected') {
        this.log.error('Failed to process deposit logs', depositResults.reason)
      }
      if (withdrawalResults.status === 'rejected') {
        this.log.error('Failed to process withdrawal logs', withdrawalResults.reason)
      }
      if (transferResults.status === 'rejected') {
        this.log.error('Failed to process transfer logs', transferResults.reason)
      }

      const success = depositResults.status === 'fulfilled' && 
                     withdrawalResults.status === 'fulfilled' && 
                     transferResults.status === 'fulfilled'

      return { deposits, withdrawals, transfers, success }

    } catch (error) {
      this.log.error('Failed to fetch logs in optimized call', { fromBlock, toBlock, error })
      throw error
    }
  }

  /**
   * Process pre-parsed deposit logs
   */
  private async processDepositLogs(logs: any[]): Promise<{ processed: number; failed: number }> {
    const records: DepositRecord[] = []
    let failed = 0

    for (const log of logs) {
      const record = await this.processDepositEvent(log)
      if (record) {
        records.push(record)
      } else {
        failed++
      }
    }

    if (records.length > 0) {
      await this.database.insertDeposits(records)
    }

    return { processed: records.length, failed }
  }

  /**
   * Process pre-parsed withdrawal logs
   */
  private async processWithdrawalLogs(logs: any[]): Promise<{ processed: number; failed: number }> {
    const records: WithdrawalRecord[] = []
    let failed = 0

    for (const log of logs) {
      const record = await this.processWithdrawalEvent(log)
      if (record) {
        records.push(record)
      } else {
        failed++
      }
    }

    if (records.length > 0) {
      await this.database.insertWithdrawals(records)
    }

    return { processed: records.length, failed }
  }

  /**
   * Process pre-parsed transfer logs
   */
  private async processTransferLogs(logs: any[]): Promise<{ processed: number; failed: number }> {
    const records: TransferRecord[] = []
    let failed = 0

    for (const log of logs) {
      const record = await this.processTransferEvent(log)
      if (record) {
        records.push(record)
      } else {
        failed++
      }
    }

    if (records.length > 0) {
      await this.database.insertTransfers(records)
    }

    return { processed: records.length, failed }
  }

  /**
   * Records indexing coverage for a processed block range
   */
  private async recordCoverage(
    fromBlock: bigint, 
    toBlock: bigint,
    status: 'completed' | 'failed' | 'partial' = 'completed',
    eventsProcessed: number = 0,
    eventsFailed: number = 0
  ): Promise<void> {
    const record: CoverageRecord = {
      address: this.config.vault.slice(2),
      startBlock: fromBlock.toString(),
      endBlock: toBlock.toString(),
      updatedAt: 'now()'
    }
    
    await this.database.insertCoverage(record, status, eventsProcessed, eventsFailed)
  }

  /**
   * Processes all events (deposits, withdrawals, transfers) for a specific block range
   * and records coverage information. Uses single getLogs call for optimal performance.
   */
  async processBlockRange(fromBlock: bigint, toBlock: bigint): Promise<{
    deposits: { processed: number; failed: number }
    withdrawals: { processed: number; failed: number }
    transfers: { processed: number; failed: number }
    success: boolean
    partialSuccess: boolean
  }> {
    const startTime = Date.now()
    this.log.verbose(`Processing block range ${fromBlock} → ${toBlock}`)

    try {
      // Use optimized single call processing
      const { deposits, withdrawals, transfers, success } = await this.processAllEventsOptimized(fromBlock, toBlock)

      const totalProcessed = deposits.processed + withdrawals.processed + transfers.processed
      const totalFailed = deposits.failed + withdrawals.failed + transfers.failed
      const allSuccessful = success
      const partialSuccess = !allSuccessful && totalProcessed > 0

      // Record coverage with detailed status
      try {
        const status = allSuccessful && totalFailed === 0 ? 'completed' 
                    : totalProcessed > 0 ? 'partial' 
                    : 'failed'
        
        await this.recordCoverage(fromBlock, toBlock, status, totalProcessed, totalFailed)
      } catch (error) {
        this.log.warn('Failed to record coverage, but continuing', { 
          fromBlock, 
          toBlock, 
          error: error instanceof Error ? error.message : String(error)
        })
      }

      const duration = Date.now() - startTime
      
      if (allSuccessful && totalFailed === 0) {
        this.log.verbose('Block range processed successfully', {
          fromBlock,
          toBlock,
          deposits: deposits.processed,
          withdrawals: withdrawals.processed,
          transfers: transfers.processed,
          duration
        })
      } else if (partialSuccess) {
        this.log.warn('Block range processed with partial failures', {
          fromBlock,
          toBlock,
          processed: { 
            deposits: deposits.processed, 
            withdrawals: withdrawals.processed, 
            transfers: transfers.processed 
          },
          failed: { 
            deposits: deposits.failed, 
            withdrawals: withdrawals.failed, 
            transfers: transfers.failed 
          },
          totalProcessed,
          totalFailed,
          duration
        })
      } else {
        this.log.error('Block range processing failed completely', {
          fromBlock,
          toBlock,
          duration
        })
      }

      return { 
        deposits, 
        withdrawals, 
        transfers, 
        success: allSuccessful && totalFailed === 0,
        partialSuccess 
      }

    } catch (error) {
      this.log.error(`Unexpected error processing block range ${fromBlock} → ${toBlock}`, error)
      throw error
    }
  }
}