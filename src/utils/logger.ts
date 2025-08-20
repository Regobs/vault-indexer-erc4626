/**
 * Enhanced console logging utility for the ERC4626 transaction indexer
 * Provides structured logging with colors, timestamps, and log levels
 */

/**
 * Log levels for controlling output verbosity
 */
export enum LogLevel {
  VERBOSE = 0,
  DEBUG = 1,
  INFO = 2,
  WARN = 3,
  ERROR = 4,
  SILENT = 5
}

/**
 * ANSI color codes for terminal output
 */
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
  gray: '\x1b[90m'
} as const

/**
 * Log level configuration
 */
interface LoggerConfig {
  level: LogLevel
  enableColors: boolean
  enableTimestamps: boolean
  enableStructured: boolean
}

/**
 * Enhanced console logger class
 */
class Logger {
  private config: LoggerConfig

  constructor(config: Partial<LoggerConfig> = {}) {
    this.config = {
      level: LogLevel.INFO,
      enableColors: true,
      enableTimestamps: true,
      enableStructured: true,
      ...config
    }
  }

  /**
   * Formats timestamp for logging
   */
  private formatTimestamp(): string {
    if (!this.config.enableTimestamps) return ''
    
    const now = new Date()
    const timestamp = now.toISOString().replace('T', ' ').slice(0, -5)
    const color = this.config.enableColors ? colors.gray : ''
    const reset = this.config.enableColors ? colors.reset : ''
    return `${color}[${timestamp}]${reset} `
  }

  /**
   * Formats log level with colors
   */
  private formatLevel(level: string, color: string): string {
    if (!this.config.enableColors) return `[${level}]`
    return `${color}[${level}]${colors.reset}`
  }

  /**
   * Formats structured data as JSON
   */
  private formatStructured(data: any): string {
    if (!this.config.enableStructured || !data) return ''
    
    try {
      const json = JSON.stringify(data, null, 2)
      const color = this.config.enableColors ? colors.dim : ''
      const reset = this.config.enableColors ? colors.reset : ''
      return `\n${color}${json}${reset}`
    } catch {
      return ''
    }
  }

  /**
   * Generic log method
   */
  private log(level: LogLevel, levelName: string, color: string, message: string, data?: any): void {
    if (level < this.config.level) return

    const timestamp = this.formatTimestamp()
    const levelFormatted = this.formatLevel(levelName, color)
    const structured = this.formatStructured(data)
    
    console.log(`${timestamp}${levelFormatted} ${message}${structured}`)
  }

  /**
   * Verbose level logging (very detailed operational info)
   */
  verbose(message: string, data?: any): void {
    this.log(LogLevel.VERBOSE, 'VERBOSE', colors.dim, message, data)
  }

  /**
   * Debug level logging
   */
  debug(message: string, data?: any): void {
    this.log(LogLevel.DEBUG, 'DEBUG', colors.gray, message, data)
  }

  /**
   * Info level logging
   */
  info(message: string, data?: any): void {
    this.log(LogLevel.INFO, 'INFO', colors.blue, message, data)
  }

  /**
   * Success logging (info level with green color)
   */
  success(message: string, data?: any): void {
    this.log(LogLevel.INFO, 'SUCCESS', colors.green, message, data)
  }

  /**
   * Warning level logging
   */
  warn(message: string, data?: any): void {
    this.log(LogLevel.WARN, 'WARN', colors.yellow, message, data)
  }

  /**
   * Error level logging
   */
  error(message: string, error?: Error | any): void {
    let errorData: any = undefined
    
    if (error instanceof Error) {
      errorData = {
        name: error.name,
        message: error.message,
        stack: error.stack?.split('\n').slice(0, 10) // Limit stack trace
      }
    } else if (error) {
      errorData = error
    }
    
    this.log(LogLevel.ERROR, 'ERROR', colors.red, message, errorData)
  }

  /**
   * Performance logging for timing operations
   */
  perf(operation: string, duration: number, data?: any): void {
    const durationStr = duration > 1000 
      ? `${(duration / 1000).toFixed(2)}s` 
      : `${duration}ms`
    
    const perfData = { duration: durationStr, ...data }
    this.log(LogLevel.INFO, 'PERF', colors.magenta, `${operation}`, perfData)
  }

  /**
   * Progress logging for long-running operations
   */
  progress(message: string, current: number, total: number): void {
    const percentage = ((current / total) * 100).toFixed(1)
    const progressMessage = `${message} ${current}/${total} (${percentage}%)`
    
    this.log(LogLevel.INFO, 'PROGRESS', colors.cyan, progressMessage)
  }

  /**
   * Network/RPC operation logging
   */
  rpc(method: string, params?: any, duration?: number): void {
    const data: any = { method }
    if (params) data.params = params
    if (duration !== undefined) {
      data.duration = duration > 1000 ? `${(duration / 1000).toFixed(2)}s` : `${duration}ms`
    }
    
    this.log(LogLevel.VERBOSE, 'RPC', colors.blue, `${method}`, data)
  }

  /**
   * Database operation logging
   */
  db(operation: string, table?: string, duration?: number, extra?: any): void {
    const data: any = { operation }
    if (table) data.table = table
    if (duration !== undefined) {
      data.duration = duration > 1000 ? `${(duration / 1000).toFixed(2)}s` : `${duration}ms`
    }
    if (extra) Object.assign(data, extra)
    
    this.log(LogLevel.VERBOSE, 'DB', colors.yellow, operation, data)
  }

  /**
   * Indexing operation logging
   */
  index(message: string, stats: {
    fromBlock?: bigint
    toBlock?: bigint
    deposits?: number
    withdrawals?: number
    transfers?: number
    duration?: number
  }): void {
    const data: any = {}
    
    if (stats.fromBlock !== undefined && stats.toBlock !== undefined) {
      data.blockRange = `${stats.fromBlock} → ${stats.toBlock}`
      data.blocks = Number(stats.toBlock - stats.fromBlock + 1n)
    }
    
    if (stats.deposits !== undefined) data.deposits = stats.deposits
    if (stats.withdrawals !== undefined) data.withdrawals = stats.withdrawals  
    if (stats.transfers !== undefined) data.transfers = stats.transfers
    
    if (stats.duration !== undefined) {
      data.duration = stats.duration > 1000 
        ? `${(stats.duration / 1000).toFixed(2)}s` 
        : `${stats.duration}ms`
    }
    
    this.log(LogLevel.INFO, 'INDEX', colors.green, message, data)
  }

  /**
   * Creates a child logger with additional context
   */
  child(context: string): Logger {
    const childLogger = new Logger(this.config)
    const originalLog = childLogger.log.bind(childLogger)
    
    childLogger.log = (level: LogLevel, levelName: string, color: string, message: string, data?: any) => {
      const contextMessage = `[${context}] ${message}`
      originalLog(level, levelName, color, contextMessage, data)
    }
    
    return childLogger
  }

  /**
   * Sets the log level
   */
  setLevel(level: LogLevel): void {
    this.config.level = level
  }

  /**
   * Enables or disables colors
   */
  setColors(enabled: boolean): void {
    this.config.enableColors = enabled
  }

  /**
   * Enables or disables timestamps
   */
  setTimestamps(enabled: boolean): void {
    this.config.enableTimestamps = enabled
  }

  /**
   * Enables or disables structured logging
   */
  setStructured(enabled: boolean): void {
    this.config.enableStructured = enabled
  }
}

/**
 * Default logger instance
 */
export const logger = new Logger()

/**
 * Create a new logger instance with custom configuration
 */
export function createLogger(config?: Partial<LoggerConfig>): Logger {
  return new Logger(config)
}

/**
 * Configure the default logger from environment variables
 */
export function configureLoggerFromEnv(): void {
  const logLevel = process.env.LOG_LEVEL?.toLowerCase()
  const noColor = process.env.NO_COLOR || process.env.CI
  
  switch (logLevel) {
    case 'verbose':
      logger.setLevel(LogLevel.VERBOSE)
      break
    case 'debug':
      logger.setLevel(LogLevel.DEBUG)
      break
    case 'info':
      logger.setLevel(LogLevel.INFO)
      break
    case 'warn':
      logger.setLevel(LogLevel.WARN)
      break
    case 'error':
      logger.setLevel(LogLevel.ERROR)
      break
    case 'silent':
      logger.setLevel(LogLevel.SILENT)
      break
  }
  
  if (noColor) {
    logger.setColors(false)
  }
}