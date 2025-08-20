/**
 * Bounded cache implementation for memory management
 * Implements LRU (Least Recently Used) eviction policy
 */

/**
 * LRU Cache node for doubly linked list
 */
class CacheNode<K, V> {
  constructor(
    public key: K,
    public value: V,
    public prev: CacheNode<K, V> | null = null,
    public next: CacheNode<K, V> | null = null
  ) {}
}

/**
 * LRU Cache implementation with bounded size
 */
export class LRUCache<K, V> {
  protected cache = new Map<K, CacheNode<K, V>>()
  protected head: CacheNode<K, V> | null = null
  protected tail: CacheNode<K, V> | null = null
  protected currentSize = 0

  constructor(private readonly maxSize: number = 10000) {
    if (maxSize <= 0) {
      throw new Error('Cache size must be positive')
    }
  }

  /**
   * Gets a value from cache and moves it to front (most recently used)
   */
  get(key: K): V | undefined {
    const node = this.cache.get(key)
    if (!node) return undefined

    // Move to front (most recently used)
    this.moveToFront(node)
    return node.value
  }

  /**
   * Sets a value in cache, evicting LRU item if necessary
   */
  set(key: K, value: V): void {
    const existingNode = this.cache.get(key)
    
    if (existingNode) {
      // Update existing node
      existingNode.value = value
      this.moveToFront(existingNode)
      return
    }

    // Create new node
    const newNode = new CacheNode(key, value)
    
    // Add to cache
    this.cache.set(key, newNode)
    this.addToFront(newNode)
    this.currentSize++

    // Evict LRU if over capacity
    if (this.currentSize > this.maxSize) {
      this.evictLRU()
    }
  }

  /**
   * Removes a key from cache
   */
  delete(key: K): boolean {
    const node = this.cache.get(key)
    if (!node) return false

    this.cache.delete(key)
    this.removeNode(node)
    this.currentSize--
    return true
  }

  /**
   * Clears all cache entries
   */
  clear(): void {
    this.cache.clear()
    this.head = null
    this.tail = null
    this.currentSize = 0
  }

  /**
   * Returns current cache size
   */
  size(): number {
    return this.currentSize
  }

  /**
   * Returns maximum cache size
   */
  capacity(): number {
    return this.maxSize
  }

  /**
   * Returns cache hit ratio (for monitoring)
   */
  getStats(): { size: number; capacity: number; hitRatio?: number } {
    return {
      size: this.currentSize,
      capacity: this.maxSize
    }
  }

  /**
   * Moves node to front of the list (most recently used)
   */
  protected moveToFront(node: CacheNode<K, V>): void {
    this.removeNode(node)
    this.addToFront(node)
  }

  /**
   * Adds node to front of the list
   */
  protected addToFront(node: CacheNode<K, V>): void {
    node.prev = null
    node.next = this.head

    if (this.head) {
      this.head.prev = node
    }
    this.head = node

    if (!this.tail) {
      this.tail = node
    }
  }

  /**
   * Removes node from the list
   */
  protected removeNode(node: CacheNode<K, V>): void {
    if (node.prev) {
      node.prev.next = node.next
    } else {
      this.head = node.next
    }

    if (node.next) {
      node.next.prev = node.prev
    } else {
      this.tail = node.prev
    }
  }

  /**
   * Evicts least recently used item
   */
  protected evictLRU(): void {
    if (!this.tail) return

    const lruKey = this.tail.key
    this.cache.delete(lruKey)
    this.removeNode(this.tail)
    this.currentSize--
  }
}

/**
 * Memory-aware cache that monitors system memory usage
 */
export class MemoryAwareCache<K, V> extends LRUCache<K, V> {
  private memoryCheckInterval: NodeJS.Timeout | null = null
  private readonly memoryThreshold: number

  constructor(
    maxSize: number = 10000,
    memoryThresholdMB: number = 500 // Start evicting at 500MB
  ) {
    super(maxSize)
    this.memoryThreshold = memoryThresholdMB * 1024 * 1024 // Convert to bytes
    
    // Check memory usage every 30 seconds
    this.memoryCheckInterval = setInterval(() => {
      this.checkMemoryUsage()
    }, 30000)
  }

  /**
   * Checks memory usage and proactively evicts if needed
   */
  private checkMemoryUsage(): void {
    const memUsage = process.memoryUsage()
    
    if (memUsage.heapUsed > this.memoryThreshold) {
      // Evict 25% of cache when memory threshold exceeded
      const itemsToEvict = Math.floor(this.size() * 0.25)
      
      for (let i = 0; i < itemsToEvict && this.size() > 0; i++) {
        super.evictLRU()
      }
      
      // Force garbage collection if available
      if (global.gc) {
        global.gc()
      }
    }
  }

  /**
   * Cleanup resources
   */
  destroy(): void {
    if (this.memoryCheckInterval) {
      clearInterval(this.memoryCheckInterval)
      this.memoryCheckInterval = null
    }
    this.clear()
  }

  /**
   * Enhanced stats with memory information
   */
  getStats(): { 
    size: number
    capacity: number
    memoryUsage: NodeJS.MemoryUsage
    memoryThresholdMB: number
  } {
    return {
      ...super.getStats(),
      memoryUsage: process.memoryUsage(),
      memoryThresholdMB: this.memoryThreshold / (1024 * 1024)
    }
  }

}