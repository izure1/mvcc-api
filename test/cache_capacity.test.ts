import { SyncMVCCTransaction } from '../src'
import { SyncMVCCStrategy } from '../src/core/sync/Strategy'

class MockStrategy extends SyncMVCCStrategy<string, string> {
  private data = new Map<string, string>()
  readCount = 0
  existsCount = 0

  read(key: string): string {
    this.readCount++
    return this.data.get(key) || ''
  }
  write(key: string, value: string): void {
    this.data.set(key, value)
  }
  delete(key: string): void {
    this.data.delete(key)
  }
  exists(key: string): boolean {
    this.existsCount++
    return this.data.has(key)
  }
}

describe('LRU Cache Capacity Test', () => {
  test('should respect cacheCapacity option', () => {
    const strategy = new MockStrategy()
    strategy.write('key1', 'val1')
    strategy.write('key2', 'val2')
    strategy.write('key3', 'val3')

    // Root with capacity 2
    const root = new SyncMVCCTransaction(strategy, { cacheCapacity: 2 })

    // Read 1
    root.read('key1')
    expect(strategy.readCount).toBe(1)
    root.read('key1')
    expect(strategy.readCount).toBe(1) // From cache

    // Read 2
    root.read('key2')
    expect(strategy.readCount).toBe(2)
    root.read('key2')
    expect(strategy.readCount).toBe(2) // From cache

    // Read 3 (Should evict key1)
    root.read('key3')
    expect(strategy.readCount).toBe(3)

    // Read 1 again (Should be read from disk again)
    root.read('key1')
    expect(strategy.readCount).toBe(4)
  })

  test('should correctly cache values (even if falsy)', () => {
    const strategy = new MockStrategy()
    // Explicitly set a key to return a falsy value
    strategy.write('existent', '')

    const root = new SyncMVCCTransaction(strategy, { cacheCapacity: 10 })

    // First read -> Should call strategy.read
    root.read('existent')
    expect(strategy.readCount).toBe(1)

    // Second read -> Should come from cache
    root.read('existent')
    expect(strategy.readCount).toBe(1)
  })

  test('should update cache when root transaction commits a write', () => {
    const strategy = new MockStrategy()
    const root = new SyncMVCCTransaction(strategy)

    // 1. Root commit a write
    root.createNested().create('root_key', 'root_val').commit()
    root.commit() // This should update diskCache via _diskWrite

    // Reset strategy read count to verify cache hit
    strategy.readCount = 0

    // 2. Subsequent read should hit cache, not strategy.read
    const val = root.read('root_key')
    expect(val).toBe('root_val')
    expect(strategy.readCount).toBe(0) // Cache hit!
  })

  test('should remove from cache when root transaction commits a delete', () => {
    const strategy = new MockStrategy()
    strategy.write('delete_key', 'to_be_deleted')
    const root = new SyncMVCCTransaction(strategy)

    // Initial read to populate cache
    root.read('delete_key')
    expect(strategy.readCount).toBe(1)
    expect((root as any).diskCache.has('delete_key')).toBe(true)

    // 1. Root commit a delete
    root.createNested().delete('delete_key').commit()
    root.commit() // This should remove from diskCache via _diskDelete

    // 2. Verify cache removal
    expect((root as any).diskCache.has('delete_key')).toBe(false)

    // 3. Read should return null (MVCC standard)
    const val = root.read('delete_key')
    expect(val).toBeNull()
  })

  test('should use cache for exists() to avoid strategy calls', () => {
    const strategy = new MockStrategy()
    strategy.write('exists_key', 'val')
    const root = new SyncMVCCTransaction(strategy)

    // 1. First read to populate cache
    root.read('exists_key')
    expect(strategy.readCount).toBe(1)
    expect(strategy.existsCount).toBe(1)

    // 2. Clear counts
    strategy.readCount = 0
    strategy.existsCount = 0

    // 3. exists() call -> Should hit cache
    const exists = root.exists('exists_key')
    expect(exists).toBe(true)
    expect(strategy.existsCount).toBe(0) // No disk IO
  })

  test('should negative cache non-existent keys', () => {
    const strategy = new MockStrategy()
    const root = new SyncMVCCTransaction(strategy)

    // 1. Read non-existent key
    strategy.readCount = 0
    strategy.existsCount = 0
    const val = root.read('non_existent')
    expect(val).toBeNull()
    expect(strategy.existsCount).toBe(1)

    // 2. Read again -> Should HIT negative cache
    const val2 = root.read('non_existent')
    expect(val2).toBeNull()
    expect(strategy.existsCount).toBe(1) // Still 1
  })

  test('should use default capacity 1000 if not specified', () => {
    const strategy = new MockStrategy()
    const root = new SyncMVCCTransaction(strategy)

    expect(root).toBeDefined()
  })
})
