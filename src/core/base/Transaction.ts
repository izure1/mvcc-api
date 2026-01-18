import type { Deferred } from '../../types'
import type { MVCCManager } from './Manager'
import type { MVCCStrategy } from './Strategy'

/**
 * MVCC Transaction abstract class.
 * Represents a logical unit of work that isolates changes until commit.
 * It manages its own write/delete buffers and enforces Snapshot Isolation.
 * @template S The strategy type used by the manager.
 * @template K The type of key used for data storage (e.g., string).
 * @template T The type of data stored (e.g., string, Buffer, object).
 * @template M The manager type that created this transaction.
 */
export abstract class MVCCTransaction<S extends MVCCStrategy<K, T>, K, T, M extends MVCCManager<S, K, T>> {
  protected readonly manager: M
  protected committed: boolean
  readonly snapshotVersion: number
  readonly writeBuffer: Map<K, T>
  readonly deleteBuffer: Set<K>

  constructor(manager: M, snapshotVersion: number) {
    this.manager = manager
    this.snapshotVersion = snapshotVersion
    this.writeBuffer = new Map()
    this.deleteBuffer = new Set()
    this.committed = false
  }

  /**
   * Schedules a creation (insert) of a key-value pair.
   * Throws if the transaction is already committed.
   * @param key The key to create.
   * @param value The value to store.
   * @returns The transaction instance for chaining.
   */
  create(key: K, value: T): this {
    if (this.committed) throw new Error('Transaction already committed')
    this.writeBuffer.set(key, value)
    return this
  }

  /**
   * Schedules a write (update) of a key-value pair.
   * Overwrites any existing value in the buffer.
   * @param key The key to write.
   * @param value The value to store.
   * @returns The transaction instance for chaining.
   */
  write(key: K, value: T): this {
    if (this.committed) throw new Error('Transaction already committed')
    this.writeBuffer.set(key, value)
    this.deleteBuffer.delete(key)
    return this
  }

  /**
   * Schedules a deletion of a key.
   * @param key The key to delete.
   * @returns The transaction instance for chaining.
   */
  delete(key: K): this {
    if (this.committed) throw new Error('Transaction already committed')
    this.deleteBuffer.add(key)
    this.writeBuffer.delete(key)
    return this
  }

  /**
   * Rolls back the transaction.
   * Clears all buffers and marks the transaction as finished.
   * @returns The transaction instance.
   */
  rollback(): this {
    this.writeBuffer.clear()
    this.deleteBuffer.clear()
    this.committed = true
    this.manager._removeTransaction(this)
    return this
  }

  /**
   * Reads a value respecting the transaction's snapshot and local changes.
   * @param key The key to read.
   * @returns The value, or null if not found.
   */
  abstract read(key: K): Deferred<T | null>

  /**
   * Commits the transaction.
   * Applies buffered changes to the permanent storage via the manager.
   * @returns The transaction instance.
   */
  abstract commit(): Deferred<this>
}
