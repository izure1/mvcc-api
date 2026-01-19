import type { Deferred } from '../../types'
import type { MVCCStrategy } from './Strategy'

/**
 * MVCC Transaction abstract class.
 * Represents a logical unit of work that isolates changes until commit.
 * It can be a RootTransaction (interacting with storage) or a NestedTransaction (interacting with parent).
 * @template S The strategy type used by the root transaction.
 * @template K The type of key used for data storage (e.g., string).
 * @template T The type of data stored (e.g., string, Buffer, object).
 */
export abstract class MVCCTransaction<S extends MVCCStrategy<K, T>, K, T> {
  public committed: boolean
  readonly snapshotVersion: number
  readonly snapshotLocalVersion: number
  readonly writeBuffer: Map<K, T>
  readonly deleteBuffer: Set<K>

  // Nested Transaction Properties
  readonly parent?: MVCCTransaction<S, K, T>
  public localVersion: number // Local version for Nested Conflict Detection
  readonly keyVersions: Map<K, number> // Key -> Local Version (When it was modified locally)

  // Root Transaction Properties (Only populated if this is Root)
  readonly root: MVCCTransaction<S, K, T>
  protected strategy?: S
  protected version: number = 0
  protected versionIndex: Map<K, Array<{ version: number, exists: boolean }>> = new Map()
  protected deletedCache: Map<K, Array<{ value: T, deletedAtVersion: number }>> = new Map()
  protected activeTransactions: Set<MVCCTransaction<S, K, T>> = new Set()

  constructor(strategy?: S, parent?: MVCCTransaction<S, K, T>, snapshotVersion?: number) {
    this.snapshotVersion = snapshotVersion ?? 0
    this.writeBuffer = new Map()
    this.deleteBuffer = new Set()
    this.committed = false
    this.parent = parent
    this.keyVersions = new Map()

    if (parent) {
      this.localVersion = parent.localVersion
      this.snapshotLocalVersion = parent.localVersion
      this.strategy = undefined
      this.root = parent.root
    } else {
      if (!strategy) throw new Error('Root Transaction must get Strategy')
      this.strategy = strategy
      this.version = 0
      this.localVersion = 0
      this.snapshotLocalVersion = 0
      this.root = this
    }
  }

  isRoot(): boolean {
    return !this.parent
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
    this.localVersion++
    this.writeBuffer.set(key, value)
    this.deleteBuffer.delete(key)
    this.keyVersions.set(key, this.localVersion)
    return this
  }

  /**
   * Schedules a deletion of a key.
   * @param key The key to delete.
   * @returns The transaction instance for chaining.
   */
  delete(key: K): this {
    if (this.committed) throw new Error('Transaction already committed')
    this.localVersion++
    this.deleteBuffer.add(key)
    this.writeBuffer.delete(key)
    this.keyVersions.set(key, this.localVersion)
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

    // Deregister from Root's active transactions for GC
    if (this.root !== this) {
      (this.root as any).activeTransactions.delete(this)
    }

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
   * If root, persists to storage.
   * If nested, merges to parent.
   * @returns The transaction instance.
   */
  abstract commit(): Deferred<this>

  /**
   * Creates a nested transaction (child) from this transaction.
   * @returns A new nested transaction instance.
   */
  abstract createNested(): MVCCTransaction<S, K, T>

  /**
   * Merges a child transaction's changes into this transaction.
   * @param child The committed child transaction.
   */
  abstract _merge(child: MVCCTransaction<S, K, T>): Deferred<void>

  /**
   * Reads a value at a specific snapshot version.
   * Used by child transactions to read from parent respecting the child's snapshot.
   * @param key The key to read.
   * @param snapshotVersion The global version to read at.
   * @param snapshotLocalVersion The local version within the parent's buffer to read at.
   */
  abstract _readSnapshot(key: K, snapshotVersion: number, snapshotLocalVersion?: number): Deferred<T | null>
}
