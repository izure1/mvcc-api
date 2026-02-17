import type { Deferred, TransactionResult, TransactionEntry, TransactionMergeFailure, MVCCOptions } from '../../types'
import type { MVCCStrategy } from './Strategy'
import { LRUMap } from '../../utils/LRUMap'

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
  public readonly snapshotVersion: number
  protected readonly snapshotLocalVersion: number
  protected readonly writeBuffer: Map<K, T>
  protected readonly deleteBuffer: Set<K>
  protected readonly createdKeys: Set<K>
  protected readonly deletedValues: Map<K, T> // delete 시 삭제 전 값 저장
  protected readonly originallyExisted: Set<K> // 트랜잭션 시작 시점에 디스크에 존재했던 키 (deleted 결과 필터링용)
  protected readonly bufferHistory: Map<K, Array<{ value: T | null, exists: boolean, version: number }>> = new Map()

  // Nested Transaction Properties
  protected readonly parent?: MVCCTransaction<S, K, T>
  public localVersion: number // Local version for Nested Conflict Detection
  protected readonly keyVersions: Map<K, number> // Key -> Local Version (When it was modified locally)

  // Root Transaction Properties (Only populated if this is Root)
  readonly root: MVCCTransaction<S, K, T>
  protected strategy?: S
  protected version: number = 0
  protected versionIndex: Map<K, Array<{ version: number, exists: boolean }>> = new Map()
  protected deletedCache: Map<K, Array<{ value: T, deletedAtVersion: number }>> = new Map()
  protected activeTransactions: Set<MVCCTransaction<S, K, T>> = new Set()
  protected readonly diskCache: LRUMap<K, T | null>

  constructor(strategy?: S, options?: MVCCOptions, parent?: MVCCTransaction<S, K, T>, snapshotVersion?: number) {
    this.snapshotVersion = snapshotVersion ?? 0
    this.writeBuffer = new Map()
    this.deleteBuffer = new Set()
    this.createdKeys = new Set()
    this.deletedValues = new Map()
    this.originallyExisted = new Set()
    this.committed = false
    this.parent = parent
    this.keyVersions = new Map()

    if (parent) {
      this.localVersion = parent.localVersion
      this.snapshotLocalVersion = parent.localVersion
      this.strategy = undefined
      this.root = parent.root
      this.diskCache = parent.diskCache
    }
    else {
      if (!strategy) throw new Error('Root Transaction must get Strategy')
      this.strategy = strategy
      this.version = 0
      this.localVersion = 0
      this.snapshotLocalVersion = 0
      this.root = this
      this.diskCache = new LRUMap<K, T | null>(options?.cacheCapacity ?? 1000)
    }
  }

  /**
   * Checks if the transaction is a root transaction.
   * @returns True if the transaction is a root transaction, false otherwise.
   */
  isRoot(): boolean {
    return !this.parent
  }

  /**
   * Checks if any ancestor transaction has already been committed.
   * A nested transaction cannot commit if its parent or any higher ancestor is committed.
   * @returns True if at least one ancestor is committed, false otherwise.
   */
  hasCommittedAncestor(): boolean {
    let current: MVCCTransaction<S, K, T> | undefined = this.parent
    while (current) {
      if (current.committed) return true
      current = current.parent
    }
    return false
  }

  /**
   * Checks if a key was written in this transaction.
   * @param key The key to check.
   * @returns True if the key was written in this transaction, false otherwise.
   */
  isWrote(key: K): boolean {
    return this.writeBuffer.has(key)
  }

  /**
   * Checks if a key was deleted in this transaction.
   * @param key The key to check.
   * @returns True if the key was deleted in this transaction, false otherwise.
   */
  isDeleted(key: K): boolean {
    return this.deleteBuffer.has(key)
  }

  /**
   * Schedules a creation (insert) of a key-value pair.
   * Throws if the key already exists.
   * @param key The key to create.
   * @param value The value to store.
   * @returns The transaction instance for chaining.
   */
  abstract create(key: K, value: T): Deferred<this>

  /**
   * Schedules a write (update) of a key-value pair.
   * Throws if the key does not exist.
   * @param key The key to write.
   * @param value The value to store.
   * @returns The transaction instance for chaining.
   */
  abstract write(key: K, value: T): Deferred<this>

  /**
   * Schedules a deletion of a key.
   * Throws if the key does not exist.
   * @param key The key to delete.
   * @returns The transaction instance for chaining.
   */
  abstract delete(key: K): Deferred<this>

  protected _recordHistory(key: K): void {
    const existsInWriteBuffer = this.writeBuffer.has(key)
    const existsInDeleteBuffer = this.deleteBuffer.has(key)
    const currentVer = this.keyVersions.get(key)

    if (currentVer !== undefined) {
      if (!this.bufferHistory.has(key)) this.bufferHistory.set(key, [])
      this.bufferHistory.get(key)!.push({
        value: existsInWriteBuffer ? this.writeBuffer.get(key)! : (this.deletedValues.get(key) ?? null),
        exists: existsInWriteBuffer || !existsInDeleteBuffer,
        version: currentVer
      })
    }
  }

  /**
   * BINARY SEARCH HELPER: Finds the index of the last element in the array 
   * where item[key] <= target. Assumes the array is sorted by 'key' ascending.
   */
  protected _findLastLE<E, P extends keyof E>(array: E[], target: number, property: P): number {
    let left = 0
    let right = array.length - 1
    let result = -1

    while (left <= right) {
      const mid = (left + right) >> 1
      if ((array[mid][property] as any) <= target) {
        result = mid
        left = mid + 1
      }
      else {
        right = mid - 1
      }
    }

    return result
  }

  /**
   * BINARY SEARCH HELPER: Finds the index of the element in the array 
   * where item[key] === target. Assumes the array is sorted by 'key' ascending.
   */
  protected _findExact<E, P extends keyof E>(array: E[], target: number, property: P): number {
    let left = 0
    let right = array.length - 1

    while (left <= right) {
      const mid = (left + right) >> 1
      const val = array[mid][property] as any
      if (val === target) return mid
      if (val < target) left = mid + 1
      else right = mid - 1
    }

    return -1
  }

  protected _bufferCreate(key: K, value: T, version?: number): void {
    if (version === undefined) this.localVersion++
    const targetVersion = version ?? this.localVersion

    this._recordHistory(key)

    this.writeBuffer.set(key, value)
    this.createdKeys.add(key)
    this.deleteBuffer.delete(key)
    this.originallyExisted.delete(key)
    this.keyVersions.set(key, targetVersion)
  }

  protected _bufferWrite(key: K, value: T, version?: number): void {
    if (version === undefined) this.localVersion++
    const targetVersion = version ?? this.localVersion

    this._recordHistory(key)

    this.writeBuffer.set(key, value)
    this.deleteBuffer.delete(key)
    this.keyVersions.set(key, targetVersion)
  }

  protected _bufferDelete(key: K, version?: number): void {
    if (version === undefined) this.localVersion++
    const targetVersion = version ?? this.localVersion

    this._recordHistory(key)

    this.deleteBuffer.add(key)
    this.writeBuffer.delete(key)
    this.createdKeys.delete(key)
    this.keyVersions.set(key, targetVersion)
  }

  /**
   * Returns the entries that will be created, updated, and deleted by this transaction.
   * @returns An object containing arrays of created, updated, and deleted entries.
   */
  getResultEntries(): {
    created: TransactionEntry<K, T>[]
    updated: TransactionEntry<K, T>[]
    deleted: TransactionEntry<K, T>[]
  } {
    const created: TransactionEntry<K, T>[] = []
    const updated: TransactionEntry<K, T>[] = []
    for (const [key, data] of this.writeBuffer.entries()) {
      if (this.createdKeys.has(key)) {
        created.push({ key, data })
      }
      else {
        updated.push({ key, data })
      }
    }
    const deleted: TransactionEntry<K, T>[] = []
    for (const key of this.deleteBuffer) {
      if (!this.originallyExisted.has(key)) continue
      const data = this.deletedValues.get(key)
      if (data !== undefined) {
        deleted.push({ key, data })
      }
    }
    return {
      created,
      updated,
      deleted
    }
  }

  /**
   * Rolls back the transaction.
   * Clears all buffers and marks the transaction as finished.
   * @returns The result object with success, created, updated, and deleted keys.
   */
  rollback(): TransactionResult<K, T> {
    const { created, updated, deleted } = this.getResultEntries()

    this.writeBuffer.clear()
    this.deleteBuffer.clear()
    this.createdKeys.clear()
    this.deletedValues.clear()
    this.originallyExisted.clear()
    this.committed = true

    // Deregister from Root's active transactions for GC
    if (this.root !== this) {
      (this.root as any).activeTransactions.delete(this)
    }

    return {
      success: true,
      created,
      updated,
      deleted
    }
  }

  /**
   * Reads a value respecting the transaction's snapshot and local changes.
   * @param key The key to read.
   * @returns The value, or null if not found.
   */
  abstract read(key: K): Deferred<T | null>

  /**
   * Checks if a key exists in the transaction's snapshot.
   * @param key The key to check.
   * @returns True if the key exists, false otherwise.
   */
  abstract exists(key: K): Deferred<boolean>

  /**
   * Commits the transaction.
   * If root, persists to storage.
   * If nested, merges to parent.
   * @param label The label for the commit.
   * @returns The result object with success, created, and obsolete keys.
   */
  abstract commit(label?: string): Deferred<TransactionResult<K, T>>

  /**
   * Creates a nested transaction (child) from this transaction.
   * @returns A new nested transaction instance.
   */
  abstract createNested(): MVCCTransaction<S, K, T>

  /**
   * Merges a child transaction's changes into this transaction.
   * @param child The committed child transaction.
   * @returns Error message if conflict, null if success.
   */
  abstract _merge(child: MVCCTransaction<S, K, T>): Deferred<TransactionMergeFailure<K, T> | null>

  /**
   * Reads a value at a specific snapshot version.
   * Used by child transactions to read from parent respecting the child's snapshot.
   * @param key The key to read.
   * @param snapshotVersion The global version to read at.
   * @param snapshotLocalVersion The local version within the parent's buffer to read at.
   */
  abstract _readSnapshot(key: K, snapshotVersion: number, snapshotLocalVersion?: number): Deferred<T | null>

  /**
   * Checks if a key exists at a specific snapshot version.
   * Used by child transactions to check existence from parent respecting the child's snapshot.
   * @param key The key to check.
   * @param snapshotVersion The global version to check at.
   * @param snapshotLocalVersion The local version within the parent's buffer to check at.
   */
  abstract _existsSnapshot(key: K, snapshotVersion: number, snapshotLocalVersion?: number): Deferred<boolean>

  /**
   * Cleans up both deletedCache and versionIndex based on minActiveVersion.
   * Root transactions call this after commit to reclaim memory.
   */
  protected _cleanupDeletedCache(): void {
    if (this.deletedCache.size === 0 && this.versionIndex.size === 0) return

    let minActiveVersion = this.version
    if (this.activeTransactions.size > 0) {
      for (const tx of this.activeTransactions) {
        if (!tx.committed && tx.snapshotVersion < minActiveVersion) {
          minActiveVersion = tx.snapshotVersion
        }
      }
    }

    // deletedCache pruning
    if (this.deletedCache.size > 0) {
      for (const [key, cachedList] of this.deletedCache) {
        const remaining = cachedList.filter(item => item.deletedAtVersion > minActiveVersion)
        if (remaining.length === 0) {
          this.deletedCache.delete(key)
        }
        else {
          this.deletedCache.set(key, remaining)
        }
      }
    }

    // versionIndex pruning
    if (this.versionIndex.size > 0) {
      for (const [key, versions] of this.versionIndex) {
        let latestInSnapshotIdx = -1
        for (let i = versions.length - 1; i >= 0; i--) {
          if (versions[i].version <= minActiveVersion) {
            latestInSnapshotIdx = i
            break
          }
        }

        if (latestInSnapshotIdx === versions.length - 1) {
          // 모든 버전이 커버됨 → 맵에서 키 자체 삭제
          this.versionIndex.delete(key)
        }
        else if (latestInSnapshotIdx > 0) {
          versions.splice(0, latestInSnapshotIdx)
        }
      }
    }
  }
}
