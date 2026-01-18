import type { Deferred, DeleteEntry } from '../../types'
import type { MVCCStrategy } from './Strategy'
import type { MVCCTransaction } from './Transaction'

/**
 * MVCC Manager abstract class.
 * Orchestrates transactions, manages the version index, and handles garbage collection.
 * It is responsible for maintaining the consistency of the data and version history.
 * @template S The strategy type used for persistence.
 * @template K The type of key used for data storage (e.g., string).
 * @template T The type of data stored (e.g., string, Buffer, object).
 */
export abstract class MVCCManager<S extends MVCCStrategy<K, T>, K, T> {
  version: number
  readonly strategy: S
  protected readonly versionIndex: Map<K, { version: number, exists: boolean }[]>
  protected readonly activeTransactions: Set<MVCCTransaction<S, K, T, this>>
  protected readonly deletedCache: Map<K, DeleteEntry<T>[]>

  constructor(strategy: S) {
    this.strategy = strategy
    this.version = 0
    this.activeTransactions = new Set()
    this.deletedCache = new Map()
    this.versionIndex = new Map()
  }

  /**
   * Creates a new transaction with the current version.
   * @returns A new accessible transaction instance.
   */
  abstract createTransaction(): MVCCTransaction<S, K, T, this>

  /**
   * Writes data to the persistent storage and indexes the new version.
   * @internal This method is for internal use only and should not be called directly.
   * @param key The key to write.
   * @param value The value to write.
   * @param version The version number for this write.
   */
  abstract _diskWrite(key: K, value: T, version: number): Deferred<void>

  /**
   * Reads data from the persistent storage for a specific snapshot version.
   * @internal This method is for internal use only and should not be called directly.
   * @param key The key to read.
   * @param shapshotVersion The transaction's snapshot version.
   * @returns The data visible to the snapshot version, or null.
   */
  abstract _diskRead(key: K, shapshotVersion: number): Deferred<T | null>

  /**
   * Deletes data from the persistent storage (records a deletion version).
   * @internal This method is for internal use only and should not be called directly.
   * @param key The key to delete.
   * @param snapshotVersion The version at which the deletion occurs.
   */
  abstract _diskDelete(key: K, snapshotVersion: number): Deferred<void>

  /**
   * Commits a transaction.
   * Validates conflicts and applies the transaction's changes.
   * @internal This method is for internal use only and should not be called directly.
   * @param tx The transaction to commit.
   */
  abstract _commit(tx: MVCCTransaction<S, K, T, this>): Deferred<void>

  _removeTransaction(tx: MVCCTransaction<S, K, T, this>): void {
    this.activeTransactions.delete(tx)
    this._cleanupDeletedCache()
  }

  protected _cleanupDeletedCache(): void {
    // 가장 오래된 활성 트랜잭션의 버전 찾기
    let minVersion = this.version
    for (const tx of this.activeTransactions) {
      minVersion = Math.min(minVersion, tx.snapshotVersion)
    }
    // 버전 메타데이터 정리
    for (const [key, versions] of this.versionIndex.entries()) {
      const toKeep = []
      let keptOldVersion = false
      let i = versions.length
      while (i--) {
        const v = versions[i]
        if (v.version > minVersion) {
          toKeep.unshift(v)
        }
        else if (!keptOldVersion) {
          toKeep.unshift(v)
          keptOldVersion = true
        }
      }
      if (toKeep.length === 0) {
        this.versionIndex.delete(key)
      }
      else {
        this.versionIndex.set(key, toKeep)
      }
    }
    // 삭제 캐시 정리
    for (const [key, versions] of this.deletedCache.entries()) {
      const filtered = versions.filter(v => v.deletedAtVersion >= minVersion)
      if (filtered.length === 0) {
        this.deletedCache.delete(key)
      }
      else {
        this.deletedCache.set(key, filtered)
      }
    }
  }
}
