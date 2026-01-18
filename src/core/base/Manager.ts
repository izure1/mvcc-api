import type { Deferred, DeleteEntry } from '../../types'
import type { MVCCStrategy } from './Strategy'
import type { MVCCTransaction } from './Transaction'

export abstract class MVCCManager<T, S extends MVCCStrategy<T>> {
  version: number
  readonly strategy: S
  protected readonly dataVersions: Map<string, number>
  protected readonly activeTransactions: Set<MVCCTransaction<T, S, this>>
  readonly deletedCache: Map<string, DeleteEntry<T>>

  constructor(strategy: S) {
    this.strategy = strategy
    this.version = 0
    this.dataVersions = new Map()
    this.activeTransactions = new Set()
    this.deletedCache = new Map()
  }

  abstract createTransaction(): MVCCTransaction<T, S, this>
  abstract _diskWrite(key: string, value: T): Deferred<void>
  abstract _diskRead(key: string): Deferred<T>
  abstract _diskDelete(key: string): Deferred<void>
  abstract _diskExists(key: string): Deferred<boolean>
  abstract _commit(tx: MVCCTransaction<T, S, this>): Deferred<void>

  _removeTransaction(tx: MVCCTransaction<T, S, this>): void {
    this.activeTransactions.delete(tx)
    this._cleanupDeletedCache()
  }

  protected _cleanupDeletedCache(): void {
    // 가장 오래된 활성 트랜잭션의 버전 찾기
    let minVersion = this.version
    for (const tx of this.activeTransactions) {
      minVersion = Math.min(minVersion, tx.snapshotVersion)
    }
    // minVersion 이전에 삭제된 항목들은 안전하게 제거 가능
    for (const [key, entry] of this.deletedCache.entries()) {
      if (entry.deletedAtVersion < minVersion) {
        this.deletedCache.delete(key)
      }
    }
  }
}
