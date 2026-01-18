import type { SyncMVCCStrategy } from './Strategy'
import { MVCCManager } from '../base'
import { SyncMVCCTransaction } from './Transaction'

export class SyncMVCCManager<T, S extends SyncMVCCStrategy<T>> extends MVCCManager<T, S> {
  constructor(strategy: S) {
    super(strategy)
  }

  createTransaction(): SyncMVCCTransaction<T, S, this> {
    return new SyncMVCCTransaction(this, this.version)
  }

  _diskWrite(key: string, value: T): void {
    this.strategy.write(key, value)
    this.dataVersions.set(key, this.version)
  }

  _diskRead(key: string): T {
    return this.strategy.read(key)
  }

  _diskDelete(key: string): void {
    if (this.strategy.exists(key)) {
      this.deletedCache.set(key, {
        value: this.strategy.read(key),
        deletedAtVersion: this.version,
      })
    }
    this.strategy.delete(key)
    this.dataVersions.set(key, this.version)
  }

  _diskExists(key: string): boolean {
    return this.strategy.exists(key)
  }

  _commit(tx: SyncMVCCTransaction<T, S, this>): void {
    // 충돌 감지: 트랜잭션이 읽은 파일이 다른 트랜잭션에 의해 수정되었는지 확인
    for (const key of tx.readSet) {
      const currentVersion = this.dataVersions.get(key) || -1
      if (currentVersion > tx.snapshotVersion) {
        throw new Error(`Commit conflict: file '${key}' was modified by another transaction`);
      }
    }
    // 쓰기 적용
    for (const [key, value] of tx.writeBuffer) {
      this._diskWrite(key, value)
    }
    // 삭제 적용
    for (const key of tx.deleteBuffer) {
      this._diskDelete(key)
    }
    // 버전 증가
    this.version++
    // 삭제 캐시 정리 (더 이상 참조하는 트랜잭션이 없는 경우)
    this._cleanupDeletedCache()
  }
}
