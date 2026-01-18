import type { SyncMVCCStrategy } from './Strategy'
import type { SyncMVCCManager } from './Manager'
import { MVCCTransaction } from '../base'

export class SyncMVCCTransaction<
  T,
  S extends SyncMVCCStrategy<T>,
  M extends SyncMVCCManager<T, S>
> extends MVCCTransaction<T, S, M> {
  read(key: string): T | null {
    if (this.committed) throw new Error('Transaction already committed')
    this.readSet.add(key)
    // 1. 먼저 로컬 writeBuffer 확인
    if (this.writeBuffer.has(key)) {
      return this.writeBuffer.get(key)!
    }
    // 2. 삭제 버퍼에 있으면 null 반환
    if (this.deleteBuffer.has(key)) {
      return null
    }
    // 3. 디스크에 있으면 반환
    if (this.manager._diskExists(key)) {
      return this.manager._diskRead(key)
    }
    // 4. 삭제된 캐시 확인 (스냅샷 격리)
    const deletedEntry = this.manager.deletedCache.get(key)
    if (deletedEntry && deletedEntry.deletedAtVersion > this.snapshotVersion) {
      // 이 트랜잭션 시작 후에 삭제되었으므로, 스냅샷에는 존재
      return deletedEntry.value
    }
    return null
  }

  commit(): this {
    if (this.committed) throw new Error('Transaction already committed')
    this.manager._commit(this)
    this.committed = true
    this.manager._removeTransaction(this)
    return this
  }
}
