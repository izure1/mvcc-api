import type { SyncMVCCStrategy } from './Strategy'
import type { SyncMVCCManager } from './Manager'
import { MVCCTransaction } from '../base'

export class SyncMVCCTransaction<
  S extends SyncMVCCStrategy<K, T>,
  K,
  T,
  M extends SyncMVCCManager<S, K, T>
> extends MVCCTransaction<S, K, T, M> {
  read(key: K): T | null {
    if (this.committed) throw new Error('Transaction already committed')
    // 1. 먼저 로컬 writeBuffer 확인
    if (this.writeBuffer.has(key)) {
      return this.writeBuffer.get(key)!
    }
    // 2. 삭제 버퍼에 있으면 null 반환
    if (this.deleteBuffer.has(key)) {
      return null
    }
    // 3. 더럽혀지지 않았다면 디스크 또는 캐시에서 읽기
    return this.manager._diskRead(key, this.snapshotVersion)
  }

  commit(): this {
    if (this.committed) throw new Error('Transaction already committed')
    this.manager._commit(this)
    this.committed = true
    this.manager._removeTransaction(this)
    return this
  }
}
