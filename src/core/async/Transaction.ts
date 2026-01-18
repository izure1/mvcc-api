import type { AsyncMVCCStrategy } from './Strategy'
import type { AsyncMVCCManager } from './Manager'
import { MVCCTransaction } from '../base'

export class AsyncMVCCTransaction<
  T,
  S extends AsyncMVCCStrategy<T>,
  M extends AsyncMVCCManager<T, S>
> extends MVCCTransaction<T, S, M> {
  async read(key: string): Promise<T | null> {
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

  async commit(): Promise<this> {
    return this.manager.writeLock(async () => {
      if (this.committed) throw new Error('Transaction already committed')
      await this.manager._commit(this)
      this.committed = true
      this.manager._removeTransaction(this)
      return this
    })
  }
}
