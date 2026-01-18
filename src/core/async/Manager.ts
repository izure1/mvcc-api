import type { AsyncMVCCStrategy } from './Strategy'
import { Ryoiki } from 'ryoiki'
import { MVCCManager } from '../base'
import { AsyncMVCCTransaction } from './Transaction'

export class AsyncMVCCManager<T, S extends AsyncMVCCStrategy<T>> extends MVCCManager<T, S> {
  readonly lock: Ryoiki

  constructor(strategy: S) {
    super(strategy)
    this.lock = new Ryoiki()
  }

  createTransaction(): AsyncMVCCTransaction<T, S, this> {
    const tx = new AsyncMVCCTransaction(this, this.version) as unknown as AsyncMVCCTransaction<T, S, this>
    this.activeTransactions.add(tx)
    return tx
  }

  async writeLock<R>(fn: () => Promise<R>): Promise<R> {
    let lockId: string
    return this.lock.writeLock(async (_lockId) => {
      lockId = _lockId
      return fn()
    }).finally(() => {
      this.lock.writeUnlock(lockId)
    })
  }

  async _diskWrite(key: string, value: T, version: number): Promise<void> {
    // 덮어쓰기 전 백업 (Copy-on-Write)
    if (await this.strategy.exists(key)) {
      const oldValue = await this.strategy.read(key)
      if (!this.deletedCache.has(key)) {
        this.deletedCache.set(key, [])
      }
      this.deletedCache.get(key)!.push({ value: oldValue, deletedAtVersion: version })
    }
    // 실제 데이터는 즉시 파일에 저장
    await this.strategy.write(key, value)
    // 버전 메타데이터 기록
    if (!this.versionIndex.has(key)) {
      this.versionIndex.set(key, [])
    }
    this.versionIndex.get(key)!.push({ version, exists: true })
  }

  async _diskRead(key: string, snapshotVersion: number): Promise<T | null> {
    // 0. 영속성 지원: 버전 인덱스가 없고 디스크에 파일이 존재하면(재시작 등) 읽기 허용
    if (!this.versionIndex.has(key) && !this.deletedCache.has(key)) {
      if (await this.strategy.exists(key)) {
        return this.strategy.read(key)
      }
      return null
    }

    // 1. 버전 체크: 스냅샷 시점에 파일이 존재했는지 확인
    const versions = this.versionIndex.get(key)
    if (versions && versions.length > 0) {
      // 내 스냅샷보다 작거나 같은 버전 중 가장 최신 버전
      const visibleVersions = versions.filter(v => v.version <= snapshotVersion && v.exists)
      if (visibleVersions.length > 0) {
        // 스냅샷 이후에 더 최신 버전이 생성되었는지 확인 (Dirty Read 방지)
        const newerVersions = versions.filter(v => v.version > snapshotVersion)
        if (newerVersions.length === 0) {
          return this.strategy.read(key)
        }
      }
    }

    // 2. 디스크에 없을 경우(또는 Dirty해서 못 읽는 경우) 삭제/백업 캐시에서 확인
    const cached = this.deletedCache.get(key)
    if (cached) {
      const visibleEntries = cached.filter((v) => v.deletedAtVersion > snapshotVersion)
      if (visibleEntries.length > 0) {
        visibleEntries.sort((a, b) => a.deletedAtVersion - b.deletedAtVersion)
        return visibleEntries[0].value
      }
    }
    return null
  }

  async _diskDelete(key: string, snapshotVersion: number): Promise<void> {
    // 1. 디스크에서 데이터 읽어서 캐시에 보관
    if (await this.strategy.exists(key)) {
      const data = await this.strategy.read(key)
      if (!this.deletedCache.has(key)) {
        this.deletedCache.set(key, [])
      }
      this.deletedCache.get(key)!.push({ deletedAtVersion: snapshotVersion, value: data })
      // 2. 디스크에서 즉시 삭제
      await this.strategy.delete(key)
    }

    // 3. 버전 메타데이터에 삭제 기록
    if (!this.versionIndex.has(key)) {
      this.versionIndex.set(key, [])
    }
    this.versionIndex.get(key)!.push({ version: snapshotVersion, exists: false })
  }

  async _commit(tx: AsyncMVCCTransaction<T, S, this>): Promise<void> {
    const isReadOnly = tx.writeBuffer.size === 0 && tx.deleteBuffer.size === 0
    // 충돌 감지 1: 스냅샷 버전보다 현재 버전이 높으면 다른 트랜잭션이 커밋됨
    if (!isReadOnly && this.version > tx.snapshotVersion) {
      // 읽은 파일이나 쓰려는 파일이 수정되었는지 확인
      const affectedKeys = new Set([
        ...tx.writeBuffer.keys(),
        ...tx.deleteBuffer
      ])
      for (const key of affectedKeys) {
        const versions = this.versionIndex.get(key)
        if (versions && versions.length > 0) {
          const latestVersion = versions[versions.length - 1].version
          if (latestVersion > tx.snapshotVersion) {
            throw new Error(`Commit conflict: file '${key}' was modified by another transaction`)
          }
        }
      }
    }
    // 버전 증가
    this.version++
    // 삭제 먼저 적용
    for (const key of tx.deleteBuffer) {
      await this._diskDelete(key, this.version)
    }
    // 쓰기 적용
    for (const [key, value] of tx.writeBuffer) {
      await this._diskWrite(key, value, this.version)
    }
  }
}
