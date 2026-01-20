import type { AsyncMVCCStrategy } from './Strategy'
import type { TransactionMergeFailure, TransactionResult } from '../../types'
import { Ryoiki } from 'ryoiki'
import { MVCCTransaction } from '../base'

export class AsyncMVCCTransaction<
  S extends AsyncMVCCStrategy<K, T>,
  K,
  T
> extends MVCCTransaction<S, K, T> {
  private lock: Ryoiki = new Ryoiki()

  private async writeLock<T>(fn: () => Promise<T>): Promise<T> {
    let lockId: string
    return this.lock.writeLock(async (_lockId) => {
      lockId = _lockId
      return fn()
    }).finally(() => {
      this.lock.writeUnlock(lockId)
    })
  }

  async create(key: K, value: T): Promise<this> {
    if (this.committed) throw new Error('Transaction already committed')
    // 이미 버퍼에 있거나 read로 존재하면 오류
    if (this.writeBuffer.has(key) || (!this.deleteBuffer.has(key) && await this.read(key) !== null)) {
      throw new Error(`Key already exists: ${key}`)
    }
    this._bufferCreate(key, value)
    return this
  }

  async write(key: K, value: T): Promise<this> {
    if (this.committed) throw new Error('Transaction already committed')
    // 버퍼에 없고 read로도 없으면 오류
    if (!this.writeBuffer.has(key) && (this.deleteBuffer.has(key) || await this.read(key) === null)) {
      throw new Error(`Key not found: ${key}`)
    }
    this._bufferWrite(key, value)
    return this
  }

  async delete(key: K): Promise<this> {
    if (this.committed) throw new Error('Transaction already committed')
    // 버퍼에 있으면 그 값을 저장, 아니면 read로 가져옴
    let valueToDelete: T | null = null
    let wasInWriteBuffer = false
    if (this.writeBuffer.has(key)) {
      valueToDelete = this.writeBuffer.get(key)!
      wasInWriteBuffer = true
    } else if (!this.deleteBuffer.has(key)) {
      valueToDelete = await this.read(key)
    }
    if (valueToDelete === null) {
      throw new Error(`Key not found: ${key}`)
    }
    this.deletedValues.set(key, valueToDelete)
    // 디스크에서 읽은 값이거나, write한 값이지만 createdKeys에 없으면 (디스크 값을 수정한 것)
    // originallyExisted에 추가 (create→delete는 제외)
    if (!wasInWriteBuffer || !this.createdKeys.has(key)) {
      this.originallyExisted.add(key)
    }
    this._bufferDelete(key)
    return this
  }

  createNested(): this {
    if (this.committed) throw new Error('Transaction already committed')
    const childVersion = this.isRoot() ? this.version : this.snapshotVersion
    const child = new AsyncMVCCTransaction(undefined, this, childVersion) as this
    (this.root as any).activeTransactions.add(child)
    return child
  }

  async read(key: K): Promise<T | null> {
    if (this.committed) throw new Error('Transaction already committed')
    // 1. 자신의 버퍼에서 먼저 확인
    if (this.writeBuffer.has(key)) return this.writeBuffer.get(key)!
    if (this.deleteBuffer.has(key)) return null

    // 2. 자식 트랜잭션은 부모의 미커밋 버퍼를 볼 수 없음
    //    오직 커밋된 데이터(디스크)만 볼 수 있음
    return (this.root as any)._diskRead(key, this.snapshotVersion)
  }

  async exists(key: K): Promise<boolean> {
    if (this.committed) throw new Error('Transaction already committed')
    // 1. 삭제 버퍼에 있으면 존재하지 않음
    if (this.deleteBuffer.has(key)) return false
    // 2. 쓰기 버퍼에 있으면 존재함
    if (this.writeBuffer.has(key)) return true
    // 3. 디스크에서 확인
    return (this.root as any)._diskExists(key, this.snapshotVersion)
  }

  async _readSnapshot(key: K, snapshotVersion: number, snapshotLocalVersion?: number): Promise<T | null> {
    // 커밋된 root라도 디스크 읽기는 가능해야 함 (자식 트랜잭션이 읽기 가능)

    // 버퍼에서 읽을 때는 snapshotLocalVersion 이전의 변경만 볼 수 있음
    if (this.writeBuffer.has(key)) {
      const keyModVersion = this.keyVersions.get(key)
      if (snapshotLocalVersion === undefined || keyModVersion === undefined || keyModVersion <= snapshotLocalVersion) {
        return this.writeBuffer.get(key)!
      }
    }

    if (this.deleteBuffer.has(key)) {
      const keyModVersion = this.keyVersions.get(key)
      if (snapshotLocalVersion === undefined || keyModVersion === undefined || keyModVersion <= snapshotLocalVersion) {
        return null
      }
    }

    if (this.parent) {
      return this.parent._readSnapshot(key, snapshotVersion, this.snapshotLocalVersion) as Promise<T | null>
    } else {
      // Root Logic: 디스크에서 읽기
      return this._diskRead(key, snapshotVersion)
    }
  }

  async commit(label?: string): Promise<TransactionResult<K, T>> {
    return this.writeLock(async () => {
      const { created, updated, deleted } = this._getResultEntries()

      if (this.committed) {
        return {
          label,
          success: false,
          error: 'Transaction already committed',
          conflict: undefined,
          created,
          updated,
          deleted,
        }
      }

      if (this.hasCommittedAncestor()) {
        return {
          label,
          success: false,
          error: 'Ancestor transaction already committed',
          conflict: undefined,
          created,
          updated,
          deleted,
        }
      }

      if (this.parent) {
        const failure = await this.parent._merge(this)
        if (failure) {
          return {
            label,
            success: false,
            error: failure.error,
            conflict: failure.conflict,
            created,
            updated,
            deleted,
          }
        }
        this.committed = true // Nested 트랜잭션은 커밋 후 사용 불가
      } else {
        // Root Logic: 커밋 후에도 계속 사용 가능
        if (this.writeBuffer.size > 0 || this.deleteBuffer.size > 0) {
          const failure = await this._merge(this) as TransactionMergeFailure<K, T> | null
          if (failure) {
            return {
              label,
              success: false,
              error: failure.error,
              conflict: failure.conflict,
              created: [],
              updated: [],
              deleted: [],
            }
          }
          this.writeBuffer.clear()
          this.deleteBuffer.clear()
          this.createdKeys.clear()
          this.deletedValues.clear()
          this.originallyExisted.clear()
          this.keyVersions.clear()
          this.localVersion = 0
        }
        // root는 committed를 true로 설정하지 않음 - 재사용 가능
      }

      return {
        label,
        success: true,
        created,
        updated,
        deleted,
      }
    })
  }

  async _merge(child: MVCCTransaction<S, K, T>): Promise<TransactionMergeFailure<K, T> | null> {
    return this.writeLock(async () => {
      if (this.parent) {
        // Nested Logic: Merge to self (Parent of Child)
        // 1. Conflict Detection between Siblings (via keyVersions)
        for (const key of child.writeBuffer.keys()) {
          const lastModLocalVer = this.keyVersions.get(key)
          if (lastModLocalVer !== undefined && lastModLocalVer > child.snapshotLocalVersion) {
            return {
              error: `Commit conflict: Key '${key}' was modified by a newer transaction (Local v${lastModLocalVer})`,
              conflict: {
                key,
                parent: await this.read(key) as T,
                child: await child._readSnapshot(key, child.snapshotVersion, child.snapshotLocalVersion)! as T,
              },
            }
          }
        }
        for (const key of child.deleteBuffer) {
          const lastModLocalVer = this.keyVersions.get(key)
          if (lastModLocalVer !== undefined && lastModLocalVer > child.snapshotLocalVersion) {
            return {
              error: `Commit conflict: Key '${key}' was modified by a newer transaction (Local v${lastModLocalVer})`,
              conflict: {
                key,
                parent: await this.read(key) as T,
                child: await child._readSnapshot(key, child.snapshotVersion, child.snapshotLocalVersion)! as T,
              },
            }
          }
        }

        // 2. Merge buffers (직접 버퍼에 추가하여 createdKeys 유지)
        const newLocalVersion = this.localVersion + 1
        for (const key of child.writeBuffer.keys()) {
          this.writeBuffer.set(key, child.writeBuffer.get(key)!)
          this.deleteBuffer.delete(key)
          this.keyVersions.set(key, newLocalVersion)
          // 자식이 create한 키면 부모의 createdKeys에도 추가
          if (child.createdKeys.has(key)) {
            this.createdKeys.add(key)
          }
        }
        for (const key of child.deleteBuffer) {
          this.deleteBuffer.add(key)
          this.writeBuffer.delete(key)
          this.createdKeys.delete(key) // 삭제된 키는 created가 아님
          this.keyVersions.set(key, newLocalVersion)
          // 자식의 deletedValues도 부모에게 전달
          const deletedValue = child.deletedValues.get(key)
          if (deletedValue !== undefined) {
            this.deletedValues.set(key, deletedValue)
          }
          // 자식의 originallyExisted도 부모에게 전달
          if (child.originallyExisted.has(key)) {
            this.originallyExisted.add(key)
          }
        }

        (this as any).localVersion = newLocalVersion;
        (this.root as any).activeTransactions.delete(child)

      } else {
        // Root Logic: Persistence
        const newVersion = this.version + 1

        // 1. Conflict Detection (Global)
        const modifiedKeys = new Set([...child.writeBuffer.keys(), ...child.deleteBuffer])
        for (const key of modifiedKeys) {
          const versions = this.versionIndex.get(key)
          if (versions && versions.length > 0) {
            const lastVer = versions[versions.length - 1].version
            if (lastVer > child.snapshotVersion) {
              return {
                error: `Commit conflict: Key '${key}' was modified by a newer transaction (v${lastVer})`,
                conflict: {
                  key,
                  parent: await this.read(key) as T,
                  child: await child._readSnapshot(key, child.snapshotVersion, child.snapshotLocalVersion)! as T,
                },
              }
            }
          }
        }

        // 2. Apply changes to Strategy
        for (const [key, value] of child.writeBuffer) {
          await this._diskWrite(key, value, newVersion)
        }
        for (const key of child.deleteBuffer) {
          await this._diskDelete(key, newVersion)
        }

        this.version = newVersion;
        (this.root as any).activeTransactions.delete(child)

        // 3. Garbage Collection
        this._cleanupDeletedCache()
      }

      return null
    })
  }

  // --- Internal IO Helpers (Root Only) ---

  async _diskWrite(key: K, value: T, version: number): Promise<void> {
    const strategy = this.strategy;
    if (!strategy) throw new Error('Root Transaction missing strategy');
    // Backup for MVCC
    if (await strategy.exists(key)) {
      const currentVal = await strategy.read(key)
      if (!this.deletedCache.has(key)) this.deletedCache.set(key, [])
      this.deletedCache.get(key)!.push({
        value: currentVal,
        deletedAtVersion: version
      })
    }

    await strategy.write(key, value)
    if (!this.versionIndex.has(key)) this.versionIndex.set(key, [])
    this.versionIndex.get(key)!.push({ version, exists: true })
  }

  async _diskRead(key: K, snapshotVersion: number): Promise<T | null> {
    const strategy = this.strategy;
    if (!strategy) throw new Error('Root Transaction missing strategy');
    const versions = this.versionIndex.get(key)
    if (!versions) {
      return (await strategy.exists(key)) ? strategy.read(key) : null
    }

    let targetVerObj: { version: number, exists: boolean } | null = null
    let nextVerObj: { version: number, exists: boolean } | null = null

    for (const v of versions) {
      if (v.version <= snapshotVersion) {
        targetVerObj = v
      } else {
        nextVerObj = v
        break
      }
    }

    if (!targetVerObj || !targetVerObj.exists) return null

    if (!nextVerObj) {
      return strategy.read(key)
    }

    const cached = this.deletedCache.get(key)
    if (cached) {
      const match = cached.find(c => c.deletedAtVersion === nextVerObj!.version)
      if (match) return match.value
    }

    return null
  }

  async _diskExists(key: K, snapshotVersion: number): Promise<boolean> {
    const strategy = this.strategy
    if (!strategy) throw new Error('Root Transaction missing strategy')
    const versions = this.versionIndex.get(key)
    if (!versions) {
      return strategy.exists(key)
    }

    let targetVerObj: { version: number, exists: boolean } | null = null

    for (const v of versions) {
      if (v.version <= snapshotVersion) {
        targetVerObj = v
      } else {
        break
      }
    }

    if (!targetVerObj) return strategy.exists(key)
    return targetVerObj.exists
  }

  async _diskDelete(key: K, snapshotVersion: number): Promise<void> {
    const strategy = this.strategy;
    if (!strategy) throw new Error('Root Transaction missing strategy');
    if (await strategy.exists(key)) {
      const currentVal = await strategy.read(key)
      if (!this.deletedCache.has(key)) this.deletedCache.set(key, [])
      this.deletedCache.get(key)!.push({
        value: currentVal,
        deletedAtVersion: snapshotVersion
      })
    }

    await strategy.delete(key)
    if (!this.versionIndex.has(key)) this.versionIndex.set(key, [])
    this.versionIndex.get(key)!.push({ version: snapshotVersion, exists: false })
  }

  _cleanupDeletedCache(): void {
    if (this.deletedCache.size === 0) return

    let minActiveVersion = this.version
    if (this.activeTransactions.size > 0) {
      for (const tx of this.activeTransactions) {
        if (!tx.committed && tx.snapshotVersion < minActiveVersion) {
          minActiveVersion = tx.snapshotVersion
        }
      }
    }

    for (const [key, cachedList] of this.deletedCache) {
      const remaining = cachedList.filter(item => item.deletedAtVersion > minActiveVersion)
      if (remaining.length === 0) {
        this.deletedCache.delete(key)
      } else {
        this.deletedCache.set(key, remaining)
      }
    }
  }
}
