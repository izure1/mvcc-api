import type { SyncMVCCStrategy } from './Strategy'
import type { TransactionResult, TransactionEntry } from '../../types'
import { MVCCTransaction } from '../base'

export class SyncMVCCTransaction<
  S extends SyncMVCCStrategy<K, T>,
  K,
  T
> extends MVCCTransaction<S, K, T> {
  create(key: K, value: T): this {
    if (this.committed) throw new Error('Transaction already committed')
    // 이미 버퍼에 있거나 read로 존재하면 오류
    if (this.writeBuffer.has(key) || (!this.deleteBuffer.has(key) && this.read(key) !== null)) {
      throw new Error(`Key already exists: ${key}`)
    }
    this._bufferCreate(key, value)
    return this
  }

  write(key: K, value: T): this {
    if (this.committed) throw new Error('Transaction already committed')
    // 버퍼에 없고 read로도 없으면 오류
    if (!this.writeBuffer.has(key) && (this.deleteBuffer.has(key) || this.read(key) === null)) {
      throw new Error(`Key not found: ${key}`)
    }
    this._bufferWrite(key, value)
    return this
  }

  delete(key: K): this {
    if (this.committed) throw new Error('Transaction already committed')
    // 버퍼에 있으면 그 값을 저장, 아니면 read로 가져옴
    let valueToDelete: T | null = null
    if (this.writeBuffer.has(key)) {
      valueToDelete = this.writeBuffer.get(key)!
    } else if (!this.deleteBuffer.has(key)) {
      valueToDelete = this.read(key)
    }
    if (valueToDelete === null) {
      throw new Error(`Key not found: ${key}`)
    }
    this.deletedValues.set(key, valueToDelete)
    this._bufferDelete(key)
    return this
  }

  createNested(): this {
    if (this.committed) throw new Error('Transaction already committed')
    const childVersion = this.isRoot() ? this.version : this.snapshotVersion
    const child = new SyncMVCCTransaction(undefined, this, childVersion) as this
    (this.root as any).activeTransactions.add(child)
    return child
  }

  read(key: K): T | null {
    if (this.committed) throw new Error('Transaction already committed')
    // 1. 자신의 버퍼에서 먼저 확인
    if (this.writeBuffer.has(key)) return this.writeBuffer.get(key)!
    if (this.deleteBuffer.has(key)) return null

    // 2. 자식 트랜잭션은 부모의 미커밋 버퍼를 볼 수 없음
    //    오직 커밋된 데이터(디스크)만 볼 수 있음
    //    root의 _diskRead를 통해 snapshotVersion 시점의 커밋된 데이터를 읽음
    return (this.root as any)._diskRead(key, this.snapshotVersion)
  }

  exists(key: K): boolean {
    if (this.committed) throw new Error('Transaction already committed')
    // 1. 삭제 버퍼에 있으면 존재하지 않음
    if (this.deleteBuffer.has(key)) return false
    // 2. 쓰기 버퍼에 있으면 존재함
    if (this.writeBuffer.has(key)) return true
    // 3. 디스크에서 확인
    return (this.root as any)._diskExists(key, this.snapshotVersion)
  }

  _readSnapshot(key: K, snapshotVersion: number, snapshotLocalVersion?: number): T | null {
    // 커밋된 root라도 디스크 읽기는 가능해야 함 (자식 트랜잭션이 읽기 가능)

    // 버퍼에서 읽을 때는 snapshotLocalVersion 이전의 변경만 볼 수 있음
    if (this.writeBuffer.has(key)) {
      const keyModVersion = this.keyVersions.get(key)
      // snapshotLocalVersion이 없거나, 키 수정 버전이 스냅샷 이전이면 볼 수 있음
      if (snapshotLocalVersion === undefined || keyModVersion === undefined || keyModVersion <= snapshotLocalVersion) {
        return this.writeBuffer.get(key)!
      }
      // 그렇지 않으면 이 버퍼의 값은 스냅샷 이후에 수정된 것이므로 더 위로 탐색
    }

    if (this.deleteBuffer.has(key)) {
      const keyModVersion = this.keyVersions.get(key)
      if (snapshotLocalVersion === undefined || keyModVersion === undefined || keyModVersion <= snapshotLocalVersion) {
        return null
      }
      // 삭제가 스냅샷 이후면 더 위로 탐색
    }

    if (this.parent) {
      // 재귀적으로 부모로 올라가면서 이 트랜잭션의 snapshotLocalVersion을 전달
      return this.parent._readSnapshot(key, snapshotVersion, this.snapshotLocalVersion) as T | null
    } else {
      // Root Logic: 디스크에서 읽기
      return this._diskRead(key, snapshotVersion)
    }
  }

  commit(): TransactionResult<K, T> {
    if (this.committed) {
      return { success: false, error: 'Transaction already committed', created: [], updated: [], deleted: [] }
    }

    const created: TransactionEntry<K, T>[] = []
    const updated: TransactionEntry<K, T>[] = []
    for (const [key, data] of this.writeBuffer.entries()) {
      if (this.createdKeys.has(key)) {
        created.push({ key, data })
      } else {
        updated.push({ key, data })
      }
    }
    const deleted: TransactionEntry<K, T>[] = []
    for (const key of this.deleteBuffer) {
      const data = this.deletedValues.get(key)
      if (data !== undefined) {
        deleted.push({ key, data })
      }
    }

    if (this.parent) {
      const error = this.parent._merge(this) as string | null
      if (error) {
        return { success: false, error, created: [], updated: [], deleted: [] }
      }
      this.committed = true // Nested 트랜잭션은 커밋 후 사용 불가
    } else {
      // Root Logic: 커밋 후에도 계속 사용 가능
      if (this.writeBuffer.size > 0 || this.deleteBuffer.size > 0) {
        const error = this._merge(this) as string | null
        if (error) {
          return { success: false, error, created: [], updated: [], deleted: [] }
        }
        this.writeBuffer.clear()
        this.deleteBuffer.clear()
        this.createdKeys.clear()
        this.deletedValues.clear()
        this.keyVersions.clear()
        this.localVersion = 0
      }
      // root는 committed를 true로 설정하지 않음 - 재사용 가능
    }

    return { success: true, created, updated, deleted }
  }

  _merge(child: MVCCTransaction<S, K, T>): string | null {
    if (this.parent) {
      // Nested Logic: Merge to self (Parent of Child)
      // 1. Conflict Detection between Siblings (via keyVersions)
      for (const key of child.writeBuffer.keys()) {
        const lastModLocalVer = this.keyVersions.get(key)
        if (lastModLocalVer !== undefined && lastModLocalVer > child.snapshotLocalVersion) {
          return `Commit conflict: Key '${key}' was modified by a newer transaction (Local v${lastModLocalVer})`
        }
      }
      for (const key of child.deleteBuffer) {
        const lastModLocalVer = this.keyVersions.get(key)
        if (lastModLocalVer !== undefined && lastModLocalVer > child.snapshotLocalVersion) {
          return `Commit conflict: Key '${key}' was modified by a newer transaction (Local v${lastModLocalVer})`
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
      }

      (this as any).localVersion = newLocalVersion;
      (this.root as any).activeTransactions.delete(child)

    } else {
      // Root Logic: Persistence

      (this.root as any).activeTransactions.delete(child)

      const newVersion = this.version + 1

      // 1. Conflict Detection (Global)
      const modifiedKeys = new Set([...child.writeBuffer.keys(), ...child.deleteBuffer])
      for (const key of modifiedKeys) {
        const versions = this.versionIndex.get(key)
        if (versions && versions.length > 0) {
          const lastVer = versions[versions.length - 1].version
          if (lastVer > child.snapshotVersion) {
            return `Commit conflict: Key '${key}' was modified by a newer transaction (v${lastVer})`
          }
        }
      }

      // 2. Apply changes to Strategy
      for (const [key, value] of child.writeBuffer) {
        this._diskWrite(key, value, newVersion)
      }
      for (const key of child.deleteBuffer) {
        this._diskDelete(key, newVersion)
      }

      this.version = newVersion

      // 3. Garbage Collection
      this._cleanupDeletedCache()
    }

    return null
  }

  // --- Internal IO Helpers (Root Only) ---

  _diskWrite(key: K, value: T, version: number): void {
    const strategy = this.strategy;
    if (!strategy) throw new Error('Root Transaction missing strategy');
    // Backup for MVCC
    if (strategy.exists(key)) {
      const currentVal = strategy.read(key)
      if (!this.deletedCache.has(key)) this.deletedCache.set(key, [])
      this.deletedCache.get(key)!.push({
        value: currentVal,
        deletedAtVersion: version
      })
    }

    strategy.write(key, value)
    if (!this.versionIndex.has(key)) this.versionIndex.set(key, [])
    this.versionIndex.get(key)!.push({ version, exists: true })
  }

  _diskRead(key: K, snapshotVersion: number): T | null {
    const strategy = this.strategy;
    if (!strategy) throw new Error('Root Transaction missing strategy');
    const versions = this.versionIndex.get(key)
    if (!versions) {
      return strategy.exists(key) ? strategy.read(key) : null
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

  _diskExists(key: K, snapshotVersion: number): boolean {
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

  _diskDelete(key: K, snapshotVersion: number): void {
    const strategy = this.strategy;
    if (!strategy) throw new Error('Root Transaction missing strategy');
    if (strategy.exists(key)) {
      const currentVal = strategy.read(key)
      if (!this.deletedCache.has(key)) this.deletedCache.set(key, [])
      this.deletedCache.get(key)!.push({
        value: currentVal,
        deletedAtVersion: snapshotVersion
      })
    }

    strategy.delete(key)
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
