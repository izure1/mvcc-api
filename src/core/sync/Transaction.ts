import type { SyncMVCCStrategy } from './Strategy'
import type { TransactionResult, TransactionMergeFailure } from '../../types'
import { MVCCTransaction } from '../base'

export class SyncMVCCTransaction<
  S extends SyncMVCCStrategy<K, T>,
  K,
  T
> extends MVCCTransaction<S, K, T> {
  create(key: K, value: T): this {
    if (this.committed) throw new Error('Transaction already committed')
    if (this.writeBuffer.has(key) || (!this.deleteBuffer.has(key) && this.read(key) !== null)) {
      throw new Error(`Key already exists: ${key}`)
    }
    this._bufferCreate(key, value)
    return this
  }

  write(key: K, value: T): this {
    if (this.committed) throw new Error('Transaction already committed')
    if (!this.writeBuffer.has(key) && (this.deleteBuffer.has(key) || this.read(key) === null)) {
      throw new Error(`Key not found: ${key}`)
    }
    this._bufferWrite(key, value)
    return this
  }

  delete(key: K): this {
    if (this.committed) throw new Error('Transaction already committed')
    let valueToDelete: T | null = null
    let wasInWriteBuffer = false
    if (this.writeBuffer.has(key)) {
      valueToDelete = this.writeBuffer.get(key)!
      wasInWriteBuffer = true
    }
    else if (!this.deleteBuffer.has(key)) {
      valueToDelete = this.read(key)
    }
    if (valueToDelete === null) {
      throw new Error(`Key not found: ${key}`)
    }
    this.deletedValues.set(key, valueToDelete)
    if (!wasInWriteBuffer || !this.createdKeys.has(key)) {
      this.originallyExisted.add(key)
    }
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
    if (this.writeBuffer.has(key)) return this.writeBuffer.get(key)!
    if (this.deleteBuffer.has(key)) return null
    if (this.parent) {
      return this.parent._readSnapshot(key, this.snapshotVersion, this.snapshotLocalVersion) as T | null
    }
    return this._diskRead(key, this.snapshotVersion)
  }

  exists(key: K): boolean {
    if (this.committed) throw new Error('Transaction already committed')
    if (this.deleteBuffer.has(key)) return false
    if (this.writeBuffer.has(key)) return true
    if (this.parent) {
      return this.parent._existsSnapshot(key, this.snapshotVersion, this.snapshotLocalVersion) as boolean
    }
    return this._diskExists(key, this.snapshotVersion)
  }

  _existsSnapshot(key: K, snapshotVersion: number, snapshotLocalVersion?: number): boolean {
    // 1. 버퍼 직접 확인 (스냅샷 시점 이전에 존재한 것이면 모두 허용)
    if (this.writeBuffer.has(key)) {
      const keyModVersion = this.keyVersions.get(key)!
      if (snapshotLocalVersion === undefined || keyModVersion <= snapshotLocalVersion) {
        return true
      }
    }
    if (this.deleteBuffer.has(key)) {
      const keyModVersion = this.keyVersions.get(key)!
      if (snapshotLocalVersion === undefined || keyModVersion <= snapshotLocalVersion) {
        return false
      }
    }

    // 2. 이력 확인 (과거 스냅샷 버전 탐색)
    const history = this.bufferHistory.get(key)
    if (history && snapshotLocalVersion !== undefined) {
      for (let i = history.length - 1; i >= 0; i--) {
        if (history[i].version <= snapshotLocalVersion) {
          return history[i].exists
        }
      }
    }

    // 3. 부모 또는 디스크로 전이
    if (this.parent) {
      return this.parent._existsSnapshot(key, snapshotVersion, this.snapshotLocalVersion) as boolean
    }
    else {
      return this._diskExists(key, snapshotVersion)
    }
  }

  _readSnapshot(key: K, snapshotVersion: number, snapshotLocalVersion?: number): T | null {
    // 1. 버퍼 직접 확인 (스냅샷 시점 이전에 존재한 것이면 모두 허용)
    if (this.writeBuffer.has(key)) {
      const keyModVersion = this.keyVersions.get(key)!
      if (snapshotLocalVersion === undefined || keyModVersion <= snapshotLocalVersion) {
        return this.writeBuffer.get(key)!
      }
    }
    if (this.deleteBuffer.has(key)) {
      const keyModVersion = this.keyVersions.get(key)!
      if (snapshotLocalVersion === undefined || keyModVersion <= snapshotLocalVersion) {
        return null
      }
    }

    // 2. 이력 확인 (과거 스냅샷 버전 탐색)
    const history = this.bufferHistory.get(key)
    if (history && snapshotLocalVersion !== undefined) {
      for (let i = history.length - 1; i >= 0; i--) {
        if (history[i].version <= snapshotLocalVersion) {
          return history[i].exists ? history[i].value : null
        }
      }
    }

    if (this.parent) {
      return this.parent._readSnapshot(key, snapshotVersion, this.snapshotLocalVersion) as T | null
    }
    else {
      return this._diskRead(key, snapshotVersion)
    }
  }

  commit(label?: string): TransactionResult<K, T> {
    const { created, updated, deleted } = this.getResultEntries()
    if (this.committed) {
      return {
        label,
        success: false,
        error: 'Transaction already committed',
        conflict: undefined,
        created,
        updated,
        deleted
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
        deleted
      }
    }

    if (this.parent) {
      const failure = this.parent._merge(this) as TransactionMergeFailure<K, T> | null
      if (failure) {
        return {
          label,
          success: false,
          error: failure.error,
          conflict: failure.conflict,
          created,
          updated,
          deleted
        }
      }
      this.committed = true
    }
    else {
      if (this.writeBuffer.size > 0 || this.deleteBuffer.size > 0) {
        const failure = this._merge(this) as TransactionMergeFailure<K, T> | null
        if (failure) {
          return {
            label,
            success: false,
            error: failure.error,
            conflict: failure.conflict,
            created: [],
            updated: [],
            deleted: []
          }
        }
        this.writeBuffer.clear()
        this.deleteBuffer.clear()
        this.createdKeys.clear()
        this.deletedValues.clear()
        this.originallyExisted.clear()
        this.keyVersions.clear()
        this.bufferHistory.clear()
        this.localVersion = 0;
        (this as any).snapshotVersion = this.version
      }
    }
    return {
      label,
      success: true,
      created,
      updated,
      deleted
    }
  }

  _merge(child: SyncMVCCTransaction<S, K, T>): TransactionMergeFailure<K, T> | null {
    if (this.parent) {
      for (const key of child.writeBuffer.keys()) {
        const lastModLocalVer = this.keyVersions.get(key)
        if (lastModLocalVer !== undefined && lastModLocalVer > child.snapshotLocalVersion) {
          return {
            error: `Commit conflict: Key '${key}' was modified by a newer transaction (Local v${lastModLocalVer})`,
            conflict: {
              key,
              parent: this.read(key) as T,
              child: child.read(key) as T
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
              parent: this.read(key) as T,
              child: child.read(key) as T
            },
          }
        }
      }

      const mergeVersion = ++this.localVersion
      for (const [key, value] of child.writeBuffer) {
        const wasCreated = child.createdKeys.has(key)
        if (wasCreated) this._bufferCreate(key, value, mergeVersion)
        else this._bufferWrite(key, value, mergeVersion)
      }
      for (const key of child.deleteBuffer) {
        const deletedValue = child.deletedValues.get(key)
        if (deletedValue !== undefined) this.deletedValues.set(key, deletedValue)
        if (child.originallyExisted.has(key) && !this.createdKeys.has(key)) {
          this.originallyExisted.add(key)
        }
        this._bufferDelete(key, mergeVersion)
      }

      (this.root as any).activeTransactions.delete(child)
    }
    else {
      if (child !== this) {
        const modifiedKeys = new Set([...child.writeBuffer.keys(), ...child.deleteBuffer])
        for (const key of modifiedKeys) {
          // 1. Global Conflict
          const versions = this.versionIndex.get(key)
          if (versions && versions.length > 0) {
            const lastVer = versions[versions.length - 1].version
            if (lastVer > child.snapshotVersion) {
              return {
                error: `Commit conflict: Key '${key}' was modified by a newer transaction (v${lastVer})`,
                conflict: {
                  key,
                  parent: this.read(key) as T,
                  child: child.read(key) as T
                },
              }
            }
          }
          // 2. Local Conflict (Session buffer conflict)
          const lastModLocalVer = this.keyVersions.get(key)
          if (lastModLocalVer !== undefined && lastModLocalVer > child.snapshotLocalVersion) {
            return {
              error: `Commit conflict: Key '${key}' was modified by a newer transaction in the same session (Local v${lastModLocalVer})`,
              conflict: {
                key,
                parent: this.read(key) as T,
                child: child.read(key) as T
              },
            }
          }
        }

        const mergeVersion = ++this.localVersion
        for (const [key, value] of child.writeBuffer) {
          const wasCreated = child.createdKeys.has(key)
          if (child.originallyExisted.has(key) && !this.createdKeys.has(key)) {
            this.originallyExisted.add(key)
          }
          if (wasCreated) this._bufferCreate(key, value, mergeVersion)
          else this._bufferWrite(key, value, mergeVersion)
        }
        for (const key of child.deleteBuffer) {
          const deletedValue = child.deletedValues.get(key)
          if (deletedValue !== undefined) this.deletedValues.set(key, deletedValue)
          if (child.originallyExisted.has(key) && !this.createdKeys.has(key)) {
            this.originallyExisted.add(key)
          }
          this._bufferDelete(key, mergeVersion)
        }
        (this.root as any).activeTransactions.delete(child)
      }
      else {
        const newVersion = this.version + 1
        for (const [key, value] of this.writeBuffer) this._diskWrite(key, value, newVersion)
        for (const key of this.deleteBuffer) this._diskDelete(key, newVersion)
        this.version = newVersion
        this._cleanupDeletedCache()
      }
    }
    return null
  }

  _diskWrite(key: K, value: T, version: number): void {
    const strategy = this.strategy
    if (!strategy) throw new Error('Root Transaction missing strategy')
    if (strategy.exists(key)) {
      const currentVal = strategy.read(key)
      if (!this.deletedCache.has(key)) this.deletedCache.set(key, [])
      this.deletedCache.get(key)!.push({ value: currentVal, deletedAtVersion: version })
    }
    strategy.write(key, value)
    if (!this.versionIndex.has(key)) this.versionIndex.set(key, [])
    this.versionIndex.get(key)!.push({ version, exists: true })
  }

  _diskRead(key: K, snapshotVersion: number): T | null {
    const strategy = this.strategy
    if (!strategy) throw new Error('Root Transaction missing strategy')
    const versions = this.versionIndex.get(key)
    if (!versions) {
      return strategy.exists(key) ? strategy.read(key) : null
    }

    let targetVerObj: { version: number; exists: boolean } | null = null
    let nextVerObj: { version: number; exists: boolean } | null = null
    for (const v of versions) {
      if (v.version <= snapshotVersion) targetVerObj = v
      else { nextVerObj = v; break }
    }

    if (!targetVerObj) {
      if (nextVerObj) {
        const cached = this.deletedCache.get(key)
        if (cached) {
          const match = cached.find(c => c.deletedAtVersion === nextVerObj!.version)
          if (match) return match.value
        }
      }
      return null
    }
    if (!targetVerObj.exists) return null
    if (!nextVerObj) {
      if (this.writeBuffer.has(key)) return this.writeBuffer.get(key)!
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

    let targetVerObj: { version: number; exists: boolean } | null = null
    let nextVerObj: { version: number; exists: boolean } | null = null
    for (const v of versions) {
      if (v.version <= snapshotVersion) targetVerObj = v
      else { nextVerObj = v; break }
    }

    if (!targetVerObj) {
      if (nextVerObj) {
        const cached = this.deletedCache.get(key)
        if (cached) {
          const match = cached.find(c => c.deletedAtVersion === nextVerObj!.version)
          if (match) return true
        }
      }
      return false
    }
    return targetVerObj.exists
  }

  _diskDelete(key: K, snapshotVersion: number): void {
    const strategy = this.strategy
    if (!strategy) throw new Error('Root Transaction missing strategy')
    if (strategy.exists(key)) {
      const currentVal = strategy.read(key)
      if (!this.deletedCache.has(key)) this.deletedCache.set(key, [])
      this.deletedCache.get(key)!.push({ value: currentVal, deletedAtVersion: snapshotVersion })
      strategy.delete(key)
    }
    if (!this.versionIndex.has(key)) this.versionIndex.set(key, [])
    this.versionIndex.get(key)!.push({ version: snapshotVersion, exists: false })
  }
}
