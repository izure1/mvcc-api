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
    if (this.writeBuffer.has(key) || (!this.deleteBuffer.has(key) && await this.read(key) !== null)) {
      throw new Error(`Key already exists: ${key}`)
    }
    this._bufferCreate(key, value)
    return this
  }

  async write(key: K, value: T): Promise<this> {
    if (this.committed) throw new Error('Transaction already committed')
    if (!this.writeBuffer.has(key) && (this.deleteBuffer.has(key) || await this.read(key) === null)) {
      throw new Error(`Key not found: ${key}`)
    }
    this._bufferWrite(key, value)
    return this
  }

  async delete(key: K): Promise<this> {
    if (this.committed) throw new Error('Transaction already committed')
    let valueToDelete: T | null = null
    let wasInWriteBuffer = false
    if (this.writeBuffer.has(key)) {
      valueToDelete = this.writeBuffer.get(key)!
      wasInWriteBuffer = true
    }
    else if (!this.deleteBuffer.has(key)) {
      valueToDelete = await this.read(key)
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
    const child = new AsyncMVCCTransaction(undefined, undefined, this, childVersion) as this
    (this.root as any).activeTransactions.add(child)
    return child
  }

  async read(key: K): Promise<T | null> {
    if (this.committed) throw new Error('Transaction already committed')
    if (this.writeBuffer.has(key)) return this.writeBuffer.get(key)!
    if (this.deleteBuffer.has(key)) return null
    if (this.parent) {
      return this.parent._readSnapshot(key, this.snapshotVersion, this.snapshotLocalVersion) as Promise<T | null>
    }
    return await this._diskRead(key, this.snapshotVersion)
  }

  async exists(key: K): Promise<boolean> {
    if (this.committed) throw new Error('Transaction already committed')
    if (this.deleteBuffer.has(key)) return false
    if (this.writeBuffer.has(key)) return true
    if (this.parent) {
      return this.parent._existsSnapshot(key, this.snapshotVersion, this.snapshotLocalVersion) as Promise<boolean>
    }
    return await this._diskExists(key, this.snapshotVersion)
  }

  async _existsSnapshot(key: K, snapshotVersion: number, snapshotLocalVersion?: number): Promise<boolean> {
    let current: AsyncMVCCTransaction<S, K, T> | undefined = this
    let slVer = snapshotLocalVersion

    while (current) {
      // 1. 버퍼 직접 확인
      if (current.writeBuffer.has(key)) {
        const keyModVersion = current.keyVersions.get(key)!
        if (slVer === undefined || keyModVersion <= slVer) return true
      }
      if (current.deleteBuffer.has(key)) {
        const keyModVersion = current.keyVersions.get(key)!
        if (slVer === undefined || keyModVersion <= slVer) return false
      }

      // 2. 이력 확인
      const history = current.bufferHistory.get(key)
      if (history && slVer !== undefined) {
        const idx = current._findLastLE(history, slVer, 'version')
        if (idx >= 0) return history[idx].exists
      }

      // 3. 부모 또는 디스크로 전이
      if (current.parent) {
        slVer = current.snapshotLocalVersion
        current = current.parent as AsyncMVCCTransaction<S, K, T>
      }
      else {
        return await current._diskExists(key, snapshotVersion)
      }
    }
    return false // Should not reach here
  }

  async _readSnapshot(key: K, snapshotVersion: number, snapshotLocalVersion?: number): Promise<T | null> {
    let current: AsyncMVCCTransaction<S, K, T> | undefined = this
    let slVer = snapshotLocalVersion

    while (current) {
      // 1. 버퍼 직접 확인
      if (current.writeBuffer.has(key)) {
        const keyModVersion = current.keyVersions.get(key)!
        if (slVer === undefined || keyModVersion <= slVer) {
          return current.writeBuffer.get(key)!
        }
      }
      if (current.deleteBuffer.has(key)) {
        const keyModVersion = current.keyVersions.get(key)!
        if (slVer === undefined || keyModVersion <= slVer) {
          return null
        }
      }

      // 2. 이력 확인
      const history = current.bufferHistory.get(key)
      if (history && slVer !== undefined) {
        const idx = current._findLastLE(history, slVer, 'version')
        if (idx >= 0) {
          return history[idx].exists ? history[idx].value : null
        }
      }

      // 3. 부모 또는 디스크로 전이
      if (current.parent) {
        slVer = current.snapshotLocalVersion
        current = current.parent as AsyncMVCCTransaction<S, K, T>
      }
      else {
        return await current._diskRead(key, snapshotVersion)
      }
    }
    return null // Should not reach here
  }

  async commit(label?: string): Promise<TransactionResult<K, T>> {
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
      const failure = await this.parent._merge(this)
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
        const failure = await this._merge(this) as TransactionMergeFailure<K, T> | null
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

  async _merge(child: AsyncMVCCTransaction<S, K, T>): Promise<TransactionMergeFailure<K, T> | null> {
    return this.writeLock(async () => {
      if (this.parent) {
        for (const key of child.writeBuffer.keys()) {
          const lastModLocalVer = this.keyVersions.get(key)
          if (lastModLocalVer !== undefined && lastModLocalVer > child.snapshotLocalVersion) {
            return {
              error: `Commit conflict: Key '${key}' was modified by a newer transaction (Local v${lastModLocalVer})`,
              conflict: {
                key,
                parent: await this.read(key) as T,
                child: await child.read(key) as T
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
                child: await child.read(key) as T
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
        return null
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
                    parent: await this.read(key) as T,
                    child: await child.read(key) as T
                  },
                }
              }
            }
            // 2. Local Conflict
            const lastModLocalVer = this.keyVersions.get(key)
            if (lastModLocalVer !== undefined && lastModLocalVer > child.snapshotLocalVersion) {
              return {
                error: `Commit conflict: Key '${key}' was modified by a newer transaction in the same session (Local v${lastModLocalVer})`,
                conflict: {
                  key,
                  parent: await this.read(key) as T,
                  child: await child.read(key) as T
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
          for (const [key, value] of this.writeBuffer) await this._diskWrite(key, value, newVersion)
          for (const key of this.deleteBuffer) await this._diskDelete(key, newVersion)
          this.version = newVersion
          this._cleanupDeletedCache()
        }
        return null
      }
    })
  }

  async _diskWrite(key: K, value: T, version: number): Promise<void> {
    const strategy = this.strategy
    if (!strategy) throw new Error('Root Transaction missing strategy')
    const rootAsAny = (this.root as any)
    if (await this._diskExists(key, version)) {
      const currentVal = rootAsAny.diskCache.has(key) ? rootAsAny.diskCache.get(key)! : await strategy.read(key)
      if (!this.deletedCache.has(key)) this.deletedCache.set(key, [])
      this.deletedCache.get(key)!.push({ value: currentVal, deletedAtVersion: version })
      // Update cache
      rootAsAny.diskCache.set(key, value)
    }
    else {
      rootAsAny.diskCache.set(key, value)
    }
    await strategy.write(key, value)
    if (!this.versionIndex.has(key)) this.versionIndex.set(key, [])
    this.versionIndex.get(key)!.push({ version, exists: true })
  }

  async _diskRead(key: K, snapshotVersion: number): Promise<T | null> {
    const strategy = this.strategy
    if (!strategy) throw new Error('Root Transaction missing strategy')
    const versions = this.versionIndex.get(key)
    if (!versions) {
      const rootAsAny = (this.root as any)
      if (await this._diskExists(key, snapshotVersion)) {
        const val = rootAsAny.diskCache.has(key) ? rootAsAny.diskCache.get(key)! : await strategy.read(key)
        rootAsAny.diskCache.set(key, val)
        return val
      }
      return null
    }

    let targetVerObj: { version: number; exists: boolean } | null = null
    let nextVerObj: { version: number; exists: boolean } | null = null
    const idx = this._findLastLE(versions, snapshotVersion, 'version')
    if (idx >= 0) targetVerObj = versions[idx]
    if (idx + 1 < versions.length) nextVerObj = versions[idx + 1]

    if (!targetVerObj) {
      if (nextVerObj) {
        const cached = this.deletedCache.get(key)
        if (cached) {
          const cIdx = this._findExact(cached, nextVerObj.version, 'deletedAtVersion')
          if (cIdx >= 0) return cached[cIdx].value
        }
      }
      return null
    }
    if (!targetVerObj.exists) return null
    if (!nextVerObj) {
      if (this.writeBuffer.has(key)) return this.writeBuffer.get(key)!
      if (await this._diskExists(key, snapshotVersion)) {
        const rootAsAny = this.root as any
        const val = rootAsAny.diskCache.has(key) ? rootAsAny.diskCache.get(key)! : await strategy.read(key)
        rootAsAny.diskCache.set(key, val)
        return val
      }
      return null
    }
    const cached = this.deletedCache.get(key)
    if (cached) {
      const cIdx = this._findExact(cached, nextVerObj.version, 'deletedAtVersion')
      if (cIdx >= 0) return cached[cIdx].value
    }
    return null
  }


  async _diskExists(key: K, snapshotVersion: number): Promise<boolean> {
    const strategy = this.strategy
    if (!strategy) throw new Error('Root Transaction missing strategy')
    const versions = this.versionIndex.get(key)
    if (!versions) {
      const rootAsAny = (this.root as any)
      if (rootAsAny.diskCache.has(key)) return rootAsAny.diskCache.get(key) !== null
      const exists = await strategy.exists(key)
      if (!exists) rootAsAny.diskCache.set(key, null)
      return exists
    }

    let targetVerObj: { version: number; exists: boolean } | null = null
    let nextVerObj: { version: number; exists: boolean } | null = null
    const idx = this._findLastLE(versions, snapshotVersion, 'version')
    if (idx >= 0) targetVerObj = versions[idx]
    if (idx + 1 < versions.length) nextVerObj = versions[idx + 1]

    if (!targetVerObj) {
      if (nextVerObj) {
        const cached = this.deletedCache.get(key)
        if (cached) {
          const cIdx = this._findExact(cached, nextVerObj.version, 'deletedAtVersion')
          if (cIdx >= 0) return true
        }
      }
      return false
    }
    return targetVerObj.exists
  }

  async _diskDelete(key: K, snapshotVersion: number): Promise<void> {
    const strategy = this.strategy
    if (!strategy) throw new Error('Root Transaction missing strategy')
    const rootAsAny = (this.root as any)
    if (await this._diskExists(key, snapshotVersion)) {
      const currentVal = rootAsAny.diskCache.has(key) ? rootAsAny.diskCache.get(key)! : await strategy.read(key)
      if (!this.deletedCache.has(key)) this.deletedCache.set(key, [])
      this.deletedCache.get(key)!.push({ value: currentVal, deletedAtVersion: snapshotVersion })
      await strategy.delete(key)
      rootAsAny.diskCache.delete(key)
    }
    if (!this.versionIndex.has(key)) this.versionIndex.set(key, [])
    this.versionIndex.get(key)!.push({ version: snapshotVersion, exists: false })
  }
}
