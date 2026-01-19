import type { SyncMVCCStrategy } from './Strategy'
import { MVCCTransaction } from '../base'

export class SyncMVCCTransaction<
  S extends SyncMVCCStrategy<K, T>,
  K,
  T
> extends MVCCTransaction<S, K, T> {
  createNested(): this {
    const childVersion = this.isRoot() ? this.version : this.snapshotVersion
    const child = new SyncMVCCTransaction(undefined, this, childVersion) as this
    if (this.isRoot()) {
      this.activeTransactions.add(child)
    }
    return child
  }

  read(key: K): T | null {
    if (this.committed) throw new Error('Transaction already committed')
    if (this.writeBuffer.has(key)) return this.writeBuffer.get(key)!
    if (this.deleteBuffer.has(key)) return null

    if (this.parent) {
      return this.parent._readSnapshot(key, this.snapshotVersion) as T | null
    } else {
      // Root Logic
      return this._diskRead(key, this.version)
    }
  }

  _readSnapshot(key: K, snapshotVersion: number): T | null {
    if (this.committed) throw new Error('Transaction already committed')

    if (this.writeBuffer.has(key)) return this.writeBuffer.get(key)!
    if (this.deleteBuffer.has(key)) return null

    if (this.parent) {
      return this.parent._readSnapshot(key, snapshotVersion) as T | null
    } else {
      // Root Logic
      return this._diskRead(key, snapshotVersion)
    }
  }

  commit(): this {
    if (this.committed) throw new Error('Transaction already committed')

    if (this.parent) {
      this.parent._merge(this)
    } else {
      // Root Logic
      if (this.writeBuffer.size > 0 || this.deleteBuffer.size > 0) {
        this._merge(this)
        this.writeBuffer.clear()
        this.deleteBuffer.clear()
      }
    }

    this.committed = true
    return this
  }

  _merge(child: MVCCTransaction<S, K, T>): void {
    if (this.parent) {
      // Nested Logic: Merge to self (Parent of Child)
      // 1. Conflict Detection between Siblings (via keyVersions)
      for (const key of child.writeBuffer.keys()) {
        const lastModLocalVer = this.keyVersions.get(key)
        if (lastModLocalVer !== undefined && lastModLocalVer > child.snapshotLocalVersion) {
          throw new Error(`Commit conflict: Key '${key}' was modified by a newer transaction (Local v${lastModLocalVer})`)
        }
      }
      for (const key of child.deleteBuffer) {
        const lastModLocalVer = this.keyVersions.get(key)
        if (lastModLocalVer !== undefined && lastModLocalVer > child.snapshotLocalVersion) {
          throw new Error(`Commit conflict: Key '${key}' was modified by a newer transaction (Local v${lastModLocalVer})`)
        }
      }

      // 2. Merge buffers
      const newLocalVersion = this.localVersion + 1
      for (const key of child.writeBuffer.keys()) {
        this.write(key, child.writeBuffer.get(key)!)
        this.keyVersions.set(key, newLocalVersion)
      }
      for (const key of child.deleteBuffer) {
        this.delete(key)
        this.keyVersions.set(key, newLocalVersion)
      }

      (this as any).localVersion = newLocalVersion;

    } else {
      // Root Logic: Persistence

      // Removed from active transactions as it's committing
      this.activeTransactions.delete(child)

      const newVersion = this.version + 1

      // 1. Conflict Detection (Global)
      const modifiedKeys = new Set([...child.writeBuffer.keys(), ...child.deleteBuffer])
      for (const key of modifiedKeys) {
        const versions = this.versionIndex.get(key)
        if (versions && versions.length > 0) {
          const lastVer = versions[versions.length - 1].version
          if (lastVer > child.snapshotVersion) {
            throw new Error(`Commit conflict: Key '${key}' was modified by a newer transaction (v${lastVer})`)
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
