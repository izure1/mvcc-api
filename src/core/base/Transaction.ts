import type { Deferred } from '../../types'
import type { MVCCManager } from './Manager'
import type { MVCCStrategy } from './Strategy'

export abstract class MVCCTransaction<T, S extends MVCCStrategy<T>, M extends MVCCManager<T, S>> {
  protected readonly manager: M
  protected committed: boolean
  readonly snapshotVersion: number
  readonly writeBuffer: Map<string, T>
  readonly deleteBuffer: Set<string>

  constructor(manager: M, snapshotVersion: number) {
    this.manager = manager
    this.snapshotVersion = snapshotVersion
    this.writeBuffer = new Map()
    this.deleteBuffer = new Set()
    this.committed = false
  }

  create(key: string, value: T): this {
    if (this.committed) throw new Error('Transaction already committed')
    this.writeBuffer.set(key, value)
    return this
  }

  write(key: string, value: T): this {
    if (this.committed) throw new Error('Transaction already committed')
    this.writeBuffer.set(key, value)
    this.deleteBuffer.delete(key)
    return this
  }

  delete(key: string): this {
    if (this.committed) throw new Error('Transaction already committed')
    this.deleteBuffer.add(key)
    this.writeBuffer.delete(key)
    return this
  }

  rollback(): this {
    this.writeBuffer.clear()
    this.deleteBuffer.clear()
    this.committed = true
    this.manager._removeTransaction(this)
    return this
  }

  abstract read(key: string): Deferred<T | null>
  abstract commit(): Deferred<this>
}
