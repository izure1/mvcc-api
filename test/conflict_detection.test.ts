import { SyncMVCCTransaction } from '../src/core/sync/Transaction'
import { SyncMVCCStrategy } from '../src/core/sync/Strategy'

class MockStrategy extends SyncMVCCStrategy<string, string> {
  private data = new Map<string, string>()
  read(key: string): string {
    const val = this.data.get(key)
    if (val === undefined) throw new Error(`Key not found: ${key}`)
    return val
  }
  write(key: string, value: string): void { this.data.set(key, value) }
  delete(key: string): void { this.data.delete(key) }
  exists(key: string): boolean { return this.data.has(key) }
}

describe('MVCCTransaction.checkConflicts', () => {
  let strategy: MockStrategy
  let root: SyncMVCCTransaction<MockStrategy, string, string>

  beforeEach(() => {
    strategy = new MockStrategy()
    root = new SyncMVCCTransaction(strategy)
  })

  it('should return no conflicts for transactions modifying different keys', () => {
    const tx1 = root.createNested()
    const tx2 = root.createNested()

    tx1.create('key1', 'value1')
    tx2.create('key2', 'value2')

    const conflicts = SyncMVCCTransaction.CheckConflicts([tx1, tx2])
    expect(conflicts).toHaveLength(0)
  })

  it('should detect conflicts when multiple transactions write to the same key', () => {
    const tx1 = root.createNested()
    const tx2 = root.createNested()

    tx1.create('key1', 'value1')
    tx2.create('key1', 'value2')

    const conflicts = SyncMVCCTransaction.CheckConflicts([tx1, tx2])
    expect(conflicts).toContain('key1')
    expect(conflicts).toHaveLength(1)
  })

  it('should detect conflicts between write and delete operations', () => {
    strategy.write('key1', 'initial') // Setup existing key

    // Refresh root snapshot or markers if necessary, though for nested it reads from parent/root
    const tx1 = root.createNested()
    const tx2 = root.createNested()

    tx1.write('key1', 'updated')
    tx2.delete('key1')

    const conflicts = SyncMVCCTransaction.CheckConflicts([tx1, tx2])
    expect(conflicts).toContain('key1')
    expect(conflicts).toHaveLength(1)
  })

  it('should detect conflicts among more than two transactions', () => {
    const tx1 = root.createNested()
    const tx2 = root.createNested()
    const tx3 = root.createNested()

    tx1.create('key1', 'v1')
    tx2.create('key2', 'v2')
    tx3.create('key1', 'v3')

    const conflicts = SyncMVCCTransaction.CheckConflicts([tx1, tx2, tx3])
    expect(conflicts).toContain('key1')
    expect(conflicts).not.toContain('key2')
    expect(conflicts).toHaveLength(1)
  })

  it('should handle nested transactions properly', () => {
    const tx1 = root.createNested()
    const tx1_1 = tx1.createNested()
    const tx2 = root.createNested()

    tx1_1.create('key1', 'v1')
    tx2.create('key1', 'v2')

    const conflicts = SyncMVCCTransaction.CheckConflicts([tx1_1, tx2])
    expect(conflicts).toContain('key1')
  })
})
