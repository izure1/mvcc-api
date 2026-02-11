import { SyncMVCCTransaction } from '../src/core/sync/Transaction'
import { AsyncMVCCTransaction } from '../src/core/async/Transaction'
import { SyncMVCCStrategy } from '../src/core/sync/Strategy'
import { AsyncMVCCStrategy } from '../src/core/async/Strategy'

class MockSyncStrategy implements SyncMVCCStrategy<string, string> {
  private data: Map<string, string> = new Map()
  read(key: string) { return this.data.get(key) || '' }
  write(key: string, value: string) { this.data.set(key, value) }
  delete(key: string) { this.data.delete(key) }
  exists(key: string) { return this.data.has(key) }
}

class MockAsyncStrategy implements AsyncMVCCStrategy<string, string> {
  private data: Map<string, string> = new Map()
  async read(key: string) { return this.data.get(key) || '' }
  async write(key: string, value: string) { this.data.set(key, value) }
  async delete(key: string) { this.data.delete(key) }
  async exists(key: string) { return this.data.has(key) }
}

describe('Memory Pruning', () => {
  it('should prune versionIndex in SyncMVCCTransaction', () => {
    const strategy = new MockSyncStrategy()
    const tx = new SyncMVCCTransaction(strategy)

    // Build up versionIndex with 10 versions
    for (let i = 0; i < 10; i++) {
      if (i === 0) tx.create('key', 'val' + i)
      else tx.write('key', 'val' + i)
      tx.commit()
    }

    const versionIndex = (tx as any).versionIndex.get('key')
    expect(versionIndex).toBeUndefined()

    // Another commit
    tx.write('key', 'final')
    tx.commit()

    const prunedIndex = (tx as any).versionIndex.get('key')
    expect(prunedIndex).toBeUndefined()
  })

  it('should not prune versions needed by active transactions in SyncMVCCTransaction', () => {
    const strategy = new MockSyncStrategy()
    const tx = new SyncMVCCTransaction(strategy)

    tx.create('key', 'v1').commit() // v1
    const child = tx.createNested() // snapshots v1

    tx.write('key', 'v2').commit() // v2
    tx.write('key', 'v3').commit() // v3

    const versionIndex = (tx as any).versionIndex.get('key')
    // v1 entry was pruned during v1 commit (no active tx at that point)
    // Only v2 and v3 remain
    expect(versionIndex.length).toBe(2)

    // Trigger cleanup - create 'other' instead of write
    tx.create('other', 'val').commit() // v4

    // Because 'child' is active with snapshotVersion 1, we must keep versions > 1
    // versions: [v2, v3] - both needed for snapshot isolation
    // minActiveVersion = 1
    // no version <= 1, so latestInSnapshotIdx = -1, no pruning
    expect((tx as any).versionIndex.get('key').length).toBe(2)

    // Release child
    child.rollback()

    // Now trigger cleanup again
    tx.write('other', 'new').commit() // v5
    // minActiveVersion = 5
    // Both v2, v3 <= 5, latestInSnapshotIdx = 1 = length-1 → key deleted entirely
    expect((tx as any).versionIndex.get('key')).toBeUndefined()
  })

  it('should prune versionIndex in AsyncMVCCTransaction', async () => {
    const strategy = new MockAsyncStrategy()
    const tx = new AsyncMVCCTransaction(strategy)

    for (let i = 0; i < 10; i++) {
      if (i === 0) await tx.create('key', 'val' + i)
      else await tx.write('key', 'val' + i)
      await tx.commit()
    }

    // Aggressive pruning expected
    expect((tx as any).versionIndex.get('key')).toBeUndefined()

    await tx.write('key', 'final')
    await tx.commit()

    expect((tx as any).versionIndex.get('key')).toBeUndefined()
  })
})
