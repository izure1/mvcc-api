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
    // commit()마다 pruning이 일어나므로, 각 루프마다 이전 버전이 정리됩니다.
    // 따라서 루프 종료 후에는 마지막 버전 1개만 남아야 합니다.
    expect(versionIndex.length).toBe(1)
    expect(versionIndex[0].version).toBe(10)

    // Another commit
    tx.write('key', 'final')
    tx.commit()

    const prunedIndex = (tx as any).versionIndex.get('key')
    expect(prunedIndex.length).toBe(1)
    expect(prunedIndex[0].version).toBe(11)
  })

  it('should not prune versions needed by active transactions in SyncMVCCTransaction', () => {
    const strategy = new MockSyncStrategy()
    const tx = new SyncMVCCTransaction(strategy)

    tx.create('key', 'v1').commit() // v1
    const child = tx.createNested() // snapshots v1

    tx.write('key', 'v2').commit() // v2
    tx.write('key', 'v3').commit() // v3

    const versionIndex = (tx as any).versionIndex.get('key')
    expect(versionIndex.length).toBe(3)

    // Trigger cleanup - create 'other' instead of write
    tx.create('other', 'val').commit() // v4

    // Because 'child' is active with snapshotVersion 1, we must keep at least the latest v <= 1
    // versions: [v1, v2, v3]
    // minActiveVersion = 1
    // latest v <= 1 is index 0
    // so no pruning should occur (latestInSnapshotIdx = 0)
    expect((tx as any).versionIndex.get('key').length).toBe(3)

    // Release child
    child.rollback()

    // Now trigger cleanup again
    tx.write('other', 'new').commit() // v5
    // minActiveVersion = 5
    // latest v <= 5 for 'key' is v3 at index 2
    // so it should prune index 0 and 1. [v3] remains.
    expect((tx as any).versionIndex.get('key').length).toBe(1)
    expect((tx as any).versionIndex.get('key')[0].version).toBe(3)
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
    expect((tx as any).versionIndex.get('key').length).toBe(1)

    await tx.write('key', 'final')
    await tx.commit()

    expect((tx as any).versionIndex.get('key').length).toBe(1)
  })
})
