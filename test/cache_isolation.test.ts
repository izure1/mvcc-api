import { SyncMVCCTransaction } from '../src'
import { SyncMVCCStrategy } from '../src/core/sync/Strategy'

class MockStrategy extends SyncMVCCStrategy<string, string> {
  private data = new Map<string, string>()
  constructor(initialData?: [string, string][]) {
    super()
    if (initialData) this.data = new Map(initialData)
  }
  read(key: string): string { return this.data.get(key) || '' }
  write(key: string, value: string): void { this.data.set(key, value) }
  delete(key: string): void { this.data.delete(key) }
  exists(key: string): boolean { return this.data.has(key) }
}

describe('Child Transaction Cache Isolation Test', () => {
  test('Child write should NOT affect root diskCache before root persist', () => {
    const strategy = new MockStrategy([['key1', 'original']])
    const root = new SyncMVCCTransaction(strategy)
    const child = root.createNested()

    // 1. Child writes something
    child.write('key1', 'child_modified')

    // Root diskCache check (should be empty or original if read, but definitely NOT child_modified)
    const cachedAtRoot = (root as any).diskCache.get('key1')
    expect(cachedAtRoot).not.toBe('child_modified')

    // 2. Child commits to root
    child.commit()

    // Root read should see child_modified from its buffer
    expect(root.read('key1')).toBe('child_modified')

    // BUT diskCache should STILL be original or not child_modified
    const cachedAtRootAfterChildCommit = (root as any).diskCache.get('key1')
    expect(cachedAtRootAfterChildCommit).not.toBe('child_modified')

    // 3. Root persists
    root.commit()

    // NOW it should be in diskCache
    const cachedAtRootFinal = (root as any).diskCache.get('key1')
    expect(cachedAtRootFinal).toBe('child_modified')
  })
})
