import { AsyncMVCCTransaction } from '../src/core/async/Transaction'
import { SyncMVCCTransaction } from '../src/core/sync/Transaction'
import { AsyncMVCCStrategy } from '../src/core/async/Strategy'
import { SyncMVCCStrategy } from '../src/core/sync/Strategy'

class MockAsyncStrategy extends AsyncMVCCStrategy<string, string> {
  private data = new Map<string, string>()
  async read(key: string): Promise<string> {
    const val = this.data.get(key)
    if (val === undefined) throw new Error(`Key not found: ${key}`)
    return val
  }
  async write(key: string, value: string): Promise<void> {
    console.log('AsyncStrategy write:', key, value)
    this.data.set(key, value)
  }
  async delete(key: string): Promise<void> {
    console.log('AsyncStrategy delete:', key)
    this.data.delete(key)
  }
  async exists(key: string): Promise<boolean> {
    console.log('AsyncStrategy exists:', key)
    return this.data.has(key)
  }
}

class MockSyncStrategy extends SyncMVCCStrategy<string, string> {
  private data = new Map<string, string>()
  read(key: string): string {
    const val = this.data.get(key)
    if (val === undefined) throw new Error(`Key not found: ${key}`)
    return val
  }
  write(key: string, value: string): void {
    console.log('SyncStrategy write:', key, value)
    this.data.set(key, value)
  }
  delete(key: string): void {
    console.log('SyncStrategy delete:', key)
    this.data.delete(key)
  }
  exists(key: string): boolean {
    console.log('SyncStrategy exists:', key)
    return this.data.has(key)
  }
}

describe('Buffer History Optimization', () => {
  test('should not record buffer history when there are no active children (Async)', async () => {
    const strategy = new MockAsyncStrategy()
    const rootTx = new AsyncMVCCTransaction(strategy)

    await rootTx.create('key1', 'val1')
    await rootTx.write('key1', 'val2')
    await rootTx.write('key1', 'val3')

    const history = (rootTx as any).bufferHistory.get('key1')

    // History should not exist or be empty since there are no active children
    expect(history === undefined || history.length === 0).toBe(true)
    expect(await rootTx.read('key1')).toBe('val3')
  })

  test('should record buffer history when active children exist (Async)', async () => {
    const strategy = new MockAsyncStrategy()
    const rootTx = new AsyncMVCCTransaction(strategy)

    await rootTx.create('key1', 'val1')

    // Create nested child. This increments activeDescendantCount on rootTx.
    const childTx = rootTx.createNested()

    // Write on rootTx. Now history should be recorded.
    await rootTx.write('key1', 'val2')

    const history = (rootTx as any).bufferHistory.get('key1')

    // History must exist because there is an active child.
    expect(history).toBeDefined()
    expect(history.length).toBeGreaterThan(0)
    expect(history[0].value).toBe('val1')
    expect(await rootTx.read('key1')).toBe('val2')

    // Inside child, it has no children, so its own history shouldn't record repeatedly
    await childTx.write('key1', 'val3')
    await childTx.write('key1', 'val4')

    const childHistory = (childTx as any).bufferHistory.get('key1')
    expect(childHistory === undefined || childHistory.length === 0).toBe(true)
    expect(await childTx.read('key1')).toBe('val4')
  })

  test('should not record buffer history when there are no active children (Sync)', () => {
    const strategy = new MockSyncStrategy()
    const rootTx = new SyncMVCCTransaction(strategy)

    rootTx.create('key2', 'val1')
    rootTx.write('key2', 'val2')
    rootTx.write('key2', 'val3')

    const history = (rootTx as any).bufferHistory.get('key2')

    // History should not exist or be empty since there are no active children
    expect(history === undefined || history.length === 0).toBe(true)
    expect(rootTx.read('key2')).toBe('val3')
  })

  test('should record buffer history when active children exist (Sync)', () => {
    const strategy = new MockSyncStrategy()
    const rootTx = new SyncMVCCTransaction(strategy)

    rootTx.create('key2', 'val1')

    // Create nested child. This increments activeDescendantCount on rootTx.
    const childTx = rootTx.createNested()

    // Write on rootTx. Now history should be recorded.
    rootTx.write('key2', 'val2')

    const history = (rootTx as any).bufferHistory.get('key2')

    // History must exist because there is an active child.
    expect(history).toBeDefined()
    expect(history.length).toBeGreaterThan(0)
    expect(history[0].value).toBe('val1')
    expect(rootTx.read('key2')).toBe('val2')

    // Inside child, it has no children, so its own history shouldn't record repeatedly
    childTx.write('key2', 'val3')
    childTx.write('key2', 'val4')

    const childHistory = (childTx as any).bufferHistory.get('key2')
    expect(childHistory === undefined || childHistory.length === 0).toBe(true)
    expect(childTx.read('key2')).toBe('val4')
  })

  test('activeDescendantCount should decrement upon child rollback or commit', async () => {
    const strategy = new MockAsyncStrategy()
    const rootTx = new AsyncMVCCTransaction(strategy)

    expect((rootTx as any).activeDescendantCount).toBe(0)

    const child1 = rootTx.createNested()
    expect((rootTx as any).activeDescendantCount).toBe(1)

    const child2 = rootTx.createNested()
    expect((rootTx as any).activeDescendantCount).toBe(2)

    await child1.commit()
    expect((rootTx as any).activeDescendantCount).toBe(1)

    child2.rollback()
    expect((rootTx as any).activeDescendantCount).toBe(0)
  })

  test('should record buffer history if any descendant is active (Grandchild case)', async () => {
    const strategy = new MockAsyncStrategy()
    const rootTx = new AsyncMVCCTransaction(strategy)

    await rootTx.create('key1', 'val1')

    const childTx = rootTx.createNested()
    const grandchildTx = childTx.createNested()

    // Even though grandchildTx is not a direct child of rootTx, 
    // rootTx must record history because a descendant exists.
    await rootTx.write('key1', 'val2')

    const rootHistory = (rootTx as any).bufferHistory.get('key1')
    expect(rootHistory).toBeDefined()
    expect(rootHistory.length).toBeGreaterThan(0)

    // Child also has an active descendant (grandchild), so it should record history.
    await childTx.write('key1', 'val3')
    await childTx.write('key1', 'val4') // Second write triggers history recording
    const childHistory = (childTx as any).bufferHistory.get('key1')
    expect(childHistory).toBeDefined()
    expect(childHistory.length).toBeGreaterThan(0)
  })

  test('sibling transactions should still overwrite if they have no own descendants', async () => {
    const strategy = new MockAsyncStrategy()
    const rootTx = new AsyncMVCCTransaction(strategy)

    await rootTx.create('key1', 'val1')

    const t1 = rootTx.createNested() // Sibling 1
    const t2 = rootTx.createNested() // Sibling 2

    const t1_1 = t1.createNested() // T1 has child

    // T1 should record history because T1_1 is active
    await t1.write('key1', 't1_v1')
    await t1.write('key1', 't1_v2') // Second write records the first value in history

    expect((t1 as any).bufferHistory.get('key1')).toBeDefined()
    expect((t1 as any).bufferHistory.get('key1').length).toBeGreaterThan(0)

    // T2 has NO children. Should overwrite its own buffer despite T1 and T1_1 being alive in other branches.
    await t2.write('key1', 't2_v1')
    await t2.write('key1', 't2_v2')
    const t2History = (t2 as any).bufferHistory.get('key1')
    expect(t2History === undefined || t2History.length === 0).toBe(true)
  })
})
