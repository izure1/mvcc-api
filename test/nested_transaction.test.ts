import { SyncMVCCTransaction } from '../src'
import { SyncMVCCStrategy } from '../src'

// Simple In-Memory Strategy for testing
class InMemoryStrategy extends SyncMVCCStrategy<string, string> {
  private data = new Map<string, string>()

  read(key: string): string {
    const val = this.data.get(key)
    if (val === undefined) throw new Error('Key not found')
    return val
  }

  write(key: string, value: string): void {
    this.data.set(key, value)
  }

  delete(key: string): void {
    this.data.delete(key)
  }

  exists(key: string): boolean {
    return this.data.has(key)
  }
}

class TestRootTransaction extends SyncMVCCTransaction<InMemoryStrategy, string, string> {
  // Public wrapper for testing protected member
  public getStrategy() {
    return this.strategy!
  }
}

describe('Nested Transactions', () => {
  test('Child sees changes from parent', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    const parent = root.createNested() // Level 1 (Parent)

    parent.write('k1', 'parent_v1')

    const child = parent.createNested() // Level 2 (Child)
    expect(child.read('k1')).toBe('parent_v1')

    child.write('k2', 'child_v1')
    expect(child.read('k2')).toBe('child_v1')
  })

  test('Parent does NOT see changes from uncommitted child', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    const parent = root.createNested()
    const child = parent.createNested()

    child.write('k1', 'child_v1')

    expect(parent.read('k1')).toBeNull()
  })

  test('Child commit merges changes to parent', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    const parent = root.createNested()
    const child = parent.createNested()

    child.write('k1', 'child_v1')
    child.commit() // Merge to parent

    expect(parent.read('k1')).toBe('child_v1') // Parent sees it now

    // Root/Strategy still null until parent commits
    expect(root.read('k1')).toBeNull()

    parent.commit() // Merge to Root
    expect(root.read('k1')).toBe('child_v1')

    root.commit() // Persist
    expect(root.getStrategy().read('k1')).toBe('child_v1')
  })

  test('Nested transaction rollback does not affect parent', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    const parent = root.createNested()

    parent.write('k1', 'parent_v1')

    const child = parent.createNested()
    child.write('k1', 'child_overwrite')
    child.write('k2', 'child_v1')

    child.rollback() // Should discard child changes

    expect(parent.read('k1')).toBe('parent_v1')
    expect(parent.read('k2')).toBeNull()
  })

  test('Three levels of nesting', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    // root is L0
    const l1 = root.createNested()
    const l2 = l1.createNested()
    const l3 = l2.createNested()

    root.write('root', 'val')
    l1.write('l1', 'val')
    l2.write('l2', 'val')

    // Propagated visibility (downwards)
    expect(l3.read('root')).toBe('val')
    expect(l3.read('l1')).toBe('val')
    expect(l3.read('l2')).toBe('val')

    // Isolation (upwards)
    expect(l1.read('l2')).toBeNull()

    l2.commit() // Merge to L1
    expect(l1.read('l2')).toBe('val')

    l1.commit() // Merge to Root
    expect(root.read('l2')).toBe('val')
  })

  test('Shadowing: Child overwrites parent value', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    const parent = root.createNested()
    parent.write('k1', 'parent_val')

    const child = parent.createNested()
    expect(child.read('k1')).toBe('parent_val')

    child.write('k1', 'child_val')
    expect(child.read('k1')).toBe('child_val')
    expect(parent.read('k1')).toBe('parent_val') // Parent still sees original

    child.commit()
    expect(parent.read('k1')).toBe('child_val') // Parent updated
  })
})
