import { SyncMVCCTransaction, SyncMVCCStrategy } from '../src'

class InMemoryStrategy extends SyncMVCCStrategy<string, number> {
  repo: Record<string, number> = {}
  read(key: string) { return this.repo[key] }
  write(key: string, value: number) { this.repo[key] = value }
  delete(key: string) { delete this.repo[key] }
  exists(key: string) { return key in this.repo }
}

describe('Root write to existing strategy key', () => {
  test('Case 1: root.write should work for key already in strategy', () => {
    const strategy = new InMemoryStrategy()
    strategy.repo = { test: 1 }

    const root = new SyncMVCCTransaction(strategy)

    // strategy에 이미 'test' 키가 존재하므로 write가 가능해야 함
    expect(() => root.write('test', 2)).not.toThrow()
    expect(root.read('test')).toBe(2)
  })

  test('Case 2: nested.write should work for key already in strategy', () => {
    const strategy = new InMemoryStrategy()
    strategy.repo = { test: 1 }

    const root = new SyncMVCCTransaction(strategy)
    const nested = root.createNested()

    // strategy에 이미 'test' 키가 존재하므로 nested에서 write가 가능해야 함
    expect(() => nested.write('test', 2)).not.toThrow()
    expect(nested.read('test')).toBe(2)
  })

  test('Case 3: nested child should see parent uncommitted create', () => {
    const strategy = new InMemoryStrategy()
    const root = new SyncMVCCTransaction(strategy)

    const parent = root.createNested()
    parent.create('key1', 100)

    const child = parent.createNested()
    // child는 parent의 미커밋 'key1'을 볼 수 있어야 하기 때문에
    // write로 수정 가능해야 함
    const val = child.read('key1')
    expect(val).toBe(100)
  })
})
