import { AsyncMVCCTransaction } from '../src/core/async/Transaction'
import { AsyncMVCCStrategy } from '../src/core/async/Strategy'

class SimpleAsyncStrategy extends AsyncMVCCStrategy<string, string> {
  repo: Record<string, string> = {}
  async read(key: string) { return this.repo[key] }
  async write(key: string, value: string) { this.repo[key] = value }
  async delete(key: string) { delete this.repo[key] }
  async exists(key: string) { return key in this.repo }
}

describe('Full Nested Transaction Bug Verification', () => {
  test('Case 1: Nested child should see parent uncommitted changes (Read/Write Bug)', async () => {
    const strategy = new SimpleAsyncStrategy()
    const root = new AsyncMVCCTransaction(strategy)

    const child1 = root.createNested()
    await child1.create('key1', 'value1')

    const child2 = child1.createNested()
    const val = await child2.read('key1')
    expect(val).toBe('value1')

    await child2.write('key1', 'value2')

    await child2.commit()
    await child1.commit()
    await root.commit()

    expect(strategy.repo['key1']).toBe('value2')
  })

  test('Case 2: Deleting a key created in parent should NOT be in final deleted results', async () => {
    const strategy = new SimpleAsyncStrategy()
    const root = new AsyncMVCCTransaction(strategy)

    const child1 = root.createNested()
    await child1.create('key2', 'value1')

    const child2 = child1.createNested()
    await child2.delete('key2')

    await child2.commit()
    await child1.commit()
    const result = await root.commit()

    expect(result.created.find(e => e.key === 'key2')).toBeUndefined()
    expect(result.updated.find(e => e.key === 'key2')).toBeUndefined()
    expect(result.deleted.find(e => e.key === 'key2')).toBeUndefined()

    expect(strategy.repo['key2']).toBeUndefined()
  })

  test('Case 3: Deleting an existing key should be in final deleted results', async () => {
    const strategy = new SimpleAsyncStrategy()
    strategy.repo['key3'] = 'initial'
    const root = new AsyncMVCCTransaction(strategy)

    const child1 = root.createNested()
    const child2 = child1.createNested()
    await child2.delete('key3')

    await child2.commit()
    await child1.commit()
    const result = await root.commit()

    expect(result.deleted.find(e => e.key === 'key3')).toBeDefined()
    expect(strategy.repo['key3']).toBeUndefined()
  })
})
