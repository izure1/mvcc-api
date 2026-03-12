import { LRUMap } from '../src/utils/LRUMap'

describe('LRUMap', () => {
  test('should store and retrieve values', () => {
    const lru = new LRUMap<string, number>(3)
    lru.set('a', 1)
    lru.set('b', 2)
    lru.set('c', 3)

    expect(lru.get('a')).toBe(1)
    expect(lru.get('b')).toBe(2)
    expect(lru.get('c')).toBe(3)
    expect(lru.size).toBe(3)
  })

  test('should update existing keys and promote them', () => {
    const lru = new LRUMap<string, number>(3)
    lru.set('a', 1)
    lru.set('b', 2)
    lru.set('c', 3)

    // Update 'a'
    lru.set('a', 10)
    expect(lru.get('a')).toBe(10)

    // 'a' should now be MRU. Adding 'd' should evict 'b' (the next oldest)
    lru.set('d', 4)
    expect(lru.has('b')).toBe(false)
    expect(lru.has('c')).toBe(true)
    expect(lru.has('a')).toBe(true)
    expect(lru.has('d')).toBe(true)
  })

  test('should evict the least recently used item', () => {
    const lru = new LRUMap<string, number>(3)
    lru.set('a', 1)
    lru.set('b', 2)
    lru.set('c', 3)

    // Access 'a' to make it most recently used
    lru.get('a')

    // Add 'd', should evict 'b' (since 'b' is now the oldest)
    lru.set('d', 4)

    expect(lru.has('b')).toBe(false)
    expect(lru.has('a')).toBe(true)
    expect(lru.has('c')).toBe(true)
    expect(lru.has('d')).toBe(true)
  })

  test('should handle deletion correctly', () => {
    const lru = new LRUMap<string, number>(3)
    lru.set('a', 1)
    lru.set('b', 2)
    lru.set('c', 3)

    expect(lru.delete('b')).toBe(true)
    expect(lru.has('b')).toBe(false)
    expect(lru.size).toBe(2)

    // Adding 'd' should not evict anything yet since size is 2/3
    lru.set('d', 4)
    expect(lru.size).toBe(3)
    expect(lru.has('a')).toBe(true)
    expect(lru.has('c')).toBe(true)
    expect(lru.has('d')).toBe(true)
  })

  test('should check existence without promoting', () => {
    const lru = new LRUMap<string, number>(3)
    lru.set('a', 1)
    lru.set('b', 2)
    lru.set('c', 3)

    // 'a' is oldest. 'has' should not promote 'a'.
    expect(lru.has('a')).toBe(true)

    // Add 'd', should evict 'a'
    lru.set('d', 4)
    expect(lru.has('a')).toBe(false)
  })

  test('should return keys in MRU to LRU order', () => {
    const lru = new LRUMap<string, number>(3)
    lru.set('a', 1)
    lru.set('b', 2)
    lru.set('c', 3)

    expect(Array.from(lru.keys())).toEqual(['c', 'b', 'a'])

    lru.get('a')
    expect(Array.from(lru.keys())).toEqual(['a', 'c', 'b'])

    lru.set('b', 20)
    expect(Array.from(lru.keys())).toEqual(['b', 'a', 'c'])
  })

  test('should clear the map', () => {
    const lru = new LRUMap<string, number>(3)
    lru.set('a', 1)
    lru.set('b', 2)
    lru.clear()

    expect(lru.size).toBe(0)
    expect(lru.has('a')).toBe(false)
    expect(lru.has('b')).toBe(false)
    expect(Array.from(lru.keys())).toEqual([])
  })

  test('should handle edge cases with capacity 1', () => {
    const lru = new LRUMap<string, number>(1)
    lru.set('a', 1)
    expect(lru.get('a')).toBe(1)

    lru.set('b', 2)
    expect(lru.has('a')).toBe(false)
    expect(lru.get('b')).toBe(2)

    lru.delete('b')
    expect(lru.size).toBe(0)
  })
})
