import fs from 'node:fs'
import fsPromises from 'node:fs/promises'
import path from 'node:path'
import { AsyncMVCCManager } from '../src'
import { AsyncFileStrategy } from './strategy/AsyncFileStrategy'

describe('Strict Async FileSystem MVCC Scenarios', () => {
  const tmpDir = path.join(__dirname, 'tmp_strict_async')

  beforeAll(() => {
    if (!fs.existsSync(tmpDir)) {
      fs.mkdirSync(tmpDir)
    }
  })

  afterAll(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  beforeEach(() => {
    if (fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
    fs.mkdirSync(tmpDir)
  })

  const getPath = (filename: string) => path.join(tmpDir, filename)

  test('Scenario 1: Long Running Reader (Async)', async () => {
    const manager = new AsyncMVCCManager(new AsyncFileStrategy())
    const historyFile = getPath('history.txt')

    // Initial State
    await (await manager.createTransaction().create(historyFile, 'Generation 0').commit())

    // Start Long Running Reader
    const reader = manager.createTransaction()
    expect(await reader.read(historyFile)).toBe('Generation 0')

    // Perform 50 sequential updates
    for (let i = 1; i <= 50; i++) {
      const writer = manager.createTransaction()
      writer.write(historyFile, `Generation ${i}`)
      await writer.commit()
    }

    // Current state should be 50
    expect(await fsPromises.readFile(historyFile, 'utf-8')).toBe('Generation 50')

    // Reader should still see 0 in snapshot
    expect(await reader.read(historyFile)).toBe('Generation 0')
  })

  test('Scenario 2: Massive Concurrent Writes (Async)', async () => {
    const manager = new AsyncMVCCManager(new AsyncFileStrategy())
    const counterFile = getPath('counter.txt')

    await (await manager.createTransaction().create(counterFile, '0').commit())

    // Simulate 50 concurrent transactions
    const txs = Array.from({ length: 50 }, () => manager.createTransaction())

    const results = await Promise.allSettled(txs.map(async (tx, index) => {
      tx.write(counterFile, `${index + 1}`)
      await tx.commit()
    }))

    const successCount = results.filter(r => r.status === 'fulfilled').length
    const failureCount = results.filter(r => r.status === 'rejected').length

    // Only ONE should succeed. 49 should fail.
    expect(successCount).toBe(1)
    expect(failureCount).toBe(49)
  })

  test('Scenario 3: Consistency (Money Transfer Async)', async () => {
    const manager = new AsyncMVCCManager(new AsyncFileStrategy())
    const accA = getPath('A.txt')
    const accB = getPath('B.txt')

    const initialTx = manager.createTransaction()
    initialTx.create(accA, '100')
    initialTx.create(accB, '0')
    await initialTx.commit()

    // Verification Transaction
    const checkConsistency = async () => {
      const tx = manager.createTransaction()
      const valA = parseInt((await tx.read(accA)) as string)
      const valB = parseInt((await tx.read(accB)) as string)
      expect(valA + valB).toBe(100)
    }

    await checkConsistency()

    // Perform 10 transfers sequentially to avoid massive conflicts, but checking consistency concurrently
    for (let i = 0; i < 10; i++) {
      const observer = manager.createTransaction()

      const txTransfer = manager.createTransaction()
      const currentA = parseInt((await txTransfer.read(accA)) as string)
      const currentB = parseInt((await txTransfer.read(accB)) as string)

      txTransfer.write(accA, (currentA - 10).toString())
      txTransfer.write(accB, (currentB + 10).toString())
      await txTransfer.commit()

      // Observer should still see old total 100
      const obsA = parseInt((await observer.read(accA)) as string)
      const obsB = parseInt((await observer.read(accB)) as string)
      expect(obsA + obsB).toBe(100)
    }

    await checkConsistency()
  })

  test('Scenario 4: Persistence with Active Transactions (Async)', async () => {
    const tempFile = getPath('atomicity.txt')

    // Manager 1
    const manager1 = new AsyncMVCCManager(new AsyncFileStrategy())
    const tx1 = manager1.createTransaction()
    tx1.create(tempFile, 'In Progress')

    // Manager 2
    const manager2 = new AsyncMVCCManager(new AsyncFileStrategy())
    const tx2 = manager2.createTransaction()

    // Should NOT see uncommitted data
    expect(await tx2.read(tempFile)).toBeNull()
    try {
      await fsPromises.access(tempFile)
      fail('File should not exist')
    } catch (e) {
      // Expected
    }

    await tx1.commit()
    expect(await fsPromises.readFile(tempFile, 'utf-8')).toBe('In Progress')

    // Manager 3
    const manager3 = new AsyncMVCCManager(new AsyncFileStrategy())
    const tx3 = manager3.createTransaction()
    expect(await tx3.read(tempFile)).toBe('In Progress')
  })

  test('Scenario 5: Read Your Own Writes (Async)', async () => {
    const manager = new AsyncMVCCManager(new AsyncFileStrategy())
    const file = getPath('own_write.txt')

    await (await manager.createTransaction().create(file, 'Initial').commit())

    const tx = manager.createTransaction()
    expect(await tx.read(file)).toBe('Initial')

    tx.write(file, 'Modified')
    expect(await tx.read(file)).toBe('Modified')

    tx.delete(file)
    expect(await tx.read(file)).toBeNull()

    await tx.commit()

    try {
      await fsPromises.access(file)
      fail('File should act be deleted')
    } catch {
      // Expected
    }
  })

  test('Scenario 6: Delete-Create Cycle (Async)', async () => {
    const manager = new AsyncMVCCManager(new AsyncFileStrategy())
    const cycleFile = getPath('cycle.txt')

    await (await manager.createTransaction().create(cycleFile, 'V1').commit())

    const oldReader = manager.createTransaction()

    const deleteTx = manager.createTransaction()
    deleteTx.delete(cycleFile)
    await deleteTx.commit()

    const createTx = manager.createTransaction()
    createTx.create(cycleFile, 'V2')
    await createTx.commit()

    const newReader = manager.createTransaction()

    expect(await newReader.read(cycleFile)).toBe('V2')
    expect(await oldReader.read(cycleFile)).toBe('V1')
  })

  test('Scenario 7: Rollback Integrity (Async)', async () => {
    const manager = new AsyncMVCCManager(new AsyncFileStrategy())
    const stableFile = getPath('stable.txt')
    const dirtyFile = getPath('dirty.txt')

    await (await manager.createTransaction().create(stableFile, 'Stable').commit())

    const tx = manager.createTransaction()
    tx.create(dirtyFile, 'Dirty')
    tx.delete(stableFile)

    tx.rollback()

    try {
      await fsPromises.access(dirtyFile)
      fail('Dirty file should not exist')
    } catch { }

    expect(await fsPromises.readFile(stableFile, 'utf-8')).toBe('Stable')
  })

  test('Scenario 8: Repeated Read (Async)', async () => {
    const manager = new AsyncMVCCManager(new AsyncFileStrategy())
    const file = getPath('repeat.txt')

    await (await manager.createTransaction().create(file, 'A').commit())

    const observer = manager.createTransaction()
    expect(await observer.read(file)).toBe('A')

    const tx1 = manager.createTransaction()
    tx1.write(file, 'B')
    await tx1.commit()
    expect(await observer.read(file)).toBe('A')

    const tx2 = manager.createTransaction()
    tx2.write(file, 'C')
    await tx2.commit()
    expect(await observer.read(file)).toBe('A')
  })

  class TestAsyncMVCCManager extends AsyncMVCCManager<AsyncFileStrategy, string, string> {
    getDeletedCacheSize(): number {
      return this.deletedCache.size
    }
  }

  test('Scenario 9: Garbage Collection Logic (Async)', async () => {
    const manager = new TestAsyncMVCCManager(new AsyncFileStrategy())
    const file = getPath('gc.txt')
    await (await manager.createTransaction().create(file, 'Init').commit())

    for (let i = 0; i < 10; i++) {
      const tx = manager.createTransaction()
      tx.write(file, `Update ${i}`)
      await tx.commit()
    }

    expect(manager.getDeletedCacheSize()).toBeGreaterThan(0)

    const triggerTx = manager.createTransaction()
    triggerTx.create(getPath('trigger_gc.txt'), 'Trigger')
    await triggerTx.commit()

    expect(manager.getDeletedCacheSize()).toBe(0)
  })

  test('Scenario 10: Non-Conflicting Parallel Writes (Async)', async () => {
    const manager = new AsyncMVCCManager(new AsyncFileStrategy())

    // Setup: 100 Transactions writing to 100 DIFFERENT files.
    // They should ALL succeed because there is no key conflict.
    // The commitments will be serialized by the lock, but no conflict logic should trip.

    const count = 100
    const txs = Array.from({ length: count }, () => manager.createTransaction())

    const results = await Promise.allSettled(txs.map(async (tx, i) => {
      const file = getPath(`parallel_${i}.txt`)
      tx.create(file, `Data ${i}`)
      await tx.commit()
    }))

    const successCount = results.filter(r => r.status === 'fulfilled').length
    const failureCount = results.filter(r => r.status === 'rejected').length

    expect(successCount).toBe(count)
    expect(failureCount).toBe(0)
  })

  test('Scenario 11: Mixed Conflict Concurrency (Async)', async () => {
    const manager = new AsyncMVCCManager(new AsyncFileStrategy())
    const fileA = getPath('mixed_A.txt')
    const fileB = getPath('mixed_B.txt')

    await (await manager.createTransaction().create(fileA, '0').commit())
    await (await manager.createTransaction().create(fileB, '0').commit())

    // 50 Txs trying to write A, 50 Txs trying to write B.
    // All start at current version.
    // We expect exactly 1 winner for A and 1 winner for B.

    const txsA = Array.from({ length: 50 }, () => manager.createTransaction())
    const txsB = Array.from({ length: 50 }, () => manager.createTransaction())

    const allrs = await Promise.allSettled([
      ...txsA.map(async (tx, i) => {
        tx.write(fileA, `A${i}`)
        await tx.commit()
      }),
      ...txsB.map(async (tx, i) => {
        tx.write(fileB, `B${i}`)
        await tx.commit()
      })
    ])

    const successCount = allrs.filter(r => r.status === 'fulfilled').length
    expect(successCount).toBe(2)
  })
})
