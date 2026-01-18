import fs from 'node:fs'
import path from 'node:path'
import { SyncMVCCManager } from '../src'
import { FileStrategy } from './strategy/FileStrategy'

describe('Strict FileSystem MVCC Scenarios', () => {
  const tmpDir = path.join(__dirname, 'tmp_strict')

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

  test('Scenario 1: Long Running Reader (Undo Log Depth)', () => {
    const manager = new SyncMVCCManager(new FileStrategy())
    const historyFile = getPath('history.txt')

    // Initial State
    manager.createTransaction().create(historyFile, 'Generation 0').commit()

    // Start Long Running Reader
    const reader = manager.createTransaction()
    expect(reader.read(historyFile)).toBe('Generation 0')

    // Perform 50 sequential updates
    for (let i = 1; i <= 50; i++) {
      const writer = manager.createTransaction()
      writer.write(historyFile, `Generation ${i}`).commit()
    }

    // Current state should be 50
    expect(fs.readFileSync(historyFile, 'utf-8')).toBe('Generation 50')

    // Reader should still see 0 in snapshot
    expect(reader.read(historyFile)).toBe('Generation 0')
  })

  test('Scenario 2: Massive Concurrent Writes', () => {
    const manager = new SyncMVCCManager(new FileStrategy())
    const counterFile = getPath('counter.txt')

    manager.createTransaction().create(counterFile, '0').commit()

    // Simulate 50 concurrent transactions reading same base and trying to update
    const txs = Array.from({ length: 50 }, () => manager.createTransaction())

    let successCount = 0
    let failureCount = 0

    txs.forEach((tx, index) => {
      try {
        tx.write(counterFile, `${index + 1}`)
        tx.commit()
        successCount++
      } catch (e) {
        failureCount++
      }
    })

    // Only ONE should succeed. 49 should fail.
    expect(successCount).toBe(1)
    expect(failureCount).toBe(49)
  })

  test('Scenario 3: Consistency (Money Transfer)', () => {
    const manager = new SyncMVCCManager(new FileStrategy())
    const accA = getPath('A.txt')
    const accB = getPath('B.txt')

    manager.createTransaction()
      .create(accA, '100')
      .create(accB, '0')
      .commit()

    // Verification Transaction
    const checkConsistency = () => {
      const tx = manager.createTransaction()
      const valA = parseInt(tx.read(accA) as string)
      const valB = parseInt(tx.read(accB) as string)
      expect(valA + valB).toBe(100)
    }

    checkConsistency()

    // Perform 10 transfers
    for (let i = 0; i < 10; i++) {
      // Start consistency check DURING transfer (Snapshot Isolation check)
      const observer = manager.createTransaction()

      const txTransfer = manager.createTransaction()
      const currentA = parseInt(txTransfer.read(accA) as string)
      const currentB = parseInt(txTransfer.read(accB) as string)

      txTransfer.write(accA, (currentA - 10).toString())
      txTransfer.write(accB, (currentB + 10).toString())
      txTransfer.commit()

      // Observer should still see old total 100
      const obsA = parseInt(observer.read(accA) as string)
      const obsB = parseInt(observer.read(accB) as string)
      expect(obsA + obsB).toBe(100)
    }

    checkConsistency()
  })

  test('Scenario 4: Persistence with Active Transactions', () => {
    const tempFile = getPath('atomicity.txt')

    // Manager 1
    const manager1 = new SyncMVCCManager(new FileStrategy())
    const tx1 = manager1.createTransaction()
    tx1.create(tempFile, 'In Progress')

    // Manager 2 (Simulate Crash/Restart before commit)
    const manager2 = new SyncMVCCManager(new FileStrategy())
    const tx2 = manager2.createTransaction()

    // Should NOT see uncommitted data
    expect(tx2.read(tempFile)).toBeNull()
    expect(fs.existsSync(tempFile)).toBe(false)

    tx1.commit()
    expect(fs.existsSync(tempFile)).toBe(true)

    // Manager 3
    const manager3 = new SyncMVCCManager(new FileStrategy())
    const tx3 = manager3.createTransaction()
    expect(tx3.read(tempFile)).toBe('In Progress')
  })

  test('Scenario 5: Read Your Own Writes', () => {
    const manager = new SyncMVCCManager(new FileStrategy())
    const file = getPath('own_write.txt')

    // Initial: "Initial"
    manager.createTransaction().create(file, 'Initial').commit()

    const tx = manager.createTransaction()
    // Read -> "Initial"
    expect(tx.read(file)).toBe('Initial')

    // Write -> "Modified"
    tx.write(file, 'Modified')
    // Read -> "Modified" (Local Buffer)
    expect(tx.read(file)).toBe('Modified')

    // Delete
    tx.delete(file)
    // Read -> null (Local Delete Buffer)
    expect(tx.read(file)).toBeNull()

    tx.commit()

    // Verify Persistence
    expect(fs.existsSync(file)).toBe(false)
  })

  test('Scenario 6: Delete-Create Cycle', () => {
    const manager = new SyncMVCCManager(new FileStrategy())
    const cycleFile = getPath('cycle.txt')

    // V1
    manager.createTransaction().create(cycleFile, 'V1').commit()

    // Old Reader starts here (Snapshot V=1)
    const oldReader = manager.createTransaction()

    // Tx1: Delete V1
    manager.createTransaction().delete(cycleFile).commit()

    // Tx2: Create V2 (Same Key)
    manager.createTransaction().create(cycleFile, 'V2').commit()

    // New Reader starts here (Snapshot V=3)
    const newReader = manager.createTransaction()

    // Verify
    expect(newReader.read(cycleFile)).toBe('V2')
    // Old reader should still see V1 (from Undo Log despite V2 overwriting V1's slot or lack thereof)
    expect(oldReader.read(cycleFile)).toBe('V1')
  })

  test('Scenario 7: Rollback Integrity', () => {
    const manager = new SyncMVCCManager(new FileStrategy())
    const stableFile = getPath('stable.txt')
    const dirtyFile = getPath('dirty.txt')

    manager.createTransaction().create(stableFile, 'Stable').commit()

    const tx = manager.createTransaction()
    tx.create(dirtyFile, 'Dirty')
    tx.delete(stableFile)

    // Rollback
    tx.rollback()

    // Verify
    expect(fs.existsSync(dirtyFile)).toBe(false)
    expect(fs.existsSync(stableFile)).toBe(true)
    expect(fs.readFileSync(stableFile, 'utf-8')).toBe('Stable')
  })

  test('Scenario 8: Repeated Read (Stability)', () => {
    const manager = new SyncMVCCManager(new FileStrategy())
    const file = getPath('repeat.txt')

    manager.createTransaction().create(file, 'A').commit()

    const observer = manager.createTransaction()
    expect(observer.read(file)).toBe('A')

    // External Update 1
    manager.createTransaction().write(file, 'B').commit()
    expect(observer.read(file)).toBe('A') // Still A

    // External Update 2
    manager.createTransaction().write(file, 'C').commit()
    expect(observer.read(file)).toBe('A') // Still A

    // External Delete
    manager.createTransaction().delete(file).commit()
    expect(observer.read(file)).toBe('A') // Still A
  })

  class TestSyncMVCCManager extends SyncMVCCManager<FileStrategy, string, string> {
    getDeletedCacheSize(): number {
      return this.deletedCache.size
    }
  }

  test('Scenario 9: Garbage Collection Logic', () => {
    // Use subclass to access protected state
    const manager = new TestSyncMVCCManager(new FileStrategy())
    const file = getPath('gc.txt')
    manager.createTransaction().create(file, 'Init').commit()

    // 10 Updates
    for (let i = 0; i < 10; i++) {
      manager.createTransaction().write(file, `Update ${i}`).commit()
    }

    // Should have accumulated versions in index and deletedCache
    expect(manager.getDeletedCacheSize()).toBeGreaterThan(0)

    // Trigger GC
    manager.createTransaction().create(getPath('trigger_gc.txt'), 'Trigger').commit()

    // Now minActiveVersion should be latest. Old versions < minActiveVersion should be purged.
    expect(manager.getDeletedCacheSize()).toBe(0)
  })
})