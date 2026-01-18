import fs from 'node:fs'
import path from 'node:path'
import { SyncMVCCManager } from '../src'
import { FileStrategy } from './strategy/FileStrategy'

describe('FileSystem MVCC', () => {
  const tmpDir = path.join(__dirname, 'tmp')

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
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
    fs.mkdirSync(tmpDir);
  })

  const getPath = (filename: string) => path.join(tmpDir, filename)

  test('Scenario 1: Basic Transaction & Snapshot Isolation', () => {
    const manager = new SyncMVCCManager(new FileStrategy())

    // Setup
    const file1 = getPath('file1.txt')
    const tx0 = manager.createTransaction()
    tx0.create(file1, 'Hello World').commit()

    expect(fs.readFileSync(file1, 'utf-8')).toBe('Hello World')

    // Isolation
    const tx1 = manager.createTransaction()
    const tx2 = manager.createTransaction()

    expect(tx1.read(file1)).toBe('Hello World')
    expect(tx2.read(file1)).toBe('Hello World')

    tx1.delete(file1).commit()

    expect(tx2.read(file1)).toBe('Hello World')

    tx2.commit()

    const tx3 = manager.createTransaction()
    expect(tx3.read(file1)).toBeNull()
  })

  test('Scenario 2: Conflict Detection', () => {
    const manager = new SyncMVCCManager(new FileStrategy())
    const file2 = getPath('file2.txt')

    manager.createTransaction().create(file2, 'Initial').commit()

    const tx3 = manager.createTransaction()
    const tx4 = manager.createTransaction()

    expect(tx3.read(file2)).toBe('Initial')

    tx4.write(file2, 'Modified by TX4').commit()

    expect(() => {
      tx3.write(file2, 'Modified by TX3').commit()
    }).toThrow(/Commit conflict/)
  })

  test('Scenario 3: Copy-on-Write', () => {
    const manager = new SyncMVCCManager(new FileStrategy())
    const file2 = getPath('file2.txt')

    manager.createTransaction().create(file2, 'Version 1').commit()

    const tx5 = manager.createTransaction()
    const tx6 = manager.createTransaction()

    expect(tx5.read(file2)).toBe('Version 1')

    tx5.write(file2, 'Version 2')

    expect(tx6.read(file2)).toBe('Version 1')
    expect(tx5.read(file2)).toBe('Version 2')

    tx5.commit()
    expect(fs.readFileSync(file2, 'utf-8')).toBe('Version 2')
  })

  test('Persistence', () => {
    const file3 = getPath('file3.txt')
    const manager1 = new SyncMVCCManager(new FileStrategy())
    manager1.createTransaction().create(file3, 'Persistent Data').commit()

    const manager2 = new SyncMVCCManager(new FileStrategy())
    const tx = manager2.createTransaction()

    expect(tx.read(file3)).toBe('Persistent Data')
  })
})
