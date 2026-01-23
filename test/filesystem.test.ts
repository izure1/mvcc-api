import fs from 'node:fs'
import path from 'node:path'
import { SyncMVCCTransaction } from '../src'
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
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
    fs.mkdirSync(tmpDir)
  })

  const getPath = (filename: string) => path.join(tmpDir, filename)

  test('Scenario 1: Basic Transaction & Snapshot Isolation', () => {
    const root = new SyncMVCCTransaction(new FileStrategy())

    // Setup
    const file1 = getPath('file1.txt')
    const tx0 = root.createNested()
    tx0.create(file1, 'Hello World').commit()

    expect(fs.readFileSync(file1, 'utf-8')).toBe('Hello World')

    // Isolation
    const tx1 = root.createNested()
    const tx2 = root.createNested()

    expect(tx1.read(file1)).toBe('Hello World')
    expect(tx2.read(file1)).toBe('Hello World')

    tx1.delete(file1).commit()

    expect(tx2.read(file1)).toBe('Hello World')

    tx2.commit()

    const tx3 = root.createNested()
    expect(tx3.read(file1)).toBeNull()
  })

  test('Scenario 2: Conflict Detection', () => {
    const root = new SyncMVCCTransaction(new FileStrategy())
    const file2 = getPath('file2.txt')

    root.createNested().create(file2, 'Initial').commit()

    const tx3 = root.createNested()
    const tx4 = root.createNested()

    expect(tx3.read(file2)).toBe('Initial')

    tx4.write(file2, 'Modified by TX4').commit()

    tx3.write(file2, 'Modified by TX3')
    const result = tx3.commit()
    expect(result.success).toBe(false)
    expect((result as any).error).toMatch(/Commit conflict/)
  })

  test('Scenario 3: Copy-on-Write', () => {
    const root = new SyncMVCCTransaction(new FileStrategy())
    const file2 = getPath('file2.txt')

    root.createNested().create(file2, 'Version 1').commit()

    const tx5 = root.createNested()
    const tx6 = root.createNested()

    expect(tx5.read(file2)).toBe('Version 1')

    tx5.write(file2, 'Version 2')

    expect(tx6.read(file2)).toBe('Version 1')
    expect(tx5.read(file2)).toBe('Version 2')

    tx5.commit()
    expect(fs.readFileSync(file2, 'utf-8')).toBe('Version 2')
  })

  test('Scenario: Read-Only Transaction Success', () => {
    const root = new SyncMVCCTransaction(new FileStrategy())
    const file = getPath('readonly.txt')
    root.createNested().create(file, 'v1').commit()

    const txRead = root.createNested()
    const txWrite = root.createNested()

    // Read in txRead
    expect(txRead.read(file)).toBe('v1')

    // Write in txWrite and commit
    txWrite.write(file, 'v2').commit()

    // txRead should still see v1 (Snapshot)
    expect(txRead.read(file)).toBe('v1')

    // txRead should commit successfully even though data changed (Snapshot Isolation)
    const result = txRead.commit()
    expect(result.success).toBe(true)
  })

  test('Scenario: Strict Write-Write Conflict', () => {
    const root = new SyncMVCCTransaction(new FileStrategy())
    const file = getPath('conflict.txt')
    root.createNested().create(file, 'start').commit()

    const tx1 = root.createNested()
    const tx2 = root.createNested()

    tx1.write(file, 'tx1')
    tx2.write(file, 'tx2')

    tx1.commit()

    // tx2 must fail
    const result = tx2.commit()
    expect(result.success).toBe(false)
    expect((result as any).error).toMatch(/Commit conflict/)
  })

  test('Persistence', () => {
    const file3 = getPath('file3.txt')
    const root1 = new SyncMVCCTransaction(new FileStrategy())
    root1.createNested().create(file3, 'Persistent Data').commit()

    // New Root instance should see data
    const root2 = new SyncMVCCTransaction(new FileStrategy())
    const tx = root2.createNested()

    expect(tx.read(file3)).toBe('Persistent Data')
  })

  test('Scenario: exists() method', () => {
    const root = new SyncMVCCTransaction(new FileStrategy())
    const file = getPath('exists_test.txt')

    // 1. 존재하지 않는 키
    const tx1 = root.createNested()
    expect(tx1.exists(file)).toBe(false)

    // 2. create 후 버퍼에서 존재
    tx1.create(file, 'test value')
    expect(tx1.exists(file)).toBe(true)
    tx1.commit()

    // 3. 커밋 후 디스크에서 존재
    const tx2 = root.createNested()
    expect(tx2.exists(file)).toBe(true)

    // 4. 삭제 버퍼에 있으면 존재하지 않음
    tx2.delete(file)
    expect(tx2.exists(file)).toBe(false)

    // 5. 스냅샷 격리: 삭제 전 스냅샷은 여전히 존재로 보임
    const tx3 = root.createNested()
    expect(tx3.exists(file)).toBe(true) // tx2가 아직 커밋 안 함

    tx2.commit()

    // 6. 삭제 커밋 후 새 트랜잭션에서는 존재하지 않음
    const tx4 = root.createNested()
    expect(tx4.exists(file)).toBe(false)
  })

  test('Scenario: Root commit and then read', () => {
    const root = new SyncMVCCTransaction(new FileStrategy())
    const file = getPath('root_commit_read.txt')

    // 1. 루트에서 직접 create → commit
    root.create(file, 'root created')
    const result1 = root.commit()
    expect(result1.success).toBe(true)

    // 2. 커밋 후 루트에서 read - 이전에는 null이 반환되던 버그
    expect(root.read(file)).toBe('root created')
    expect(root.exists(file)).toBe(true)

    // 3. 루트에서 write → commit → read
    root.write(file, 'root updated')
    const result2 = root.commit()
    expect(result2.success).toBe(true)
    expect(root.read(file)).toBe('root updated')

    // 4. 루트에서 delete → commit → read
    root.delete(file)
    const result3 = root.commit()
    expect(result3.success).toBe(true)
    expect(root.read(file)).toBeNull()
    expect(root.exists(file)).toBe(false)

    // 5. 루트 재사용: 새 파일 생성
    const file2 = getPath('root_commit_read2.txt')
    root.create(file2, 'another file')
    root.commit()
    expect(root.read(file2)).toBe('another file')
  })
})
