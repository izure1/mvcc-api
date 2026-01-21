
import fs from 'node:fs'
import path from 'node:path'
import { SyncMVCCTransaction } from '../src'
import { FileStrategy } from './strategy/FileStrategy'

describe('Concurrent Access Reproduction', () => {
  const tmpDir = path.join(__dirname, 'tmp_repro_concurrent')

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

  test('Scenario 1: Pre-existing File Deletion (Concurrent Read)', () => {
    const file = getPath('pre_existing_del.txt')
    fs.writeFileSync(file, 'Original Content') // Created outside MVCC

    const root = new SyncMVCCTransaction(new FileStrategy())

    // 1. Start Reader (Snapshot at start)
    const reader = root.createNested()

    // 2. Start Deleter
    const deleter = root.createNested()
    deleter.delete(file).commit()

    // 3. Verify physical deletion
    expect(fs.existsSync(file)).toBe(false)

    // 4. Reader tries to read
    // Expected: Should read 'Original Content' from deletedCache
    const content = reader.read(file)
    console.log('Scenario 1 Read Result:', content)
    expect(content).toBe('Original Content')
  })

  test('Scenario 2: Pre-existing File Update (Concurrent Read)', () => {
    const file = getPath('pre_existing_upd.txt')
    fs.writeFileSync(file, 'Original Content') // Created outside MVCC

    const root = new SyncMVCCTransaction(new FileStrategy())

    // 1. Start Reader
    const reader = root.createNested()

    // 2. Start Writer
    const writer = root.createNested()
    writer.write(file, 'Updated Content').commit()

    // 3. Verify physical update
    expect(fs.readFileSync(file, 'utf-8')).toBe('Updated Content')

    // 4. Reader tries to read
    // Expected: Should read 'Original Content' from deletedCache (backup)
    const content = reader.read(file)
    console.log('Scenario 2 Read Result:', content)
    expect(content).toBe('Original Content')
  })

  test('Scenario 3: Normal Lifecycle (Create -> Delete -> Read)', () => {
    const file = getPath('normal_lifecycle.txt')
    const root = new SyncMVCCTransaction(new FileStrategy())

    // V1 Create
    root.createNested().create(file, 'Version 1').commit()

    const reader = root.createNested()

    // V2 Delete
    root.createNested().delete(file).commit()

    // Reader should see V1
    expect(reader.read(file)).toBe('Version 1')
  })
})
