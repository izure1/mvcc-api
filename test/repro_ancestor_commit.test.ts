
import { SyncMVCCTransaction, AsyncMVCCTransaction } from '../src'
import { FileStrategy } from './strategy/FileStrategy'
import { AsyncFileStrategy } from './strategy/AsyncFileStrategy'
import * as fs from 'node:fs'
import * as path from 'node:path'

const TEST_DIR = path.join(__dirname, 'repro_ancestor_temp')

describe('Ancestor Commit Validation Repro', () => {
  beforeAll(() => {
    if (!fs.existsSync(TEST_DIR)) fs.mkdirSync(TEST_DIR)
  })

  afterAll(() => {
    if (fs.existsSync(TEST_DIR)) fs.rmSync(TEST_DIR, { recursive: true, force: true })
  })

  describe('Sync Transaction', () => {
    it('should fail commit if direct parent is already committed and report entries', () => {
      const root = new SyncMVCCTransaction(new FileStrategy())

      const t1 = root.createNested()
      const t2 = t1.createNested()

      // t2에 변경 사항 추가
      t2.create('key1', 'value1')

      // 부모(t1)를 먼저 커밋
      const t1Result = t1.commit()
      expect(t1Result.success).toBe(true)

      // 자식(t2)을 커밋 시도
      const t2Result = t2.commit()

      // 실패해야 하며, key1 항목을 포함해야 함
      expect(t2Result.success).toBe(false)
      expect((t2Result as any).error).toBe('Ancestor transaction already committed')
      expect(t2Result.created.length).toBe(1)
      expect(t2Result.created[0].key).toBe('key1')
    })

    it('should fail commit if grandparent is already committed', () => {
      const root = new SyncMVCCTransaction(new FileStrategy())

      const t1 = root.createNested()
      const t2 = t1.createNested()
      const t3 = t2.createNested()

      t3.create('key_grand', 'val')

      // 조상(t1)을 먼저 커밋
      t1.commit()

      // 증손자(t3) 커밋 시도
      const t3Result = t3.commit()

      expect(t3Result.success).toBe(false)
      expect((t3Result as any).error).toBe('Ancestor transaction already committed')
      expect(t3Result.created.length).toBe(1)
    })
  })

  describe('Async Transaction', () => {
    it('should fail commit if ancestor is already committed and report entries', async () => {
      const root = new AsyncMVCCTransaction(new AsyncFileStrategy())

      const t1 = root.createNested()
      const t2 = t1.createNested()

      // t2에 변경 사항 추가
      await t2.create('key_async', 'value_async')

      // 부모(t1) 커밋
      await t1.commit()

      // 자식(t2) 커밋 시도
      const t2Result = await t2.commit()

      expect(t2Result.success).toBe(false)
      expect((t2Result as any).error).toBe('Ancestor transaction already committed')
      expect(t2Result.created.length).toBe(1)
      expect(t2Result.created[0].key).toBe('key_async')
    })
  })
})
