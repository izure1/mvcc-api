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
  // 자식은 부모가 커밋한 데이터만 볼 수 있음
  test('Child sees ONLY committed data from parent', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())

    // root가 값을 쓰고 커밋
    root.write('k1', 'committed_v1')
    root.commit()

    const parent = root.createNested()

    // parent가 미커밋 상태로 값을 변경
    parent.write('k1', 'uncommitted_v2')
    parent.write('k2', 'uncommitted_new')

    const child = parent.createNested()

    // 자식은 부모의 미커밋 버퍼를 볼 수 없음 - 커밋된 값만 봄
    expect(child.read('k1')).toBe('committed_v1') // 부모가 변경했지만 커밋 안 함
    expect(child.read('k2')).toBeNull() // 존재하지 않음 (커밋 안 됨)

    // 자식 자신이 쓴 값은 볼 수 있음
    child.write('k3', 'child_val')
    expect(child.read('k3')).toBe('child_val')
  })

  test('Parent does NOT see changes from uncommitted child', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('k1', 'root_val')
    root.commit()

    const parent = root.createNested()
    const child = parent.createNested()

    child.write('k1', 'child_v1')

    // parent는 child의 미커밋 변경을 볼 수 없음
    expect(parent.read('k1')).toBe('root_val')
  })

  test('Child commit merges changes to parent buffer', () => {
    const strategy = new InMemoryStrategy()
    const root = new TestRootTransaction(strategy)
    root.write('base', 'base_val')
    root.commit()

    const parent = root.createNested()
    const child = parent.createNested()

    child.write('k1', 'child_v1')
    child.commit() // Merge to parent's buffer

    // Parent's buffer now has child's change
    expect(parent.read('k1')).toBe('child_v1')

    // Strategy still has no k1 (only base was committed)
    expect(strategy.exists('k1')).toBe(false)

    parent.commit() // Merge to Root and persist immediately
    // Now it's persisted
    expect(strategy.read('k1')).toBe('child_v1')
  })

  test('Nested transaction rollback does not affect parent', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('k1', 'root_val')
    root.commit()

    const parent = root.createNested()
    parent.write('k1', 'parent_val') // 미커밋 상태

    const child = parent.createNested()
    child.write('k1', 'child_overwrite')
    child.write('k2', 'child_v1')

    child.rollback() // Should discard child changes

    // parent의 버퍼 값은 유지됨
    expect(parent.read('k1')).toBe('parent_val')
    expect(parent.read('k2')).toBeNull()
  })

  test('Three levels of nesting with commits', () => {
    const strategy = new InMemoryStrategy()
    const root = new TestRootTransaction(strategy)

    root.write('root', 'val')
    root.commit() // 이제 root='val'은 커밋됨

    const l1 = root.createNested()
    l1.write('l1', 'val')

    const l2 = l1.createNested()
    // l2는 l1의 미커밋 버퍼를 볼 수 없음 - 커밋된 root 데이터만 봄
    expect(l2.read('root')).toBe('val')
    expect(l2.read('l1')).toBeNull() // l1은 아직 커밋 안 함

    l2.write('l2', 'val')

    // Isolation (upwards)
    expect(l1.read('l2')).toBeNull()

    l2.commit() // Merge to L1's buffer
    expect(l1.read('l2')).toBe('val')

    l1.commit() // Merge to Root's buffer
    // Root는 커밋됐으므로 strategy에서 확인
    root.commit() // 최종 영속화
    expect(strategy.read('l2')).toBe('val')
    expect(strategy.read('l1')).toBe('val')
  })

  test('Shadowing: Child overwrites parent value after commit', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('k1', 'root_val')
    root.commit()

    const parent = root.createNested()
    parent.write('k1', 'parent_val')
    parent.commit() // 이제 k1='parent_val'은 커밋됨

    const child = root.createNested() // 새로운 자식
    expect(child.read('k1')).toBe('parent_val') // 커밋된 값

    child.write('k1', 'child_val')
    expect(child.read('k1')).toBe('child_val') // 자신의 버퍼

    child.commit()
    expect(root.getStrategy().read('k1')).toBe('child_val')
  })
})

// ============================================
// 엄격한 스냅샷 독립(Snapshot Isolation) 테스트
// 자식은 오직 커밋된 데이터만 볼 수 있음
// ============================================
describe('Strict Snapshot Isolation Tests', () => {
  // 자식은 부모의 미커밋 버퍼를 볼 수 없음
  test('자식은 부모의 미커밋 버퍼를 볼 수 없음', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())

    root.write('k1', 'committed_v1')
    root.commit()

    const parent = root.createNested()
    parent.write('k1', 'uncommitted_v2') // 미커밋
    parent.write('k2', 'uncommitted_new')

    const child = parent.createNested()

    // 자식은 커밋된 값만 볼 수 있음
    expect(child.read('k1')).toBe('committed_v1')
    expect(child.read('k2')).toBeNull()

    // 부모의 이후 변경도 자식에게 영향 없음
    parent.write('k1', 'uncommitted_v3')
    expect(child.read('k1')).toBe('committed_v1')
  })

  // 자식 생성 이후 다른 트랜잭션이 커밋해도 자식은 스냅샷 유지
  test('자식 생성 이후 커밋된 데이터는 보이지 않음', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('k1', 'v1')
    root.commit() // version 1

    const tx1 = root.createNested() // snapshotVersion = 1

    // 다른 트랜잭션이 값을 변경하고 커밋
    const tx2 = root.createNested()
    tx2.write('k1', 'v2')
    tx2.commit() // version 2

    // tx1은 여전히 version 1의 스냅샷을 봄
    expect(tx1.read('k1')).toBe('v1')
  })

  // 형제 트랜잭션끼리는 서로의 변경을 볼 수 없음
  test('형제 트랜잭션 독립: 서로의 미커밋 변경을 보지 못함', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('shared', 'committed_val')
    root.commit()

    const sibling1 = root.createNested()
    const sibling2 = root.createNested()

    sibling1.write('s1_key', 's1_val')
    sibling1.write('shared', 'sibling1_val')

    sibling2.write('s2_key', 's2_val')
    sibling2.write('shared', 'sibling2_val')

    // 각 형제는 자신의 값만 보고 상대방의 값은 못 봄
    expect(sibling1.read('s2_key')).toBeNull()
    expect(sibling2.read('s1_key')).toBeNull()

    // shared 키는 각자 자신의 버퍼 값을 봄
    expect(sibling1.read('shared')).toBe('sibling1_val')
    expect(sibling2.read('shared')).toBe('sibling2_val')
  })

  // 자식 커밋 후 새로 생성된 트랜잭션은 커밋된 값을 봄
  test('커밋 후 새 트랜잭션은 커밋된 값을 봄', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('base', 'base_val')
    root.commit()

    const tx1 = root.createNested()
    tx1.write('k1', 'tx1_val')
    tx1.commit() // version 2

    // 커밋 후 새로운 트랜잭션
    const tx2 = root.createNested() // snapshotVersion = 2
    expect(tx2.read('k1')).toBe('tx1_val')
  })

  // 깊은 중첩(5단계)에서 최하위가 커밋된 최상위 값만 읽음
  test('5단계 중첩: 최하위가 커밋된 최상위 값만 읽음', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())

    root.write('root_key', 'root_val')
    root.commit() // 영속화

    const l1 = root.createNested()
    l1.write('l1_key', 'l1_val')

    const l2 = l1.createNested()
    l2.write('l2_key', 'l2_val')

    const l3 = l2.createNested()
    l3.write('l3_key', 'l3_val')

    const l4 = l3.createNested()
    l4.write('l4_key', 'l4_val')

    const l5 = l4.createNested()

    // L5는 커밋된 root_key만 봄 (나머지는 미커밋)
    expect(l5.read('root_key')).toBe('root_val')
    expect(l5.read('l1_key')).toBeNull()
    expect(l5.read('l2_key')).toBeNull()
    expect(l5.read('l3_key')).toBeNull()
    expect(l5.read('l4_key')).toBeNull()
  })

  // 롤백 후에도 커밋된 스냅샷은 유지됨
  test('자식 롤백 후에도 커밋된 스냅샷 유지', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('k1', 'committed_v1')
    root.commit()

    const child1 = root.createNested()
    const child2 = root.createNested()

    child1.write('k1', 'child1_overwrites')
    child1.rollback() // child1 취소

    // child2는 여전히 커밋된 값을 봄
    expect(child2.read('k1')).toBe('committed_v1')
  })

  // 동시에 여러 계층에서 같은 키를 수정: 자신의 버퍼만 봄
  test('같은 키를 여러 계층에서 수정: 자신의 버퍼 우선', () => {
    const strategy = new InMemoryStrategy()
    const root = new TestRootTransaction(strategy)

    root.write('key', 'root')
    root.commit()

    const l1 = root.createNested()
    l1.write('key', 'l1')

    const l2 = l1.createNested()
    l2.write('key', 'l2')

    const l3 = l2.createNested()
    l3.write('key', 'l3')

    // 각 계층은 자신의 버퍼 값을 봄
    expect(l3.read('key')).toBe('l3')
    expect(l2.read('key')).toBe('l2')
    expect(l1.read('key')).toBe('l1')
    expect(strategy.read('key')).toBe('root')

    // 커밋 체인
    l3.commit()
    expect(l2.read('key')).toBe('l3') // l3 값이 l2로 병합

    l2.commit()
    expect(l1.read('key')).toBe('l3') // 병합된 값이 l1으로

    l1.commit()
    expect(strategy.read('key')).toBe('l3')
  })
})

// ============================================
// 키 충돌 감지 테스트
// ============================================
describe('Key Conflict Detection Tests', () => {
  // 같은 키를 부모와 자식이 수정하면 충돌
  test('같은 키 수정 시 충돌 발생', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('k1', 'initial')
    root.commit()

    const parent = root.createNested()
    const child = parent.createNested() // snapshotLocalVersion 기록

    // 자식 생성 후 부모가 같은 키를 수정
    parent.write('A', 'parent_value')

    // 자식도 같은 키를 수정
    child.write('A', 'child_value')

    // 자식 커밋 시 충돌 발생
    expect(() => child.commit()).toThrow(/conflict/i)
  })

  // 다른 키를 수정하면 충돌 없음
  test('다른 키 수정 시 충돌 없음', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('k1', 'initial')
    root.commit()

    const parent = root.createNested()
    const child = parent.createNested()

    // 부모는 'B' 키 수정
    parent.write('B', 'parent_value')

    // 자식은 'A' 키 수정
    child.write('A', 'child_value')

    // 충돌 없이 정상 커밋
    expect(() => child.commit()).not.toThrow()
    expect(parent.read('A')).toBe('child_value')
  })

  // 3단계 중첩: c에서 수정, b에서 수정 시 충돌
  test('3단계 중첩: a-b-c에서 b와 c가 같은 키 수정 시 충돌', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('base', 'v1')
    root.commit()

    const a = root.createNested()
    const b = a.createNested()
    const c = b.createNested()

    // c 생성 후 b가 같은 키를 수정
    b.write('shared', 'b_value')

    // c도 같은 키를 수정
    c.write('shared', 'c_value')

    // c 커밋 시 충돌
    expect(() => c.commit()).toThrow(/conflict.*shared/i)
  })

  // 3단계 중첩: c에서 수정, b에서 다른 키 수정 시 충돌 없음
  test('3단계 중첩: a-b-c에서 b와 c가 다른 키 수정 시 충돌 없음', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('base', 'v1')
    root.commit()

    const a = root.createNested()
    const b = a.createNested()
    const c = b.createNested()

    // b는 'B' 키 수정
    b.write('B', 'b_value')

    // c는 'C' 키 수정
    c.write('C', 'c_value')

    // 충돌 없이 커밋
    expect(() => c.commit()).not.toThrow()
    expect(b.read('C')).toBe('c_value')
    expect(b.read('B')).toBe('b_value')
  })

  // 형제 트랜잭션 간 같은 키 수정 시 충돌
  test('형제 트랜잭션 간 같은 키 수정: 선 커밋자가 승리', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('k1', 'initial')
    root.commit()

    const parent = root.createNested()

    const sibling1 = parent.createNested()
    const sibling2 = parent.createNested()

    // 양쪽 모두 같은 키 수정
    sibling1.write('shared', 'sibling1_val')
    sibling2.write('shared', 'sibling2_val')

    // sibling1 먼저 커밋 - 성공
    expect(() => sibling1.commit()).not.toThrow()

    // sibling2 커밋 시 충돌 (sibling1이 이미 같은 키를 커밋함)
    expect(() => sibling2.commit()).toThrow(/conflict.*shared/i)
  })

  // 형제 트랜잭션 간 다른 키 수정 시 충돌 없음
  test('형제 트랜잭션 간 다른 키 수정: 모두 성공', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('k1', 'initial')
    root.commit()

    const parent = root.createNested()

    const sibling1 = parent.createNested()
    const sibling2 = parent.createNested()

    // 각자 다른 키 수정
    sibling1.write('key1', 'sibling1_val')
    sibling2.write('key2', 'sibling2_val')

    // 둘 다 성공적으로 커밋
    expect(() => sibling1.commit()).not.toThrow()
    expect(() => sibling2.commit()).not.toThrow()

    expect(parent.read('key1')).toBe('sibling1_val')
    expect(parent.read('key2')).toBe('sibling2_val')
  })

  // delete와 write 충돌
  test('delete와 write 충돌: 같은 키에서 충돌', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('target', 'initial')
    root.commit()

    const parent = root.createNested()
    const child = parent.createNested()

    // 부모가 키를 삭제
    parent.delete('target')

    // 자식이 같은 키를 수정
    child.write('target', 'child_value')

    // 충돌 발생
    expect(() => child.commit()).toThrow(/conflict.*target/i)
  })

  // TransactionResult 반환값 테스트
  test('commit 결과: created, updated, deleted 구분', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('existing', 'v1')
    root.commit()

    const tx = root.createNested()
    tx.create('new_key', 'new_value')  // created
    tx.write('existing', 'v2')          // updated
    tx.delete('existing')               // deleted (updated에서 제거)
    tx.create('another_new', 'val')     // created

    const result = tx.commit()

    expect(result.success).toBe(true)
    expect(result.created).toContain('another_new')
    expect(result.deleted).toContain('existing')
    // 'new_key'는 create 후 delete하지 않았지만 existing은 delete됨
    expect(result.updated).toEqual([])
  })

  // rollback 결과 테스트
  test('rollback 결과: 버퍼에 있던 키 반환', () => {
    const root = new TestRootTransaction(new InMemoryStrategy())
    root.write('k1', 'v1')
    root.commit()

    const tx = root.createNested()
    tx.create('created_key', 'val')
    tx.write('k1', 'updated_val')
    tx.delete('k1')

    const result = tx.rollback()

    expect(result.success).toBe(true)
    expect(result.created).toContain('created_key')
    expect(result.deleted).toContain('k1')
  })
})
