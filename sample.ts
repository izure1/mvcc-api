import { SyncMVCCManager, SyncMVCCStrategy } from './src'
import fs from 'node:fs'

class SyncStrategy extends SyncMVCCStrategy<string> {
  read(key: string): string {
    return fs.readFileSync(key, 'utf-8');
  }

  write(key: string, value: string): void {
    fs.writeFileSync(key, value);
  }

  delete(key: string): void {
    fs.unlinkSync(key);
  }

  exists(key: string): boolean {
    return fs.existsSync(key);
  }
}

const manager = new SyncMVCCManager(new SyncStrategy());

// 초기 데이터 설정
const tx0 = manager.createTransaction();
tx0.create('file1.txt', 'Hello World')
  .create('file2.txt', 'MVCC Storage')
  .commit();

console.log('=== 초기 상태 ===')

// 시나리오 1: 두 트랜잭션이 동시에 시작
console.log('\n=== 시나리오 1: 스냅샷 격리 ===');
const tx1 = manager.createTransaction();
const tx2 = manager.createTransaction();

console.log('TX1 reads file1:', tx1.read('file1.txt'));
console.log('TX2 reads file1:', tx2.read('file1.txt'));

// TX1이 file1을 삭제하고 커밋
tx1.delete('file1.txt').commit();
console.log('TX1 deleted file1 and committed');
console.log('Deleted cache:', [...manager.deletedCache.entries()]);

// TX2는 여전히 file1을 읽을 수 있음 (스냅샷 격리)
console.log('TX2 still reads file1:', tx2.read('file1.txt'));
tx2.commit();

// 시나리오 2: 충돌 감지
console.log('\n=== 시나리오 2: 충돌 감지 ===');
const tx3 = manager.createTransaction();
const tx4 = manager.createTransaction();

tx3.read('file2.txt'); // TX3이 file2를 읽음
console.log('TX3 read file2:', tx3.read('file2.txt'));

// TX4가 file2를 수정하고 커밋
tx4.write('file2.txt', 'Modified by TX4').commit();
console.log('TX4 modified and committed file2');

// TX3이 file2를 수정하려고 시도 -> 충돌 발생
try {
  tx3.write('file2.txt', 'Modified by TX3').commit();
} catch (error) {
  console.log('TX3 commit failed:', error);
}

// 시나리오 3: Copy-on-Write
console.log('\n=== 시나리오 3: Copy-on-Write ===');
const tx5 = manager.createTransaction();
const tx6 = manager.createTransaction();

console.log('TX5 reads file2:', tx5.read('file2.txt'));
tx5.write('file2.txt', 'TX5 version');
console.log('TX5 modified file2 (not committed)');

console.log('TX6 reads file2:', tx6.read('file2.txt')); // 원본 읽음
console.log('TX5 reads file2:', tx5.read('file2.txt')); // 수정본 읽음

tx5.commit();
