import { MVCCStrategy } from '../base'

export abstract class SyncMVCCStrategy<K, T> extends MVCCStrategy<K, T> {
  abstract read(key: K): T
  abstract write(key: K, value: T): void
  abstract delete(key: K): void
  abstract exists(key: K): boolean
}
