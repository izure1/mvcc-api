import { MVCCStrategy } from '../base'

export abstract class AsyncMVCCStrategy<K, T> extends MVCCStrategy<K, T> {
  abstract read(key: K): Promise<T>
  abstract write(key: K, value: T): Promise<void>
  abstract delete(key: K): Promise<void>
  abstract exists(key: K): Promise<boolean>
}
