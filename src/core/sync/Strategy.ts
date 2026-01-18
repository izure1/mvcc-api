import { MVCCStrategy } from '../base'

export abstract class SyncMVCCStrategy<T> extends MVCCStrategy<T> {
  abstract read(key: string): T
  abstract write(key: string, value: T): void
  abstract delete(key: string): void
  abstract exists(key: string): boolean
}
