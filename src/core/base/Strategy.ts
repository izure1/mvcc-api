import type { Deferred } from '../../types'

export abstract class MVCCStrategy<T> {
  abstract read(key: string): Deferred<T>
  abstract write(key: string, value: T): Deferred<void>
  abstract delete(key: string): Deferred<void>
  abstract exists(key: string): Deferred<boolean>
}
