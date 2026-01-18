import { MVCCStrategy } from '../base'

export abstract class AsyncMVCCStrategy<T> extends MVCCStrategy<T> {
  abstract read(key: string): Promise<T>
  abstract write(key: string, value: T): Promise<void>
  abstract delete(key: string): Promise<void>
  abstract exists(key: string): Promise<boolean>
}
