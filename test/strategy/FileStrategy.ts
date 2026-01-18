import fs from 'node:fs'
import { SyncMVCCStrategy } from '../../src'

export class FileStrategy extends SyncMVCCStrategy<string, string> {
  read(key: string): string {
    return fs.readFileSync(key, 'utf-8')
  }

  write(key: string, value: string): void {
    fs.writeFileSync(key, value)
  }

  delete(key: string): void {
    fs.unlinkSync(key)
  }

  exists(key: string): boolean {
    return fs.existsSync(key)
  }
}
