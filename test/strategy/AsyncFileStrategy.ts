import fs from 'node:fs/promises'
import { AsyncMVCCStrategy } from '../../src/core/async/Strategy'

export class AsyncFileStrategy extends AsyncMVCCStrategy<string> {
  async read(key: string): Promise<string> {
    return fs.readFile(key, 'utf-8')
  }
  async write(key: string, value: string): Promise<void> {
    await fs.writeFile(key, value, 'utf-8')
  }
  async delete(key: string): Promise<void> {
    await fs.unlink(key)
  }
  async exists(key: string): Promise<boolean> {
    try {
      await fs.access(key)
      return true
    } catch {
      return false
    }
  }
}
