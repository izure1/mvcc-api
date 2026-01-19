import fs from 'node:fs'
import { AsyncMVCCStrategy } from '../../src'

export class AsyncFileStrategy extends AsyncMVCCStrategy<string, string> {
  async read(key: string): Promise<string> {
    return fs.promises.readFile(key, 'utf-8')
  }

  async write(key: string, value: string): Promise<void> {
    await fs.promises.writeFile(key, value, 'utf-8')
  }

  async delete(key: string): Promise<void> {
    await fs.promises.unlink(key)
  }

  async exists(key: string): Promise<boolean> {
    return fs.existsSync(key)
  }
}
