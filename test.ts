import fs from 'node:fs'
import { AsyncMVCCStrategy, AsyncMVCCTransaction } from './src'

interface User {
  name: string
  balance: number
}

export class AsyncFileStrategy extends AsyncMVCCStrategy<string, User> {
  async read(key: string): Promise<User> {
    return JSON.parse(await fs.promises.readFile(key, 'utf-8'))
  }
  async write(key: string, value: User): Promise<void> {
    await fs.promises.writeFile(key, JSON.stringify(value, null, 2), 'utf-8')
  }
  async delete(key: string): Promise<void> {
    await fs.promises.unlink(key)
  }
  async exists(key: string): Promise<boolean> {
    return fs.existsSync(key)
  }
}

async function main() {
  const strategy = new AsyncFileStrategy()
  const root = new AsyncMVCCTransaction(strategy)

  // Start a transaction
  let tx1 = root.createNested()
  let tx2 = root.createNested()
  let tx3 = tx2.createNested()

  try {
    await tx3.create('./user2.json', { name: 'Alice', balance: 100 })
    await tx2.write('./user.json', { name: 'Alice', balance: 101 })
    await tx1.write('./user.json', { name: 'Alice', balance: 102 })

    // Commit changes to storage
    console.log(await tx3.commit('tx3'))
    console.log(await tx2.commit('tx2'))
    console.log(await tx1.commit('tx1'))
    console.log(await root.commit('root'))
  } catch (err) {
    console.error('Transaction failed:', err)
    tx1.rollback()
    tx2.rollback()
  }
}

main()
