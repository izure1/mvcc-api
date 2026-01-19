export type Deferred<T> = Promise<T> | T

export type DeleteEntry<T> = {
  value: T
  deletedAtVersion: number
}

export type TransactionResult<K> = {
  success: boolean
  created: K[]
  updated: K[]
  deleted: K[]
}
