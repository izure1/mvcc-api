export type Deferred<T> = Promise<T> | T

export type DeleteEntry<T> = {
  value: T
  deletedAtVersion: number
}

export type TransactionEntry<K, T> = {
  key: K
  data: T
}

export type TransactionResult<K, T> = {
  success: boolean
  error?: string
  created: TransactionEntry<K, T>[]
  updated: TransactionEntry<K, T>[]
  deleted: TransactionEntry<K, T>[]
}
