export type Deferred<T> = Promise<T> | T

export type DeleteEntry<T> = {
  value: T
  deletedAtVersion: number
}

export type TransactionEntry<K, T> = {
  key: K
  data: T
}

export type TransactionConflict<K, T> = {
  key: K
  parent: T
  child: T
}

export type TransactionMergeFailure<K, T> = {
  error: string
  conflict: TransactionConflict<K, T>
}

export type TransactionResult<K, T> = {
  label?: string
  success: true
  created: TransactionEntry<K, T>[]
  updated: TransactionEntry<K, T>[]
  deleted: TransactionEntry<K, T>[]
} | {
  label?: string
  success: false
  error: string
  conflict?: TransactionConflict<K, T>
  created: TransactionEntry<K, T>[]
  updated: TransactionEntry<K, T>[]
  deleted: TransactionEntry<K, T>[]
}
