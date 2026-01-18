export type Deferred<T> = Promise<T> | T

export type DeleteEntry<T> = {
  value: T
  deletedAtVersion: number
}
