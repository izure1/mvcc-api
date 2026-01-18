import type { Deferred } from '../../types'

/**
 * MVCC Strategy abstract class.
 * Defines the interface for data storage strategies (e.g., File System, In-Memory).
 * To implement a new storage backend, extend this class and implement the abstract methods.
 * @template K The type of key used for data storage (e.g., string).
 * @template T The type of data stored (e.g., string, Buffer, object).
 */
export abstract class MVCCStrategy<K, T> {
  /**
   * Reads a value from the storage.
   * @param key The key to read.
   * @returns The value corresponding to the key.
   */
  abstract read(key: K): Deferred<T>

  /**
   * Writes a value to the storage.
   * @param key The key to write.
   * @param value The value to write.
   */
  abstract write(key: K, value: T): Deferred<void>

  /**
   * Deletes a value from the storage.
   * @param key The key to delete.
   */
  abstract delete(key: K): Deferred<void>

  /**
   * Checks if a key exists in the storage.
   * @param key The key to check.
   * @returns True if the key exists, false otherwise.
   */
  abstract exists(key: K): Deferred<boolean>
}
