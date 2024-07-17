# Ignite vault module
This module provides Vault API implementation.

Vault is the local persistent key-value storage where node maintains its local state. The data stored in the vault is
semantically divided in the following categories:
* User-level local configuration properties (such as memory limits, network timeouts, etc). User-level configuration
  properties can be written both at runtime (not all properties will be applied at runtime, however, - some of them will
  require a full node restart) and when a node is shut down (in order to be able to change properties that prevent node
  startup for some reason)
* System-level private properties (such as computed local statistics, node-local common paths, etc). System-level
  private properties are computed locally based on the information available at node locally (not based on metastorage
  watched values)
* System-level distributed metastorage projected properties (such as paths to partition files, etc). System-level
  projected properties are associated with one or more metastorage properties and are computed based on the local node
  state and the metastorage properties values. System-level projected properties values are semantically bound to a
  particular revision of the dependee properties and must be recalculated when dependees are changed (see
  [reliable watch processing](../runner/README.md#reliable-watch-processing)). 
  
The main components of the module are the following: 
* [VaultEntry](src/main/java/org/apache/ignite/internal/vault/VaultEntry.java) is the vault unit as entry with comparable key represented as a [ByteArray](../core/src/main/java/org/apache/ignite/lang/ByteArray.java) and value represented as an array of bytes.
* [VaultService](src/main/java/org/apache/ignite/internal/vault/VaultService.java) defines interface for accessing to a vault service. 
  There are standard methods for working with keys and values like get [VaultEntry](src/main/java/org/apache/ignite/internal/vault/VaultEntry.java) by key, 
  put value with key to vault, remove value with a key from vault, putAll method that inserts or updates entries with given keys and given values.
  Also there is a range method that returns a view of the portion of vault whose keys range from fromKey, inclusive, to toKey, exclusive.
* [VaultManager](src/main/java/org/apache/ignite/internal/vault/VaultManager.java) is responsible for handling [VaultService](src/main/java/org/apache/ignite/internal/vault/VaultService.java)
  lifecycle and providing interface for managing local keys.

There are two implementations of [VaultService](src/main/java/org/apache/ignite/internal/vault/VaultService.java): 
[InMemoryVaultService](src/test/java/org/apache/ignite/internal/vault/inmemory/InMemoryVaultService.java) and 
[PersistentVaultService](src/main/java/org/apache/ignite/internal/vault/persistence/PersistentVaultService.java).
The first one is the in-memory implementation of [VaultService](src/main/java/org/apache/ignite/internal/vault/VaultService.java) 
and mostly used for testing purposes.

The second one is [VaultService](src/main/java/org/apache/ignite/internal/vault/VaultService.java) implementation based on [RocksDB](https://github.com/facebook/rocksdb).
[RocksDB](https://github.com/facebook/rocksdb) is a storage engine with key/value interface, where keys and values are arbitrary byte streams.
For more info about RocksDB see corresponding [wiki](https://github.com/facebook/rocksdb/wiki).
