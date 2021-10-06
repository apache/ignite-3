# Ignite vault module
This module provides Vault API implementation.

Vault is the local persistent key-value storage where node maintains its' local state. The data stored in the vault is
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
  [reliable watch processing](../runner/README.md#reliable-watch-processing). 
  
There is an implementation of Vault based on RocksDB.
