# Metastorage idempotent command cache eviction module

Module responsible for metastorage idempotent command cache eviction.

Metastorage idempotent command cache is a mapping of commandId -> command evaluation result that is used to store the results of
the invoke and multi-invoke commands in order not to re-evaluate invoke condition in case of operation retry. By the definition it's
necessary to store such results for the command-processing-timeout + max clock skew, thus after given interval corresponding cached command
may re evicted. IdempotentCacheVacuumizer is an actor to trigger such evictions. It would be reasonable to put it inside metastorage module
itself instead of creating new one, however it's not possible because of cyclic dependency. IdempotentCacheVacuumizer requires maxClockSkew
that is stored in distributed configuration, that on it's turn requires metastorage. That's why the new module was introduced.