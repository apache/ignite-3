/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// THIS IS AUTO-GENERATED FILE. DO NOT EDIT.

#pragma once

#include <cstdint>

namespace ignite {

namespace error {

using underlying_t = std::uint32_t;

static inline const std::uint32_t group_shift = 16;

// Error groups codes.
enum class group : underlying_t {
    CMN = 0x1,
    TBL = 0x2,
    CLIENT = 0x3,
    SQL = 0x4,
    META = 0x5,
    IDX = 0x6,
    TX = 0x7,
    REP = 0x8,
    STORAGE = 0x9,
    DISTRZONES = 0xa,
    NETWORK = 0xb,
    NODECFG = 0xc,
    CODEDEPLOY = 0xd,
    GC = 0xe,
    AUTHENTICATION = 0xf,
    COMPUTE = 0x10,
    CATALOG = 0x11,
    PLACEMENTDRIVER = 0x12,
    WORKERS = 0x13,
    RECOVERY = 0x14
};

inline group get_group_by_error_code(const underlying_t code) {
    return group(code >> group_shift);
}

// Error codes.
enum class code : underlying_t {
    // Common group. Group code: 1
    NODE_STOPPING = 0x10001,
    COMPONENT_NOT_STARTED = 0x10002,
    ILLEGAL_ARGUMENT = 0x10003,
    SSL_CONFIGURATION = 0x10004,
    NODE_LEFT = 0x10005,
    CURSOR_ALREADY_CLOSED = 0x10006,
    RESOURCE_CLOSING = 0x10007,
    INTERNAL = 0x1ffff,

    // Table group. Group code: 2
    TABLE_ALREADY_EXISTS = 0x20001,
    TABLE_NOT_FOUND = 0x20002,
    COLUMN_ALREADY_EXISTS = 0x20003,
    COLUMN_NOT_FOUND = 0x20004,
    TABLE_STOPPING = 0x20005,
    TABLE_DEFINITION = 0x20006,
    SCHEMA_VERSION_MISMATCH = 0x20007,

    // Client group. Group code: 3
    CONNECTION = 0x30001,
    PROTOCOL = 0x30002,
    PROTOCOL_COMPATIBILITY = 0x30003,
    TABLE_ID_NOT_FOUND = 0x30004,
    AUTHENTICATION = 0x30005,
    AUTHORIZATION = 0x30006,
    CONFIGURATION = 0x30007,
    CLUSTER_ID_MISMATCH = 0x30008,
    CLIENT_SSL_CONFIGURATION = 0x30009,
    HANDSHAKE_HEADER = 0x3000a,

    // Sql group. Group code: 4
    QUERY_NO_RESULT_SET = 0x40002,
    SCHEMA_NOT_FOUND = 0x40003,
    STMT_PARSE = 0x40005,
    STMT_VALIDATION = 0x40006,
    CONSTRAINT_VIOLATION = 0x40007,
    EXECUTION_CANCELLED = 0x40008,
    RUNTIME = 0x40009,
    PLANNING_TIMEOUT = 0x4000a,
    MAPPING = 0x4000b,
    TX_CONTROL_INSIDE_EXTERNAL_TX = 0x4000c,

    // MetaStorage group. Group code: 5
    STARTING_STORAGE = 0x50001,
    RESTORING_STORAGE = 0x50002,
    COMPACTION = 0x50003,
    OP_EXECUTION = 0x50004,
    OP_EXECUTION_TIMEOUT = 0x50005,

    // Index group. Group code: 6
    INVALID_INDEX_DEFINITION = 0x60001,
    INDEX_NOT_FOUND = 0x60002,
    INDEX_ALREADY_EXISTS = 0x60003,

    // Transactions group. Group code: 7
    TX_STATE_STORAGE = 0x70001,
    TX_STATE_STORAGE_STOPPED = 0x70002,
    TX_UNEXPECTED_STATE = 0x70003,
    ACQUIRE_LOCK = 0x70004,
    ACQUIRE_LOCK_TIMEOUT = 0x70005,
    TX_COMMIT = 0x70006,
    TX_ROLLBACK = 0x70007,
    TX_FAILED_READ_WRITE_OPERATION = 0x70008,
    TX_REPLICA_UNAVAILABLE = 0x70009,
    TX_STATE_STORAGE_REBALANCE = 0x7000a,
    TX_READ_ONLY_TOO_OLD = 0x7000b,
    TX_INCOMPATIBLE_SCHEMA = 0x7000c,
    TX_PRIMARY_REPLICA_EXPIRED = 0x7000d,
    TX_ALREADY_FINISHED = 0x7000e,
    TX_STALE_OPERATION = 0x7000f,

    // Replicator group. Group code: 8
    REPLICA_COMMON = 0x80001,
    REPLICA_IS_ALREADY_STARTED = 0x80002,
    REPLICA_TIMEOUT = 0x80003,
    REPLICA_UNSUPPORTED_REQUEST = 0x80004,
    REPLICA_UNAVAILABLE = 0x80005,
    REPLICA_MISS = 0x80006,
    CURSOR_CLOSE = 0x80007,
    REPLICA_STOPPING = 0x80008,
    REPLICATION_SAFE_TIME_REORDERING = 0x80009,

    // Storage group. Group code: 9
    GENERIC = 0x90001,
    DIRECTORY_CREATION = 0x90002,
    ALREADY_CLOSED = 0x90003,
    STORAGE_REBALANCE = 0x90004,
    ALREADY_DESTROYED = 0x90005,
    INDEX_NOT_BUILT = 0x90006,
    INCONSISTENT_INDEX_STATE = 0x90007,

    // DistributionZones group. Group code: 10
    ZONE_NOT_FOUND = 0xa0001,

    // Network group. Group code: 11
    UNRESOLVABLE_CONSISTENT_ID = 0xb0001,
    PORT_IN_USE = 0xb0002,
    RECIPIENT_LEFT = 0xb0005,
    ADDRESS_UNRESOLVED = 0xb0006,

    // NodeConfiguration group. Group code: 12
    CONFIG_READ = 0xc0001,
    CONFIG_FILE_CREATE = 0xc0002,
    CONFIG_WRITE = 0xc0003,
    CONFIG_PARSE = 0xc0004,

    // CodeDeployment group. Group code: 13
    UNIT_NOT_FOUND = 0xd0001,
    UNIT_ALREADY_EXISTS = 0xd0002,
    UNIT_CONTENT_READ = 0xd0003,
    UNIT_UNAVAILABLE = 0xd0004,

    // GarbageCollector group. Group code: 14
    CLOSED = 0xe0001,

    // Authentication group. Group code: 15
    UNSUPPORTED_AUTHENTICATION_TYPE = 0xf0001,
    INVALID_CREDENTIALS = 0xf0002,
    BASIC_PROVIDER = 0xf0003,

    // Compute group. Group code: 16
    CLASS_PATH = 0x100001,
    CLASS_LOADER = 0x100002,
    CLASS_INITIALIZATION = 0x100003,
    QUEUE_OVERFLOW = 0x100004,
    COMPUTE_JOB_STATE_TRANSITION = 0x100005,
    CANCELLING = 0x100006,
    RESULT_NOT_FOUND = 0x100007,
    FAIL_TO_GET_JOB_STATUS = 0x100008,
    COMPUTE_JOB_FAILED = 0x100009,
    CHANGE_JOB_PRIORITY_NO_JOB = 0x10000a,
    CHANGE_JOB_PRIORITY_JOB_EXECUTING = 0x10000b,
    PRIMARY_REPLICA_RESOLVE = 0x10000c,
    CHANGE_JOB_PRIORITY = 0x10000d,
    NODE_NOT_FOUND = 0x10000e,

    // Catalog group. Group code: 17
    VALIDATION = 0x110001,

    // PlacementDriver group. Group code: 18
    PRIMARY_REPLICA_AWAIT_TIMEOUT = 0x120001,
    PRIMARY_REPLICA_AWAIT = 0x120002,

    // CriticalWorkers group. Group code: 19
    SYSTEM_WORKER_BLOCKED = 0x130001,
    SYSTEM_CRITICAL_OPERATION_TIMEOUT = 0x130002,

    // DisasterRecovery group. Group code: 20
    PARTITIONS_NOT_FOUND = 0x140002,
    NODES_NOT_FOUND = 0x140003
};

} // namespace error

} // namespace ignite
