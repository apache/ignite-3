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

package org.apache.ignite.lang;

/**
 * Defines error groups and its errors.
 */
@SuppressWarnings("PublicInnerClass")
public class ErrorGroups {
    /** Common error group. */
    public static class Common {
        /** Unknown error group. */
        public static final ErrorGroup COMMON_ERR_GROUP = ErrorGroup.newGroup("CMN", 1);

        /** Unexpected error. */
        public static final int UNEXPECTED_ERR = COMMON_ERR_GROUP.registerErrorCode(1);

        /** Node stopping error. */
        public static final int NODE_STOPPING_ERR = COMMON_ERR_GROUP.registerErrorCode(2);

        /** Component not started error. */
        public static final int COMPONENT_NOT_STARTED_ERR = COMMON_ERR_GROUP.registerErrorCode(3);

        /** Illegal argument or argument in a wrong format has been passed. */
        public static final int ILLEGAL_ARGUMENT_ERR = COMMON_ERR_GROUP.registerErrorCode(4);

        /** Unknown error. */
        @Deprecated
        public static final int UNKNOWN_ERR = COMMON_ERR_GROUP.registerErrorCode(0xFFFF);
    }

    /** Tables error group. */
    public static class Table {
        /** Table error group. */
        public static final ErrorGroup TABLE_ERR_GROUP = ErrorGroup.newGroup("TBL", 2);

        /** Table already exists. */
        public static final int TABLE_ALREADY_EXISTS_ERR = TABLE_ERR_GROUP.registerErrorCode(1);

        /** Table not found. */
        public static final int TABLE_NOT_FOUND_ERR = TABLE_ERR_GROUP.registerErrorCode(2);

        /** Column already exists. */
        public static final int COLUMN_ALREADY_EXISTS_ERR = TABLE_ERR_GROUP.registerErrorCode(3);

        /** Column not found. */
        public static final int COLUMN_NOT_FOUND_ERR = TABLE_ERR_GROUP.registerErrorCode(4);
    }

    /** Client error group. */
    public static class Client {
        /** Client error group. */
        public static final ErrorGroup CLIENT_ERR_GROUP = ErrorGroup.newGroup("CLIENT", 3);

        /** Connection failed. */
        public static final int CONNECTION_ERR = CLIENT_ERR_GROUP.registerErrorCode(1);

        /** Protocol breakdown. */
        public static final int PROTOCOL_ERR = CLIENT_ERR_GROUP.registerErrorCode(2);

        /** Incompatible protocol version. */
        public static final int PROTOCOL_COMPATIBILITY_ERR = CLIENT_ERR_GROUP.registerErrorCode(3);

        /** Table not found by ID. */
        public static final int TABLE_ID_NOT_FOUND_ERR = CLIENT_ERR_GROUP.registerErrorCode(4);

        /** Authentication error. */
        public static final int AUTHENTICATION_ERR = CLIENT_ERR_GROUP.registerErrorCode(5);

        /** Authorization error. */
        public static final int AUTHORIZATION_ERR = CLIENT_ERR_GROUP.registerErrorCode(6);

        /** Configuration error. */
        public static final int CONFIGURATION_ERR = CLIENT_ERR_GROUP.registerErrorCode(7);

        /** Cluster ID mismatch error. */
        public static final int CLUSTER_ID_MISMATCH_ERR = CLIENT_ERR_GROUP.registerErrorCode(8);
    }

    /** SQL error group. */
    public static class Sql {
        /** SQL error group. */
        public static final ErrorGroup SQL_ERR_GROUP = ErrorGroup.newGroup("SQL", 4);

        /** No more pages in the cursor error. */
        public static final int CURSOR_NO_MORE_PAGES_ERR = SQL_ERR_GROUP.registerErrorCode(1);

        /** Session not found error. */
        public static final int SESSION_NOT_FOUND_ERR = SQL_ERR_GROUP.registerErrorCode(2);

        /** Invalid query error. */
        public static final int QUERY_INVALID_ERR = SQL_ERR_GROUP.registerErrorCode(3);

        /** Query without a result set error. */
        public static final int QUERY_NO_RESULT_SET_ERR = SQL_ERR_GROUP.registerErrorCode(4);

        /** Missing primary key error. */
        public static final int PRIMARY_KEY_MISSING_ERR = SQL_ERR_GROUP.registerErrorCode(5);

        /** Multiple primary keys error. */
        public static final int PRIMARY_KEYS_MULTIPLE_ERR = SQL_ERR_GROUP.registerErrorCode(6);

        /** Schema not found. */
        public static final int SCHEMA_NOT_FOUND_ERR = SQL_ERR_GROUP.registerErrorCode(7);

        /** Storage engine not valid. */
        public static final int STORAGE_ENGINE_NOT_VALID_ERR = SQL_ERR_GROUP.registerErrorCode(8);

        /** Cursor is already closed error. */
        public static final int CURSOR_CLOSED_ERR = SQL_ERR_GROUP.registerErrorCode(9);

        /** Constraint violation: some keys can't be inserted because they violate unique constraint PK. */
        public static final int DUPLICATE_KEYS_ERR = SQL_ERR_GROUP.registerErrorCode(10);

        /** Constraint violation: deleting a column that belongs to the primary key. */
        public static final int DEL_PK_COMUMN_CONSTRAINT_ERR = SQL_ERR_GROUP.registerErrorCode(11);

        /**  Too many grouping expressions. */
        public static final int TOO_MANY_GROUPING_EXPRESSIONS_ERR = SQL_ERR_GROUP.registerErrorCode(12);

        /** Unsupported sql operation. */
        public static final int UNSUPPORTED_SQL_OPERATION_KIND_ERR = SQL_ERR_GROUP.registerErrorCode(13);

        /** Unsupported DDL operation. */
        public static final int UNSUPPORTED_DDL_OPERATION_ERR = SQL_ERR_GROUP.registerErrorCode(14);

        /** Query validation error. */
        public static final int QUERY_VALIDATION_ERR = SQL_ERR_GROUP.registerErrorCode(15);

        /** Object is not found in schema. */
        public static final int OBJECT_NOT_FOUND_ERR = SQL_ERR_GROUP.registerErrorCode(16);

        /** Object already exists in schema. */
        public static final int OBJECT_ALREADY_EXISTS_ERR = SQL_ERR_GROUP.registerErrorCode(17);

        /** Table version not found. */
        public static final int TABLE_VER_NOT_FOUND_ERR = SQL_ERR_GROUP.registerErrorCode(18);

        /** Query mapping error. */
        public static final int QUERY_MAPPING_ERR = SQL_ERR_GROUP.registerErrorCode(19);

        /** DDL execution error. */
        public static final int DDL_EXEC_ERR = SQL_ERR_GROUP.registerErrorCode(20);

        /** DML result error. */
        public static final int INVALID_DML_RESULT_ERR = SQL_ERR_GROUP.registerErrorCode(21);

        /** SQL data type to relational conversion error. */
        public static final int SQL_TO_REL_CONVERSION_ERR = SQL_ERR_GROUP.registerErrorCode(22);

        /** Relational expression serialization error. */
        public static final int REL_SERIALIZATION_ERR = SQL_ERR_GROUP.registerErrorCode(23);

        /** Relational expression deserialization error. */
        public static final int REL_DESERIALIZATION_ERR = SQL_ERR_GROUP.registerErrorCode(24);

        /** Class not found error. */
        public static final int CLASS_NOT_FOUND_ERR = SQL_ERR_GROUP.registerErrorCode(25);

        /** Expression compilation error. */
        public static final int EXPRESSION_COMPILATION_ERR = SQL_ERR_GROUP.registerErrorCode(26);

        /** Node left the cluster. */
        public static final int NODE_LEFT_ERR = SQL_ERR_GROUP.registerErrorCode(27);

        /** Message send error. */
        public static final int MESSAGE_SEND_ERR = SQL_ERR_GROUP.registerErrorCode(28);

        /** Operation aborted/interrupted error. */
        public static final int OPERATION_INTERRUPTED_ERR = SQL_ERR_GROUP.registerErrorCode(29);

        /** An error occurred while canceling the operation. */
        public static final int CANCEL_OPERATION_ERR = SQL_ERR_GROUP.registerErrorCode(30);

        /** Session expired error. */
        public static final int SESSION_EXPIRED_ERR = SQL_ERR_GROUP.registerErrorCode(31);

        /** Schema evaluation error. */
        public static final int SCHEMA_EVALUATION_ERR = SQL_ERR_GROUP.registerErrorCode(32);

        /** Execution cancelled. */
        public static final int EXECUTION_CANCELLED_ERR = SQL_ERR_GROUP.registerErrorCode(33);
    }

    /** Meta storage error group. */
    public static class MetaStorage {
        /** Meta storage error group. */
        public static final ErrorGroup META_STORAGE_ERR_GROUP = ErrorGroup.newGroup("META", 5);

        /** Failed to start the underlying key value storage. */
        public static final int STARTING_STORAGE_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(1);

        /** Failed to restore the underlying key value storage. */
        public static final int RESTORING_STORAGE_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(2);

        /** Failed to close the underlying key value storage. */
        public static final int CLOSING_STORAGE_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(3);

        /** Failed to compact the underlying key value storage. */
        public static final int COMPACTION_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(4);

        /** Failed to perform an operation on the underlying key value storage. */
        public static final int OP_EXECUTION_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(5);

        /** Failed to perform an operation within a specified time period. Usually in such cases the operation should be retried. */
        public static final int OP_EXECUTION_TIMEOUT_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(6);

        /** Failed to iterate over the underlying key value storage. */
        public static final int WATCH_EXECUTION_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(7);

        /** Failed to stop a watcher. */
        public static final int WATCH_STOPPING_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(8);

        /** Failed to deploy or update a watcher. */
        public static final int DEPLOYING_WATCH_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(9);

        /** Failed to iterate over meta storage cursor. */
        public static final int CURSOR_EXECUTION_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(10);

        /** Failed to close a cursor. */
        public static final int CURSOR_CLOSING_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(11);
    }

    /** Index error group. */
    public static class Index {
        /** Index error group. */
        public static final ErrorGroup INDEX_ERR_GROUP = ErrorGroup.newGroup("IDX", 6);

        /** Invalid index definition. */
        public static final int INVALID_INDEX_DEFINITION_ERR = INDEX_ERR_GROUP.registerErrorCode(1);

        /** Index not found. */
        public static final int INDEX_NOT_FOUND_ERR = INDEX_ERR_GROUP.registerErrorCode(2);
    }

    /** Transactions error group. */
    public static class Transactions {
        /** Transactions error group. */
        public static final ErrorGroup TX_ERR_GROUP = ErrorGroup.newGroup("TX", 7);

        /** Error of tx state storage. */
        public static final int TX_STATE_STORAGE_ERR = TX_ERR_GROUP.registerErrorCode(1);

        /** Tx state storage is stopped. */
        public static final int TX_STATE_STORAGE_STOPPED_ERR = TX_ERR_GROUP.registerErrorCode(2);

        /** Error of unexpected tx state on state change. */
        public static final int TX_UNEXPECTED_STATE_ERR = TX_ERR_GROUP.registerErrorCode(3);

        /** Failed to release a lock on a key. */
        public static final int RELEASE_LOCK_ERR = TX_ERR_GROUP.registerErrorCode(4);

        /** Failed to acquire a lock on a key due to a conflict. */
        public static final int ACQUIRE_LOCK_ERR = TX_ERR_GROUP.registerErrorCode(5);

        /** Failed to downgrade a lock on a key due to a conflict. */
        public static final int DOWNGRADE_LOCK_ERR = TX_ERR_GROUP.registerErrorCode(6);

        /** Failed to commit a transaction. */
        public static final int TX_COMMIT_ERR = TX_ERR_GROUP.registerErrorCode(7);

        /** Failed to rollback a transaction. */
        public static final int TX_ROLLBACK_ERR = TX_ERR_GROUP.registerErrorCode(8);

        /** Failed to enlist read-write operation into read-only transaction. */
        public static final int TX_INSUFFICIENT_READ_WRITE_OPERATION_ERR = TX_ERR_GROUP.registerErrorCode(9);
    }

    /** Replicator error group. */
    public static class Replicator {
        /** Replicator error group. */
        public static final ErrorGroup REPLICATOR_ERR_GROUP = ErrorGroup.newGroup("REP", 8);

        /** Common error for the replication procedure. */
        public static final int REPLICA_COMMON_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(1);

        /** Replica with the same identifier is already existed. */
        public static final int REPLICA_IS_ALREADY_STARTED_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(2);

        /** Timeout has happened during the replication procedure. */
        public static final int REPLICA_TIMEOUT_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(3);

        /** The error happens when the replication level try to handle an unsupported request. */
        public static final int REPLICA_UNSUPPORTED_REQUEST_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(4);

        /** The error happens when the replica is not ready to handle a request. */
        public static final int REPLICA_UNAVAILABLE_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(5);

        /** The error happens when the replica is not the current primary replica. */
        public static final int REPLICA_MISS_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(6);

        /** Failed to close cursor. */
        public static final int CURSOR_CLOSE_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(7);

    }

    /** Storage error group. */
    public static class Storage {
        /** Storage error group. */
        public static final ErrorGroup STORAGE_ERR_GROUP = ErrorGroup.newGroup("STORAGE", 9);

        /** Failed to create a directory. */
        public static final int DIRECTORY_CREATION_ERR = STORAGE_ERR_GROUP.registerErrorCode(1);
    }

    /** Distribution zones error group. */
    public static class DistributionZones {
        /** Distribution zones group. */
        public static final ErrorGroup DISTRIBUTION_ZONES_ERR_GROUP = ErrorGroup.newGroup("DISTRZONES", 10);

        /** Distribution zone already exists. */
        public static final int ZONE_ALREADY_EXISTS_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode(1);

        /** Distribution zone is not found. */
        public static final int ZONE_NOT_FOUND_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode(2);

        /** Distribution zone rename error. */
        public static final int ZONE_RENAME_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode(3);

        /** Distribution zone update error. */
        public static final int ZONE_UPDATE_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode(4);
    }

    /** Network error group. */
    public static class Network {
        /** Network error group. */
        public static final ErrorGroup NETWORK_ERR_GROUP = ErrorGroup.newGroup("NETWORK", 11);

        /** Unresolvable consistent ID. */
        public static final int UNRESOLVABLE_CONSISTENT_ID_ERR = NETWORK_ERR_GROUP.registerErrorCode(1);
    }
}
