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
        /** Common error group. */
        public static final ErrorGroup COMMON_ERR_GROUP = ErrorGroup.newGroup("CMN", 1);

        /** Node stopping error. */
        public static final int NODE_STOPPING_ERR = COMMON_ERR_GROUP.registerErrorCode(1);

        /** Component not started error. */
        public static final int COMPONENT_NOT_STARTED_ERR = COMMON_ERR_GROUP.registerErrorCode(2);

        /** Illegal argument or argument in a wrong format has been passed. */
        public static final int ILLEGAL_ARGUMENT_ERR = COMMON_ERR_GROUP.registerErrorCode(3);

        /** SSL can not be configured error. */
        public static final int SSL_CONFIGURATION_ERR = COMMON_ERR_GROUP.registerErrorCode(4);

        /**
         * This error code represents an internal error caused by faulty logic or coding in the Ignite codebase.
         * In general, this error code should be considered as a non-recoverable error
         */
        public static final int INTERNAL_ERR = COMMON_ERR_GROUP.registerErrorCode(0xFFFF);
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

        /** Table is stopping. */
        public static final int TABLE_STOPPING_ERR = TABLE_ERR_GROUP.registerErrorCode(5);

        /** Table definition is incorrect. */
        public static final int TABLE_DEFINITION_ERR = TABLE_ERR_GROUP.registerErrorCode(6);
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

        /** Client SSL configuration error. */
        public static final int CLIENT_SSL_CONFIGURATION_ERR = CLIENT_ERR_GROUP.registerErrorCode(9);

        /** Client handshake header error. */
        public static final int HANDSHAKE_HEADER_ERR = CLIENT_ERR_GROUP.registerErrorCode(10);
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

        /** Constraint violation: deleting a column that belongs to the index. */
        public static final int DROP_IDX_COLUMN_CONSTRAINT_ERR = SQL_ERR_GROUP.registerErrorCode(11);

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

        /** Failed to compact the underlying key value storage. */
        public static final int COMPACTION_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(3);

        /** Failed to perform an operation on the underlying key value storage. */
        public static final int OP_EXECUTION_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(4);

        /** Failed to perform an operation within a specified time period. Usually in such cases the operation should be retried. */
        public static final int OP_EXECUTION_TIMEOUT_ERR = META_STORAGE_ERR_GROUP.registerErrorCode(5);
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

        /** Failed to acquire a lock on a key due to a conflict. */
        public static final int ACQUIRE_LOCK_ERR = TX_ERR_GROUP.registerErrorCode(4);

        /** Failed to acquire a lock on a key within a timeout. */
        public static final int ACQUIRE_LOCK_TIMEOUT_ERR = TX_ERR_GROUP.registerErrorCode(5);

        /** Failed to commit a transaction. */
        public static final int TX_COMMIT_ERR = TX_ERR_GROUP.registerErrorCode(6);

        /** Failed to rollback a transaction. */
        public static final int TX_ROLLBACK_ERR = TX_ERR_GROUP.registerErrorCode(7);

        /** Failed to enlist read-write operation into read-only transaction. */
        public static final int TX_FAILED_READ_WRITE_OPERATION_ERR = TX_ERR_GROUP.registerErrorCode(8);

        /** The error happens when the replica is not ready to handle a request. */
        public static final int TX_REPLICA_UNAVAILABLE_ERR = TX_ERR_GROUP.registerErrorCode(9);

        /** Tx state storage rebalancing error. */
        public static final int TX_STATE_STORAGE_REBALANCE_ERR = TX_ERR_GROUP.registerErrorCode(10);

        /** Error occurred when trying to create a read-only transaction with a timestamp older than the data available in the tables. */
        public static final int TX_READ_ONLY_TOO_OLD_ERR = TX_ERR_GROUP.registerErrorCode(11);

        /** Failure due to an incompatible schema change. */
        public static final int TX_INCOMPATIBLE_SCHEMA_ERR = TX_ERR_GROUP.registerErrorCode(12);
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

        /** Stopping replica exception code. */
        public static final int REPLICA_STOPPING_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(8);

    }

    /** Storage error group. */
    public static class Storage {
        /** Storage error group. */
        public static final ErrorGroup STORAGE_ERR_GROUP = ErrorGroup.newGroup("STORAGE", 9);

        /** Default error code when nothing else is specified. */
        public static final int GENERIC_ERR = STORAGE_ERR_GROUP.registerErrorCode(1);

        /** Failed to create a directory. */
        public static final int DIRECTORY_CREATION_ERR = STORAGE_ERR_GROUP.registerErrorCode(2);

        /** Operation on closed storage. */
        public static final int ALREADY_CLOSED_ERR = STORAGE_ERR_GROUP.registerErrorCode(3);

        /** Storage rebalancing error. */
        public static final int STORAGE_REBALANCE_ERR = STORAGE_ERR_GROUP.registerErrorCode(4);
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

        /** Distribution zone is a default distribution zone or bound to table. */
        public static final int ZONE_DROP_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode(4);

        /** Distribution zone definition error. */
        public static final int ZONE_DEFINITION_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode(5);
    }

    /** Network error group. */
    public static class Network {
        /** Network error group. */
        public static final ErrorGroup NETWORK_ERR_GROUP = ErrorGroup.newGroup("NETWORK", 11);

        /** Unresolvable consistent ID. */
        public static final int UNRESOLVABLE_CONSISTENT_ID_ERR = NETWORK_ERR_GROUP.registerErrorCode(1);

        /** Port is in use. */
        public static final int PORT_IN_USE_ERR = NETWORK_ERR_GROUP.registerErrorCode(2);
    }

    /** Node configuration error group. */
    public static class NodeConfiguration {
        /** Node configuration error group. */
        public static final ErrorGroup NODE_CONFIGURATION_ERR_GROUP = ErrorGroup.newGroup("NODECFG", 12);

        /** Config read error. */
        public static final int CONFIG_READ_ERR = NODE_CONFIGURATION_ERR_GROUP.registerErrorCode(1);

        /** Config file creation error. */
        public static final int CONFIG_FILE_CREATE_ERR = NODE_CONFIGURATION_ERR_GROUP.registerErrorCode(2);

        /** Config write error. */
        public static final int CONFIG_WRITE_ERR = NODE_CONFIGURATION_ERR_GROUP.registerErrorCode(3);

        /** Config parse error. */
        public static final int CONFIG_PARSE_ERR = NODE_CONFIGURATION_ERR_GROUP.registerErrorCode(4);
    }

    /** Code deployment error group. */
    public static class CodeDeployment {
        /** Code deployment error group. */
        public static final ErrorGroup CODE_DEPLOYMENT_ERR_GROUP = ErrorGroup.newGroup("CODEDEPLOY", 13);

        /** Access to non-existing deployment unit. */
        public static final int UNIT_NOT_FOUND_ERR = CODE_DEPLOYMENT_ERR_GROUP.registerErrorCode(1);

        /** Unit duplicate error. */
        public static final int UNIT_ALREADY_EXISTS_ERR = CODE_DEPLOYMENT_ERR_GROUP.registerErrorCode(2);

        /** Deployment unit content read error. */
        public static final int UNIT_CONTENT_READ_ERR = CODE_DEPLOYMENT_ERR_GROUP.registerErrorCode(3);

        /** Deployment unit is unavailable for computing. */
        public static final int UNIT_UNAVAILABLE_ERR = CODE_DEPLOYMENT_ERR_GROUP.registerErrorCode(4);
    }

    /**
     * Garbage collector error group.
     */
    public static class GarbageCollector {
        /** Garbage collector error group. */
        public static final ErrorGroup GC_ERR_GROUP = ErrorGroup.newGroup("GC", 14);

        /** Garbage collector closed error. */
        public static final int CLOSED_ERR = GC_ERR_GROUP.registerErrorCode(1);
    }

    /**
     * Authentication error group.
     */
    public static class Authentication {
        /** Authentication error group. */
        public static final ErrorGroup AUTHENTICATION_ERR_GROUP = ErrorGroup.newGroup("AUTHENTICATION", 15);

        /** General authentication error. */
        public static final int COMMON_AUTHENTICATION_ERR = AUTHENTICATION_ERR_GROUP.registerErrorCode(1);
    }

    /**
     * Compute error group.
     */
    public static class Compute {
        /** Compute error group. */
        public static final ErrorGroup COMPUTE_ERR_GROUP = ErrorGroup.newGroup("COMPUTE", 16);

        /** Classpath error. */
        public static final int CLASS_PATH_ERR = COMPUTE_ERR_GROUP.registerErrorCode(1);

        /** Class loader error. */
        public static final int CLASS_LOADER_ERR = COMPUTE_ERR_GROUP.registerErrorCode(2);
    }
}
