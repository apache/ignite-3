/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
        public static final ErrorGroup TX_ERR_GROUP = ErrorGroup.newGroup("TX", 3);

        /** Error on creation of tx state storage. */
        public static int TX_STATE_STORAGE_CREATE_ERR = TX_ERR_GROUP.registerErrorCode(1);

        /** Error on destruction of tx state storage. */
        public static int TX_STATE_STORAGE_DESTROY_ERR = TX_ERR_GROUP.registerErrorCode(2);

        /** Error of tx state storage. */
        public static int TX_STATE_STORAGE_ERR = TX_ERR_GROUP.registerErrorCode(3);
    }

    /** Replicator error group. */
    public static class Replicator {
        /** Replicator error group. */
        public static final ErrorGroup REPLICATOR_ERR_GROUP = ErrorGroup.newGroup("REP", 4);

        /** Common error for the replication procedure. */
        public static int REPLICA_COMMON_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(1);

        /** Replica with the same identifier is already existed. */
        public static int REPLICA_ALREADY_IS_STARTED_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(2);

        /** Timeout has happened during the replication procedure. */
        public static int REPLICA_TIMEOUT_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(3);

        /** The error happens when the replication level try to handle an unsupported request.  */
        public static int REPLICA_UNSUPPORTED_REQUEST_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(4);

        /** The error happens when the replica is not ready to handle a request.  */
        public static int REPLICA_UNAVAILABLE_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(5);

        /** The error happens when the replica is not the current primary replica. */
        public static int REPLICA_MISS_ERR = REPLICATOR_ERR_GROUP.registerErrorCode(6);
    }
}
