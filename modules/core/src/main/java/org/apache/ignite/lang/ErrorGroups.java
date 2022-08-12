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

        /** Unknown error. */
        public static final int UNKNOWN_ERR = COMMON_ERR_GROUP.registerErrorCode(1);
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
    }
}
