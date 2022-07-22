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
public class ErrorGroups {
    /** Common error group. */
    public static class Common {
        /** Unknown error group. */
        public static final ErrorGroup COMMON_ERR_GROUP = ErrorGroup.newGroup("CMN", 1);

        /** Unknown error. */
        public static int UNKNOWN_ERR = COMMON_ERR_GROUP.registerErrorCode(1);
    }

    /** Tables error group. */
    public static class Table {
        /** Table error group. */
        public static final ErrorGroup TABLE_ERR_GROUP = ErrorGroup.newGroup("TBL", 2);

        /** Table already exists. */
        public static int TABLE_ALREADY_EXISTS_ERR = TABLE_ERR_GROUP.registerErrorCode(1);

        /** Table not found. */
        public static int TABLE_NOT_FOUND_ERR = TABLE_ERR_GROUP.registerErrorCode(2);

        /** Column already exists. */
        public static int COLUMN_ALREADY_EXISTS_ERR = TABLE_ERR_GROUP.registerErrorCode(3);

        /** Column not found. */
        public static int COLUMN_NOT_FOUND_ERR = TABLE_ERR_GROUP.registerErrorCode(4);
    }

    /** Client error group. */
    public static class Client {
        /** Client error group. */
        public static final ErrorGroup CLIENT_ERR_GROUP = ErrorGroup.newGroup("CLIENT", 3);

        /** Connection failed. */
        public static int CONNECTION_ERR = CLIENT_ERR_GROUP.registerErrorCode(1);

        /** Protocol breakdown. */
        public static int PROTOCOL_ERR = CLIENT_ERR_GROUP.registerErrorCode(2);

        /** Incompatible protocol version. */
        public static int PROTOCOL_COMPATIBILITY_ERR = CLIENT_ERR_GROUP.registerErrorCode(3);

        /** Table not found by ID. */
        public static int TABLE_ID_NOT_FOUND_ERR = CLIENT_ERR_GROUP.registerErrorCode(4);

        /** Authentication error. */
        public static int AUTHENTICATION_ERR = CLIENT_ERR_GROUP.registerErrorCode(5);

        /** Authorization error. */
        public static int AUTHORIZATION_ERR = CLIENT_ERR_GROUP.registerErrorCode(6);

        /** Configuration error. */
        public static int CONFIGURATION_ERR = CLIENT_ERR_GROUP.registerErrorCode(7);
    }

    /** SQL error group. */
    public static class Sql {
        /** SQL error group. */
        public static final ErrorGroup SQL_ERR_GROUP = ErrorGroup.newGroup("SQL", 4);

        /** No more pages in the cursor error. */
        public static int CURSOR_NO_MORE_PAGES_ERR = SQL_ERR_GROUP.registerErrorCode(1);

        /** Session not found error. */
        public static int SESSION_NOT_FOUND_ERR = SQL_ERR_GROUP.registerErrorCode(2);

        /** Invalid query error. */
        public static int INVALID_QUERY_ERR = SQL_ERR_GROUP.registerErrorCode(2);
    }
}
