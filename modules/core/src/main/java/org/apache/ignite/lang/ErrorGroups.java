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
}
