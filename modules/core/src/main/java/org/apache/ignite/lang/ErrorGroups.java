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
}
