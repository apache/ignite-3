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

package org.apache.ignite.distributed.replicator.action;

import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;

/**
 * Transaction operation type.
 */
public class RequestTypes {
    /**
     * Returns {@code true} if the operation works with a single PK and it's RW.
     */
    public static boolean isSingleRowRwPkOnly(RequestType type) {
        switch (type) {
            case RW_GET:
            case RW_DELETE:
            case RW_GET_AND_DELETE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns {@code true} if the operation works with a single row (full row) and it's RW.
     */
    public static boolean isSingleRowRwFullRow(RequestType type) {
        switch (type) {
            case RW_DELETE_EXACT:
            case RW_INSERT:
            case RW_UPSERT:
            case RW_GET_AND_UPSERT:
            case RW_GET_AND_REPLACE:
            case RW_REPLACE_IF_EXIST:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns {@code true} if the operation works with a single row and it's RW.
     */
    public static boolean isSingleRowRw(RequestType type) {
        return isSingleRowRwPkOnly(type) || isSingleRowRwFullRow(type);
    }

    /**
     * Returns {@code true} if the operation works with a single row (full row) and it's an RW read.
     */
    public static boolean isSingleRowFullRowRwRead(RequestType type) {
        return isSingleRowRwFullRow(type) && type.isRwRead();
    }

    /**
     * Returns {@code true} if the operation works with a single PK and it's an RW key-only read.
     */
    public static boolean isSingleRowRwPkOnlyRead(RequestType type) {
        return isSingleRowRwPkOnly(type) && type.isRwRead();
    }

    /**
     * Returns {@code true} if the operation works with a single row and it's a write.
     */
    public static boolean isSingleRowWrite(RequestType type) {
        return isSingleRowRw(type) && type.isWrite();
    }

    /**
     * Returns {@code true} if the operation works with a single row (full row) and it's a write.
     */
    public static boolean isSingleRowFullRowWrite(RequestType type) {
        return isSingleRowRwFullRow(type) && type.isWrite();
    }

    /**
     * Returns {@code true} if the operation works with a single PK and it's a write.
     */
    public static boolean isSingleRowPkOnlyWrite(RequestType type) {
        return isSingleRowRwPkOnly(type) && type.isWrite();
    }

    /**
     * Returns {@code true} if the operation works with multiple PKs and it's RW.
     */
    public static boolean isMultipleRowsRwPkOnly(RequestType type) {
        switch (type) {
            case RW_GET_ALL:
            case RW_DELETE_ALL:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns {@code true} if the operation works with multiple rows (full rows) and it's RW.
     */
    public static boolean isMultipleRowsRwFullRows(RequestType type) {
        switch (type) {
            case RW_DELETE_EXACT_ALL:
            case RW_INSERT_ALL:
            case RW_UPSERT_ALL:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns {@code true} if the operation works with multiple rows and it's RW.
     */
    public static boolean isMultipleRowsRw(RequestType type) {
        return isMultipleRowsRwPkOnly(type) || isMultipleRowsRwFullRows(type);
    }

    /**
     * Returns {@code true} if the operation works with multiple (full rows) and it is a read.
     */
    public static boolean isMultipleRowsRwFullRowsRead(RequestType type) {
        return isMultipleRowsRwFullRows(type) && type.isRwRead();
    }

    /**
     * Returns {@code true} if the operation works with multiple PKs and it is a read.
     */
    public static boolean isMultipleRowsRwPkOnlyRead(RequestType type) {
        return isMultipleRowsRwPkOnly(type) && type.isRwRead();
    }

    /**
     * Returns {@code true} if the operation works with multiple rows and it is a write.
     */
    public static boolean isMultipleRowsWrite(RequestType type) {
        return isMultipleRowsRw(type) && type.isWrite();
    }

    /**
     * Returns {@code true} if the operation works with multiple rows (full rows) and it is a write.
     */
    public static boolean isMultipleRowsFullRowsWrite(RequestType type) {
        return isMultipleRowsRwFullRows(type) && type.isWrite();
    }

    /**
     * Returns {@code true} if the operation works with multiple PKs and it is a write.
     */
    public static boolean isMultipleRowsPkOnlyWrite(RequestType type) {
        return isMultipleRowsRwPkOnly(type) && type.isWrite();
    }

    /**
     * Returns {@code true} if the operation requires a key-only tuple.
     */
    public static boolean isKeyOnly(RequestType type) {
        switch (type) {
            case RW_GET:
            case RW_GET_ALL:
            case RW_DELETE:
            case RW_DELETE_ALL:
            case RW_GET_AND_DELETE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns {@code true} if the operation looks up for an existing row (to return/delete/update/replace it).
     */
    public static boolean looksUpFirst(RequestType type) {
        switch (type) {
            case RW_INSERT:
            case RW_INSERT_ALL:
                return false;
            default:
                return true;
        }
    }
}
