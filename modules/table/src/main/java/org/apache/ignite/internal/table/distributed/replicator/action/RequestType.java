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

package org.apache.ignite.internal.table.distributed.replicator.action;

/**
 * Transaction operation type.
 */
public enum RequestType {
    RW_GET,

    RW_GET_ALL,

    RW_DELETE,

    RW_DELETE_ALL,

    RW_DELETE_EXACT,

    RW_DELETE_EXACT_ALL,

    RW_INSERT,

    RW_INSERT_ALL,

    RW_UPSERT,

    RW_UPSERT_ALL,

    RW_REPLACE,

    RW_REPLACE_IF_EXIST,

    RW_GET_AND_DELETE,

    RW_GET_AND_REPLACE,

    RW_GET_AND_UPSERT,

    RW_SCAN,

    RO_GET,

    RO_GET_ALL,

    RO_SCAN;

    /**
     * Returns {@code true} if the operation is an RW read.
     */
    public boolean isRwRead() {
        switch (this) {
            case RW_GET:
            case RW_GET_ALL:
            case RW_SCAN:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns {@code true} if the operation is a write.
     */
    public boolean isWrite() {
        switch (this) {
            case RW_DELETE:
            case RW_DELETE_ALL:
            case RW_DELETE_EXACT:
            case RW_DELETE_EXACT_ALL:
            case RW_INSERT:
            case RW_INSERT_ALL:
            case RW_UPSERT:
            case RW_UPSERT_ALL:
            case RW_REPLACE:
            case RW_REPLACE_IF_EXIST:
            case RW_GET_AND_DELETE:
            case RW_GET_AND_REPLACE:
            case RW_GET_AND_UPSERT:
                return true;
            default:
                return false;
        }
    }
}
