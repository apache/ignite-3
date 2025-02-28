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

package org.apache.ignite.internal.partition.replicator.network.replication;

import org.apache.ignite.internal.network.annotations.TransferableEnum;

/**
 * Transaction operation type.
 */
public enum RequestType implements TransferableEnum {
    RW_GET(0),

    RW_GET_ALL(1),

    RW_DELETE(2),

    RW_DELETE_ALL(3),

    RW_DELETE_EXACT(4),

    RW_DELETE_EXACT_ALL(5),

    RW_INSERT(6),

    RW_INSERT_ALL(7),

    RW_UPSERT(8),

    RW_UPSERT_ALL(9),

    RW_REPLACE(10),

    RW_REPLACE_IF_EXIST(11),

    RW_GET_AND_DELETE(12),

    RW_GET_AND_REPLACE(13),

    RW_GET_AND_UPSERT(14),

    RW_SCAN(15),

    RO_GET(16),

    RO_GET_ALL(17),

    RO_SCAN(18);

    private final int transferableId;

    RequestType(int transferableId) {
        this.transferableId = transferableId;
    }

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

    @Override
    public int transferableId() {
        return transferableId;
    }
}
