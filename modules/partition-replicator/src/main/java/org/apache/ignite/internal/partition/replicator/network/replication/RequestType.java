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

/**
 * Transaction operation type.
 */
public enum RequestType {
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

    private final int requestTypeId;

    RequestType(int id) {
        this.requestTypeId = id;
    }

    public int id() {
        return requestTypeId;
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

    /**
     * Returns the enumerated value from its id.
     *
     * @param requestTypeId Id of request type.
     * @throws IllegalArgumentException If there is no request type with such id.
     */
    public static RequestType fromId(int requestTypeId) throws IllegalArgumentException {
        switch (requestTypeId) {
            case 0: return RW_GET;
            case 1: return RW_GET_ALL;
            case 2: return RW_DELETE;
            case 3: return RW_DELETE_ALL;
            case 4: return RW_DELETE_EXACT;
            case 5: return RW_DELETE_EXACT_ALL;
            case 6: return RW_INSERT;
            case 7: return RW_INSERT_ALL;
            case 8: return RW_UPSERT;
            case 9: return RW_UPSERT_ALL;
            case 10: return RW_REPLACE;
            case 11: return RW_REPLACE_IF_EXIST;
            case 12: return RW_GET_AND_DELETE;
            case 13: return RW_GET_AND_REPLACE;
            case 14: return RW_GET_AND_UPSERT;
            case 15: return RW_SCAN;
            case 16: return RO_GET;
            case 17: return RO_GET_ALL;
            case 18: return RO_SCAN;
            default:
                throw new IllegalArgumentException("No enum constant from id: " + requestTypeId);
        }
    }
}
