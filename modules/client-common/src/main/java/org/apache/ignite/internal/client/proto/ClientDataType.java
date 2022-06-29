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

package org.apache.ignite.internal.client.proto;

/**
 * Client data types.
 */
public enum ClientDataType {
    /** Byte. */
    INT8(1),

    /** Short. */
    INT16(2),

    /** Int. */
    INT32(3),

    /** Long. */
    INT64(4),

    /** Float. */
    FLOAT(5),

    /** Double. */
    DOUBLE(6),

    /** Decimal. */
    DECIMAL(7),

    /** UUID. */
    UUID(8),

    /** String. */
    STRING(9),

    /** Byte array. */
    BYTES(10),

    /** BitMask. */
    BITMASK(11),

    /** Date. */
    DATE(12),

    /** Time. */
    TIME(13),

    /** DateTime. */
    DATETIME(14),

    /** Timestamp. */
    TIMESTAMP(15),

    /** Number. */
    NUMBER(16),

    /** Boolean. */
    BOOLEAN(17),

    /** Big Integer. */
    BIGINTEGER(18),

    /** Duration. */
    DURATION(19),

    /** Period. */
    PERIOD(20);

    /** Number type representation. */
    private int type;

    ClientDataType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }
}
