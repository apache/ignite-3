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

import java.util.Objects;

/**
 * Ignite-specific extension type codes.
 */
public class ClientMsgPackType {
    /** Number. */
    public static final byte NUMBER = 1;

    /** Decimal. */
    public static final byte DECIMAL = 2;

    /** UUID. */
    public static final byte UUID = 3;

    /** Date. */
    public static final byte DATE = 4;

    /** Time. */
    public static final byte TIME = 5;

    /** DateTime. */
    public static final byte DATETIME = 6;

    /** DateTime. */
    public static final byte TIMESTAMP = 7;

    /** Bit mask. */
    public static final byte BITMASK = 8;

    /** Duration. */
    public static final byte DURATION = 9;

    /** Period. */
    public static final byte PERIOD = 10;

    /** Absent value for a column. */
    public static final byte NO_VALUE = 100;

    /** Variable type size. */
    private static final int UNKNOWN_SIZE = -1;

    /** Size per type. */
    private static final int [] sizePerType = {UNKNOWN_SIZE, UNKNOWN_SIZE, UNKNOWN_SIZE, 16, 6, 7, 13, 12, UNKNOWN_SIZE, 12, 12};

    /**
     * Returns size if it belongs to constant len object, throws exception otherwise.
     *
     * @param type Type for which the size should be defined.
     * @return Size or throws exception if fails.
     */
    public static int sizeForType(byte type) {
        Objects.checkIndex(type, sizePerType.length);

        int size = sizePerType[type];

        if (size == UNKNOWN_SIZE) {
            throw new IllegalArgumentException("Size can`t be defined for type=" + type);
        }

        return size;
    }
}
