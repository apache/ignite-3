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

package org.apache.ignite.internal.metastorage.server;

import java.nio.ByteBuffer;

/**
 * Simple result of statement execution, backed by byte[] array.
 * Provides some shortcut methods to represent the values of some primitive types.
 */
public class StatementResult {

    /** Result data. */
    private final byte[] result;

    /**
     * Constructs result from the byte array.
     *
     * @param result byte array.
     */
    public StatementResult(byte[] result) {
        this.result = result;
    }

    /**
     * Constructs result from the boolean value.
     *
     * @param result boolean.
     */
    public StatementResult(boolean result) {
        this.result = new byte[] {(byte) (result ? 1 : 0)};
    }

    /**
     * Constructs result from the int value.
     *
     * @param result int.
     */
    public StatementResult(int result) {
        this.result = ByteBuffer.allocate(4).putInt(result).array();
    }

    /**
     * Returns result value as a boolean.
     *
     * @return boolean result.
     */
    public boolean getAsBoolean() {
        return result[0] != 0;
    }

    /**
     * Returns result as an int.
     *
     * @return int result.
     */
    public Integer getAsInt() {
        return ByteBuffer.wrap(result).getInt();
    }

    /**
     * Returns backed byte array.
     *
     * @return backed byte array.
     */
    public byte[] bytes() {
        return result;
    }
}
