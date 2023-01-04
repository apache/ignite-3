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

package org.apache.ignite.internal.metastorage.server;

import java.nio.ByteBuffer;

/**
 * Simple result of statement execution, backed by byte[] array.
 * Provides some shortcut methods to represent the values of some primitive types.
 */
public class StatementResult {
    /** Result data. */
    private final byte[] res;

    /**
     * Constructs result from the byte array.
     *
     * @param res byte array.
     */
    public StatementResult(byte[] res) {
        this.res = res;
    }

    /**
     * Constructs result from the boolean value.
     *
     * @param res boolean.
     */
    public StatementResult(boolean res) {
        this.res = new byte[] {(byte) (res ? 1 : 0)};
    }

    /**
     * Constructs result from the int value.
     *
     * @param res int.
     */
    public StatementResult(int res) {
        this.res = ByteBuffer.allocate(4).putInt(res).array();
    }

    /**
     * Returns backed byte array.
     *
     * @return backed byte array.
     */
    public byte[] bytes() {
        return res;
    }
}
