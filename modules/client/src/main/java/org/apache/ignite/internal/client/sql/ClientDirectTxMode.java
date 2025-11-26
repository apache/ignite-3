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

package org.apache.ignite.internal.client.sql;

/**
 * Represents the level of support for direct transaction.
 *
 * <p>Direct transactions allow clients to interact with partition-aware nodes more efficiently by skipping
 * intermediary coordination when conditions permit. This enum defines whether and how such optimizations
 * are supported.
 */
public enum ClientDirectTxMode {
    /** Direct transactions are not supported for the statement. Operation must be executed in proxy mode.  */
    NOT_SUPPORTED((byte) 0),

    /**
     * Direct transactions are supported without additional conditions.
     *
     * <p>The client may directly route transactional request to the appropriate node without coordination.
     */
    SUPPORTED((byte) 1),

    /**
     * Direct transactions are supported, but the operation should be tracked.
     *
     * <p>The tracking is required to postpone transaction finalization until all in-flight operations are completed.
     */
    SUPPORTED_TRACKING_REQUIRED((byte) 2);

    private static final ClientDirectTxMode[] VALS = {NOT_SUPPORTED, SUPPORTED, SUPPORTED_TRACKING_REQUIRED};

    public static ClientDirectTxMode fromId(byte id) {
        return id < VALS.length ? VALS[id] : NOT_SUPPORTED;
    }

    public final byte id;

    ClientDirectTxMode(byte id) {
        this.id = id;
    }
}
