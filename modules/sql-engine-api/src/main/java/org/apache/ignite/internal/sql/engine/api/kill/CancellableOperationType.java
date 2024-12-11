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

package org.apache.ignite.internal.sql.engine.api.kill;

/**
 * Type of the operation that can be cancelled.
 */
public enum CancellableOperationType {
    QUERY(0),
    TRANSACTION(1),
    COMPUTE(2);

    private static final CancellableOperationType[] VALS = new CancellableOperationType[values().length];

    private final int id;

    CancellableOperationType(int id) {
        this.id = id;
    }

    static {
        for (CancellableOperationType type : values()) {
            assert VALS[type.id] == null : "Found duplicate id " + type.id;

            VALS[type.id()] = type;
        }
    }

    /** Returns operation type by identifier. */
    public static CancellableOperationType fromId(int id) {
        if (id >= 0 && id < VALS.length) {
            return VALS[id];
        }

        throw new IllegalArgumentException("Incorrect operation type identifier: " + id);
    }

    public int id() {
        return id;
    }
}
