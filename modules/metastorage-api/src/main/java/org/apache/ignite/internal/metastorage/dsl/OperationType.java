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

package org.apache.ignite.internal.metastorage.dsl;

/**
 * Defines possible operation types.
 */
public enum OperationType {
    /** No-op operation. */
    NO_OP(0),

    /** Put (insert/replace) operation. */
    PUT(1),

    /** Remove operation. */
    REMOVE(2);

    private final int id;

    OperationType(int id) {
        this.id = id;
    }

    /**
     * Returns the enumerated value from its id.
     *
     * @param id Id of enumeration constant.
     * @throws IllegalArgumentException If no enumeration constant by id.
     */
    public static OperationType fromId(int id) throws IllegalArgumentException {
        switch (id) {
            case 0: return NO_OP;
            case 1: return PUT;
            case 2: return REMOVE;
            default:
                throw new IllegalArgumentException("No enum constant from id: " + id);
        }
    }

    public int id() {
        return id;
    }
}
