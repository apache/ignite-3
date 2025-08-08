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
 * Classifier of SQL queries depending on the type of result returned.
 */
public enum AllowedQueryType {
    /** SELECT-like queries. */
    ALLOW_ROW_SET_RESULT(0),

    /** DML-like queries. */
    ALLOW_AFFECTED_ROWS_RESULT(1),

    /** DDL-like queries. */
    ALLOW_APPLIED_RESULT(2),

    /** Queries with multiple statements. */
    ALLOW_MULTISTATEMENT_RESULT(3);

    private static final AllowedQueryType[] VALS = new AllowedQueryType[values().length];

    static {
        for (AllowedQueryType type : values()) {
            assert VALS[type.id] == null : "Found duplicate id " + type.id;

            VALS[type.id()] = type;
        }
    }

    private final int id;

    AllowedQueryType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    /** Returns allowed query type by identifier. */
    public static AllowedQueryType fromId(int id) {
        if (id >= 0 && id < VALS.length) {
            return VALS[id];
        }

        throw new IllegalArgumentException("Unexpected query type identifier: " + id);
    }
}
