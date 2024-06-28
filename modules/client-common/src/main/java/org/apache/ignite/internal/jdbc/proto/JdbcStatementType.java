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

package org.apache.ignite.internal.jdbc.proto;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * JDBC statement type.
 */
public enum JdbcStatementType {
    /** Any statement type. */
    ANY_STATEMENT_TYPE((byte) 0),

    /** Select statement type. */
    SELECT_STATEMENT_TYPE((byte) 1),

    /** DML / DDL statement type. */
    UPDATE_STATEMENT_TYPE((byte) 2);

    private static final Map<Byte, JdbcStatementType> STATEMENT_TYPE_IDX;

    static {
        STATEMENT_TYPE_IDX = Arrays.stream(values()).collect(
                Collectors.toMap(JdbcStatementType::getId, Function.identity()));
    }

    /**
     * Gets statement type value by its id.
     *
     * @param id The id.
     * @return JdbcStatementType value.
     * @throws IllegalArgumentException If statement is not found.
     */
    public static JdbcStatementType getStatement(byte id) {
        JdbcStatementType value = STATEMENT_TYPE_IDX.get(id);

        Objects.requireNonNull(value, () -> String.format("Unknown jdbcStatementType %s", id));

        return value;
    }

    private final byte id;

    JdbcStatementType(byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }
}
