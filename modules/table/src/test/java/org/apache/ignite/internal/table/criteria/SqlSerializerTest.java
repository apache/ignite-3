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

package org.apache.ignite.internal.table.criteria;

import static org.apache.ignite.table.criteria.Criteria.and;
import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;
import static org.apache.ignite.table.criteria.Criteria.in;
import static org.apache.ignite.table.criteria.Criteria.not;
import static org.apache.ignite.table.criteria.Criteria.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * SQL generation test.
 */
class SqlSerializerTest {
    @Test
    void testEquals() {
        var ser = new SqlSerializer.Builder()
                .tableName("test")
                .columns(Set.of("A"))
                .where(columnValue("a", equalTo("a")))
                .build();

        assertEquals("SELECT * FROM test WHERE A = ?", ser.toString());
        assertArrayEquals(new Object[] {"a"}, ser.getArguments());
    }

    @Test
    void testNonEquals() {
        var ser = new SqlSerializer.Builder()
                .tableName("test")
                .columns(Set.of("A"))
                .where(not(columnValue("a", equalTo("a"))))
                .build();

        assertEquals("SELECT * FROM test WHERE NOT A = ?", ser.toString());
        assertArrayEquals(new Object[] {"a"}, ser.getArguments());
    }

    @Test
    void testIn() {
        var ser = new SqlSerializer.Builder()
                .tableName("test")
                .columns(Set.of("A"))
                .where(not(columnValue("a", in("a", "b", "c"))))
                .build();

        assertEquals("SELECT * FROM test WHERE NOT A IN (?, ?, ?)", ser.toString());
        assertArrayEquals(new Object[] {"a", "b", "c"}, ser.getArguments());
    }

    @Test
    void testAnd() {
        var ser = new SqlSerializer.Builder()
                .tableName("test")
                .columns(Set.of("A", "B"))
                .where(and(columnValue("a", nullValue()), not(columnValue("b", in("a", "b", "c")))))
                .build();

        assertEquals("SELECT * FROM test WHERE (A IS NULL) AND (NOT B IN (?, ?, ?))", ser.toString());
        assertArrayEquals(new Object[] {"a", "b", "c"}, ser.getArguments());
    }

    @Test
    void testSqlInjection() {
        IllegalArgumentException iae = assertThrows(
                IllegalArgumentException.class,
                () -> new SqlSerializer.Builder()
                        .tableName("test")
                        .where(columnValue("a", equalTo("a")))
                        .build()
        );

        assertThat(iae.getMessage(), containsString("The columns of the table must be specified to prevent SQL injection"));

        iae = assertThrows(
                IllegalArgumentException.class,
                () -> new SqlSerializer.Builder()
                        .tableName("test")
                        .columns(Set.of("B"))
                        .where(columnValue("a", equalTo("a")))
                        .build()
        );

        assertThat(iae.getMessage(), containsString("Unexpected column name: A"));
    }
}
