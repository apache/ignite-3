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

import static org.apache.ignite.internal.table.criteria.CriteriaElement.equalTo;
import static org.apache.ignite.internal.table.criteria.CriteriaElement.in;
import static org.apache.ignite.internal.table.criteria.Criterias.and;
import static org.apache.ignite.internal.table.criteria.Criterias.columnValue;
import static org.apache.ignite.internal.table.criteria.Criterias.not;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * SQL generation test.
 */
class SqlSerializerTest {
    @Test
    void testEquals() {
        var ser = new SqlSerializer.Builder()
                .tableName("test")
                .where(columnValue("a", equalTo("a")))
                .build();

        assertEquals("SELECT * FROM test WHERE a = ?", ser.toString());
        assertArrayEquals(new Object[] {"a"}, ser.getArguments());
    }

    @Test
    void testNonEquals() {
        var ser = new SqlSerializer.Builder()
                .tableName("test")
                .where(not(columnValue("a", equalTo("a"))))
                .build();

        assertEquals("SELECT * FROM test WHERE not a = ?", ser.toString());
        assertArrayEquals(new Object[] {"a"}, ser.getArguments());
    }

    @Test
    void testIn() {
        var ser = new SqlSerializer.Builder()
                .tableName("test")
                .where(not(columnValue("a", in("a", "b", "c"))))
                .build();

        assertEquals("SELECT * FROM test WHERE not a IN (?, ?, ?)", ser.toString());
        assertArrayEquals(new Object[] {"a", "b", "c"}, ser.getArguments());
    }

    @Test
    void testAnd() {
        var ser = new SqlSerializer.Builder()
                .tableName("test")
                .where(and(columnValue("a", equalTo("a")), not(columnValue("b", in("a", "b", "c")))))
                .build();

        assertEquals("SELECT * FROM test WHERE (a = ?) AND (not b IN (?, ?, ?))", ser.toString());
        assertArrayEquals(new Object[] {"a", "a", "b", "c"}, ser.getArguments());
    }
}
