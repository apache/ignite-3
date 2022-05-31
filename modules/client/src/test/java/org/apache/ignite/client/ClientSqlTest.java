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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.junit.jupiter.api.Test;

/**
 * SQL tests.
 */
public class ClientSqlTest extends AbstractClientTableTest {
    @Test
    public void testExecute() {
        Session session = client.sql().createSession();
        AsyncResultSet resultSet = session.executeAsync(null, "SELECT 1").join();

        assertTrue(resultSet.hasRowSet());
        assertFalse(resultSet.wasApplied());
        assertEquals(1, resultSet.currentPageSize());

        SqlRow row = resultSet.currentPage().iterator().next();
        assertEquals(1, row.intValue(0));
    }

    @Test
    public void testSessionPropertiesPropagation() {
        Session session = client.sql().sessionBuilder()
                .defaultSchema("SCHEMA1")
                .defaultTimeout(123, TimeUnit.SECONDS)
                .defaultPageSize(234)
                .property("prop1", 1)
                .property("prop2", 2)
                .build();

        AsyncResultSet resultSet = session.executeAsync(null, "SELECT PROPS").join();

        Map<String, Object> props = StreamSupport.stream(resultSet.currentPage().spliterator(), false)
                .collect(Collectors.toMap(x -> x.stringValue(0), x -> x.value(1)));

        assertEquals("SCHEMA1", props.get("schema"));
        assertEquals(123000L, props.get("timeout"));
        assertEquals(234, props.get("pageSize"));
        assertEquals(1, props.get("prop1"));
        assertEquals(2, props.get("prop2"));
    }

    @Test
    public void testStatementPropertiesOverrideSessionProperties() {
        Session session = client.sql().sessionBuilder()
                .defaultSchema("SCHEMA1")
                .defaultTimeout(123, TimeUnit.SECONDS)
                .defaultPageSize(234)
                .property("prop1", 1)
                .property("prop2", 2)
                .build();

        Statement statement = client.sql().statementBuilder()
                .query("SELECT PROPS")
                .defaultSchema("SCHEMA2")
                .queryTimeout(124, TimeUnit.SECONDS)
                .pageSize(235)
                .property("prop2", 22)
                .property("prop3", 3)
                .build();

        AsyncResultSet resultSet = session.executeAsync(null, statement).join();

        Map<String, Object> props = StreamSupport.stream(resultSet.currentPage().spliterator(), false)
                .collect(Collectors.toMap(x -> x.stringValue(0), x -> x.value(1)));

        assertEquals("SCHEMA2", props.get("schema"));
        assertEquals(124000L, props.get("timeout"));
        assertEquals(235, props.get("pageSize"));
        assertEquals(1, props.get("prop1"));
        assertEquals(22, props.get("prop2"));
        assertEquals(3, props.get("prop3"));
    }
}
