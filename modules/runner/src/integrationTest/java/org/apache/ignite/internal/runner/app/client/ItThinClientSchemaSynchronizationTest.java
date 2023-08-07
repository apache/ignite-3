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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for client schema synchronization.
 */
@SuppressWarnings("resource")
public class ItThinClientSchemaSynchronizationTest extends ItAbstractThinClientTest {
    @Test
    void testClientUsesLatestSchemaOnWrite() throws InterruptedException {
        IgniteClient client = client();
        Session ses = client.sql().createSession();

        // Create table, insert data.
        String tableName = "testClientUsesLatestSchemaOnWrite";
        ses.execute(null, "CREATE TABLE " + tableName + "(ID INT NOT NULL PRIMARY KEY, NAME VARCHAR NOT NULL)");

        waitForTableOnAllNodes(tableName);
        RecordView<Tuple> recordView = client.tables().table(tableName).recordView();

        Tuple rec = Tuple.create().set("ID", 1).set("NAME", "name");
        recordView.insert(null, rec);

        // Modify table, insert data - client will use old schema, receive error, retry with new schema, fail due to an extra column.
        // The process is transparent for the user: updated schema is in effect immediately.
        ses.execute(null, "ALTER TABLE " + tableName + " DROP COLUMN NAME");

        Tuple rec2 = Tuple.create().set("ID", 2).set("NAME", "name2");
        Throwable ex = assertThrowsWithCause(() -> recordView.upsert(null, rec2), IllegalArgumentException.class);
        assertEquals("Tuple doesn't match schema: schemaVersion=2, extraColumns=[NAME]", ex.getMessage());
    }

    @Test
    void testClientUsesLatestSchemaOnRead() throws InterruptedException {
        IgniteClient client = client();
        Session ses = client.sql().createSession();

        // Create table, insert data.
        String tableName = "testClientUsesLatestSchemaOnRead";
        ses.execute(null, "CREATE TABLE " + tableName + "(ID INT NOT NULL PRIMARY KEY)");

        waitForTableOnAllNodes(tableName);
        RecordView<Tuple> recordView = client.tables().table(tableName).recordView();

        Tuple rec = Tuple.create().set("ID", 1);
        recordView.insert(null, rec);

        // Modify table, insert data - client will use old schema, receive error, retry with new schema.
        // The process is transparent for the user: updated schema is in effect immediately.
        ses.execute(null, "ALTER TABLE " + tableName + " ADD COLUMN NAME VARCHAR DEFAULT 'def_name'");
        assertEquals("def_name", recordView.get(null, rec).stringValue(1));
    }

    @Test
    void testClientUsesLatestSchemaOnReadWithNotNullColumn() throws InterruptedException {
        IgniteClient client = client();
        Session ses = client.sql().createSession();

        // Create table, insert data.
        String tableName = "testClientUsesLatestSchemaOnReadWithNotNullColumn";
        ses.execute(null, "CREATE TABLE " + tableName + "(ID INT NOT NULL PRIMARY KEY)");

        waitForTableOnAllNodes(tableName);
        RecordView<Tuple> recordView = client.tables().table(tableName).recordView();

        Tuple rec = Tuple.create().set("ID", 1);
        recordView.insert(null, rec);

        // Modify table and get old row.
        // It still has null value in the old column, even though it is not allowed by the new schema.
        ses.execute(null, "ALTER TABLE " + tableName + " ADD COLUMN NAME VARCHAR NOT NULL");
        assertNull(recordView.get(null, rec).stringValue(1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testClientReloadsTupleSchemaOnUnmappedColumnException(boolean useGetAndUpsert) throws InterruptedException {
        IgniteClient client = client();
        Session ses = client.sql().createSession();

        String tableName = "testClientReloadsTupleSchemaOnUnmappedColumnException_" + useGetAndUpsert;
        ses.execute(null, "CREATE TABLE " + tableName + "(ID INT NOT NULL PRIMARY KEY)");

        waitForTableOnAllNodes(tableName);
        RecordView<Tuple> recordView = client.tables().table(tableName).recordView();

        Tuple rec = Tuple.create().set("ID", 1).set("NAME", "name");
        Runnable action = useGetAndUpsert ?
                () -> recordView.getAndUpsert(null, rec) :
                () -> recordView.insert(null, rec);

        // Insert fails, because there is no NAME column.
        var ex = assertThrows(IgniteException.class, action::run);
        assertEquals("Tuple doesn't match schema: schemaVersion=1, extraColumns=[NAME]", ex.getMessage());

        // Modify table, insert again - client will use old schema, throw ClientSchemaMismatchException,
        // reload schema, retry with new schema and succeed.
        ses.execute(null, "ALTER TABLE " + tableName + " ADD COLUMN NAME VARCHAR NOT NULL");
        action.run();

        assertEquals("name", recordView.get(null, rec).stringValue(1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testClientReloadsKvTupleSchemaOnUnmappedColumnException(boolean useGetAndPut) throws InterruptedException {
        IgniteClient client = client();
        Session ses = client.sql().createSession();

        String tableName = "testClientReloadsKvTupleSchemaOnUnmappedColumnException_" + useGetAndPut;
        ses.execute(null, "CREATE TABLE " + tableName + "(ID INT NOT NULL PRIMARY KEY)");

        waitForTableOnAllNodes(tableName);
        KeyValueView<Tuple, Tuple> kvView = client.tables().table(tableName).keyValueView();

        // Insert fails, because there is no NAME column.
        Tuple key = Tuple.create().set("ID", 1);
        Tuple val = Tuple.create().set("NAME", "name");

        Runnable action = useGetAndPut ?
                () -> kvView.getAndPut(null, key, val) :
                () -> kvView.put(null, key, val);

        var ex = assertThrows(IgniteException.class, action::run);
        assertEquals("Value tuple doesn't match schema: schemaVersion=1, extraColumns=[NAME]", ex.getMessage());

        // Modify table, insert again - client will use old schema, throw ClientSchemaMismatchException,
        // reload schema, retry with new schema and succeed.
        ses.execute(null, "ALTER TABLE " + tableName + " ADD COLUMN NAME VARCHAR NOT NULL");
        action.run();

        assertEquals("name", kvView.get(null, key).stringValue(0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testClientReloadsPojoSchemaOnUnmappedColumnException(boolean useGetAndUpsert) throws InterruptedException {
        IgniteClient client = client();
        Session ses = client.sql().createSession();

        String tableName = "testClientReloadsPojoSchemaOnUnmappedColumnException_" + useGetAndUpsert;
        ses.execute(null, "CREATE TABLE " + tableName + "(ID INT NOT NULL PRIMARY KEY)");

        waitForTableOnAllNodes(tableName);
        RecordView<Pojo> recordView = client.tables().table(tableName).recordView(Mapper.of(Pojo.class));

        // Insert fails, because there is no NAME column.
        Pojo rec = new Pojo(1, "name");
        Runnable action = useGetAndUpsert ?
                () -> recordView.getAndUpsert(null, rec) :
                () -> recordView.insert(null, rec);

        var ex = assertThrows(IgniteException.class, action::run);
        assertEquals(
                "Fields [name] of type org.apache.ignite.internal.runner.app.client.ItThinClientSchemaSynchronizationTest$Pojo "
                        + "are not mapped to columns.",
                ex.getMessage());

        // Modify table, insert again - client will use old schema, throw ClientSchemaMismatchException,
        // reload schema, retry with new schema and succeed.
        ses.execute(null, "ALTER TABLE " + tableName + " ADD COLUMN NAME VARCHAR NOT NULL");
        action.run();

        assertEquals("name", recordView.get(null, rec).name);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testClientReloadsKvPojoSchemaOnUnmappedColumnException(boolean useGetAndPut) throws InterruptedException {
        IgniteClient client = client();
        Session ses = client.sql().createSession();

        String tableName = "testClientReloadsKvPojoSchemaOnUnmappedColumnException_" + useGetAndPut;
        ses.execute(null, "CREATE TABLE " + tableName + "(ID INT NOT NULL PRIMARY KEY)");

        waitForTableOnAllNodes(tableName);
        KeyValueView<Integer, ValPojo> kvView = client.tables().table(tableName)
                .keyValueView(Mapper.of(Integer.class), Mapper.of(ValPojo.class));

        // Insert fails, because there is no NAME column.
        Integer key = 1;
        ValPojo val = new ValPojo("name");

        Runnable action = useGetAndPut ?
                () -> kvView.getAndPut(null, key, val) :
                () -> kvView.put(null, key, val);

        var ex = assertThrows(IgniteException.class, action::run);
        assertEquals(
                "Fields [name] of type "
                        + "org.apache.ignite.internal.runner.app.client.ItThinClientSchemaSynchronizationTest$ValPojo "
                        + "are not mapped to columns.",
                ex.getMessage());

        // Modify table, insert again - client will use old schema, throw ClientSchemaMismatchException,
        // reload schema, retry with new schema and succeed.
        ses.execute(null, "ALTER TABLE " + tableName + " ADD COLUMN NAME VARCHAR NOT NULL");
        action.run();

        assertEquals("name", kvView.get(null, key).name);
    }

    // TODO: Fix doSchemaOutInOpAsync, and add tests for "read after alter table".

    private static class Pojo {
        public int id;
        public String name;

        public Pojo() {
        }

        public Pojo(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    private static class ValPojo {
        public String name;

        public ValPojo() {
        }

        public ValPojo(String name) {
            this.name = name;
        }
    }
}
