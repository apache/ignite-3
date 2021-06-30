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

package org.apache.ignite.internal.runner.app;

import org.apache.ignite.internal.table.ColumnNotFoundException;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Ignition interface tests.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14581")
class SchemaChangeKVViewTest extends AbstractSchemaChangeTest {
    /**
     * Check add a new column to table schema.
     */
    @Test
    public void testDropColumn() {
        checkDropColumn(
                "val2",
                (grid) -> {
                    final KeyValueBinaryView kvView = grid.get(1).tables().table(TABLE).kvView();

                    kvView.put(kvView.tupleBuilder().set("key", 1L).build(),
                            kvView.tupleBuilder().set("val1", 111).set("val2", "str").build());
                },
                (grid) -> {
                    KeyValueBinaryView kvView = grid.get(1).tables().table(TABLE).kvView();

                    // Check old row conversion.
                    final Tuple keyTuple = kvView.tupleBuilder().set("key", 1L).build();

                    assertEquals(111, (Integer) kvView.get(keyTuple).value("val1"));
                    assertThrows(ColumnNotFoundException.class, () -> kvView.get(keyTuple).value("val2"));

                    // Check tuple of outdated schema.
                    assertThrows(ColumnNotFoundException.class, () -> kvView.put(
                            kvView.tupleBuilder().set("key", 2L).build(),
                            kvView.tupleBuilder().set("val1", -222).set("val2", "str").build())
                    );

                    // Check tuple of correct schema.
                    kvView.put(kvView.tupleBuilder().set("key", 2L).build(), kvView.tupleBuilder().set("val1", 222).build());

                    final Tuple keyTuple2 = kvView.tupleBuilder().set("key", 2L).build();

                    assertEquals(222, (Integer) kvView.get(keyTuple2).value("val1"));
                    assertThrows(ColumnNotFoundException.class, () -> kvView.get(keyTuple2).value("val2"));
                }
        );
    }


    /**
     * Check drop column from table schema.
     */
    @Test
    public void testAddNewColumn() {
        checkAddNewColumn(
                SchemaBuilders.column("val2", ColumnType.string()).asNullable().withDefaultValue("default").build(),
                grid -> {
                    // Put data on node.
                    KeyValueBinaryView kvView = grid.get(1).tables().table(TABLE).kvView();

                    kvView.put(kvView.tupleBuilder().set("key", 1L).build(), kvView.tupleBuilder().set("val1", 222).build());

                    assertThrows(ColumnNotFoundException.class, () -> kvView.put(
                            kvView.tupleBuilder().set("key", 1L).build(),
                            kvView.tupleBuilder().set("val1", -111).set("val2", "str").build())
                    );
                },
                grid -> {
                    KeyValueBinaryView kvView = grid.get(2).tables().table(TABLE).kvView();

                    // Check old row conversion.
                    Tuple keyTuple = kvView.tupleBuilder().set("key", 1L).build();

                    assertEquals(111, (Integer) kvView.get(keyTuple).value("val1"));
                    assertEquals("default", kvView.get(keyTuple).value("val2"));

                    // Check tuple of new schema.
                    kvView.put(kvView.tupleBuilder().set("key", 2L).build(),
                            kvView.tupleBuilder().set("val1", 222).set("val2", "str").build());

                    Tuple keyTuple2 = kvView.tupleBuilder().set("key", 2L).build();

                    assertEquals(222, (Integer) kvView.get(keyTuple2).value("val1"));
                    assertEquals("str", kvView.get(keyTuple2).value("val2"));
                });
    }

    /**
     * Check rename column from table schema.
     */
    @Test
    public void testRenameColumn() {
        checkRenameColumn(
                grid -> {
                    KeyValueBinaryView kvView = grid.get(1).tables().table(TABLE).kvView();

                    kvView.put(kvView.tupleBuilder().set("key", 1L).build(), kvView.tupleBuilder().set("val1", 111).build());

                    assertThrows(ColumnNotFoundException.class, () -> kvView.put(
                            kvView.tupleBuilder().set("key", 2L).build(),
                            kvView.tupleBuilder().set("val2", 222).build())
                    );
                },
                grid -> {
                    KeyValueBinaryView kvView = grid.get(2).tables().table(TABLE).kvView();

                    // Check old row conversion.
                    Tuple keyTuple1 = kvView.tupleBuilder().set("key", 1L).build();

                    assertEquals(1, (Long) kvView.get(keyTuple1).value("key"));
                    assertEquals(111, (Integer) kvView.get(keyTuple1).value("val2"));
                    assertThrows(ColumnNotFoundException.class, () -> kvView.get(keyTuple1).value("val1"));

                    // Check tuple of correct schema.
                    assertThrows(ColumnNotFoundException.class, () -> kvView.put(
                            kvView.tupleBuilder().set("key", 2L).build(),
                            kvView.tupleBuilder().set("val1", -222).build())
                    );

                    // Check tuple of new schema.
                    kvView.put(kvView.tupleBuilder().set("key", 2L).build(), kvView.tupleBuilder().set("val2", 222).build());

                    Tuple keyTuple2 = kvView.tupleBuilder().set("key", 2L).build();

                    assertEquals(222, (Integer) kvView.get(keyTuple2).value("val2"));
                    assertThrows(ColumnNotFoundException.class, () -> kvView.get(keyTuple2).value("val1"));
                });
    }
}
