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

package org.apache.ignite.table;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.table.impl.TestTableStorageImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class TableOperationsTest {
    /**
     *
     */
    @Test
    public void testUpsert() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        Table tbl = new TableImpl(new TestTableStorageImpl(), new DummySchemaManagerImpl(schema));

        final Tuple tup1 = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple tup1v2 = tbl.tupleBuilder().set("id", 1L).set("val", 111L).build();
        final Tuple tup2 = tbl.tupleBuilder().set("id", 2L).set("val", 22L).build();

        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertNull(tbl.get(tup1));

        // Insert new tuple.
        tbl.upsert(tup1);

        assertEqualsRows(schema, tup1, tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertEqualsRows(schema, tup1, tbl.get(tup1));

        assertNull(tbl.get(tup2));

        // Ignore insert operation for exited row.
        tbl.upsert(tup1v2);

        assertEqualsRows(schema, tup1v2, tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertEqualsRows(schema, tup1v2, tbl.get(tup1v2));

    }

    void assertEqualsRows(SchemaDescriptor schema, Tuple t1, Tuple t2) {
        assertEqualsKeys(schema, t1, t2);
        assertEqualsValues(schema, t1, t2);
    }

    void assertEqualsKeys(SchemaDescriptor schema, Tuple t1, Tuple t2) {
        int nonNullKey = 0;

        for (int i = 0; i < schema.keyColumns().length(); i++) {
            final Column col = schema.keyColumns().column(i);

            final Object val1 = t1.value(col.name());
            final Object val2 = t2.value(col.name());

            Assertions.assertEquals(val1, val2, "Value columns equality check failed: colIdx=" + col.schemaIndex());

            if (schema.keyColumn(i) && val1 != null)
                nonNullKey++;
        }

        assertTrue(nonNullKey > 0, "At least one non-null key column must exist.");
    }

    void assertEqualsValues(SchemaDescriptor schema, Tuple t1, Tuple t2) {
        for (int i = 0; i < schema.valueColumns().length(); i++) {
            final Column col = schema.valueColumns().column(i);

            final Object val1 = t1.value(col.name());
            final Object val2 = t2.value(col.name());

            Assertions.assertEquals(val1, val2, "Key columns equality check failed: colIdx=" + col.schemaIndex());
        }
    }
}
