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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.schema.BinaryRowMatcher.equalToRow;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;

import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** For {@link BinaryRowUpdater} testing. */
public class BinaryRowUpdaterTest {
    private static final SchemaDescriptor SCHEMA_V_1 = new SchemaDescriptor(
            1,
            new Column[]{new Column("k", INT32, false)},
            new Column[]{new Column("v", INT32, true)}
    );

    private static final SchemaDescriptor SCHEMA_V_2 = new SchemaDescriptor(
            2,
            new Column[]{new Column("k", INT32, false)},
            new Column[]{
                    new Column("v", INT32, true),
                    new Column("vLong", INT64, true)
            }
    );

    private static final SchemaDescriptor SCHEMA_V_3 = new SchemaDescriptor(
            3,
            new Column[]{new Column("k", INT32, false)},
            new Column[]{
                    new Column("v", INT32, true),
                    new Column("vLong", INT64, true),
                    new Column("s", STRING, true, DefaultValueProvider.constantProvider("foo"))
            }
    );

    private static final SchemaRegistry SCHEMA_REGISTRY = new SchemaRegistryImpl(schemaVersion -> {
        if (SCHEMA_V_1.version() == schemaVersion) {
            return SCHEMA_V_1;
        } else if (SCHEMA_V_2.version() == schemaVersion) {
            return SCHEMA_V_2;
        } else if (SCHEMA_V_3.version() == schemaVersion) {
            return SCHEMA_V_3;
        }

        return null;
    }, SCHEMA_V_1);

    @BeforeAll
    static void beforeAll() {
        SCHEMA_V_2.columnMapping(SchemaUtils.columnMapper(SCHEMA_V_1, SCHEMA_V_2));
        SCHEMA_V_3.columnMapping(SchemaUtils.columnMapper(SCHEMA_V_2, SCHEMA_V_3));
    }

    @Test
    void testNoUpdateRow() {
        BinaryRow source = binaryRow(SCHEMA_V_1, 1, 2);

        assertThat(updateRow(SCHEMA_V_1, source), sameInstance(source));
    }

    @Test
    void testUpdateRow() {
        BinaryRow source = binaryRow(SCHEMA_V_1, 1, 2);
        BinaryRow expected = binaryRow(SCHEMA_V_2, 1, 2, null);

        assertThat(updateRow(SCHEMA_V_2, source), equalToRow(expected));
    }

    @Test
    void testUpdateRowWithStringDefault() {
        BinaryRow source = binaryRow(SCHEMA_V_2, 1, 2, 10L);
        BinaryRow expected = binaryRow(SCHEMA_V_3, 1, 2, 10L, "foo");

        assertThat(updateRow(SCHEMA_V_3, source), equalToRow(expected));
    }

    private static BinaryRow binaryRow(SchemaDescriptor schema, Object... values) {
        var rowAssembler = new RowAssembler(schema, -1);

        for (Object value : values) {
            rowAssembler.appendValue(value);
        }

        return new BinaryRowImpl(schema.version(), rowAssembler.build().tupleSlice());
    }

    private static BinaryRow updateRow(SchemaDescriptor targetSchema, BinaryRow source) {
        return new BinaryRowUpdater(SCHEMA_REGISTRY, targetSchema).update(source);
    }
}
