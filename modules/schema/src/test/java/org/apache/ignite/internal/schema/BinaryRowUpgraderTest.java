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
import static org.apache.ignite.internal.schema.SchemaTestUtils.binaryRow;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;

import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** For {@link BinaryRowUpgrader} testing. */
public class BinaryRowUpgraderTest {
    private static final SchemaDescriptor ORIGINAL_SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("k", INT32, false)},
            new Column[]{new Column("v", INT32, true)}
    );

    private static final SchemaDescriptor NULLABLE_COLUMN_ADDED = new SchemaDescriptor(
            2,
            new Column[]{new Column("k", INT32, false)},
            new Column[]{
                    new Column("v", INT32, true),
                    new Column("vLong", INT64, true)
            }
    );

    private static final SchemaDescriptor DEFAULTED_COLUMN_ADDED = new SchemaDescriptor(
            3,
            new Column[]{new Column("k", INT32, false)},
            new Column[]{
                    new Column("v", INT32, true),
                    new Column("vLong", INT64, true),
                    new Column("s", STRING, true, DefaultValueProvider.constantProvider("foo"))
            }
    );

    private static final SchemaDescriptor DEFAULT_COLUMN_CHANGED = new SchemaDescriptor(
            4,
            new Column[]{new Column("k", INT32, false)},
            new Column[]{
                    new Column("v", INT32, true),
                    new Column("vLong", INT64, true),
                    new Column("s", STRING, true, DefaultValueProvider.constantProvider("bar"))
            }
    );

    private static final SchemaRegistry SCHEMA_REGISTRY = new SchemaRegistryImpl(schemaVersion -> {
        if (ORIGINAL_SCHEMA.version() == schemaVersion) {
            return ORIGINAL_SCHEMA;
        } else if (NULLABLE_COLUMN_ADDED.version() == schemaVersion) {
            return NULLABLE_COLUMN_ADDED;
        } else if (DEFAULTED_COLUMN_ADDED.version() == schemaVersion) {
            return DEFAULTED_COLUMN_ADDED;
        } else if (DEFAULT_COLUMN_CHANGED.version() == schemaVersion) {
            return DEFAULT_COLUMN_CHANGED;
        }

        return null;
    }, ORIGINAL_SCHEMA);

    @BeforeAll
    static void beforeAll() {
        NULLABLE_COLUMN_ADDED.columnMapping(SchemaUtils.columnMapper(ORIGINAL_SCHEMA, NULLABLE_COLUMN_ADDED));
        DEFAULTED_COLUMN_ADDED.columnMapping(SchemaUtils.columnMapper(NULLABLE_COLUMN_ADDED, DEFAULTED_COLUMN_ADDED));
        DEFAULT_COLUMN_CHANGED.columnMapping(SchemaUtils.columnMapper(DEFAULTED_COLUMN_ADDED, DEFAULT_COLUMN_CHANGED));
    }

    @Test
    void testNoUpgradeRow() {
        BinaryRow source = binaryRow(ORIGINAL_SCHEMA, 1, 2);

        assertThat(upgradeRow(ORIGINAL_SCHEMA, source), sameInstance(source));
    }

    @Test
    void testUpgradeRow() {
        BinaryRow source = binaryRow(ORIGINAL_SCHEMA, 1, 2);
        BinaryRow expected = binaryRow(NULLABLE_COLUMN_ADDED, 1, 2, null);

        assertThat(upgradeRow(NULLABLE_COLUMN_ADDED, source), equalToRow(expected));
    }

    @Test
    void testUpgradeRowWithStringDefault() {
        BinaryRow source = binaryRow(NULLABLE_COLUMN_ADDED, 1, 2, 10L);
        BinaryRow expected = binaryRow(DEFAULTED_COLUMN_ADDED, 1, 2, 10L, "foo");

        assertThat(upgradeRow(DEFAULTED_COLUMN_ADDED, source), equalToRow(expected));
    }

    @Test
    void testDontDowngradeSourceBinaryRow() {
        BinaryRow source = binaryRow(DEFAULT_COLUMN_CHANGED, 1, 2, 10L, "bar");

        assertThat(upgradeRow(DEFAULTED_COLUMN_ADDED, source), sameInstance(source));
    }

    private static BinaryRow upgradeRow(SchemaDescriptor targetSchema, BinaryRow source) {
        return new BinaryRowUpgrader(SCHEMA_REGISTRY, targetSchema).upgrade(source);
    }
}
