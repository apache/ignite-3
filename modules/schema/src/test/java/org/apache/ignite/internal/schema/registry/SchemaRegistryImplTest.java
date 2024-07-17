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

package org.apache.ignite.internal.schema.registry;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor.INITIAL_TABLE_VERSION;
import static org.apache.ignite.internal.schema.mapping.ColumnMapping.createMapper;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Schema manager test.
 */
public class SchemaRegistryImplTest {
    private final SchemaDescriptor schemaV1 = new SchemaDescriptor(1,
            new Column[]{new Column("keyLongCol", INT64, false)},
            new Column[]{new Column("valBytesCol", BYTES, true)});

    private final SchemaDescriptor schemaV2 = new SchemaDescriptor(2,
            new Column[]{new Column("keyLongCol", INT64, false)},
            new Column[]{
                    new Column("valBytesCol", BYTES, true),
                    new Column("valStringCol", STRING, true)
            });

    @BeforeEach
    void setupSchemas() {
        Column column = schemaV2.column("valStringCol");

        assertNotNull(column);

        schemaV2.columnMapping(createMapper(schemaV2).add(column));
    }

    /**
     * Check registration of schema with wrong versions.
     */
    @Test
    public void testWrongSchemaVersionRegistration() {
        final SchemaDescriptor schemaWithZeroVersion = new SchemaDescriptor(0,
                new Column[]{new Column("keyLongCol", INT64, false)},
                new Column[]{new Column("valBytesCol", BYTES, true)});

        final SchemaRegistryImpl reg = new SchemaRegistryImpl(v -> null, schemaV1);

        assertEquals(INITIAL_TABLE_VERSION, reg.lastKnownSchemaVersion());
        assertNotNull(reg.lastKnownSchema());

        // Try to register schema with initial version.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.onSchemaRegistered(schemaV1));
        assertEquals(INITIAL_TABLE_VERSION, reg.lastKnownSchemaVersion());

        assertNotNull(reg.schema(INITIAL_TABLE_VERSION));

        // Try to register schema with version of 0-zero.
        assertThrows(SchemaRegistryException.class, () -> reg.onSchemaRegistered(schemaWithZeroVersion));
        assertEquals(INITIAL_TABLE_VERSION, reg.lastKnownSchemaVersion());

        assertNotNull(reg.schema(0));

        // Try to register schema with version of 2.
        reg.onSchemaRegistered(schemaV2);
        assertEquals(schemaV2.version(), reg.lastKnownSchemaVersion());

        assertNotNull(reg.schema(INITIAL_TABLE_VERSION));
        assertNotNull(reg.schema(0));
        assertNotNull(reg.schema(1));
        assertNotNull(reg.schema(2));
    }

    /**
     * Check initial schema registration.
     */
    @Test
    public void testSchemaRegistration() {
        final SchemaDescriptor schemaV4 = new SchemaDescriptor(4,
                new Column[]{new Column("keyLongCol", INT64, false)},
                new Column[]{
                        new Column("valBytesCol", BYTES, true),
                        new Column("valStringCol", STRING, true)
                });

        final SchemaRegistryImpl reg = new SchemaRegistryImpl(v -> null, schemaV1);

        assertEquals(INITIAL_TABLE_VERSION, reg.lastKnownSchemaVersion());
        assertNotNull(reg.lastKnownSchema());

        // Register schema with first version again.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.onSchemaRegistered(schemaV1));

        assertEquals(1, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV1, reg.lastKnownSchema());
        assertSameSchema(schemaV1, reg.schema(1));

        // Register schema with next version.
        reg.onSchemaRegistered(schemaV2);

        assertEquals(2, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV2, reg.lastKnownSchema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));

        // Try to register schema with version of 4.
        assertThrows(SchemaRegistryException.class, () -> reg.onSchemaRegistered(schemaV4));

        assertEquals(2, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV2, reg.lastKnownSchema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
        try {
            assertThat(supplyAsync(() -> reg.schema(3)), not(completedFuture()));
            assertThat(supplyAsync(() -> reg.schema(4)), not(completedFuture()));
        } finally {
            reg.close();
        }
    }

    /**
     * Check schema registration.
     */
    @Test
    public void testDuplicateSchemaRegistration() {
        final SchemaDescriptor wrongSchema = new SchemaDescriptor(1,
                new Column[]{new Column("keyLongCol", INT64, false)},
                new Column[]{
                        new Column("valBytesCol", BYTES, true),
                        new Column("valStringCol", STRING, true)
                });

        final SchemaRegistryImpl reg = new SchemaRegistryImpl(v -> null, schemaV1);

        assertEquals(INITIAL_TABLE_VERSION, reg.lastKnownSchemaVersion());

        // Register schema with very first version.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.onSchemaRegistered(schemaV1));

        assertEquals(1, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV1, reg.lastKnownSchema());
        assertSameSchema(schemaV1, reg.schema(1));

        // Try to register same schema once again.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.onSchemaRegistered(schemaV1));

        assertEquals(1, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV1, reg.lastKnownSchema());
        assertSameSchema(schemaV1, reg.schema(1));

        // Try to register another schema with same version and check nothing was registered.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.onSchemaRegistered(wrongSchema));

        assertEquals(1, reg.lastKnownSchemaVersion());
        assertEquals(1, reg.lastKnownSchema().version());

        assertSameSchema(schemaV1, reg.lastKnownSchema());
        assertSameSchema(schemaV1, reg.schema(1));

        // Register schema with next version.
        reg.onSchemaRegistered(schemaV2);

        assertEquals(2, reg.lastKnownSchemaVersion());

        assertSameSchema(schemaV2, reg.lastKnownSchema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
    }

    /**
     * Check schema registration with full history.
     */
    @Test
    public void testInitialSchemaWithFullHistory() {
        final SchemaDescriptor schemaV3 = new SchemaDescriptor(3,
                new Column[]{new Column("keyLongCol", INT64, false)},
                new Column[]{
                        new Column("valStringCol", STRING, true)
                });

        final SchemaDescriptor schemaV4 = new SchemaDescriptor(4,
                new Column[]{new Column("keyLongCol", INT64, false)},
                new Column[]{
                        new Column("valBytesCol", BYTES, true),
                        new Column("valStringCol", STRING, true)
                });

        Map<Integer, SchemaDescriptor> history = schemaHistory(schemaV1, schemaV2);

        final SchemaRegistryImpl reg = new SchemaRegistryImpl(history::get, schemaV2);

        assertEquals(2, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV2, reg.lastKnownSchema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));

        // Register schema with duplicate version.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.onSchemaRegistered(schemaV1));

        assertEquals(2, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV2, reg.lastKnownSchema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));

        // Register schema with out-of-order version.
        assertThrows(SchemaRegistryException.class, () -> reg.onSchemaRegistered(schemaV4));

        assertEquals(2, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV2, reg.lastKnownSchema());

        // Register schema with next version.
        reg.onSchemaRegistered(schemaV3);

        assertEquals(3, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV3, reg.lastKnownSchema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
        assertSameSchema(schemaV3, reg.schema(3));
    }

    /**
     * Check schema registration with history tail.
     */
    @Test
    public void testInitialSchemaWithTailHistory() {
        final SchemaDescriptor schemaV3 = new SchemaDescriptor(3,
                new Column[]{new Column("keyLongCol", INT64, false)},
                new Column[]{new Column("valStringCol", STRING, true)});

        final SchemaDescriptor schemaV4 = new SchemaDescriptor(4,
                new Column[]{new Column("keyLongCol", INT64, false)},
                new Column[]{
                        new Column("valBytesCol", BYTES, true),
                        new Column("valStringCol", STRING, true)
                });

        final SchemaDescriptor schemaV5 = new SchemaDescriptor(5,
                new Column[]{new Column("keyLongCol", INT64, false)},
                new Column[]{new Column("valStringCol", STRING, true)});

        Map<Integer, SchemaDescriptor> history = schemaHistory(schemaV2, schemaV3);

        final SchemaRegistryImpl reg = new SchemaRegistryImpl(history::get, schemaV3);

        assertEquals(3, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV3, reg.lastKnownSchema());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
        assertSameSchema(schemaV3, reg.schema(3));

        // Register schema with duplicate version.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.onSchemaRegistered(schemaV2));

        assertEquals(3, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV3, reg.lastKnownSchema());

        // Register schema with out-of-order version.
        assertThrows(SchemaRegistryException.class, () -> reg.onSchemaRegistered(schemaV5));

        assertEquals(3, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV3, reg.lastKnownSchema());

        // Register schema with outdated version.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.onSchemaRegistered(schemaV1));

        assertEquals(3, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV3, reg.lastKnownSchema());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
        assertSameSchema(schemaV3, reg.schema(3));

        // Register schema with next version.
        reg.onSchemaRegistered(schemaV4);

        assertEquals(4, reg.lastKnownSchemaVersion());
        assertSameSchema(schemaV4, reg.lastKnownSchema());
        assertSameSchema(schemaV2, reg.schema(2));
        assertSameSchema(schemaV3, reg.schema(3));
        assertSameSchema(schemaV4, reg.schema(4));
    }

    @Test
    void schemaAsyncReturnsExpectedResults() {
        Map<Integer, SchemaDescriptor> history = schemaHistory(schemaV1);

        SchemaRegistryImpl reg = new SchemaRegistryImpl(history::get, schemaV1);

        assertThat(reg.schemaAsync(0), willThrow(SchemaRegistryException.class));
        assertThat(reg.schemaAsync(1), willBe(schemaV1));

        CompletableFuture<SchemaDescriptor> schema2Future = reg.schemaAsync(2);
        assertThat(schema2Future, willTimeoutIn(100, TimeUnit.MILLISECONDS));

        reg.onSchemaRegistered(schemaV2);

        assertThat(schema2Future, willBe(schemaV2));
    }

    /**
     * SchemaHistory.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param history Table schema history.
     * @return Schema history map.
     */
    private Map<Integer, SchemaDescriptor> schemaHistory(SchemaDescriptor... history) {
        return Arrays.stream(history).collect(toMap(SchemaDescriptor::version, Function.identity()));
    }

    /**
     * Validate schemas are equals.
     *
     * @param schemaDesc1 Schema descriptor to compare with.
     * @param schemaDesc2 Schema descriptor to compare.
     */
    private void assertSameSchema(SchemaDescriptor schemaDesc1, SchemaDescriptor schemaDesc2) {
        assertEquals(schemaDesc1.version(), schemaDesc2.version(), "Descriptors of different versions.");

        assertTrue(SchemaUtils.equalSchemas(schemaDesc1, schemaDesc2), "Schemas are not equal.");
    }
}
