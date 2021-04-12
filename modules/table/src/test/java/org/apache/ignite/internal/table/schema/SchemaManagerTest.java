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

package org.apache.ignite.internal.table.schema;

import java.util.List;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.NativeType.BYTES;
import static org.apache.ignite.internal.schema.NativeType.LONG;
import static org.apache.ignite.internal.schema.NativeType.STRING;
import static org.apache.ignite.internal.table.schema.SchemaRegistry.INITIAL_SCHEMA_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Schema manager test.
 */
public class SchemaManagerTest {
    /**
     * Check registration of schema with wrong versions.
     */
    @Test
    public void testWrongSchemaVersionRegistration() {
        final SchemaDescriptor schemaV0 = new SchemaDescriptor(INITIAL_SCHEMA_VERSION,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        final SchemaDescriptor schemaV1 = new SchemaDescriptor(0,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        final SchemaDescriptor schemaV2 = new SchemaDescriptor(2,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        final SchemaRegistry reg = new SchemaRegistry();

        assertEquals(INITIAL_SCHEMA_VERSION, reg.lastSchemaVersion());
        assertThrows(SchemaRegistryException.class, () -> reg.schema());

        // Try to register schema with initial version.
        assertThrows(SchemaRegistryException.class, () -> reg.registerSchema(schemaV0));
        assertEquals(INITIAL_SCHEMA_VERSION, reg.lastSchemaVersion());

        assertThrows(SchemaRegistryException.class, () -> reg.schema());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(INITIAL_SCHEMA_VERSION));

        // Try to register schema with version of 0-zero.
        assertThrows(SchemaRegistryException.class, () -> reg.registerSchema(schemaV1));
        assertEquals(INITIAL_SCHEMA_VERSION, reg.lastSchemaVersion());

        assertThrows(SchemaRegistryException.class, () -> reg.schema(INITIAL_SCHEMA_VERSION));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(0));

        // Try to register schema with version of 2.
        assertThrows(SchemaRegistryException.class, () -> reg.registerSchema(schemaV2));
        assertEquals(INITIAL_SCHEMA_VERSION, reg.lastSchemaVersion());

        assertThrows(SchemaRegistryException.class, () -> reg.schema(INITIAL_SCHEMA_VERSION));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(0));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(1));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(2));
    }

    /**
     * Check initial schema registration.
     */
    @Test
    public void testSchemaRegistration() {
        final SchemaDescriptor schemaV1 = new SchemaDescriptor(1,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        final SchemaDescriptor schemaV2 = new SchemaDescriptor(2,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaDescriptor schemaV4 = new SchemaDescriptor(4,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaRegistry reg = new SchemaRegistry();

        assertEquals(INITIAL_SCHEMA_VERSION, reg.lastSchemaVersion());
        assertThrows(SchemaRegistryException.class, () -> reg.schema());

        // Register schema with very first version.
        reg.registerSchema(schemaV1);

        assertEquals(1, reg.lastSchemaVersion());
        assertSameSchema(schemaV1, reg.schema());
        assertSameSchema(schemaV1, reg.schema(1));

        // Register schema with next version.
        reg.registerSchema(schemaV2);

        assertEquals(2, reg.lastSchemaVersion());
        assertSameSchema(schemaV2, reg.schema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));

        // Try to register schema with version of 4.
        assertThrows(SchemaRegistryException.class, () -> reg.registerSchema(schemaV4));

        assertEquals(2, reg.lastSchemaVersion());
        assertSameSchema(schemaV2, reg.schema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(3));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(4));
    }

    /**
     * Check schema registration.
     */
    @Test
    public void testDuplucateSchemaRegistration() {
        final SchemaDescriptor schemaV1 = new SchemaDescriptor(1,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        final SchemaDescriptor wrongSchema = new SchemaDescriptor(1,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaDescriptor schemaV2 = new SchemaDescriptor(2,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaRegistry reg = new SchemaRegistry();

        assertEquals(INITIAL_SCHEMA_VERSION, reg.lastSchemaVersion());

        // Register schema with very first version.
        reg.registerSchema(schemaV1);

        assertEquals(1, reg.lastSchemaVersion());
        assertSameSchema(schemaV1, reg.schema());
        assertSameSchema(schemaV1, reg.schema(1));

        // Try to register same schema once again.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.registerSchema(schemaV1));

        assertEquals(1, reg.lastSchemaVersion());
        assertSameSchema(schemaV1, reg.schema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(2));

        // Try to register another schema with same version and check nothing was registered.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.registerSchema(wrongSchema));

        assertEquals(1, reg.lastSchemaVersion());
        assertEquals(1, reg.schema().version());

        assertSameSchema(schemaV1, reg.schema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(2));

        // Register schema with next version.
        reg.registerSchema(schemaV2);

        assertEquals(2, reg.lastSchemaVersion());

        assertSameSchema(schemaV2, reg.schema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
    }

    /**
     * Check schema cleanup.
     */
    @Test
    public void testSchemaCleanup() {
        final SchemaDescriptor schemaV1 = new SchemaDescriptor(1,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        final SchemaDescriptor schemaV2 = new SchemaDescriptor(2,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaDescriptor schemaV3 = new SchemaDescriptor(3,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valStringCol", STRING, true)
            });

        final SchemaDescriptor schemaV4 = new SchemaDescriptor(4,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaRegistry reg = new SchemaRegistry();

        assertEquals(INITIAL_SCHEMA_VERSION, reg.lastSchemaVersion());

        // Fail to cleanup initial schema
        assertThrows(SchemaRegistryException.class, () -> reg.cleanupBefore(INITIAL_SCHEMA_VERSION));
        assertThrows(SchemaRegistryException.class, () -> reg.cleanupBefore(1));

        // Register schema with very first version.
        reg.registerSchema(schemaV1);

        assertEquals(1, reg.lastSchemaVersion());
        assertNotNull(reg.schema());
        assertNotNull(reg.schema(1));

        // Remove non-existed schemas.
        reg.cleanupBefore(1);

        assertEquals(1, reg.lastSchemaVersion());
        assertNotNull(reg.schema());
        assertNotNull(reg.schema(1));

        // Register new schema with next version.
        reg.registerSchema(schemaV2);
        reg.registerSchema(schemaV3);

        assertEquals(3, reg.lastSchemaVersion());
        assertNotNull(reg.schema(1));
        assertNotNull(reg.schema(2));
        assertNotNull(reg.schema(3));

        // Remove outdated schema 1.
        reg.cleanupBefore(2);

        assertEquals(3, reg.lastSchemaVersion());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(1));
        assertNotNull(reg.schema(2));
        assertNotNull(reg.schema(3));

        // Remove non-existed schemas.
        reg.cleanupBefore(2);

        assertEquals(3, reg.lastSchemaVersion());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(1));
        assertNotNull(reg.schema(2));
        assertNotNull(reg.schema(3));

        // Register new schema with next version.
        reg.registerSchema(schemaV4);

        assertEquals(4, reg.lastSchemaVersion());
        assertNotNull(reg.schema(2));
        assertNotNull(reg.schema(3));
        assertNotNull(reg.schema(4));

        // Remove non-existed schemas.
        reg.cleanupBefore(2);

        assertEquals(4, reg.lastSchemaVersion());
        assertSameSchema(schemaV4, reg.schema());
        assertSameSchema(schemaV2, reg.schema(2));
        assertSameSchema(schemaV3, reg.schema(3));
        assertSameSchema(schemaV4, reg.schema(4));

        // Multiple remove.
        reg.cleanupBefore(4);

        assertEquals(4, reg.lastSchemaVersion());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(1));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(2));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(3));
        assertSameSchema(schemaV4, reg.schema());
        assertSameSchema(schemaV4, reg.schema(4));

        // Once again.
        reg.cleanupBefore(4);

        assertEquals(4, reg.lastSchemaVersion());
        assertSameSchema(schemaV4, reg.schema(4));
    }

    /**
     * Check schema registration with full history.
     */
    @Test
    public void testInitialSchemaWithFullHistory() {
        final SchemaDescriptor schemaV1 = new SchemaDescriptor(1,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        final SchemaDescriptor schemaV2 = new SchemaDescriptor(2,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaDescriptor schemaV3 = new SchemaDescriptor(3,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valStringCol", STRING, true)
            });

        final SchemaDescriptor schemaV4 = new SchemaDescriptor(4,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaRegistry reg = new SchemaRegistry(List.of(schemaV1, schemaV2));

        assertEquals(2, reg.lastSchemaVersion());
        assertSameSchema(schemaV2, reg.schema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));

        // Register schema with duplicate version.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.registerSchema(schemaV1));

        assertEquals(2, reg.lastSchemaVersion());
        assertSameSchema(schemaV2, reg.schema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(3));

        // Register schema with out-of-order version.
        assertThrows(SchemaRegistryException.class, () -> reg.registerSchema(schemaV4));

        assertEquals(2, reg.lastSchemaVersion());
        assertSameSchema(schemaV2, reg.schema());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(3));

        // Register schema with next version.
        reg.registerSchema(schemaV3);

        assertEquals(3, reg.lastSchemaVersion());
        assertSameSchema(schemaV3, reg.schema());
        assertSameSchema(schemaV1, reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
        assertSameSchema(schemaV3, reg.schema(3));
    }

    /**
     * Check schema registration with history tail.
     */
    @Test
    public void testInitialSchemaWithTailHistory() {
        final SchemaDescriptor schemaV1 = new SchemaDescriptor(1,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        final SchemaDescriptor schemaV2 = new SchemaDescriptor(2,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaDescriptor schemaV3 = new SchemaDescriptor(3,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {new Column("valStringCol", STRING, true)});

        final SchemaDescriptor schemaV4 = new SchemaDescriptor(4,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaDescriptor schemaV5 = new SchemaDescriptor(5,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {new Column("valStringCol", STRING, true)});

        final SchemaRegistry reg = new SchemaRegistry(List.of(schemaV2, schemaV3));

        assertEquals(3, reg.lastSchemaVersion());
        assertSameSchema(schemaV3, reg.schema());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
        assertSameSchema(schemaV3, reg.schema(3));

        // Register schema with duplicate version.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.registerSchema(schemaV2));

        assertEquals(3, reg.lastSchemaVersion());
        assertSameSchema(schemaV3, reg.schema());

        // Register schema with out-of-order version.
        assertThrows(SchemaRegistryException.class, () -> reg.registerSchema(schemaV5));

        assertEquals(3, reg.lastSchemaVersion());
        assertSameSchema(schemaV3, reg.schema());

        // Register schema with outdated version.
        assertThrows(SchemaRegistrationConflictException.class, () -> reg.registerSchema(schemaV1));

        assertEquals(3, reg.lastSchemaVersion());
        assertSameSchema(schemaV3, reg.schema());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
        assertSameSchema(schemaV3, reg.schema(3));

        // Register schema with next version.
        reg.registerSchema(schemaV4);

        assertEquals(4, reg.lastSchemaVersion());
        assertSameSchema(schemaV4, reg.schema());
        assertSameSchema(schemaV2, reg.schema(2));
        assertSameSchema(schemaV3, reg.schema(3));
        assertSameSchema(schemaV4, reg.schema(4));
    }

    /**
     * Check schema cleanup.
     */
    @Test
    public void testSchemaWithHistoryCleanup() {
        final SchemaDescriptor schemaV2 = new SchemaDescriptor(2,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaDescriptor schemaV3 = new SchemaDescriptor(3,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valStringCol", STRING, true)
            });

        final SchemaDescriptor schemaV4 = new SchemaDescriptor(4,
            new Column[] {new Column("keyLongCol", LONG, true)},
            new Column[] {
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true)
            });

        final SchemaRegistry reg = new SchemaRegistry(List.of(schemaV2, schemaV3, schemaV4));

        assertEquals(4, reg.lastSchemaVersion());
        assertSameSchema(schemaV4, reg.schema());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(1));
        assertSameSchema(schemaV2, reg.schema(2));
        assertSameSchema(schemaV3, reg.schema(3));
        assertSameSchema(schemaV4, reg.schema(4));

        reg.cleanupBefore(1);
        assertEquals(4, reg.lastSchemaVersion());
        assertNotNull(reg.schema(2));
        assertNotNull(reg.schema(3));
        assertNotNull(reg.schema(4));

        reg.cleanupBefore(2);
        assertEquals(4, reg.lastSchemaVersion());
        assertNotNull(reg.schema(2));
        assertNotNull(reg.schema(3));
        assertNotNull(reg.schema(4));

        reg.cleanupBefore(4);

        assertEquals(4, reg.lastSchemaVersion());
        assertThrows(SchemaRegistryException.class, () -> reg.schema(2));
        assertThrows(SchemaRegistryException.class, () -> reg.schema(3));
        assertNotNull(reg.schema(4));
    }

    /**
     * Validate schemas are equals.
     *
     * @param schemaDesc1 Schema descriptor to compare with.
     * @param schemaDesc2 Schema descriptor to compare.
     */
    private void assertSameSchema(SchemaDescriptor schemaDesc1, SchemaDescriptor schemaDesc2) {
        assertEquals(schemaDesc1.version(), schemaDesc2.version(), "Descriptors of different versions.");

        assertTrue(TableSchemaManagerImpl.equalSchemas(schemaDesc1, schemaDesc2), "Schemas are not equals.");
    }
}
