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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/** Tests for the {@link BinaryTupleSchema} class. */
public class BinaryTupleSchemaTest {

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(1, new Column[]{
            new Column("id".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
    }, new Column[]{
            new Column("val".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
    });

    @Test
    public void rowSchema() {
        BinaryTupleSchema schema = BinaryTupleSchema.createRowSchema(SCHEMA);
        assertEquals(0, schema.columnIndex(0));
        assertEquals(1, schema.columnIndex(1));
        assertTrue(schema.convertible());
    }

    @Test
    public void keySchema() {
        BinaryTupleSchema schema = BinaryTupleSchema.createKeySchema(SCHEMA);
        assertEquals(0, schema.columnIndex(0));
        assertTrue(schema.convertible());
    }

    @Test
    public void valueSchema() {
        BinaryTupleSchema schema = BinaryTupleSchema.createValueSchema(SCHEMA);
        assertEquals(1, schema.columnIndex(0));
        assertFalse(schema.convertible());
    }
}
