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

package org.apache.ignite.internal.schema.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.internal.schema.testutils.builder.PrimaryKeyDefinitionBuilder;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.PrimaryKeyDefinition;
import org.junit.jupiter.api.Test;

/**
 * Primary key builder test.
 */
public class PrimaryKeyDefinitionDefinitionBuilderTest {
    /** Test primary key parameters. */
    @Test
    public void testPrimaryKeyWithColocationColumns() {
        PrimaryKeyDefinitionBuilder builder = SchemaBuilders.primaryKey();

        builder.withColumns("A", "B", "C").withColocationColumns("B").build();

        PrimaryKeyDefinition idx = builder.build();

        assertEquals(3, idx.columns().size());
        assertEquals(1, idx.colocationColumns().size());

        assertTrue(idx.columns().containsAll(List.of("A", "B", "C")));
        assertTrue(idx.colocationColumns().contains("B"));

        assertFalse(idx.colocationColumns().contains("A"));
        assertFalse(idx.colocationColumns().contains("C"));
    }

    /** Test primary key parameters. */
    @Test
    public void testPrimaryKey() {
        PrimaryKeyDefinitionBuilder builder = SchemaBuilders.primaryKey();

        builder.withColumns("A", "B", "C").build();

        PrimaryKeyDefinition idx = builder.build();

        assertEquals(3, idx.columns().size());
        assertEquals(3, idx.colocationColumns().size());

        assertTrue(idx.columns().containsAll(List.of("A", "B", "C")));
        assertTrue(idx.colocationColumns().containsAll(List.of("A", "B", "C")));
    }

    /** Test primary key parameters. */
    @Test
    public void testPrimaryKeyWrongColocationColumn() {
        PrimaryKeyDefinitionBuilder builder = SchemaBuilders.primaryKey()
                .withColumns("A", "B")
                .withColocationColumns("C");

        assertThrows(IllegalStateException.class, builder::build);
    }
}
