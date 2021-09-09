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

package org.apache.ignite.internal.schema.builder;

import java.util.List;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.PrimaryKey;
import org.apache.ignite.schema.definition.builder.PrimaryKeyBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Primary key builder test.
 */
public class PrimaryKeyBuilderTest {
    /** Test primary key parameters. */
    @Test
    public void testPrimaryKeyWithAffinityColumns() {
        PrimaryKeyBuilder builder = SchemaBuilders.primaryKey();

        builder.withColumns("A", "B", "C").withAffinityColumns("B").build();

        PrimaryKey idx = builder.build();

        assertEquals(3, idx.columns().size());
        assertEquals(1, idx.affinityColumns().size());

        assertTrue(idx.columns().containsAll(List.of("A", "B", "C")));
        assertTrue(idx.affinityColumns().contains("B"));

        assertFalse(idx.affinityColumns().contains("A"));
        assertFalse(idx.affinityColumns().contains("C"));
    }

    /** Test primary key parameters. */
    @Test
    public void testPrimaryKey() {
        PrimaryKeyBuilder builder = SchemaBuilders.primaryKey();

        builder.withColumns("A", "B", "C").build();

        PrimaryKey idx = builder.build();

        assertEquals(3, idx.columns().size());
        assertEquals(3, idx.affinityColumns().size());

        assertTrue(idx.columns().containsAll(List.of("A", "B", "C")));
        assertTrue(idx.affinityColumns().containsAll(List.of("A", "B", "C")));
    }

    /** Test primary key parameters. */
    @Test
    public void testPrimaryKeyWrongAffinityColumn() {
        PrimaryKeyBuilder builder = SchemaBuilders.primaryKey()
                                        .withColumns("A", "B")
                                        .withAffinityColumns("C");

        assertThrows(IllegalStateException.class, builder::build);
    }
}
