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

import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.index.PrimaryIndex;
import org.apache.ignite.schema.definition.index.PrimaryKeyBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Primary key builder test.
 */
public class PrimaryKeyBuilderTest {
    /** Test primary index parameters. */
    @Test
    public void testPrimaryKey() {
        PrimaryKeyBuilder builder = SchemaBuilders.pkIndex();

        builder.withColumns("A", "B", "C").withAffinityColumns("B").build();

        PrimaryIndex idx = builder.build();

        assertEquals(3, idx.columns().size());
        assertEquals(3, idx.affinityColumns().size());
        assertTrue(idx.unique());
        assertEquals("A", idx.columns().get(0).name());
        assertEquals("B", idx.columns().get(1).name());
        assertEquals("C", idx.columns().get(2).name());
        assertEquals("B", idx.affinityColumns().get(0));
    }
}
