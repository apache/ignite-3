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

import java.util.Collections;
import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.builder.HashIndexBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for hash index builder.
 */
public class HashIndexBuilderTest {
    /**
     * Build index and check its parameters.
     */
    @Test
    public void testBuild() {
        HashIndexBuilder builder = SchemaBuilders.hashIndex("testHI")
            .withColumns("A", "B", "C")
            .withHints(Collections.singletonMap("param","value"));
        HashIndex idx = builder.build();

        assertEquals("testHI", idx.name());
        assertEquals(3, idx.columns().size());
    }

    /**
     * Try to create index without columns and check error.
     */
    @Test
    public void testBuildNoColumns() {
        HashIndexBuilder builder = SchemaBuilders.hashIndex("testHI");

        assertThrows(AssertionError.class, builder::build);
    }
}
