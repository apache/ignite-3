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

import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.builder.SchemaTableBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for schema table builder.
 */
public class SchemaTableBuilderTest {
    /**
     * Create schema table and check its parameters.
     */
    @Test
    public void testBuild() {
        SchemaTableBuilder builder = SchemaBuilders.tableBuilder("SNAME","TNAME")
            .columns(
                SchemaBuilders.column("COL1", ColumnType.DOUBLE).build(),
                SchemaBuilders.column("COL2", ColumnType.DOUBLE).build())
            .withPrimaryKey("COL1");

        SchemaTable tbl = builder.build();

        assertEquals("SNAME.TNAME", tbl.canonicalName());
        assertEquals("TNAME", tbl.name());
        assertEquals(1, tbl.keyColumns().size());
    }
}
