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

package org.apache.ignite.internal.schema.assembler;

import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.schema.AbstractSchemaAssembler;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaAssemblerImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AbstractAssemblerTest {
    @Test
    public void test() {
        AbstractSchemaAssembler assembler = SchemaAssemblerImpl.INSTANCE;

        SchemaDescriptor desc = new SchemaDescriptor(UUID.randomUUID(), 100500,
            new Column[] {
                new Column("A", NativeTypes.INT8, false),
                new Column("B", NativeTypes.UUID, false),
                new Column("C", NativeTypes.decimalOf(10,20), false),
            },
            new Column[] {
                new Column("D", NativeTypes.INT8, false),
                new Column("E", NativeTypes.UUID, false),
                new Column("FG", NativeTypes.numberOf(10), true),
            }
        );

        byte[] serialize = assembler.serialize(desc);
        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        assertEquals(desc.version(), deserialize.version());
        assertEquals(desc.tableId(), deserialize.tableId());

        assertArrayEquals(desc.keyColumns().columns(), deserialize.keyColumns().columns());
        assertArrayEquals(desc.valueColumns().columns(), deserialize.valueColumns().columns());
        assertArrayEquals(desc.affinityColumns(), deserialize.affinityColumns());

        assertEquals(desc, desc);
    }
}
