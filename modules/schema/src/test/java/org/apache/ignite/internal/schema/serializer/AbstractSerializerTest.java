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

package org.apache.ignite.internal.schema.serializer;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.schema.AbstractSchemaSerializer;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SchemaDescriptor (de)serializer test.
 */
public class AbstractSerializerTest {
    /**
     * (de)Serialize schema test.
     */
    @Test
    public void test() {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        SchemaDescriptor desc = new SchemaDescriptor( 100500,
            new Column[] {
                new Column("A", NativeTypes.INT8, false),
                new Column("B", NativeTypes.INT16, false),
                new Column("C", NativeTypes.INT32, false),
                new Column("D", NativeTypes.INT64, false),
                new Column("E", NativeTypes.UUID, false),
                new Column("F", NativeTypes.FLOAT, false),
                new Column("G", NativeTypes.DOUBLE, false),
                new Column("H", NativeTypes.DATE, false),
            },
            new Column[] {
                new Column("A1", NativeTypes.stringOf(128), false),
                new Column("B1", NativeTypes.numberOf(255), false),
                new Column("C1", NativeTypes.decimalOf(128, 64), false),
                new Column("D1", NativeTypes.bitmaskOf(256), false),
                new Column("E1", NativeTypes.datetime(8), false),
                new Column("F1", NativeTypes.time(8), false),
                new Column("G1", NativeTypes.timestamp(8), true)
            }
        );

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        assertEquals(desc.version(), deserialize.version());

        assertArrayEquals(desc.keyColumns().columns(), deserialize.keyColumns().columns());
        assertArrayEquals(desc.valueColumns().columns(), deserialize.valueColumns().columns());
        assertArrayEquals(desc.affinityColumns(), deserialize.affinityColumns());
    }
}
