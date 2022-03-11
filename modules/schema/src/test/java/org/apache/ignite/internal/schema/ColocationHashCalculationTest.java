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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.util.HashCalculator;
import org.junit.jupiter.api.Test;

/**
 * Check calculation hash by colocation columns specified at the schema.
 */
public class ColocationHashCalculationTest {

    @Test
    public void allTypes() {
        Column[] keyCols = SchemaTestUtils.ALL_TYPES.stream()
                .map(t -> new Column("id_" + t.spec().name(), t, false))
                .toArray(Column[]::new);



        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols,
                new Column[]{new Column("val", INT32, true).copy(keyCols.length)});

        RowAssembler rasm = new RowAssembler(schema, 1, 0);

        rasm.appendByte((byte) 1);
        rasm.appendInt(2);
        rasm.appendString("key_" + 3);
        rasm.appendInt(0);

        Row r = new Row(schema, rasm.build());

        HashCalculator hashCalc = new HashCalculator();
        hashCalc.appendByte((byte) 1);
        hashCalc.appendInt(2);
        hashCalc.appendString("key_" + 3);

        assertEquals(hashCalc.hash(), colocationHash(r));
    }

    @Test
    public void simple() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{
                        new Column("id0", INT8, false).copy(0),
                        new Column("id1", INT32, false).copy(1),
                        new Column("id2", STRING, false).copy(2),
                },
                new Column[]{new Column("val", INT32, true).copy(3)});

        RowAssembler rasm = new RowAssembler(schema, 1, 0);

        rasm.appendByte((byte) 1);
        rasm.appendInt(2);
        rasm.appendString("key_" + 3);
        rasm.appendInt(0);

        Row r = new Row(schema, rasm.build());

        HashCalculator hashCalc = new HashCalculator();
        hashCalc.appendByte((byte) 1);
        hashCalc.appendInt(2);
        hashCalc.appendString("key_" + 3);

        assertEquals(hashCalc.hash(), colocationHash(r));
    }

    private int colocationHash(Row r) {
        HashCalculator hashCalc = new HashCalculator();
        for (Column c : r.schema().colocationColumns()) {
            hashCalc.append(r.value(c.schemaIndex()));
        }

        return hashCalc.hash();
    }
}
