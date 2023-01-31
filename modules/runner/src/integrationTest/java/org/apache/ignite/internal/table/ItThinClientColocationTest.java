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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.table.ItPublicApiColocationTest.generateValueByType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.ignite.client.handler.requests.table.ClientTableCommon;
import org.apache.ignite.internal.client.table.ClientColumn;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.client.table.ClientTupleSerializer;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that client and server have matching colocation logic.
 */
public class ItThinClientColocationTest {
    @ParameterizedTest(name = "type=" + ARGUMENTS_PLACEHOLDER)
    @MethodSource("nativeTypes")
    public void test(NativeType type)
            throws TupleMarshallerException {
        var columnName = "col1";
        var colocationColumns = new String[]{columnName};
        var column = new Column(columnName, type, false);
        var columns = new Column[]{column};
        var schema = new SchemaDescriptor(1, columns, colocationColumns, new Column[0]);
        var marsh = new TupleMarshallerImpl(new DummySchemaManagerImpl(schema));

        var clientColumn = new ClientColumn(
                columnName,
                ClientTableCommon.getClientDataType(type.spec()),
                false,
                true,
                true,
                0,
                ClientTableCommon.getDecimalScale(type),
                ClientTableCommon.getPrecision(type));

        ClientSchema clientSchema = new ClientSchema(0, new ClientColumn[]{clientColumn});

        for (int i = 0; i < 10; i++) {
            var val = generateValueByType(i, type.spec());
            assertNotNull(val);

            var tuple = Tuple.create().set(columnName, val);
            var clientHash = ClientTupleSerializer.getColocationHash(clientSchema, tuple);

            var serverRow = marsh.marshal(tuple);
            var serverHash = serverRow.colocationHash();

            assertEquals(serverHash, clientHash);
        }
    }

    private static Stream<Arguments> nativeTypes() {
        // TODO: Use loop to populate precision.
        var nativeTypes = new NativeType[]{
                NativeTypes.INT8,
                NativeTypes.INT16,
                NativeTypes.INT32,
                NativeTypes.INT64,
                NativeTypes.FLOAT,
                NativeTypes.DOUBLE,
                NativeTypes.STRING,
                NativeTypes.BYTES,
                NativeTypes.UUID,
                NativeTypes.numberOf(0),
                NativeTypes.numberOf(1),
                NativeTypes.numberOf(2),
                NativeTypes.numberOf(3),
                NativeTypes.bitmaskOf(8),
                NativeTypes.DATE,
                NativeTypes.time(0),
                NativeTypes.time(1),
                NativeTypes.time(2),
                NativeTypes.time(3),
                NativeTypes.time(4),
                NativeTypes.time(5),
                NativeTypes.time(6),
                NativeTypes.time(7),
                NativeTypes.time(8),
                NativeTypes.time(9),
                NativeTypes.datetime(0),
                NativeTypes.datetime(1),
                NativeTypes.datetime(2),
                NativeTypes.datetime(3),
                NativeTypes.datetime(4),
                NativeTypes.datetime(5),
                NativeTypes.datetime(6),
                NativeTypes.datetime(7),
                NativeTypes.datetime(8),
                NativeTypes.datetime(9),
                NativeTypes.timestamp(0),
                NativeTypes.timestamp(1),
                NativeTypes.timestamp(2),
                NativeTypes.timestamp(3),
                NativeTypes.timestamp(4),
                NativeTypes.timestamp(5),
                NativeTypes.timestamp(6),
                NativeTypes.timestamp(7),
                NativeTypes.timestamp(8),
                NativeTypes.timestamp(9),
        };

        return Arrays.stream(nativeTypes).map(Arguments::of);
    }
}
