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

import java.util.ArrayList;
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
import org.apache.ignite.internal.schema.TemporalNativeType;
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
    @ParameterizedTest()
    @MethodSource("nativeTypes")
    public void testClientAndServerColocationHashesAreSame(NativeType type)
            throws TupleMarshallerException {
        var columnName = "col1";
        var marsh = tupleMarshaller(type, columnName);
        var clientSchema = clientSchema(type, columnName);

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

    private static ClientSchema clientSchema(NativeType type, String columnName) {
        var clientColumn = new ClientColumn(
                columnName,
                ClientTableCommon.getClientDataType(type.spec()),
                false,
                true,
                true,
                0,
                ClientTableCommon.getDecimalScale(type),
                ClientTableCommon.getPrecision(type));

        return new ClientSchema(0, new ClientColumn[]{clientColumn});
    }

    private static TupleMarshallerImpl tupleMarshaller(NativeType type, String columnName) {
        var column = new Column(columnName, type, false);
        var columns = new Column[]{column};
        var colocationColumns = new String[]{columnName};
        var schema = new SchemaDescriptor(1, columns, colocationColumns, new Column[0]);

        return new TupleMarshallerImpl(new DummySchemaManagerImpl(schema));
    }

    private static Stream<Arguments> nativeTypes() {
        var types = new NativeType[]{
                NativeTypes.INT8,
                NativeTypes.INT16,
                NativeTypes.INT32,
                NativeTypes.INT64,
                NativeTypes.FLOAT,
                NativeTypes.DOUBLE,
                NativeTypes.STRING,
                NativeTypes.BYTES,
                NativeTypes.UUID,
                NativeTypes.bitmaskOf(8),
                NativeTypes.DATE,
        };

        var types2 = new ArrayList<NativeType>();

        for (int i = 0; i < TemporalNativeType.MAX_TIME_PRECISION; i++) {
            types2.add(NativeTypes.time(i));
            types2.add(NativeTypes.datetime(i));
            types2.add(NativeTypes.timestamp(i));
            types2.add(NativeTypes.numberOf(i));
            types2.add(NativeTypes.decimalOf(i + 10, i));
        }

        return Stream.concat(Arrays.stream(types), types2.stream()).map(Arguments::of);
    }
}
