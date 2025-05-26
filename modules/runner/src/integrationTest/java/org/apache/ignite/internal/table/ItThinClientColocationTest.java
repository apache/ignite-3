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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.handler.requests.table.ClientTableCommon;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.client.table.ClientColumn;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.client.table.ClientTupleSerializer;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that client and server have matching colocation logic.
 */
public class ItThinClientColocationTest extends ClusterPerClassIntegrationTest {
    private static final ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

    @ParameterizedTest
    @MethodSource("nativeTypes")
    public void testClientAndServerColocationHashesAreSame(NativeType type) {
        var columnName = "COL1";

        TupleMarshaller serverMarshaller = tupleMarshaller(type, columnName);
        ClientSchema clientSchema = clientSchema(type, columnName);

        for (int i = 0; i < 10; i++) {
            Object val = SqlTestUtils.generateValueByType(type);
            Tuple tuple = Tuple.create().set(columnName, val);

            int clientHash = ClientTupleSerializer.getColocationHash(clientSchema, tuple);

            Row serverRow = serverMarshaller.marshal(tuple);
            int serverHash = serverRow.colocationHash();

            assertEquals(serverHash, clientHash);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCustomColocationColumnOrder(boolean reverseColocationOrder) throws Exception {
        String tableName = "testCustomColocationColumnOrder_" + reverseColocationOrder;

        sql("create table " + tableName + "(id integer, id0 bigint, id1 varchar, v INTEGER, "
                + "primary key(id, id0, id1)) colocate by " + (reverseColocationOrder ? "(id1, id0)" : "(id0, id1)"));

        Table serverTable = CLUSTER.aliveNode().tables().table(tableName);
        RecordBinaryViewImpl serverView = Wrappers.unwrap(serverTable.recordView(), RecordBinaryViewImpl.class);
        TupleMarshaller marsh = serverView.marshaller(1);

        try (IgniteClient client = IgniteClient.builder().addresses("localhost").build()) {
            // Perform get to populate schema.
            Table clientTable = client.tables().table(tableName);
            clientTable.recordView().get(null, Tuple.create().set("id", 1).set("id0", 2L).set("id1", "3"));

            Map<Integer, CompletableFuture<ClientSchema>> schemas = IgniteTestUtils.getFieldValue(clientTable, "schemas");

            for (int i = 0; i < 100; i++) {
                Tuple key = Tuple.create().set("id", 1 + i).set("id0", 2L + i).set("id1", Integer.toString(3 + i));
                int serverHash = marsh.marshal(key).colocationHash();
                int clientHash = ClientTupleSerializer.getColocationHash(schemas.values().iterator().next().get(), key);

                assertEquals(serverHash, clientHash);
            }
        }
    }

    private static ClientSchema clientSchema(NativeType type, String columnName) {
        var clientColumn = new ClientColumn(
                columnName,
                type.spec(),
                false,
                0,
                -1,
                -1,
                0,
                ClientTableCommon.getDecimalScale(type),
                ClientTableCommon.getPrecision(type));

        return new ClientSchema(0, new ClientColumn[]{clientColumn}, marshallers);
    }

    private static TupleMarshallerImpl tupleMarshaller(NativeType type, String columnName) {
        var column = new Column(columnName, type, false);
        var columns = List.of(column);
        var colocationColumns = List.of(columnName);
        var schema = new SchemaDescriptor(1, columns, colocationColumns, colocationColumns);

        return new TupleMarshallerImpl(schema);
    }

    private static Stream<Arguments> nativeTypes() {
        var types = new NativeType[]{
                NativeTypes.BOOLEAN,
                NativeTypes.INT8,
                NativeTypes.INT16,
                NativeTypes.INT32,
                NativeTypes.INT64,
                NativeTypes.FLOAT,
                NativeTypes.DOUBLE,
                NativeTypes.stringOf(100),
                NativeTypes.blobOf(100),
                NativeTypes.UUID,
                NativeTypes.DATE,
        };

        var types2 = new ArrayList<NativeType>();

        for (int i = 0; i <= CatalogUtils.MAX_TIME_PRECISION; i++) {
            types2.add(NativeTypes.time(i));
            types2.add(NativeTypes.datetime(i));
            types2.add(NativeTypes.timestamp(i));
            types2.add(NativeTypes.decimalOf(i + 10, i));
        }

        return Stream.concat(Arrays.stream(types), types2.stream()).map(Arguments::of);
    }
}
