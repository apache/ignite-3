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

package org.apache.ignite.internal.client.sql;

import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.DATE;
import static org.apache.ignite.internal.type.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.type.NativeTypes.FLOAT;
import static org.apache.ignite.internal.type.NativeTypes.INT16;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.datetime;
import static org.apache.ignite.internal.type.NativeTypes.time;
import static org.apache.ignite.internal.type.NativeTypes.timestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.client.handler.requests.table.ClientTableCommon;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.table.AbstractImmutableTupleTest;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link ClientSqlRow} tuple implementation.
 */
public class ClientSqlRowTest extends AbstractImmutableTupleTest {
    /** Schema descriptor for default test tuple. */
    private final SchemaDescriptor schema = new SchemaDescriptor(
            42,
            List.of(
                    new Column("ID", INT64, false),
                    new Column("SIMPLENAME", STRING, true),
                    new Column("QuotedName", STRING, true),
                    new Column("NOVALUE", STRING, true)
            ),
            List.of("ID"),
            null
    );

    /** Schema descriptor for tuple with columns of all the supported types. */
    private final SchemaDescriptor fullSchema = new SchemaDescriptor(42,
            List.of(
                    new Column("valBoolCol".toUpperCase(), BOOLEAN, true),
                    new Column("valByteCol".toUpperCase(), INT8, true),
                    new Column("valShortCol".toUpperCase(), INT16, true),
                    new Column("valIntCol".toUpperCase(), INT32, true),
                    new Column("valLongCol".toUpperCase(), INT64, true),
                    new Column("valFloatCol".toUpperCase(), FLOAT, true),
                    new Column("valDoubleCol".toUpperCase(), DOUBLE, true),
                    new Column("valDateCol".toUpperCase(), DATE, true),
                    new Column("keyUuidCol".toUpperCase(), NativeTypes.UUID, false),
                    new Column("valUuidCol".toUpperCase(), NativeTypes.UUID, false),
                    new Column("valTimeCol".toUpperCase(), time(TIME_PRECISION), true),
                    new Column("valDateTimeCol".toUpperCase(), datetime(TIMESTAMP_PRECISION), true),
                    new Column("valTimeStampCol".toUpperCase(), timestamp(TIMESTAMP_PRECISION), true),
                    new Column("valBytesCol".toUpperCase(), BYTES, false),
                    new Column("valStringCol".toUpperCase(), STRING, false),
                    new Column("valDecimalCol".toUpperCase(), NativeTypes.decimalOf(25, 5), false)
            ),
            List.of("keyUuidCol".toUpperCase()),
            null
    );

    @Test
    @Override
    public void testSerialization() {
        Assumptions.abort(ClientSqlRow.class.getSimpleName() + " is not serializable.");
    }

    @Override
    protected Tuple createTuple(Function<Tuple, Tuple> transformer) {
        Tuple tuple = Tuple.create().set("ID", 1L);

        tuple = transformer.apply(tuple);

        return createClientSqlRow(schema, tuple);
    }

    @Override
    protected Tuple createNullValueTuple(ColumnType valueType) {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("ID", INT32, false)},
                new Column[]{new Column("VAL", SchemaTestUtils.specToType(valueType), true)}
        );

        BinaryRow binaryRow = SchemaTestUtils.binaryRow(schema, 1, null);
        BinaryTupleReader binaryTuple = new BinaryTupleReader(2, binaryRow.tupleSlice());

        ResultSetMetadata resultSetMetadata = new ResultSetMetadataImpl(List.of(
                new ColumnMetadataImpl("ID", ColumnType.INT32, 0, 0, false, null),
                new ColumnMetadataImpl("VAL", valueType, 0, 0, true, null)
        ));

        return new ClientSqlRow(binaryTuple, resultSetMetadata);
    }

    @Override
    protected Tuple getTuple() {
        Tuple tuple = Tuple.create();

        tuple = addColumnsForDefaultSchema(tuple);

        return createClientSqlRow(schema, tuple);
    }

    @Override
    protected Tuple getTupleWithColumnOfAllTypes() {
        Tuple tuple = Tuple.create().set("keyUuidCol", UUID_VALUE);

        tuple = addColumnOfAllTypes(tuple);

        return createClientSqlRow(fullSchema, tuple);
    }

    @Override
    protected Tuple createTupleOfSingleColumn(ColumnType type, String columnName, Object value) {
        NativeType nativeType = NativeTypes.fromObject(value);
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column(columnName.toUpperCase(), nativeType, false)},
                new Column[]{}
        );

        Tuple tuple = Tuple.create().set(columnName, value);

        return createClientSqlRow(schema, tuple);
    }

    private static Tuple createClientSqlRow(SchemaDescriptor schema, Tuple valuesTuple) {
        List<ColumnMetadata> columnsMeta = new ArrayList<>(valuesTuple.columnCount());
        BinaryTupleBuilder binaryTupleBuilder = new BinaryTupleBuilder(valuesTuple.columnCount());
        BinaryTupleSchema binaryTupleSchema = BinaryTupleSchema.createRowSchema(schema);

        for (int i = 0; i < valuesTuple.columnCount(); i++) {
            Column field = schema.columns().get(i);
            Object value = valuesTuple.value(IgniteNameUtils.quoteIfNeeded(field.name()));

            binaryTupleSchema.appendValue(binaryTupleBuilder, i, value);

            int precision = ClientTableCommon.getPrecision(field.type());
            int scale = ClientTableCommon.getDecimalScale(field.type());

            columnsMeta.add(new ColumnMetadataImpl(field.name(), field.type().spec(), precision, scale, field.nullable(), null));
        }

        BinaryTupleReader binaryTuple = new BinaryTupleReader(valuesTuple.columnCount(), binaryTupleBuilder.build());

        return new ClientSqlRow(binaryTuple, new ResultSetMetadataImpl(columnsMeta));
    }
}
