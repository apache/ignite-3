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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.IDENTITY_ROW_CONVERTER;
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
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.api.AsyncResultSetImpl.SqlRowImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRowImpl;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.table.KeyValueTestUtils;
import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.NativeTypes.StructTypeBuilder;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.AbstractImmutableTupleTest;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link SqlRow} tuple implementation.
 */
public class SqlRowTest extends AbstractImmutableTupleTest {
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

    @Override
    protected Tuple createTuple(Function<Tuple, Tuple> transformer) {
        Tuple tuple = Tuple.create().set("ID", 1L);

        tuple = transformer.apply(tuple);

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schema);

        return TableRow.tuple(marshaller.marshal(tuple));
    }

    @Override
    protected Tuple createNullValueTuple(ColumnType valueType) {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("ID", INT32, false)},
                new Column[]{new Column("VAL", SchemaTestUtils.specToType(valueType), true)}
        );

        return createSqlRow(schema, Tuple.create().set("ID", 1).set("VAL", null));
    }

    @Test
    @Override
    public void testTupleEquality() {
        Assumptions.abort("SqlRow doesn't implement equals.");
    }

    @Override
    public void testTupleColumnsEquality(ColumnType type) {
        Assumptions.abort("SqlRow doesn't implement equals.");
    }

    @Test
    @Override
    public void testSerialization() {
        Assumptions.abort("SqlRow is not serializable.");
    }

    @Override
    protected Tuple getTuple() {
        Tuple tuple = Tuple.create();

        tuple = addColumnsForDefaultSchema(tuple);

        return createSqlRow(schema, tuple);
    }

    @Override
    protected Tuple getTupleWithColumnOfAllTypes() {
        Tuple tuple = Tuple.create().set("keyUuidCol", UUID_VALUE);

        tuple = addColumnOfAllTypes(tuple);

        return createSqlRow(fullSchema, tuple);
    }

    @Override
    protected Tuple createTupleOfSingleColumn(ColumnType type, String columnName, Object value) {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column(columnName.toUpperCase(), NativeTypes.fromObject(value), false)},
                new Column[]{}
        );

        Tuple tuple = Tuple.create().set(columnName, value);

        return createSqlRow(schema, tuple);
    }

    private static Tuple createSqlRow(SchemaDescriptor descriptor, Tuple valuesTuple) {
        SqlRowHandler handler = SqlRowHandler.INSTANCE;
        StructTypeBuilder structTypeBuilder = NativeTypes.structBuilder();
        List<ColumnMetadata> columnsMeta = new ArrayList<>(valuesTuple.columnCount());
        BinaryTupleBuilder binaryTupleBuilder = new BinaryTupleBuilder(valuesTuple.columnCount());
        BinaryTupleSchema binaryTupleSchema = BinaryTupleSchema.createRowSchema(descriptor);

        for (int i = 0; i < valuesTuple.columnCount(); i++) {
            Column column = descriptor.columns().get(i);
            Object value = valuesTuple.value(IgniteNameUtils.quoteIfNeeded(column.name()));

            binaryTupleSchema.appendValue(binaryTupleBuilder, i, value);
            structTypeBuilder.addField(column.name(), column.type(), column.nullable());
            columnsMeta.add(new ColumnMetadataImpl(column.name(), column.type().spec(), -1, -1, column.nullable(), null));
        }

        BinaryTupleReader binaryTuple = new BinaryTupleReader(valuesTuple.columnCount(), binaryTupleBuilder.build());

        RowFactory<RowWrapper> factory = handler.create(structTypeBuilder.build());
        RowWrapper binaryTupleRow = factory.create(binaryTuple);

        InternalSqlRowImpl<RowWrapper> internalSqlRow =
                new InternalSqlRowImpl<>(binaryTupleRow, SqlRowHandler.INSTANCE, IDENTITY_ROW_CONVERTER);

        return new SqlRowImpl(internalSqlRow, new ResultSetMetadataImpl(columnsMeta));
    }
}
