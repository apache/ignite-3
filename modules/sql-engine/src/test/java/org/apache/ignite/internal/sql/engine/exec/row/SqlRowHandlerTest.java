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

package org.apache.ignite.internal.sql.engine.exec.row;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.generateValueByType;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.columnType2NativeType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema.Builder;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link SqlRowHandler}.
 */
public class SqlRowHandlerTest extends IgniteAbstractTest {
    private static final RowHandler<RowWrapper> handler = SqlRowHandler.INSTANCE;

    private static final long seed = ThreadLocalRandom.current().nextLong();

    private static final Random rnd = new Random(seed);

    private static final List<ColumnType> columnTypes =
            new ArrayList<>(EnumSet.complementOf(EnumSet.of(ColumnType.PERIOD, ColumnType.DURATION)));

    @BeforeEach
    void init() {
        log.info("Using seed: " + seed);

        Collections.shuffle(columnTypes, rnd);
    }

    @Test
    public void testMarshal() {
        Object[] sourceData = values();
        RowSchema schema = rowSchema(sourceData);

        int elementsCount = schema.fields().size();

        RowFactory<RowWrapper> factory = handler.factory(schema);

        RowWrapper src = factory.create(sourceData);

        for (int i = 0; i < elementsCount; i++) {
            assertThat(handler.get(i, src), equalTo(sourceData[i]));
        }

        ByteBuffer buf = handler.toByteBuffer(src);

        BinaryTuple tuple = new BinaryTuple(elementsCount, buf);
        RowWrapper dest = factory.create(buf);
        RowWrapper wrap = factory.wrap(tuple);

        for (int i = 0; i < elementsCount; i++) {
            TypeSpec type = schema.fields().get(i);

            Object expected = TypeUtils.toInternal(sourceData[i]);

            assertThat(type.toString(), schema.value(i, tuple), equalTo(expected));
            assertThat(type.toString(), handler.get(i, dest), equalTo(expected));
            assertThat(type.toString(), handler.get(i, wrap), equalTo(expected));
        }
    }

    @ParameterizedTest(name = "{4} - {5}")
    @MethodSource("concatTestParameters")
    public void testConcat(RowSchema schema1, RowSchema schema2, Object[] data1, Object[] data2, RowWrapper left, RowWrapper right) {
        RowWrapper concatenated = handler.concat(left, right);

        int expElementsCount = schema1.fields().size() + schema2.fields().size();

        assertThat(handler.columnCount(concatenated), equalTo(expElementsCount));

        Builder builder = RowSchema.builder();
        schema1.fields().forEach(builder::addField);
        schema2.fields().forEach(builder::addField);

        RowSchema concatenatedSchema = builder.build();

        ByteBuffer buf = handler.toByteBuffer(concatenated);

        concatenated = handler.factory(concatenatedSchema).create(buf);

        for (int i = 0; i < data1.length; i++) {
            assertThat(handler.get(i, concatenated), equalTo(TypeUtils.toInternal(data1[i])));
        }

        int offset = data1.length;

        for (int i = 0; i < data2.length; i++) {
            assertThat(handler.get(offset + i, concatenated), equalTo(TypeUtils.toInternal(data2[i])));
        }
    }

    private static Stream<Arguments> concatTestParameters() {
        Object[] data1 = values();
        Object[] data2 = values();

        RowSchema schema1 = rowSchema(data1);
        RowSchema schema2 = rowSchema(data2);

        RowFactory<RowWrapper> factory1 = handler.factory(schema1);
        RowFactory<RowWrapper> factory2 = handler.factory(schema2);

        RowWrapper left = factory1.create(data1);
        RowWrapper right = factory2.create(data2);

        RowWrapper leftTuple = factory1.create(factory2.handler().toByteBuffer(left));
        RowWrapper rightTuple = factory2.create(factory2.handler().toByteBuffer(right));

        return Stream.of(
                Arguments.of(schema1, schema2, data1, data2, Named.of("array", left), Named.of("array", right)),
                Arguments.of(schema1, schema2, data1, data2, Named.of("array", left), Named.of("tuple", rightTuple)),
                Arguments.of(schema1, schema2, data1, data2, Named.of("tuple", leftTuple), Named.of("array", right)),
                Arguments.of(schema1, schema2, data1, data2, Named.of("tuple", leftTuple), Named.of("tuple", rightTuple))
        );
    }

    private static RowSchema rowSchema(Object[] values) {
        Builder schemaBuilder = RowSchema.builder();

        for (int i = 0; i < values.length; i++) {
            ColumnType type = columnTypes.get(i);

            if (type == ColumnType.NULL) {
                schemaBuilder.addField(new NullTypeSpec());

                continue;
            }

            NativeType nativeType = values[i] == null
                    ? columnType2NativeType(type, 9, 3, 20)
                    : NativeTypes.fromObject(values[i]);

            schemaBuilder.addField(nativeType, values[i] == null || rnd.nextBoolean());
        }

        return schemaBuilder.build();
    }

    private static Object[] values() {
        Object[] values = new Object[columnTypes.size()];
        int baseValue = rnd.nextInt();

        for (int i = 0; i < values.length; i++) {
            ColumnType type = columnTypes.get(i);

            values[i] = type == ColumnType.NULL || rnd.nextInt(4) == 1 ? null : generateValueByType(baseValue, type);
        }

        return values;
    }
}
