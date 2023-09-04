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

    private final long seed = ThreadLocalRandom.current().nextLong();

    private final Random rnd = new Random(seed);

    @BeforeEach
    void printSeed() {
        log.info("Using seed: " + seed);
    }

    @Test
    public void testBytebufferSerialization() {
        List<ColumnType> columnTypes = columnTypes();
        Object[] sourceData = values(columnTypes);
        RowSchema schema = rowSchema(columnTypes, sourceData);

        int elementsCount = schema.fields().size();

        RowFactory<RowWrapper> factory = handler.factory(schema);
        RowWrapper src = factory.create(sourceData);

        // Serialization to binary tuple representation.
        ByteBuffer buf = handler.toByteBuffer(src);

        BinaryTuple tuple = new BinaryTuple(elementsCount, buf);
        RowWrapper destWrap = factory.wrap(tuple);
        RowWrapper dest = factory.create(buf);

        for (int i = 0; i < elementsCount; i++) {
            String msg = schema.fields().get(i).toString();

            assertThat(msg, handler.get(i, src), equalTo(sourceData[i]));
            assertThat(msg, schema.value(tuple, i), equalTo(sourceData[i]));

            // Binary tuple wrapper must return data in internal format.
            Object expected = TypeUtils.toInternal(sourceData[i]);

            assertThat(msg, handler.get(i, dest), equalTo(expected));
            assertThat(msg, handler.get(i, destWrap), equalTo(expected));
        }
    }

    @ParameterizedTest(name = "{0} - {1}")
    @MethodSource("concatTestArguments")
    public void testConcat(boolean leftTupleRequired, boolean rightTupleRequired) {
        ConcatTestParameters params = new ConcatTestParameters(leftTupleRequired, rightTupleRequired);

        RowWrapper concatenated = handler.concat(params.left, params.right);

        int leftLen = params.leftData.length;
        int rightLen = params.rightData.length;
        int totalElementsCount = leftLen + rightLen;

        assertThat(handler.columnCount(concatenated), equalTo(totalElementsCount));

        // Build combined schema.
        Builder builder = RowSchema.builder();
        params.leftSchema.fields().forEach(builder::addField);
        params.rightSchema.fields().forEach(builder::addField);

        RowSchema concatenatedSchema = builder.build();

        // Serialize.
        ByteBuffer buf = handler.toByteBuffer(concatenated);

        // Wrap into row.
        RowWrapper result = handler.factory(concatenatedSchema).wrap(new BinaryTuple(totalElementsCount, buf));

        for (int i = 0; i < leftLen; i++) {
            assertThat(handler.get(i, result), equalTo(TypeUtils.toInternal(params.leftData[i])));
        }

        for (int i = 0; i < rightLen; i++) {
            assertThat(handler.get(leftLen + i, result), equalTo(TypeUtils.toInternal(params.rightData[i])));
        }
    }

    private static Stream<Arguments> concatTestArguments() {
        return Stream.of(
                Arguments.of(Named.of("array", false), Named.of("array", false)),
                Arguments.of(Named.of("array", false), Named.of("tuple", true)),
                Arguments.of(Named.of("tuple", true), Named.of("array", false)),
                Arguments.of(Named.of("tuple", true), Named.of("tuple", true))
        );
    }

    private RowSchema rowSchema(List<ColumnType> columnTypes, Object[] values) {
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

    private Object[] values(List<ColumnType> columnTypes) {
        Object[] values = new Object[columnTypes.size()];
        int baseValue = rnd.nextInt();

        for (int i = 0; i < values.length; i++) {
            ColumnType type = columnTypes.get(i);

            values[i] = type == ColumnType.NULL ? null : generateValueByType(baseValue, type);
        }

        return values;
    }

    private List<ColumnType> columnTypes() {
        List<ColumnType> columnTypes = new ArrayList<>(EnumSet.complementOf(EnumSet.of(ColumnType.PERIOD, ColumnType.DURATION)));

        Collections.shuffle(columnTypes, rnd);

        return columnTypes;
    }

    private final class ConcatTestParameters {
        final RowSchema leftSchema;
        final RowSchema rightSchema;
        final Object[] leftData;
        final Object[] rightData;
        final RowWrapper left;
        final RowWrapper right;

        ConcatTestParameters(boolean leftTupleRequired, boolean rightTupleRequired) {
            List<ColumnType> columnTypes1 = columnTypes();
            List<ColumnType> columnTypes2 = columnTypes();

            leftData = values(columnTypes1);
            rightData = values(columnTypes2);
            leftSchema = rowSchema(columnTypes1, leftData);
            rightSchema = rowSchema(columnTypes2, rightData);

            RowFactory<RowWrapper> factory1 = handler.factory(leftSchema);
            RowFactory<RowWrapper> factory2 = handler.factory(rightSchema);

            RowWrapper left = factory1.create(leftData);
            RowWrapper right = factory2.create(rightData);

            this.left = leftTupleRequired ? factory1.create(handler.toByteBuffer(left)) : left;
            this.right = rightTupleRequired ? factory2.create(handler.toByteBuffer(right)) : right;
        }
    }
}
