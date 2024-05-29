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

package org.apache.ignite.internal.schema.marshaller.reflection;

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.sizeInBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Marshaller object statistics test.
 */
public class ObjectStatisticsTest {
    private static final int PRECISION = 10;

    private static final int HUGE_DECIMAL_SCALE = Short.MAX_VALUE * 10;

    /** The test checks that when calculating the row size, the passed decimal value is compacted. */
    @ParameterizedTest(name = "scale = {0}")
    @ValueSource(ints = {3, 1024, CatalogUtils.MAX_DECIMAL_SCALE - PRECISION})
    void testDecimalSizeEstimation(int columnScale) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(1,
                new Column[]{new Column("KEY", NativeTypes.decimalOf(PRECISION, columnScale), false)},
                new Column[]{new Column("UNUSED", NativeTypes.INT32, true)});

        MarshallersProvider marshallers = new ReflectionMarshallersProvider();
        Marshaller marshaller = marshallers.getKeysMarshaller(schema.marshallerSchema(), Mapper.of(BigDecimal.class), true, false);

        BigDecimal hugeScaledDecimal = new BigDecimal(1, new MathContext(PRECISION))
                .setScale(HUGE_DECIMAL_SCALE, RoundingMode.HALF_UP);

        assertThat(sizeInBytes(hugeScaledDecimal), is(greaterThan(16384)));

        ObjectStatistics statistics = ObjectStatistics.collectObjectStats(schema, schema.keyColumns(), marshaller, hugeScaledDecimal);
        assertThat(statistics.estimatedValueSize(), is(3));
        assertThat(sizeInBytes((BigDecimal) statistics.value(0)), is(3));

        BinaryTupleBuilder builder = new BinaryTupleBuilder(1, statistics.estimatedValueSize());
        builder.appendDecimal(statistics.value(0), columnScale);
        assertThat(builder.build().capacity(), is(statistics.estimatedValueSize() + BinaryTupleCommon.HEADER_SIZE + 1));
    }
}
