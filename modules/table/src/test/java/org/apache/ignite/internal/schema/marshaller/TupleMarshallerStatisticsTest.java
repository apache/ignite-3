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

package org.apache.ignite.internal.schema.marshaller;

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.sizeInBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl.TuplePart;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl.ValuesWithStatistics;
import org.apache.ignite.internal.table.KeyValueTestUtils;
import org.apache.ignite.internal.table.impl.TestTupleBuilder;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests binary tuple size estimation in {@link TupleMarshallerImpl}.
 */
public class TupleMarshallerStatisticsTest {
    private static final int PRECISION = 10;

    private static final int HUGE_DECIMAL_SCALE = Short.MAX_VALUE * 10;

    /** The test checks that when calculating the tuple size, the passed decimal value is compacted. */
    @ParameterizedTest(name = "scale = {0}")
    @ValueSource(ints = {3, 1024, CatalogUtils.MAX_DECIMAL_SCALE - PRECISION})
    public void testDecimalSizeEstimation(int columnScale) {
        SchemaDescriptor schema = new SchemaDescriptor(1,
                new Column[]{new Column("KEY", NativeTypes.decimalOf(PRECISION + columnScale, columnScale), false)},
                new Column[]{new Column("UNUSED", NativeTypes.INT32, true)});

        TupleMarshallerImpl marshaller = KeyValueTestUtils.createMarshaller(schema);

        BigDecimal hugeScaledDecimal = new BigDecimal(1, new MathContext(PRECISION))
                .setScale(HUGE_DECIMAL_SCALE, RoundingMode.HALF_UP);
        Tuple tuple = new TestTupleBuilder().set("KEY", hugeScaledDecimal);

        assertThat(sizeInBytes(hugeScaledDecimal), is(greaterThan(16384)));

        ValuesWithStatistics statistics = new ValuesWithStatistics(1);
        marshaller.gatherStatistics(TuplePart.KEY, tuple, statistics);
        assertThat(statistics.estimatedValueSize(), is(3));
        BigDecimal compactedValue = (BigDecimal) statistics.value("KEY");
        assertNotNull(compactedValue);
        assertThat(sizeInBytes(compactedValue), is(3));

        BinaryTupleBuilder builder = new BinaryTupleBuilder(1, statistics.estimatedValueSize());
        builder.appendDecimal(compactedValue, columnScale);
        assertThat(builder.build().capacity(), is(statistics.estimatedValueSize() + BinaryTupleCommon.HEADER_SIZE + 1));
    }
}
