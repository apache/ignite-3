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

package org.apache.ignite.client.handler.requests.table;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBitSet;
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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.table.MutableRowTupleAdapter;
import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClientHandlerTuple}.
 */
public class ClientHandlerTupleTests {
    private static final int NANOS_IN_SECOND = 9;
    private static final int TIMESTAMP_PRECISION = 6;
    private static final int TIME_PRECISION = 0;

    /** Schema descriptor. */
    private final SchemaDescriptor schema = new SchemaDescriptor(
            42,
            new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
            new Column[]{new Column("name".toUpperCase(), NativeTypes.STRING, false)}
    );

    /** Schema descriptor. */
    private final SchemaDescriptor fullSchema = new SchemaDescriptor(42,
            new Column[]{new Column("keyUuidCol".toUpperCase(), NativeTypes.UUID, false)},
            new Column[]{
                    new Column("valByteCol".toUpperCase(), INT8, true),
                    new Column("valShortCol".toUpperCase(), INT16, true),
                    new Column("valIntCol".toUpperCase(), INT32, true),
                    new Column("valLongCol".toUpperCase(), INT64, true),
                    new Column("valFloatCol".toUpperCase(), FLOAT, true),
                    new Column("valDoubleCol".toUpperCase(), DOUBLE, true),
                    new Column("valDateCol".toUpperCase(), DATE, true),
                    new Column("valTimeCol".toUpperCase(), time(TIME_PRECISION), true),
                    new Column("valDateTimeCol".toUpperCase(), datetime(TIMESTAMP_PRECISION), true),
                    new Column("valTimeStampCol".toUpperCase(), timestamp(TIMESTAMP_PRECISION), true),
                    new Column("valBitmask1Col".toUpperCase(), NativeTypes.bitmaskOf(22), true),
                    new Column("valBytesCol".toUpperCase(), BYTES, false),
                    new Column("valStringCol".toUpperCase(), STRING, false),
                    new Column("valNumberCol".toUpperCase(), NativeTypes.numberOf(20), false),
                    new Column("valDecimalCol".toUpperCase(), NativeTypes.decimalOf(25, 5), false),
            }
    );


    @Test
    public void testVariousColumnTypes() throws TupleMarshallerException {
        Random rnd = new Random();

        TupleMarshaller marshaller = new TupleMarshallerImpl(fullSchema);

        Tuple tuple = Tuple.create()
                .set("valByteCol", (byte) 1)
                .set("valShortCol", (short) 2)
                .set("valIntCol", 3)
                .set("valLongCol", 4L)
                .set("valFloatCol", 0.055f)
                .set("valDoubleCol", 0.066d)
                .set("keyUuidCol", UUID.randomUUID())
                .set("valDateCol", LocalDate.now())
                .set("valDateTimeCol", truncatedLocalDateTimeNow())
                .set("valTimeCol", truncatedLocalTimeNow())
                .set("valTimeStampCol", truncatedInstantNow())
                .set("valBitmask1Col", randomBitSet(rnd, 12))
                .set("valBytesCol", IgniteTestUtils.randomBytes(rnd, 13))
                .set("valStringCol", IgniteTestUtils.randomString(rnd, 14))
                .set("valNumberCol", BigInteger.valueOf(rnd.nextLong()))
                .set("valDecimalCol", BigDecimal.valueOf(rnd.nextLong(), 5));

        Tuple rowTuple = TableRow.tuple(marshaller.marshal(tuple));

        assertEquals(tuple, rowTuple);

        rowTuple.set("foo", "bar"); // Force row to tuple conversion.
        tuple.set("foo", "bar"); // Force row to tuple conversion.

        assertEquals(tuple, rowTuple);
        TableTestUtils.createTable();
    }

    private Instant truncatedInstantNow() {
        return truncateToDefaultPrecision(Instant.now());
    }

    private LocalTime truncatedLocalTimeNow() {
        return truncateToDefaultPrecision(LocalTime.now());
    }

    private LocalDateTime truncatedLocalDateTimeNow() {
        return truncateToDefaultPrecision(LocalDateTime.now());
    }
}
