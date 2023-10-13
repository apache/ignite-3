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

package org.apache.ignite.internal.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link BinaryTuplePrefix} class.
 */
public class BinaryTuplePrefixTest {

    /**
     * Tests construction of a BinaryTuple prefix.
     */
    @Test
    public void testPrefix() {
        LocalDate date = LocalDate.now();

        ByteBuffer tuple = new BinaryTuplePrefixBuilder(3, 5)
                .appendInt(42)
                .appendString("foobar")
                .appendDate(date)
                .build();

        assertTrue((tuple.get(0) & BinaryTupleCommon.PREFIX_FLAG) != 0);

        var prefix = new BinaryTuplePrefix(5, tuple);

        assertThat(prefix.elementCount(), is(3));
        assertThat(prefix.elementCount(), is(3));

        assertThat(prefix.intValue(0), is(42));
        assertThat(prefix.stringValue(1), is("foobar"));
        assertThat(prefix.dateValue(2), is(date));
        assertThat(prefix.uuidValue(3), is(nullValue()));
        assertThat(prefix.doubleValueBoxed(4), is(nullValue()));
    }

    /**
     * Tests a corner case when a new internal buffer needs to be allocated to add the count value.
     */
    @Test
    public void testInternalBufferReallocation() {
        ByteBuffer tuple = new BinaryTuplePrefixBuilder(1, 1, 4)
                .appendInt(Integer.MAX_VALUE)
                .build();

        var prefix = new BinaryTuplePrefix(1, tuple);

        assertThat(prefix.elementCount(), is(1));
        assertThat(prefix.intValue(0), is(Integer.MAX_VALUE));
    }

    @Test
    public void testCreatePrefixFromBinaryTuple1() {
        int prefixSize = 1;

        ByteBuffer buffer = new BinaryTupleBuilder(prefixSize)
                .appendInt(10)
                .build();

        BinaryTuplePrefix prefix = BinaryTuplePrefix.fromBinaryTuple(4, new BinaryTuple(prefixSize, buffer));

        assertThat(prefix.elementCount(), equalTo(prefixSize));
        assertThat(prefix.intValue(0), equalTo(10));
        assertThat(prefix.hasNullValue(1), equalTo(true));
        assertThat(prefix.hasNullValue(2), equalTo(true));
        assertThat(prefix.hasNullValue(3), equalTo(true));
    }

    @Test
    public void testCreatePrefixFromBinaryTuple2() {
        int prefixSize = 2;

        ByteBuffer buffer = new BinaryTupleBuilder(prefixSize)
                .appendInt(10)
                .appendString("foo")
                .build();

        BinaryTuplePrefix prefix = BinaryTuplePrefix.fromBinaryTuple(4, new BinaryTuple(prefixSize, buffer));

        assertThat(prefix.elementCount(), equalTo(prefixSize));
        assertThat(prefix.intValue(0), equalTo(10));
        assertThat(prefix.stringValue(1), equalTo("foo"));
        assertThat(prefix.hasNullValue(2), equalTo(true));
        assertThat(prefix.hasNullValue(3), equalTo(true));
    }

    @Test
    public void testCreatePrefixFromBinaryTuple3() {
        int prefixSize = 3;

        ByteBuffer buffer = new BinaryTupleBuilder(prefixSize)
                .appendInt(10)
                .appendString("foo")
                .appendNull()
                .build();

        BinaryTuplePrefix prefix = BinaryTuplePrefix.fromBinaryTuple(4, new BinaryTuple(prefixSize, buffer));

        assertThat(prefix.elementCount(), equalTo(prefixSize));
        assertThat(prefix.intValue(0), equalTo(10));
        assertThat(prefix.stringValue(1), equalTo("foo"));
        assertThat(prefix.hasNullValue(2), equalTo(true));
        assertThat(prefix.hasNullValue(3), equalTo(true));
    }

    @Test
    public void testCreatePrefixFromBinaryTuple4() {
        int prefixSize = 4;

        ByteBuffer buffer = new BinaryTupleBuilder(prefixSize)
                .appendInt(10)
                .appendString("foo")
                .appendNull()
                .appendBoolean(false)
                .build();

        BinaryTuplePrefix prefix = BinaryTuplePrefix.fromBinaryTuple(4, new BinaryTuple(prefixSize, buffer));

        assertThat(prefix.elementCount(), equalTo(prefixSize));
        assertThat(prefix.intValue(0), equalTo(10));
        assertThat(prefix.stringValue(1), equalTo("foo"));
        assertThat(prefix.hasNullValue(2), equalTo(true));
        assertThat(prefix.booleanValue(3), equalTo(false));
    }

    @Test
    public void testCreatePrefixFromBinaryTuple5() {
        int prefixSize = 5;

        ByteBuffer buffer = new BinaryTupleBuilder(prefixSize)
                .appendInt(10)
                .appendString("foo")
                .appendNull()
                .appendBoolean(false)
                .appendString("truncated value")
                .build();

        BinaryTuplePrefix prefix = BinaryTuplePrefix.fromBinaryTuple(4, new BinaryTuple(prefixSize, buffer));

        assertThat(prefix.elementCount(), equalTo(4));
        assertThat(prefix.intValue(0), equalTo(10));
        assertThat(prefix.stringValue(1), equalTo("foo"));
        assertThat(prefix.hasNullValue(2), equalTo(true));
        assertThat(prefix.booleanValue(3), equalTo(false));
    }

    /**
     * Tests construction of an invalid prefix.
     */
    @Test
    public void testInvalidPrefix() {
        Exception e = assertThrows(IllegalStateException.class, () -> {
            var builder = new BinaryTuplePrefixBuilder(3, 5);

            builder.appendInt(42).build();
        });

        assertThat(e.getMessage(), is("Unexpected amount of elements in a BinaryTuple prefix. Expected: 3, actual 1"));

        e = assertThrows(IllegalStateException.class, () -> {
            var builder = new BinaryTuplePrefixBuilder(3, 5);

            builder.appendInt(42)
                    .appendInt(42)
                    .appendInt(42)
                    .appendInt(42)
                    .build();
        });

        assertThat(e.getMessage(), is("Unexpected amount of elements in a BinaryTuple prefix. Expected: 3, actual 4"));
    }
}
