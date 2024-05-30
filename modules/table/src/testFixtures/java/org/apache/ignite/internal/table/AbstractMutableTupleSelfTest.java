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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tuple interface test.
 */
public abstract class AbstractMutableTupleSelfTest extends AbstractTupleSelfTest {
    @Test
    public void testValueReturnsOverwrittenValue() {
        Assertions.assertEquals("foo", getTuple().set("SimpleName", "foo").value("SimpleName"));
        assertNull(getTuple().set("SimpleName", null).value("SimpleName"));

        Assertions.assertEquals("foo", getTuple().set("\"QuotedName\"", "foo").value("\"QuotedName\""));
        assertNull(getTuple().set("\"QuotedName\"", null).value("\"QuotedName\""));

    }

    @Test
    public void testValueOrDefaultReturnsOverwrittenValue() {
        Assertions.assertEquals("foo", getTuple().set("SimpleName", "foo").valueOrDefault("SimpleName", "bar"));
        assertNull(getTuple().set("SimpleName", null).valueOrDefault("SimpleName", "foo"));

        Assertions.assertEquals("foo", getTuple().set("\"QuotedName\"", "foo").valueOrDefault("\"QuotedName\"", "bar"));
        assertNull(getTuple().set("\"QuotedName\"", null).valueOrDefault("\"QuotedName\"", "foo"));
    }

    @Test
    public void testColumnCountReturnsActualSchemaSize() {
        Tuple tuple = getTuple();

        assertEquals(4, tuple.columnCount());

        tuple.valueOrDefault("SimpleName", "foo");
        assertEquals(4, tuple.columnCount());

        tuple.valueOrDefault("foo", "bar");
        assertEquals(4, tuple.columnCount());

        tuple.set("foo", "bar");
        assertEquals(5, tuple.columnCount());
    }

    @Test
    public void testMutableTupleEquality() {
        Assertions.assertEquals(createTuple(), createTuple());
        Assertions.assertEquals(createTuple().hashCode(), createTuple().hashCode());

        Assertions.assertEquals(createTuple().set("foo", null), createTuple().set("foo", null));
        Assertions.assertEquals(createTuple().set("foo", null).hashCode(), createTuple().set("foo", null).hashCode());

        Assertions.assertEquals(createTuple().set("foo", "bar"), createTuple().set("foo", "bar"));
        Assertions.assertEquals(createTuple().set("foo", "bar").hashCode(), createTuple().set("foo", "bar").hashCode());

        Assertions.assertNotEquals(createTuple().set("foo", null), createTuple().set("bar", null));
        Assertions.assertNotEquals(createTuple().set("foo", "foo"), createTuple().set("bar", "bar"));

        Assertions.assertEquals(createTuple().set("foo", "bar"), createTuple().set("FOO", "bar"));
        Assertions.assertEquals(createTuple().set("foo", "bar"), createTuple().set("\"FOO\"", "bar"));
        Assertions.assertEquals(createTuple().set("\"foo\"", "bar"), createTuple().set("\"foo\"", "bar"));

        Assertions.assertNotEquals(createTuple().set("foo", "foo"), createTuple().set("\"foo\"", "bar"));
        Assertions.assertNotEquals(createTuple().set("FOO", "foo"), createTuple().set("\"foo\"", "bar"));

        Tuple tuple = createTuple();
        Tuple tuple2 = createTuple();

        assertEquals(tuple, tuple);

        tuple.set("foo", "bar");

        assertEquals(tuple, tuple);
        assertNotEquals(tuple, tuple2);
        assertNotEquals(tuple2, tuple);

        tuple2.set("foo", "baz");

        assertNotEquals(tuple, tuple2);
        assertNotEquals(tuple2, tuple);

        tuple2.set("foo", "bar");

        assertEquals(tuple, tuple2);
        assertEquals(tuple2, tuple);
    }

    @Test
    public void testTupleEqualityDifferentColumnOrder() {
        Random rnd = new Random();

        Tuple tuple = getTuple();

        List<Integer> randomIdx = IntStream.range(0, tuple.columnCount()).boxed().collect(Collectors.toList());

        Collections.shuffle(randomIdx, rnd);

        Tuple shuffledTuple = createTuple(t -> {
            for (Integer i : randomIdx) {
                t.set("\"" + tuple.columnName(i) + "\"", tuple.value(i));
            }
            return t;
        });

        assertEquals(tuple, shuffledTuple);
        assertEquals(shuffledTuple, tuple);
        assertEquals(tuple.hashCode(), shuffledTuple.hashCode());
    }
}
