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

package org.apache.ignite.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * Tests server tuple builder implementation.
 */
public class TupleImplTest extends AbstractImmutableTupleTest {
    @Override
    protected Tuple createTuple(Function<Tuple, Tuple> transformer) {
        return transformer.apply(new TupleImpl());
    }

    @Test
    void testTupleFactoryMethods() {
        assertEquals(Tuple.create(), Tuple.create(10));
        assertEquals(Tuple.create().set("id", 42L), Tuple.create(10).set("id", 42L));

        assertEquals(getTuple(), Tuple.copy(getTuple()));
        assertEquals(getTupleWithColumnOfAllTypes(), Tuple.copy(getTupleWithColumnOfAllTypes()));

        assertEquals(Tuple.create().set("id", 42L).set("NAME", "universe"),
                Tuple.create(Map.of("ID", 42L, "name", "universe")));

        assertEquals(Tuple.create().set("ID", 42L).set("\"name\"", "universe"),
                Tuple.create(Map.of("\"ID\"", 42L, "\"name\"", "universe")));

        assertEquals(Tuple.create().set("id", 42L).set("NAME", "universe"),
                Tuple.copy(Tuple.create().set("ID", 42L).set("name", "universe")));

        assertEquals(Tuple.create().set("id", 42L).set("\"name\"", "universe"),
                Tuple.copy(Tuple.create().set("\"ID\"", 42L).set("\"name\"", "universe")));
    }
}
