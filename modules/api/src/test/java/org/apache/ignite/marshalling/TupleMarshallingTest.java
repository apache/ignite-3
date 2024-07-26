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

package org.apache.ignite.marshalling;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TupleMarshallingTest {
    static Stream<Arguments> oneFieldTuple() {
        return Stream.of(
                Tuple.create().set("col", 1)
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("oneFieldTuple")
    void serDeOneFieldTuple(Tuple tuple) {
        byte[] marshalled = TupleMarshalling.marshal(tuple);
        assertEquals(tuple, TupleMarshalling.unmarshal(marshalled));
    }
}
