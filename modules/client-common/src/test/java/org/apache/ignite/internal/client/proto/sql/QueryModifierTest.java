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

package org.apache.ignite.internal.client.proto.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.internal.client.sql.QueryModifier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link QueryModifier}.
 */
public class QueryModifierTest {
    @ParameterizedTest
    @EnumSource(QueryModifier.class)
    public void testPackingUnpackingAllModifiers(QueryModifier modifier) {
        Set<QueryModifier> result = QueryModifier.unpack(QueryModifier.pack(EnumSet.of(modifier)));

        assertThat(result, hasSize(1));
        assertThat(result.iterator().next(), is(modifier));
    }

    @ParameterizedTest
    @MethodSource("testPackingUnpackingArgs")
    public void testPackingUnpacking(Set<QueryModifier> modifiers) {
        byte resultByte = QueryModifier.pack(modifiers);
        Set<QueryModifier> result = QueryModifier.unpack(resultByte);

        assertEquals(modifiers, result);
    }

    private static Stream<Arguments> testPackingUnpackingArgs() {
        return Stream.of(Arguments.of(EnumSet.noneOf(QueryModifier.class)),
                Arguments.of(Set.of(QueryModifier.ALLOW_ROW_SET_RESULT, QueryModifier.ALLOW_AFFECTED_ROWS_RESULT)),
                Arguments.of(Set.of(QueryModifier.ALLOW_AFFECTED_ROWS_RESULT, QueryModifier.ALLOW_APPLIED_RESULT)),
                Arguments.of(Set.of(QueryModifier.ALLOW_APPLIED_RESULT, QueryModifier.ALLOW_TX_CONTROL)),
                Arguments.of(Set.of(QueryModifier.ALLOW_TX_CONTROL, QueryModifier.ALLOW_MULTISTATEMENT)),
                Arguments.of(Set.of(QueryModifier.ALLOW_MULTISTATEMENT, QueryModifier.ALLOW_ROW_SET_RESULT)),
                Arguments.of(Set.of(QueryModifier.ALLOW_ROW_SET_RESULT, QueryModifier.ALLOW_MULTISTATEMENT,
                        QueryModifier.ALLOW_APPLIED_RESULT)),
                Arguments.of(QueryModifier.ALL));
    }
}
