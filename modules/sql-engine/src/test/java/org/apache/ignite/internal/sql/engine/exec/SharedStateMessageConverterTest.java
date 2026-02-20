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

package org.apache.ignite.internal.sql.engine.exec;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.sql.engine.message.SharedStateMessage;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for class {@link SharedStateMessageConverter}.
 */
public class SharedStateMessageConverterTest extends BaseIgniteAbstractTest {
    private Random rnd;

    /**
     * Initialization.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        log.info("Using seed: " + seed + "L;");

        rnd = new Random(seed);
    }

    @ParameterizedTest
    @MethodSource("allTypes")
    void singleValue(NativeType type) {
        Object value = SchemaTestUtils.generateRandomValue(rnd, type);

        SharedState state = new SharedState();
        state.correlatedVariable(1, TypeUtils.toInternal(value, type.spec()));

        SharedState converted = doConversions(state);

        assertThat(converted.correlations(), equalTo(state.correlations()));
    }

    @Test
    void multipleValuesWithAllTypes() {
        SharedState state = new SharedState();

        Set<ColumnType> simpleColumnTypes = EnumSet.complementOf(EnumSet.of(ColumnType.STRUCT, ColumnType.NULL));

        int fieldIdx = 0;

        for (ColumnType typeSpec : simpleColumnTypes) {
            Object value;

            if (typeSpec == ColumnType.PERIOD) {
                value = Period.between(LocalDate.of(1, 1, 1), LocalDate.now());
            } else if (typeSpec == ColumnType.DURATION) {
                value = Duration.ofMillis(Instant.now().toEpochMilli());
            } else {
                value = SchemaTestUtils.generateRandomValue(rnd, SchemaTestUtils.specToType(typeSpec));
            }

            state.correlatedVariable(fieldIdx++, TypeUtils.toInternal(value, typeSpec));
        }

        SharedState converted = doConversions(state);

        assertThat(converted.correlations(), equalTo(state.correlations()));
    }

    @Test
    void nullValue() {
        SharedState state = new SharedState();
        state.correlatedVariable(1, null);

        SharedState converted = doConversions(state);

        assertThat(converted, is(notNullValue()));

        assertThat(converted.correlations(), equalTo(state.correlations()));
    }

    @Test
    void nullState() {
        assertThat(doConversions(null), is(Matchers.nullValue()));
    }

    @Test
    void emptyState() {
        SharedState state = new SharedState();

        SharedState converted = doConversions(state);

        assertThat(converted, is(notNullValue()));

        assertThat(converted.correlations(), is(state.correlations()));
    }

    private static @Nullable SharedState doConversions(@Nullable SharedState state) {
        SharedStateMessage msg = SharedStateMessageConverter.toMessage(state);

        return SharedStateMessageConverter.fromMessage(msg);
    }

    private static List<NativeType> allTypes() {
        return SchemaTestUtils.ALL_TYPES;
    }
}
