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
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Random;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for class {@link SharedState}.
 */
public class SharedStateTest extends BaseIgniteAbstractTest {
    private final SharedState state = new SharedState();

    @ParameterizedTest
    @MethodSource("allTypes")
    void canSetValue(NativeType type) {
        long seed = System.currentTimeMillis();

        log.info("Using seed: " + seed);

        Random rnd = new Random(seed);

        Object value = SchemaTestUtils.generateRandomValue(rnd, type);
        long corrId = rnd.nextLong();

        state.correlatedVariable(corrId, value);

        assertThat(state.correlatedVariable(corrId), is(value));
    }

    @Test
    void canSetNullAsValue() {
        state.correlatedVariable(1, null);

        assertThat(state.correlatedVariable(1), is(Matchers.nullValue()));
    }

    @Test
    void throwsExceptionNonExistingValue() {
        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrows(
                IllegalStateException.class,
                () -> state.correlatedVariable(1),
                "Correlated variable is not set [id=1]"
        );
    }

    private static List<NativeType> allTypes() {
        return SchemaTestUtils.ALL_TYPES;
    }
}
