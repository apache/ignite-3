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

package org.apache.ignite.internal.sql.engine.property;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Test to verify {@link SqlPropertiesImpl}.
 */
class SqlPropertiesImplTest {

    @SuppressWarnings("WeakerAccess")
    private static class TestProps {
        public static Property<Integer> PROP = new Property<>("int_prop", Integer.class);
        public static Property<Integer> ANOTHER_PROP = new Property<>("another_int_prop", Integer.class);
    }

    /** Simple test covering basic api of the {@link SqlProperties}. */
    @Test
    public void test() {
        SqlProperties holder = SqlPropertiesHelper.newBuilder()
                .set(TestProps.PROP, 42)
                .build();

        assertThat(holder.get(TestProps.PROP), is(42));
        assertThrows(PropertyNotFoundException.class, () -> holder.get(TestProps.ANOTHER_PROP));
        assertThat(holder.getOrDefault(TestProps.ANOTHER_PROP, 52), is(52));
    }

}