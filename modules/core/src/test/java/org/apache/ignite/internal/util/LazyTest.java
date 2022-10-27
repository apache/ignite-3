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

package org.apache.ignite.internal.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Tests to validate Lazy value.
 */
public class LazyTest {
    /** Basic test to validate value is computed only once. */
    @Test
    public void test() {
        List<Object> values = Arrays.asList(new Object[]{null, 2, "3"});

        for (Object value : values) {
            AtomicInteger invocationCounter = new AtomicInteger();

            Lazy<Object> lazy = new Lazy<>(() -> {
                invocationCounter.incrementAndGet();

                return value;
            });

            // call get several times to validate value is computed only once
            assertThat(lazy.get(), equalTo(value));
            assertThat(lazy.get(), equalTo(value));
            assertThat(lazy.get(), equalTo(value));

            assertThat(invocationCounter.get(), equalTo(1));
        }
    }

}