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

package org.apache.ignite.internal.cli.core.repl.registry.impl;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

class LazyObjectRefTest {

    @Test
    void returnsOk() {
        // Given not throwing supplier
        LazyObjectRef<String> ref = new LazyObjectRef<>(() -> "Hi");

        // Expect returns value from supplier
        await().untilAsserted(
                () -> assertThat(ref.get(), equalTo("Hi"))
        );
    }

    @Test
    void returnsOkLazy() {
        // Given supplier that returns a value only on a second call
        CountDownLatch latch = new CountDownLatch(1);
        LazyObjectRef<String> ref = new LazyObjectRef<>(() -> {
            if (latch.getCount() == 1L) {
                latch.countDown();
                throw new RuntimeException();
            } else {
                return "Hi from the second attempt";
            }
        });

        // When
        await().untilAsserted(
                () -> assertThat(ref.get(), equalTo("Hi from the second attempt"))
        );
        // And the latch is counted down
        assertThat(latch.getCount(), is(0L));
    }
}
