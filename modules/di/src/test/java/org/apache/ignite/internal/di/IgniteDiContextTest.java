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

package org.apache.ignite.internal.di;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link IgniteDiContext}.
 */
public class IgniteDiContextTest {
    @Test
    void buildsRunningContext() {
        ApplicationContext ctx = IgniteDiContext.builder().build();

        try {
            assertTrue(ctx.isRunning());
        } finally {
            ctx.close();
        }
    }

    @Test
    void seedSingletonsAreInjectable() {
        String seedValue = "test-node";

        ApplicationContext ctx = IgniteDiContext.builder()
                .withSingleton(seedValue)
                .build();

        try {
            String retrieved = ctx.getBean(String.class);
            assertThat(retrieved, is(sameInstance(seedValue)));
        } finally {
            ctx.close();
        }
    }

    @Test
    void seedSingletonsOfDifferentTypesCoexist() {
        SeedA seedA = new SeedA("a");
        SeedB seedB = new SeedB(42);

        ApplicationContext ctx = IgniteDiContext.builder()
                .withSingleton(seedA)
                .withSingleton(seedB)
                .build();

        try {
            assertThat(ctx.getBean(SeedA.class), is(sameInstance(seedA)));
            assertThat(ctx.getBean(SeedB.class), is(sameInstance(seedB)));
        } finally {
            ctx.close();
        }
    }

    @Test
    void twoContextsCoexistIndependently() {
        SeedA seedForCtx1 = new SeedA("ctx1");
        SeedA seedForCtx2 = new SeedA("ctx2");

        ApplicationContext ctx1 = IgniteDiContext.builder()
                .withSingleton(seedForCtx1)
                .build();

        ApplicationContext ctx2 = IgniteDiContext.builder()
                .withSingleton(seedForCtx2)
                .build();

        try {
            assertThat(ctx1.getBean(SeedA.class).value, is("ctx1"));
            assertThat(ctx2.getBean(SeedA.class).value, is("ctx2"));

            assertTrue(ctx1.isRunning());
            assertTrue(ctx2.isRunning());
        } finally {
            ctx1.close();
            ctx2.close();
        }
    }

    @Test
    void contextDiscoversBeanDefinitions() {
        ApplicationContext ctx = IgniteDiContext.builder().build();

        try {
            // TestComponentFactory (from test classpath) should be discovered.
            assertThat(
                    ctx.getBean(TestComponentFactory.TestPhase1Component.class),
                    is(notNullValue())
            );
        } finally {
            ctx.close();
        }
    }

    @Test
    void closedContextStopsCleanly() {
        ApplicationContext ctx = IgniteDiContext.builder().build();

        assertTrue(ctx.isRunning());
        assertDoesNotThrow(ctx::close);
        assertFalse(ctx.isRunning());
    }

    /** Simple seed type for testing. */
    private static class SeedA {
        final String value;

        SeedA(String value) {
            this.value = value;
        }
    }

    /** Another seed type for testing. */
    private static class SeedB {
        final int value;

        SeedB(int value) {
            this.value = value;
        }
    }
}
