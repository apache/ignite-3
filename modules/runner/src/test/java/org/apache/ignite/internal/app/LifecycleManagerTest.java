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

package org.apache.ignite.internal.app;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class LifecycleManagerTest extends BaseIgniteAbstractTest {
    private final LifecycleManager lifecycleManager = new LifecycleManager("test");

    private final ComponentContext context = new ComponentContext();

    @Test
    void startExceptionFromComponentIsPropagatedToStartFuture() throws Exception {
        IgniteComponent componentFailingStart = mock(IgniteComponent.class);
        when(componentFailingStart.startAsync(context)).thenReturn(failedFuture(new RuntimeException("Oops")));

        CompletableFuture<Void> startFuture = lifecycleManager.startComponentsAsync(context, componentFailingStart);

        assertThat(startFuture, willThrow(RuntimeException.class, "Oops"));
        assertThat(lifecycleManager.allComponentsStartFuture(ForkJoinPool.commonPool()), willThrow(RuntimeException.class, "Oops"));
    }

    @Test
    void stopExceptionFromComponentIsPropagatedToStopFuture() throws Exception {
        IgniteComponent componentFailingStop = mock(IgniteComponent.class);
        when(componentFailingStop.startAsync(context)).thenReturn(nullCompletedFuture());
        when(componentFailingStop.stopAsync(context)).thenReturn(failedFuture(new RuntimeException("Oops")));

        lifecycleManager.startComponentsAsync(context, componentFailingStop);
        lifecycleManager.onStartComplete();

        CompletableFuture<Void> stopFuture = lifecycleManager.stopNode(context);

        assertThat(stopFuture, willThrow(RuntimeException.class, "Oops"));
    }
}
