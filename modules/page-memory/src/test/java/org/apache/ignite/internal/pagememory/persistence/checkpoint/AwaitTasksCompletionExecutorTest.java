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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.Executor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * For {@link AwaitTasksCompletionExecutor} testing.
 */
public class AwaitTasksCompletionExecutorTest extends BaseIgniteAbstractTest {
    @Test
    void testNoTasks() throws Exception {
        Runnable updateHeartbeat = mock(Runnable.class);

        Executor executor = mock(Executor.class);

        AwaitTasksCompletionExecutor awaitTasksCompletionExecutor = new AwaitTasksCompletionExecutor(executor, updateHeartbeat);

        runAsync(awaitTasksCompletionExecutor::awaitPendingTasksFinished).get(1, SECONDS);

        verify(updateHeartbeat, never()).run();
        verify(executor, never()).execute(any(Runnable.class));
    }

    @Test
    void testSimple() throws Exception {
        Runnable updateHeartbeat = mock(Runnable.class);

        Executor executor = spy(new Executor() {
            /** {@inheritDoc} */
            @Override
            public void execute(Runnable command) {
                command.run();
            }
        });

        AwaitTasksCompletionExecutor awaitTasksCompletionExecutor = new AwaitTasksCompletionExecutor(executor, updateHeartbeat);

        awaitTasksCompletionExecutor.execute(() -> {});
        awaitTasksCompletionExecutor.execute(() -> {});

        runAsync(awaitTasksCompletionExecutor::awaitPendingTasksFinished).get(1, SECONDS);

        verify(updateHeartbeat, times(2)).run();
        verify(executor, times(2)).execute(any(Runnable.class));
    }
}
