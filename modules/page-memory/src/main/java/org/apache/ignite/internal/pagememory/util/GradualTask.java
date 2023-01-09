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

package org.apache.ignite.internal.pagememory.util;

/**
 * A possibly long-running task that is separated to steps each of which does not take too much time to execute.
 * Such tasks are to be executed by a {@link GradualTaskExecutor} so that, even if there are more tasks running
 * at the same time than there are threads in the underlying thread pool, all tasks will have progress due to their
 * cooperative execution model (execution of their steps will be interleaved).
 *
 * <p>The task is stateful, it must track its progress itself.
 *
 * <p>The task might be considered a <a href="https://en.wikipedia.org/wiki/Continuation">continuation</a>.
 *
 * @see GradualTaskExecutor
 */
public interface GradualTask {
    /**
     * Returns an already completed task.
     */
    static GradualTask completed() {
        return CompletedGradualTask.INSTANCE;
    }

    /**
     * Runs next task step. This should not take too long to let other gradual tasks in the same executor pass forward.
     *
     * @throws Exception If something goes wrong.
     */
    void runStep() throws Exception;

    /**
     * Returns {@code true} if the task is completed (so no steps should be run), or {@code false} if there are more
     * steps to execute.
     */
    boolean isCompleted();

    /**
     * Executes some cleanup that must be done after the task finishes on any reason (all work done or exception thrown);
     * it is analogous to {@code finally} block in Java.
     */
    default void cleanup() {
        // No-op.
    }
}
