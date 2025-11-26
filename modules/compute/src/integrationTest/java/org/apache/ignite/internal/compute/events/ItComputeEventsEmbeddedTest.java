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

package org.apache.ignite.internal.compute.events;

import static org.apache.ignite.internal.compute.events.EventMatcher.embeddedJobEvent;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_CANCELED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_EXECUTING;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_QUEUED;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsInRelativeOrder;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.compute.utils.InteractiveJobs;
import org.apache.ignite.internal.compute.utils.InteractiveTasks;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

class ItComputeEventsEmbeddedTest extends ItComputeEventsTest {
    @Override
    protected IgniteCompute compute() {
        return node(0).compute();
    }

    @Override
    protected EventMatcher jobEvent(IgniteEventType eventType, Type jobType, @Nullable UUID jobId, String jobClassName, String targetNode) {
        return embeddedJobEvent(eventType, jobType, jobId, jobClassName, targetNode, node(0).name());
    }

    // Cancel tests are hard to implement without global signals, let's test them in the embedded mode only.
    @Test
    void taskSplitCanceled() {
        CancelHandle cancelHandle = CancelHandle.create();
        TaskExecution<List<String>> execution = startTask(cancelHandle.token());

        cancelHandle.cancel();

        UUID taskId = execution.idAsync().join(); // Safe to join since execution is complete.

        assertEvents(
                taskEvent(COMPUTE_TASK_QUEUED, InteractiveTasks.GlobalApi.name(), taskId),
                taskEvent(COMPUTE_TASK_EXECUTING, InteractiveTasks.GlobalApi.name(), taskId),
                taskEvent(COMPUTE_TASK_CANCELED, InteractiveTasks.GlobalApi.name(), taskId)
        );
    }

    @Test
    void taskReduceCanceled() {
        CancelHandle cancelHandle = CancelHandle.create();
        TaskExecution<List<String>> execution = startTask(cancelHandle.token());
        InteractiveTasks.GlobalApi.finishSplit();
        InteractiveJobs.all().finishReturnWorkerNames();
        InteractiveTasks.GlobalApi.assertReduceAlive();

        cancelHandle.cancel();

        UUID taskId = execution.idAsync().join(); // Safe to join since execution is complete.

        // There are a lot of job events, skip them
        await().until(logInspector::events, containsInRelativeOrder(
                taskEvent(COMPUTE_TASK_QUEUED, InteractiveTasks.GlobalApi.name(), taskId),
                taskEvent(COMPUTE_TASK_EXECUTING, InteractiveTasks.GlobalApi.name(), taskId),
                taskEvent(COMPUTE_TASK_CANCELED, InteractiveTasks.GlobalApi.name(), taskId)
        ));
    }

    private TaskExecution<List<String>> startTask(@Nullable CancellationToken cancellationToken) {
        TaskExecution<List<String>> taskExecution = compute().submitMapReduce(
                TaskDescriptor.<String, List<String>>builder(InteractiveTasks.GlobalApi.name()).build(), null, cancellationToken
        );
        InteractiveTasks.GlobalApi.assertSplitAlive();
        return taskExecution;
    }
}
