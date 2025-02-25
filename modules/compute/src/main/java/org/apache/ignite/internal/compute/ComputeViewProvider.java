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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;

/** Provider that creates system view exposes information about currently running compute tasks hosted by a node. */
public class ComputeViewProvider {
    private final DelegatingPublisher publisher = new DelegatingPublisher();

    /** Initializes with compute aware manager. */
    public void init(ExecutionManager executionManager) {
        publisher.executionManager = executionManager;
    }

    public void stop() {
        publisher.executionManager = null;
    }

    /** Returns system view exposes information about currently running tasks. */
    public SystemView<?> get() {
        NativeType idType = stringOf(36);

        NativeType timestampType = NativeTypes.timestamp(NativeTypes.MAX_TIME_PRECISION);

        return SystemViews.<JobState>nodeViewBuilder()
                .name("COMPUTE_TASKS")
                .nodeNameColumnAlias("COORDINATOR_NODE_ID")
                .<String>addColumn("COMPUTE_TASK_ID", idType, info -> info.id().toString())
                .<String>addColumn("COMPUTE_TASK_STATUS", stringOf(10), info -> info.status().name())
                .<Instant>addColumn("COMPUTE_TASK_CREATE_TIME", timestampType, JobState::createTime)
                .<Instant>addColumn("COMPUTE_TASK_START_TIME", timestampType, JobState::startTime)
                .<Instant>addColumn("COMPUTE_TASK_FINISH_TIME", timestampType, JobState::finishTime)
                // TODO https://issues.apache.org/jira/browse/IGNITE-24589: Next columns are deprecated and should be removed.
                //  They are kept for compatibility with 3.0 version, to allow columns being found by their old names.
                .<String>addColumn("ID", idType, info -> info.id().toString())
                .<String>addColumn("STATUS", stringOf(10), info -> info.status().name())
                .<Instant>addColumn("CREATE_TIME", timestampType, JobState::createTime)
                .<Instant>addColumn("START_TIME", timestampType, JobState::startTime)
                .<Instant>addColumn("FINISH_TIME", timestampType, JobState::finishTime)
                // End of legacy columns list. New columns must be added below this line.
                .dataProvider(publisher)
                .build();
    }

    private static class DelegatingPublisher implements Publisher<JobState> {
        @Nullable private volatile ExecutionManager executionManager;

        @Override
        public void subscribe(Subscriber<? super JobState> subscriber) {
            ExecutionManager execManager = executionManager;

            Publisher<JobState> jobStatePublisher = execManager != null
                    ? SubscriptionUtils.fromIterable(execManager.localStatesAsync())
                    : SubscriptionUtils.fromIterable(List.of());

            jobStatePublisher.subscribe(subscriber);
        }
    }
}
