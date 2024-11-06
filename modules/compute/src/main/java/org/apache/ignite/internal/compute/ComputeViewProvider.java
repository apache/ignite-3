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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;

/** Provider that creates system view exposes information about currently running compute tasks hosted by a node. */
public class ComputeViewProvider {
    private final CompletableFuture<ExecutionManager> startFuture = new CompletableFuture<>();

    /** Initializes with compute aware manager. */
    public void init(ExecutionManager executionManager) {
        startFuture.complete(executionManager);
    }

    /** Returns system view exposes information about currently running tasks. */
    public SystemView<?> get() {
        NativeType idType = stringOf(36);

        NativeType timestampType = NativeTypes.timestamp(NativeTypes.MAX_TIME_PRECISION);

        return SystemViews.<JobState>nodeViewBuilder()
                .name("COMPUTE_TASKS")
                .nodeNameColumnAlias("COORDINATOR_NODE_ID")
                .addColumn("ID", idType, info -> info.id().toString())
                .addColumn("STATUS", stringOf(10), info -> info.status().name())
                .addColumn("CREATE_TIME", timestampType, JobState::createTime)
                .addColumn("START_TIME", timestampType, JobState::startTime)
                .addColumn("FINISH_TIME", timestampType, JobState::finishTime)
                .dataProvider(buildDataProvider())
                .build();
    }

    private Publisher<JobState> buildDataProvider() {
        return subscriber -> {
            CompletableFuture<List<JobState>> localStates = startFuture.thenCompose(ExecutionManager::localStatesAsync);

            Subscription subscription = new Subscription() {
                @Nullable Iterator<JobState> it;
                private boolean isDone;
                private int processed = 0;
                private final long threadId = Thread.currentThread().getId();

                @Override
                public void request(long n) {
                    assert threadId == Thread.currentThread().getId();

                    if (isDone) {
                        return;
                    }

                    localStates.whenComplete((results, ex) -> {
                        if (it == null) {
                            it = results.iterator();
                        }

                        while (it.hasNext() && processed != n) {
                            processed++;
                            subscriber.onNext(it.next());
                        }
                    });

                    processed = 0;

                    subscriber.onComplete();
                }

                @Override
                public void cancel() {
                    isDone = true;
                    it = null;
                }
            };

            subscriber.onSubscribe(subscription);
        };
    }
}
