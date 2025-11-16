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

package org.apache.ignite.internal.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tx.TxState;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Listener that collects {@link IndexBuildTask} statistics during execution and logs the aggregated results when the index build
 * completes.
 */
class IndexBuildTaskStatisticsLoggingListener implements IndexBuildTaskListener {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuildTaskStatisticsLoggingListener.class);

    private final IndexBuildTaskId taskId;

    private final boolean afterDisasterRecovery;

    private final AtomicLong startTime = new AtomicLong();

    private final AtomicInteger successfulRaftCallCount = new AtomicInteger(0);

    private final AtomicInteger failedRaftCallCount = new AtomicInteger(0);

    private final AtomicLong rowIndexedCount = new AtomicLong(0);

    private final ConcurrentMap<TxState, AtomicInteger> resolvedWriteIntentCount = new ConcurrentHashMap<>();

    IndexBuildTaskStatisticsLoggingListener(IndexBuildTaskId taskId, boolean afterDisasterRecovery) {
        this.taskId = taskId;
        this.afterDisasterRecovery = afterDisasterRecovery;
    }

    @Override
    public void onIndexBuildStarted(IndexBuildTaskId taskId) {
        checkTaskId(taskId);

        startTime.set(System.currentTimeMillis());
    }

    @Override
    public void onWriteIntentResolved(IndexBuildTaskId taskId, TxState txState) {
        checkTaskId(taskId);

        resolvedWriteIntentCount.computeIfAbsent(txState, unused -> new AtomicInteger(0)).incrementAndGet();
    }

    @Override
    public void onRaftCallSuccess(IndexBuildTaskId taskId) {
        checkTaskId(taskId);

        successfulRaftCallCount.incrementAndGet();
    }

    @Override
    public void onRaftCallFailure(IndexBuildTaskId taskId) {
        checkTaskId(taskId);

        failedRaftCallCount.incrementAndGet();
    }

    @Override
    public void onBatchProcessed(IndexBuildTaskId taskId, int rowCount) {
        checkTaskId(taskId);

        rowIndexedCount.addAndGet(rowCount);
    }

    @Override
    public void onIndexBuildSuccess(IndexBuildTaskId taskId) {
        checkTaskId(taskId);

        logStatistics(null);
    }

    @Override
    public void onIndexBuildFailure(IndexBuildTaskId taskId, Throwable throwable) {
        checkTaskId(taskId);

        logStatistics(throwable);
    }

    private void checkTaskId(IndexBuildTaskId taskId) {
        if (!this.taskId.equals(taskId)) {
            String message = String.format(
                    "Listener invoked with unexpected index id. Expected: [%s], but got: [%s]",
                    this.taskId, taskId
            );

            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    private void logStatistics(@Nullable Throwable throwable) {
        String status = throwable == null
                ? "success"
                : String.format("failure (%s: %s)", throwable.getClass().getName(), throwable.getMessage());
        String reason = afterDisasterRecovery ? "disaster recovery of an AVAILABLE index" : "normal build";

        LOG.info(
                "Index build statistics: ["
                        + "task id: {}, "
                        + "status: {}, "
                        + "build reason: {}, "
                        + "time: {} ms, "
                        + "rows indexed: {}, "
                        + "successful raft calls: {}, "
                        + "failed raft calls: {}, "
                        + "resolved write intents: {}]",
                taskId,
                status,
                reason,
                getBuildTime(),
                rowIndexedCount,
                successfulRaftCallCount,
                failedRaftCallCount,
                resolvedWriteIntentCount
        );
    }

    private long getBuildTime() {
        if (startTime.get() == 0) {
            String message = "Index build start time has not been set.";
            LOG.error(message);

            throw new IllegalStateException(message);
        }

        return System.currentTimeMillis() - startTime.get();
    }

    @TestOnly
    AtomicLong startTime() {
        return startTime;
    }

    @TestOnly
    Map<TxState, AtomicInteger> resolvedWriteIntentCount() {
        return resolvedWriteIntentCount;
    }

    @TestOnly
    AtomicLong rowIndexedCount() {
        return rowIndexedCount;
    }

    @TestOnly
    AtomicInteger successfulRaftCallCount() {
        return successfulRaftCallCount;
    }

    @TestOnly
    AtomicInteger failedRaftCallCount() {
        return failedRaftCallCount;
    }
}
