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

package org.apache.ignite.internal.compute.state;

import static org.apache.ignite.compute.JobState.CANCELED;
import static org.apache.ignite.compute.JobState.CANCELING;
import static org.apache.ignite.compute.JobState.COMPLETED;
import static org.apache.ignite.compute.JobState.EXECUTING;
import static org.apache.ignite.compute.JobState.FAILED;
import static org.apache.ignite.compute.JobState.QUEUED;
import static org.apache.ignite.internal.testframework.matchers.AnythingMatcher.anything;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobStatusMatcher.jobStatusWithState;
import static org.apache.ignite.internal.testframework.matchers.JobStatusMatcher.jobStatusWithStateAndCreateTimeStartTimeFinishTime;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for {@link InMemoryComputeStateMachine}.
 */
@ExtendWith(ConfigurationExtension.class)
public class InMemoryComputeStateMachineTest extends BaseIgniteAbstractTest {
    private ComputeStateMachine stateMachine;

    @InjectConfiguration
    private ComputeConfiguration configuration;

    private UUID jobId;

    @BeforeEach
    public void setup() {
        stateMachine = new InMemoryComputeStateMachine(configuration, "testNode");
        stateMachine.start();
        jobId = stateMachine.initJob();
    }

    @AfterEach
    public void clean() {
        stateMachine.stop();
    }

    @Test
    public void testSubmit() {
        assertThat(jobId, is(notNullValue()));
        assertThat(stateMachine.currentStatus(jobId), jobStatusWithState(QUEUED));
    }

    @Test
    public void testCompleteWay() {
        executeJob(false);
        completeJob(false);
    }

    @Test
    public void testCancel() {
        cancelJob(false);
    }

    @Test
    public void testCancelFromExecuting() {
        executeJob(false);
        cancelingJob(false);
        cancelJob(false);
    }

    @Test
    public void testCompleteCanceling() {
        executeJob(false);
        cancelingJob(false);
        completeJob(false);
    }

    @Test
    public void testFailCanceling() {
        executeJob(false);
        cancelingJob(false);
        failJob(false);
    }

    @Test
    public void testFailExecuting() {
        executeJob(false);
        failJob(false);
    }

    @Test
    public void testCompleteExecution() {
        executeJob(false);
        completeJob(false);
    }

    @Test
    public void testQueue() {
        executeJob(false);
        queueJob(false);
    }

    @Test
    public void testDoubleExecution() {
        executeJob(false);
        executeJob(true);
    }

    @Test
    public void testDoubleComplete() {
        executeJob(false);

        completeJob(false);
        completeJob(true);
    }

    @Test
    public void testDoubleFail() {
        executeJob(false);

        failJob(false);
        failJob(true);
    }

    @Test
    public void testDoubleQueue() {
        executeJob(false);

        queueJob(false);
        queueJob(true);
    }

    @Test
    public void testCleanStates() throws InterruptedException {
        assertThat(configuration.change(change -> change.changeStatesLifetimeMillis(100)), willCompleteSuccessfully());

        stateMachine = new InMemoryComputeStateMachine(configuration, "testNode");
        stateMachine.start();

        jobId = stateMachine.initJob();
        executeJob(false);
        completeJob(false);
        ConditionFactory await = await().timeout(300, TimeUnit.MILLISECONDS);
        await.untilAsserted(() -> assertThat(stateMachine.currentStatus(jobId), is(nullValue())));

        jobId = stateMachine.initJob();
        executeJob(false);
        failJob(false);
        await.untilAsserted(() -> assertThat(stateMachine.currentStatus(jobId), is(nullValue())));

        jobId = stateMachine.initJob();
        cancelJob(false);
        await.untilAsserted(() -> assertThat(stateMachine.currentStatus(jobId), is(nullValue())));

        jobId = stateMachine.initJob();
        executeJob(false);
        cancelingJob(false);
        cancelJob(false);
        await.untilAsserted(() -> assertThat(stateMachine.currentStatus(jobId), is(nullValue())));

        stateMachine.stop();
    }

    private void cancelJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.cancelJob(jobId);
            assertThat(
                    stateMachine.currentStatus(jobId),
                    jobStatusWithStateAndCreateTimeStartTimeFinishTime(
                            equalTo(CANCELED),
                            notNullValue(Instant.class),
                            anything(),
                            notNullValue(Instant.class)
                    )
            );
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.cancelJob(jobId));
        }
    }

    private void cancelingJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.cancelingJob(jobId);
            assertThat(
                    stateMachine.currentStatus(jobId),
                    jobStatusWithStateAndCreateTimeStartTimeFinishTime(
                            equalTo(CANCELING),
                            notNullValue(Instant.class),
                            anything(),
                            nullValue(Instant.class)
                    )
            );
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.cancelJob(jobId));
        }
    }

    private void executeJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.executeJob(jobId);
            assertThat(
                    stateMachine.currentStatus(jobId),
                    jobStatusWithStateAndCreateTimeStartTimeFinishTime(
                            equalTo(EXECUTING),
                            notNullValue(Instant.class),
                            notNullValue(Instant.class),
                            nullValue(Instant.class)
                    )
            );
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.executeJob(jobId));
        }
    }

    private void completeJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.completeJob(jobId);
            assertThat(
                    stateMachine.currentStatus(jobId),
                    jobStatusWithStateAndCreateTimeStartTimeFinishTime(
                            equalTo(COMPLETED),
                            notNullValue(Instant.class),
                            notNullValue(Instant.class),
                            notNullValue(Instant.class)
                    )
            );
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.completeJob(jobId));
        }
    }

    private void failJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.failJob(jobId);
            assertThat(
                    stateMachine.currentStatus(jobId),
                    jobStatusWithStateAndCreateTimeStartTimeFinishTime(
                            equalTo(FAILED),
                            notNullValue(Instant.class),
                            anything(),
                            notNullValue(Instant.class)
                    )
            );
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.failJob(jobId));
        }
    }

    private void queueJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.queueJob(jobId);
            assertThat(
                    stateMachine.currentStatus(jobId),
                    jobStatusWithStateAndCreateTimeStartTimeFinishTime(
                            equalTo(QUEUED),
                            notNullValue(Instant.class),
                            notNullValue(Instant.class),
                            nullValue(Instant.class)
                    )
            );
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.queueJob(jobId));
        }
    }
}
