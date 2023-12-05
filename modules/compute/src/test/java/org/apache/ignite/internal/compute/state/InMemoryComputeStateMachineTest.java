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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
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
        stateMachine = new InMemoryComputeStateMachine(configuration);
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
    }

    @Test
    public void testCompleteWay() {
        executeJob(false);
        completeJob(false);
    }

    @Test
    public void testCancel() {
        stateMachine.cancelJob(jobId);
        assertThat(stateMachine.currentState(jobId), is(CANCELED));
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
    public void testCleanStates() throws InterruptedException {
        assertThat(configuration.change(change -> change.changeStatesLifetimeMillis(100)), willCompleteSuccessfully());

        stateMachine = new InMemoryComputeStateMachine(configuration);
        stateMachine.start();

        jobId = stateMachine.initJob();
        executeJob(false);
        completeJob(false);
        IgniteTestUtils.waitForCondition(() -> stateMachine.currentState(jobId) == null, 100);

        jobId = stateMachine.initJob();
        executeJob(false);
        failJob(false);
        IgniteTestUtils.waitForCondition(() -> stateMachine.currentState(jobId) == null, 100);

        jobId = stateMachine.initJob();
        cancelJob(false);
        IgniteTestUtils.waitForCondition(() -> stateMachine.currentState(jobId) == null, 100);

        jobId = stateMachine.initJob();
        executeJob(false);
        cancelingJob(false);
        cancelJob(false);
        IgniteTestUtils.waitForCondition(() -> stateMachine.currentState(jobId) == null, 100);

        stateMachine.stop();
    }

    private void cancelJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.cancelJob(jobId);
            assertThat(stateMachine.currentState(jobId), is(CANCELED));
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.cancelJob(jobId));
        }
    }

    private void cancelingJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.cancelingJob(jobId);
            assertThat(stateMachine.currentState(jobId), is(CANCELING));
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.cancelJob(jobId));
        }
    }

    private void executeJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.executeJob(jobId);
            assertThat(stateMachine.currentState(jobId), is(EXECUTING));
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.executeJob(jobId));
        }
    }

    private void completeJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.completeJob(jobId);
            assertThat(stateMachine.currentState(jobId), is(COMPLETED));
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.completeJob(jobId));
        }
    }

    private void failJob(boolean shouldFail) {
        if (!shouldFail) {
            stateMachine.failJob(jobId);
            assertThat(stateMachine.currentState(jobId), is(FAILED));
        } else {
            assertThrows(IllegalJobStateTransition.class, () -> stateMachine.failJob(jobId));
        }
    }
}
