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

package org.apache.ignite.internal.compute.executor;

import static org.apache.ignite.compute.JobState.CANCELED;
import static org.apache.ignite.compute.JobState.EXECUTING;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.state.InMemoryComputeStateMachine;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
class ComputeExecutorTest extends BaseIgniteAbstractTest {
    @Mock
    private Ignite ignite;

    @InjectConfiguration
    private ComputeConfiguration computeConfiguration;

    private ComputeExecutor computeExecutor;

    @BeforeEach
    void setUp() {
        computeExecutor = new ComputeExecutorImpl(ignite, new InMemoryComputeStateMachine(computeConfiguration), computeConfiguration);
        computeExecutor.start();
    }

    @AfterEach
    void tearDown() {
        computeExecutor.stop();
    }

    @Test
    void threadInterruption() {
        JobExecution<Integer> execution = computeExecutor.executeJob(ExecutionOptions.DEFAULT, InterruptingJob.class, new Object[]{});
        await().until(() -> execution.state() == EXECUTING);
        execution.cancel();
        await().untilAsserted(() -> assertThat(execution.state(), is(CANCELED)));
    }

    private static class InterruptingJob implements ComputeJob<Integer> {
        @Override
        public Integer execute(JobExecutionContext context, Object... args) {
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return 0;
                }
            }
        }
    }

    @Test
    void cooperativeCancellation() {
        JobExecution<Integer> execution = computeExecutor.executeJob(ExecutionOptions.DEFAULT, CancellingJob.class, new Object[]{});
        await().until(() -> execution.state() == EXECUTING);
        execution.cancel();
        await().untilAsserted(() -> assertThat(execution.state(), is(CANCELED)));
    }

    private static class CancellingJob implements ComputeJob<Integer> {
        @Override
        public Integer execute(JobExecutionContext context, Object... args) {
            while (true) {
                try {
                    if (context.isInterrupted()) {
                        return 0;
                    }
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

}
