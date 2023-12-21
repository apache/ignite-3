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

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for worker node shutdown failover.
 */
@SuppressWarnings("resource")
class ItWorkerShutdownTest extends ClusterPerTestIntegrationTest {
    private static final BlockingQueue<Signal> SIGNALS = new LinkedBlockingQueue<>();
    private static final BlockingQueue<Object> CHANNEL = new LinkedBlockingQueue<>();
    private static final Object ack = new Object();
    private static final Set<Integer> STOPPED_NODES_INDEXES = new CopyOnWriteArraySet<>();
    private static final Map<String, Integer> NODES_NAMES_TO_INDEXES = new HashMap<>();

    @NotNull
    private Set<ClusterNode> clusterNodesByNames(Set<String> nodes) {
        return nodes.stream()
                .map(name -> NODES_NAMES_TO_INDEXES.get(name))
                .map(this::node)
                .map(IgniteImpl::node)
                .collect(Collectors.toSet());
    }

    @BeforeEach
    void setUp() {
        for (int i = 0; i < 3; i++) {
            NODES_NAMES_TO_INDEXES.put(node(i).name(), i);
        }
        executeSql("DROP TABLE IF EXISTS PUBLIC.TEST");
    }

    @Test
    void remoteExecutionWorkerShutdown() throws Exception {
        // Given entry node
        IgniteImpl entryNode = node(0);
        // And remote candidates to execute a job
        Set<String> remoteWorkerCandidates = workerCandidates(node(1), node(2));

        // When execute job
        CompletableFuture<String> fut = executeInteractiveJob(entryNode, remoteWorkerCandidates);

        // Then one of candidates became a worker and run the job
        String workerNodeName = getWorkerNodeNameFromInteractiveJob();
        // And job is running
        checkInteractiveJobAlive(fut);

        // When stop worker node
        stopNode(workerNodeName);
        // And remove it from candidates
        remoteWorkerCandidates.remove(workerNodeName);

        // Then the job is alive: it has been restarted on another candidate
        checkInteractiveJobAlive(fut);
        // And remaining candidate was chosen as a failover worker
        String failoverWorker = getWorkerNodeNameFromInteractiveJob();
        assertThat(remoteWorkerCandidates, hasItem(failoverWorker));

        // When finish job
        finishJob();

        // Then it is successfully finished
        assertThat(fut.get(10, TimeUnit.SECONDS), equalTo("Done"));
    }

    @Test
    void remoteExecutionSingleWorkerShutdown() throws InterruptedException {
        // Given
        IgniteImpl entryNode = node(0);
        // And only one remote candidate to execute a job
        Set<String> remoteWorkerCandidates = workerCandidates(node(1));

        // When execute job
        CompletableFuture<String> fut = executeInteractiveJob(entryNode, remoteWorkerCandidates);

        // Then the job is running on worker node
        String workerNodeName = getWorkerNodeNameFromInteractiveJob();
        assertThat(remoteWorkerCandidates, hasItem(workerNodeName));
        // And
        checkInteractiveJobAlive(fut);

        // When stop worker node
        stopNode(workerNodeName);

        // Then the job is failed, because there is no any failover worker
        assertThat(fut.isCompletedExceptionally(), equalTo(true));
    }

    private Set<String> workerCandidates(IgniteImpl... nodes) {
        return Arrays.stream(nodes)
                .map(IgniteImpl::node)
                .map(ClusterNode::name)
                .collect(Collectors.toSet());
    }

    private void finishJob() {
        SIGNALS.offer(Signal.RETURN);
    }

    private void stopNode(String name) {
        int ind = NODES_NAMES_TO_INDEXES.get(name);
        node(ind).stop();

        STOPPED_NODES_INDEXES.add(ind);
    }

    private void checkInteractiveJobAlive(CompletableFuture<?> fut) throws InterruptedException {
        SIGNALS.offer(Signal.CONTINUE);
        assertThat(CHANNEL.poll(10, TimeUnit.SECONDS), equalTo(ack));

        assertThat(fut.isDone(), equalTo(false));
    }

    private String getWorkerNodeNameFromInteractiveJob() throws InterruptedException {
        SIGNALS.offer(Signal.GET_WORKER_NAME);
        var nodeName = (String) CHANNEL.poll(10, TimeUnit.SECONDS);
        return nodeName;
    }

    private CompletableFuture<String> executeInteractiveJob(IgniteImpl entryNode, Set<String> nodes) {
        return entryNode.compute().executeAsync(clusterNodesByNames(nodes), List.of(), InteractiveJob.class.getName());
    }

    private void createTestTableWithOneRow() {
        executeSql("CREATE TABLE test (k int, v int, CONSTRAINT PK PRIMARY KEY (k))");
        executeSql("INSERT INTO test(k, v) VALUES (1, 101)");
    }

    private List<String> allNodeNames() {
        return IntStream.range(0, initialNodes())
                .mapToObj(this::node)
                .map(Ignite::name)
                .collect(toList());
    }

    protected List<DeploymentUnit> units() {
        return List.of();
    }

    public enum Signal {
        CONTINUE, THROW, RETURN, GET_WORKER_NAME
    }

    private static class GetNodeNameJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return context.ignite().name();
        }
    }

    private static class InteractiveJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            while (true) {
                Signal recievedSignal = null;
                try {
                    recievedSignal = SIGNALS.take();

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                switch (recievedSignal) {
                    case THROW:
                        throw new RuntimeException();
                    case CONTINUE:
                        CHANNEL.offer(ack);
                        break;
                    case RETURN:
                        return "Done";
                    case GET_WORKER_NAME:
                        CHANNEL.add(context.ignite().name());
                }
            }
        }
    }
}
