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
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for worker node shutdown failover.
 */
@SuppressWarnings("resource")
class ItWorkerShutdownTest extends ClusterPerTestIntegrationTest {
    /**
     * ACK for {@link Signal#CONTINUE}. Returned by a job that has received the signal. Used to check that the job is alive.
     */
    private static final Object ack = new Object();
    /**
     * Map from node name to node index in CLUSTER.
     */
    private static final Map<String, Integer> NODES_NAMES_TO_INDEXES = new HashMap<>();
    /**
     * Class-wide queue that is used as a communication channel between {@link GlobalInteractiveJob} and test code. You can send a signal to
     * the job via this channel and get a response from the job via {@link #GLOBAL_CHANNEL}.
     */
    private static BlockingQueue<Signal> GLOBAL_SIGNALS;

    /**
     * Class-wide queue that is used as a communication channel between {@link GlobalInteractiveJob} and test code. You can send a signal to
     * the job via {@link #GLOBAL_SIGNALS} and get a response from the job via this channel.
     */
    private static BlockingQueue<Object> GLOBAL_CHANNEL;

    /**
     * Node-specific queues that are used as a communication channel between {@link InteractiveJob} and test code. The semantics are the
     * same as for {@link #GLOBAL_SIGNALS} except that each node has its own queue. So, test code can communicate with a
     * {@link InteractiveJob} that is running on specific node.
     */
    private static Map<String, BlockingQueue<Signal>> NODE_SIGNALS;

    /**
     * Node-specific queues that are used as a communication channel between {@link InteractiveJob} and test code. The semantics are the
     * same as for {@link #GLOBAL_CHANNEL} except that each node has its own queue. So, test code can communicate with a
     * {@link InteractiveJob} that is running on specific node.
     */
    private static Map<String, BlockingQueue<Object>> NODE_CHANNELS;

    /**
     * Node-specific counters that are used to count how many times {@link InteractiveJob} has been run on specific node.
     */
    private static Map<String, Integer> INTERACTIVE_JOB_RUN_TIMES;

    @NotNull
    private Set<ClusterNode> clusterNodesByNames(Set<String> nodes) {
        return nodes.stream()
                .map(NODES_NAMES_TO_INDEXES::get)
                .map(this::node)
                .map(IgniteImpl::node)
                .collect(Collectors.toSet());
    }

    /**
     * Initializes channels. Assumption: there is no any running job on the cluster.
     */
    @BeforeEach
    void setUp() {
        GLOBAL_SIGNALS = new LinkedBlockingQueue<>();
        GLOBAL_CHANNEL = new LinkedBlockingQueue<>();

        NODE_SIGNALS = new ConcurrentHashMap<>();
        NODE_CHANNELS = new ConcurrentHashMap<>();

        INTERACTIVE_JOB_RUN_TIMES = new ConcurrentHashMap<>();
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
        CompletableFuture<String> fut = executeGlobalInteractiveJob(entryNode, remoteWorkerCandidates);

        // Then one of candidates became a worker and run the job
        String workerNodeName = getWorkerNodeNameFromGlobalInteractiveJob();
        // And job is running
        checkGlobalInteractiveJobAlive(fut);

        // When stop worker node
        stopNode(workerNodeName);
        // And remove it from candidates
        remoteWorkerCandidates.remove(workerNodeName);

        // Then the job is alive: it has been restarted on another candidate
        checkGlobalInteractiveJobAlive(fut);
        // And remaining candidate was chosen as a failover worker
        String failoverWorker = getWorkerNodeNameFromGlobalInteractiveJob();
        assertThat(remoteWorkerCandidates, hasItem(failoverWorker));

        // When finish job
        finishGlobalJob();

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
        CompletableFuture<String> fut = executeGlobalInteractiveJob(entryNode, remoteWorkerCandidates);

        // Then the job is running on worker node
        String workerNodeName = getWorkerNodeNameFromGlobalInteractiveJob();
        assertThat(remoteWorkerCandidates, hasItem(workerNodeName));
        // And
        checkGlobalInteractiveJobAlive(fut);

        // When stop worker node
        stopNode(workerNodeName);

        // Then the job is failed, because there is no any failover worker
        assertThat(fut.isCompletedExceptionally(), equalTo(true));
    }

    @Test
    void localExecutionWorkerShutdown() throws InterruptedException {
        // Given entry node
        IgniteImpl entryNode = node(0);

        // When execute job locally
        CompletableFuture<String> fut = executeGlobalInteractiveJob(entryNode, Set.of(entryNode.name()));

        // Then the job is running
        checkGlobalInteractiveJobAlive(fut);

        // And it is running on entry node
        assertThat(getWorkerNodeNameFromGlobalInteractiveJob(), equalTo(entryNode.name()));

        // When stop entry node
        stopNode(entryNode.name());

        // Then the job is failed, because there is no any failover worker
        assertThat(fut.isCancelled(), equalTo(true));
    }

    @Test
    void broadcastExecutionWorkerShutdown() {
        // Given entry node
        IgniteImpl entryNode = node(0);
        // And prepare communication channel
        initChannels(allNodeNames());

        // When start broadcast job
        Map<ClusterNode, CompletableFuture<Object>> futures = entryNode.compute().broadcastAsync(
                clusterNodesByNames(workerCandidates(node(0), node(1), node(2))),
                List.of(),
                InteractiveJob.class.getName()
        );

        assertThat(futures.size(), is(3));

        // Then all three jobs are alive
        futures.forEach(this::checkInteractiveJobAlive);

        // When stop one of workers
        stopNode(node(1).name());

        // Then two jobs are alive
        futures.forEach((node, future) -> {
            if (node.name().equals(node(1).name())) {
                assertThat(future.isCancelled(), equalTo(true));
            } else {
                checkInteractiveJobAlive(node, future);
            }
        });

        finishAllInteractiveJobs();

        checkAllInteractiveJobCalledOnce();
    }

    private void checkAllInteractiveJobCalledOnce() {
        INTERACTIVE_JOB_RUN_TIMES.forEach((nodeName, runTimes) -> assertThat(runTimes, equalTo(1)));
    }

    private void finishAllInteractiveJobs() {
        NODE_SIGNALS.forEach((nodeName, channel) -> {
            try {
                channel.offer(Signal.RETURN, 10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20864")
    void colocatedExecutionWorkerShutdown() throws InterruptedException {
        // Given table
        initChannels(allNodeNames());
        createTestTableWithOneRow();
        TableImpl table = (TableImpl) node(0).tables().table("test");
        // And partition leader for K=1
        ClusterNode leader = table.leaderAssignment(table.partition(Tuple.create(1).set("K", 1)));

        // When start colocated job on node that is not leader
        IgniteImpl entryNode = anyNodeExcept(leader);
        CompletableFuture<String> fut = entryNode.compute().executeColocatedAsync(
                "test",
                Tuple.create(1).set("K", 1),
                List.of(),
                GlobalInteractiveJob.class.getName()
        );

        // Then the job is alive
        checkGlobalInteractiveJobAlive(fut);

        // And it is running on leader node
        String firstWorkerNodeName = getWorkerNodeNameFromGlobalInteractiveJob();
        assertThat(firstWorkerNodeName, equalTo(leader.name()));
        // And leader node is NOT an entry node, it is remote.
        assertThat(entryNode.name(), not(equalTo(firstWorkerNodeName)));

        // When stop worker node
        stopNode(nodeByName(firstWorkerNodeName));

        // Then the job is restarted on another node
        checkGlobalInteractiveJobAlive(fut);

        // And it is running on another node
        String failoverNodeName = getWorkerNodeNameFromGlobalInteractiveJob();
        assertThat(failoverNodeName, in(allNodeNames()));
        // But
        assertThat(failoverNodeName, not(equalTo(firstWorkerNodeName)));

    }

    private void stopNode(IgniteImpl ignite) {
        stopNode(ignite.name());
    }

    private void stopNode(String name) {
        int ind = NODES_NAMES_TO_INDEXES.get(name);
        node(ind).stop();
    }

    private IgniteImpl anyNodeExcept(ClusterNode except) {
        String candidateName = allNodeNames()
                .stream()
                .filter(name -> !name.equals(except.name()))
                .findFirst()
                .orElseThrow();

        return nodeByName(candidateName);
    }

    private IgniteImpl nodeByName(String candidateName) {
        return cluster.runningNodes().filter(node -> node.name().equals(candidateName)).findFirst().orElseThrow();
    }

    private void initChannels(List<String> nodes) {
        for (String nodeName : nodes) {
            NODE_CHANNELS.put(nodeName, new LinkedBlockingQueue<>());
            NODE_SIGNALS.put(nodeName, new LinkedBlockingQueue<>());
            INTERACTIVE_JOB_RUN_TIMES.put(nodeName, 0);
        }
    }

    private Set<String> workerCandidates(IgniteImpl... nodes) {
        return Arrays.stream(nodes)
                .map(IgniteImpl::node)
                .map(ClusterNode::name)
                .collect(Collectors.toSet());
    }

    private void finishGlobalJob() {
        GLOBAL_SIGNALS.offer(Signal.RETURN);
    }


    private void checkGlobalInteractiveJobAlive(CompletableFuture<?> fut) throws InterruptedException {
        GLOBAL_SIGNALS.offer(Signal.CONTINUE);
        assertThat(GLOBAL_CHANNEL.poll(10, TimeUnit.SECONDS), equalTo(ack));

        assertThat(fut.isDone(), equalTo(false));
    }

    private void checkInteractiveJobAlive(ClusterNode clusterNode, CompletableFuture<?> fut) {
        NODE_SIGNALS.get(clusterNode.name()).offer(Signal.CONTINUE);
        try {
            assertThat(NODE_CHANNELS.get(clusterNode.name()).poll(10, TimeUnit.SECONDS), equalTo(ack));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertThat(fut.isDone(), equalTo(false));
    }

    private @Nullable String getWorkerNodeNameFromGlobalInteractiveJob() throws InterruptedException {
        GLOBAL_SIGNALS.offer(Signal.GET_WORKER_NAME);
        var nodeName = (String) GLOBAL_CHANNEL.poll(10, TimeUnit.SECONDS);
        return nodeName;
    }

    private CompletableFuture<String> executeInteractiveJob(IgniteImpl entryNode, Set<String> nodes) {
        return entryNode.compute().executeAsync(clusterNodesByNames(nodes), List.of(), InteractiveJob.class.getName());
    }

    private CompletableFuture<String> executeGlobalInteractiveJob(IgniteImpl entryNode, Set<String> nodes) {
        return entryNode.compute().executeAsync(clusterNodesByNames(nodes), List.of(), GlobalInteractiveJob.class.getName());
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

    public enum Signal {
        CONTINUE, THROW, RETURN, GET_WORKER_NAME
    }

    private static class GlobalInteractiveJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            while (true) {
                Signal recievedSignal = null;
                try {
                    recievedSignal = GLOBAL_SIGNALS.take();

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                switch (recievedSignal) {
                    case THROW:
                        throw new RuntimeException();
                    case CONTINUE:
                        GLOBAL_CHANNEL.offer(ack);
                        break;
                    case RETURN:
                        return "Done";
                    case GET_WORKER_NAME:
                        GLOBAL_CHANNEL.add(context.ignite().name());
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + recievedSignal);
                }
            }
        }
    }

    private static class InteractiveJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            String workerNodeName = context.ignite().name();
            INTERACTIVE_JOB_RUN_TIMES.put(workerNodeName, INTERACTIVE_JOB_RUN_TIMES.get(workerNodeName) + 1);
            BlockingQueue<Signal> channel = NODE_SIGNALS.get(workerNodeName);
            while (true) {
                Signal recievedSignal = null;
                try {
                    recievedSignal = channel.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                switch (recievedSignal) {
                    case THROW:
                        throw new RuntimeException();
                    case CONTINUE:
                        NODE_CHANNELS.get(workerNodeName).offer(ack);
                        break;
                    case RETURN:
                        return "Done";
                    case GET_WORKER_NAME:
                        NODE_CHANNELS.get(workerNodeName).add(context.ignite().name());
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + recievedSignal);
                }
            }
        }
    }
}
