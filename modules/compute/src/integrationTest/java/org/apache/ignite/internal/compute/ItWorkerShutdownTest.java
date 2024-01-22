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
import static org.apache.ignite.compute.JobState.CANCELED;
import static org.apache.ignite.compute.JobState.COMPLETED;
import static org.apache.ignite.compute.JobState.EXECUTING;
import static org.apache.ignite.compute.JobState.FAILED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for worker node shutdown failover.
 *
 * <p>The logic is that if we run the job on remote node and this node has left the topology then we should restart a job on
 * another node. This is not true for broadcast and local jobs. They should not be restarted.
 */
@SuppressWarnings("resource")
class ItWorkerShutdownTest extends ClusterPerTestIntegrationTest {
    /**
     * ACK for {@link Signal#CONTINUE}. Returned by a job that has received the signal. Used to check that the job is alive.
     */
    private static final Object ack = new Object();

    /**
     * Map from node name to node index in {@link super#cluster}.
     */
    private static final Map<String, Integer> NODES_NAMES_TO_INDEXES = new HashMap<>();

    /**
     * Class-wide queue that is used as a communication channel between {@link GlobalInteractiveJob} and test code. You can send a signal to
     * the job via this channel and get a response from the job via {@link #GLOBAL_CHANNEL}.
     */
    private static BlockingQueue<Signal> GLOBAL_SIGNALS = new LinkedBlockingQueue<>();

    /**
     * Class-wide queue that is used as a communication channel between {@link GlobalInteractiveJob} and test code. You can send a signal to
     * the job via {@link #GLOBAL_SIGNALS} and get a response from the job via this channel.
     */
    private static BlockingQueue<Object> GLOBAL_CHANNEL = new LinkedBlockingQueue<>();

    /**
     * Node-specific queues that are used as a communication channel between {@link InteractiveJob} and test code. The semantics are the
     * same as for {@link #GLOBAL_SIGNALS} except that each node has its own queue. So, test code can communicate with a
     * {@link InteractiveJob} that is running on specific node.
     */
    private static Map<String, BlockingQueue<Signal>> NODE_SIGNALS = new ConcurrentHashMap<>();

    /**
     * Node-specific queues that are used as a communication channel between {@link InteractiveJob} and test code. The semantics are the
     * same as for {@link #GLOBAL_CHANNEL} except that each node has its own queue. So, test code can communicate with a
     * {@link InteractiveJob} that is running on specific node.
     */
    private static Map<String, BlockingQueue<Object>> NODE_CHANNELS = new ConcurrentHashMap<>();

    /**
     * Node-specific counters that are used to count how many times {@link InteractiveJob} has been run on specific node.
     */
    private static Map<String, Integer> INTERACTIVE_JOB_RUN_TIMES = new ConcurrentHashMap<>();

    private static void checkAllInteractiveJobsCalledOnce() {
        INTERACTIVE_JOB_RUN_TIMES.forEach((nodeName, runTimes) -> assertThat(runTimes, equalTo(1)));
    }

    private static void finishAllInteractiveJobs() {
        NODE_SIGNALS.forEach((nodeName, channel) -> {
            try {
                channel.offer(Signal.RETURN, 10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void initChannels(List<String> nodes) {
        for (String nodeName : nodes) {
            NODE_CHANNELS.put(nodeName, new LinkedBlockingQueue<>());
            NODE_SIGNALS.put(nodeName, new LinkedBlockingQueue<>());
            INTERACTIVE_JOB_RUN_TIMES.put(nodeName, 0);
        }
    }

    private static Set<String> workerCandidates(IgniteImpl... nodes) {
        return Arrays.stream(nodes)
                .map(IgniteImpl::node)
                .map(ClusterNode::name)
                .collect(Collectors.toSet());
    }

    private static void finishGlobalJob() {
        GLOBAL_SIGNALS.offer(Signal.RETURN);
    }

    private static void checkGlobalInteractiveJobAlive(JobExecution<?> execution)
            throws InterruptedException, ExecutionException, TimeoutException {
        GLOBAL_SIGNALS.offer(Signal.CONTINUE);
        assertThat(GLOBAL_CHANNEL.poll(10, TimeUnit.SECONDS), equalTo(ack));

        assertThat(execution.resultAsync().isDone(), equalTo(false));
        assertThat(idSync(execution), notNullValue());

        // During the fob failover we might get a job that is restarted, the state will be not EXECUTING for some short time.
        await().until(() -> execution.statusAsync().get(10, TimeUnit.SECONDS).state() == EXECUTING);
    }

    private static void checkInteractiveJobAlive(ClusterNode clusterNode, JobExecution<?> execution) {
        NODE_SIGNALS.get(clusterNode.name()).offer(Signal.CONTINUE);
        try {
            assertThat(NODE_CHANNELS.get(clusterNode.name()).poll(10, TimeUnit.SECONDS), equalTo(ack));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertThat(execution.resultAsync().isDone(), equalTo(false));
    }

    private static JobStatus statusSync(JobExecution<String> execution) throws InterruptedException, ExecutionException, TimeoutException {
        return execution.statusAsync().get(10, TimeUnit.SECONDS);
    }

    private Set<ClusterNode> clusterNodesByNames(Set<String> nodes) {
        return nodes.stream()
                .map(NODES_NAMES_TO_INDEXES::get)
                .map(this::node)
                .map(IgniteImpl::node)
                .collect(Collectors.toSet());
    }

    private static UUID idSync(JobExecution<?> execution) throws InterruptedException, ExecutionException, TimeoutException {
        return execution.idAsync().get(10, TimeUnit.SECONDS);
    }

    /**
     * Initializes channels. Assumption: there is no any running job on the cluster.
     */
    @BeforeEach
    void setUp() {
        GLOBAL_SIGNALS.clear();
        GLOBAL_CHANNEL.clear();
        NODE_SIGNALS.clear();
        NODE_CHANNELS.clear();
        INTERACTIVE_JOB_RUN_TIMES.clear();
        NODES_NAMES_TO_INDEXES.clear();

        for (int i = 0; i < 3; i++) {
            NODES_NAMES_TO_INDEXES.put(node(i).name(), i);
        }

        executeSql("DROP TABLE IF EXISTS PUBLIC.TEST");
    }

    @Test
    void remoteExecutionWorkerShutdown() throws Exception {
        // Given entry node.
        IgniteImpl entryNode = node(0);
        // And remote candidates to execute a job.
        Set<String> remoteWorkerCandidates = workerCandidates(node(1), node(2));

        // When execute job.
        JobExecution<String> execution = executeGlobalInteractiveJob(entryNode, remoteWorkerCandidates);

        // Then one of candidates became a worker and run the job.
        String workerNodeName = getWorkerNodeNameFromGlobalInteractiveJob();
        // And job is running.
        checkGlobalInteractiveJobAlive(execution);

        // And save state BEFORE worker has failed.
        long createTimeBeforeFail = statusSync(execution).createTime().toEpochMilli();
        long startTimeBeforeFail = statusSync(execution).startTime().toEpochMilli();
        UUID jobIdBeforeFail = idSync(execution);

        // When stop worker node.
        stopNodeByName(workerNodeName);
        // And remove it from candidates.
        remoteWorkerCandidates.remove(workerNodeName);

        // Then the job is alive: it has been restarted on another candidate.
        checkGlobalInteractiveJobAlive(execution);
        // And remaining candidate was chosen as a failover worker.
        String failoverWorker = getWorkerNodeNameFromGlobalInteractiveJob();
        assertThat(remoteWorkerCandidates, hasItem(failoverWorker));

        // And check create time was not changed but start time changed.
        long createTimeAfterFail = statusSync(execution).createTime().toEpochMilli();
        long startTimeAfterFail = statusSync(execution).startTime().toEpochMilli();
        assertThat(createTimeAfterFail, equalTo(createTimeBeforeFail));
        assertThat(startTimeAfterFail, greaterThan(startTimeBeforeFail));
        // And id was not changed
        assertThat(idSync(execution), equalTo(jobIdBeforeFail));

        // When finish job.
        finishGlobalJob();

        // Then it is successfully finished.
        assertThat(execution.resultAsync().get(10, TimeUnit.SECONDS), equalTo("Done"));
        // And.
        assertThat(execution.statusAsync().get().state(), is(COMPLETED));
        // And finish time is greater then create time and start time.
        long finishTime = execution.statusAsync().get().finishTime().toEpochMilli();
        assertThat(finishTime, greaterThan(createTimeAfterFail));
        assertThat(finishTime, greaterThan(startTimeAfterFail));
        // And job id the same
        assertThat(idSync(execution), equalTo(jobIdBeforeFail));
    }

    @Test
    void remoteExecutionSingleWorkerShutdown() throws Exception {
        // Given.
        IgniteImpl entryNode = node(0);
        // And only one remote candidate to execute a job.
        Set<String> remoteWorkerCandidates = workerCandidates(node(1));

        // When execute job.
        JobExecution<String> execution = executeGlobalInteractiveJob(entryNode, remoteWorkerCandidates);

        // Then the job is running on worker node.
        String workerNodeName = getWorkerNodeNameFromGlobalInteractiveJob();
        assertThat(remoteWorkerCandidates, hasItem(workerNodeName));
        // And.
        checkGlobalInteractiveJobAlive(execution);

        // When stop worker node.
        stopNodeByName(workerNodeName);

        // Then the job is failed, because there is no any failover worker.
        assertThat(execution.resultAsync(), willThrow(IgniteException.class));
        assertThat(execution.statusAsync().isCompletedExceptionally(), equalTo(true));
    }

    @Test
    void localExecutionWorkerShutdown() throws Exception {
        // Given entry node.
        IgniteImpl entryNode = node(0);

        // When execute job locally.
        JobExecution<String> execution = executeGlobalInteractiveJob(entryNode, Set.of(entryNode.name()));

        // Then the job is running.
        checkGlobalInteractiveJobAlive(execution);

        // And it is running on entry node.
        assertThat(getWorkerNodeNameFromGlobalInteractiveJob(), equalTo(entryNode.name()));

        // When stop entry node.
        stopNodeByName(entryNode.name());

        // Then the job is failed, because there is no any failover worker.
        assertThat(execution.resultAsync().isCompletedExceptionally(), equalTo(true));
        assertThat(execution.statusAsync().get().state(), is(FAILED));
    }

    @Test
    void broadcastExecutionWorkerShutdown() {
        // Given entry node.
        IgniteImpl entryNode = node(0);
        // And prepare communication channels.
        initChannels(allNodeNames());

        // When start broadcast job.
        Map<ClusterNode, JobExecution<Object>> executions = entryNode.compute().broadcastAsync(
                clusterNodesByNames(workerCandidates(node(0), node(1), node(2))),
                List.of(),
                InteractiveJob.class.getName()
        );

        // Then all three jobs are alive.
        assertThat(executions.size(), is(3));
        executions.forEach(ItWorkerShutdownTest::checkInteractiveJobAlive);

        // When stop one of workers.
        stopNodeByName(node(1).name());

        // Then two jobs are alive.
        executions.forEach((node, execution) -> {
            if (node.name().equals(node(1).name())) {
                assertThat(execution.resultAsync(), willThrow(IgniteException.class));
            } else {
                checkInteractiveJobAlive(node, execution);
            }
        });

        // When.
        finishAllInteractiveJobs();

        // Then every job ran once because broadcast execution does not require failover.
        checkAllInteractiveJobsCalledOnce();
    }

    @Test
    void cancelRemoteExecutionOnRestartedJob() throws Exception {
        // Given entry node.
        IgniteImpl entryNode = node(0);
        // And remote candidates to execute a job.
        Set<String> remoteWorkerCandidates = workerCandidates(node(1), node(2));

        // When execute job.
        JobExecution<String> execution = executeGlobalInteractiveJob(entryNode, remoteWorkerCandidates);

        // Then one of candidates became a worker and run the job.
        String workerNodeName = getWorkerNodeNameFromGlobalInteractiveJob();
        // And job is running.
        checkGlobalInteractiveJobAlive(execution);

        // When stop worker node.
        stopNodeByName(workerNodeName);
        // And remove it from candidates.
        remoteWorkerCandidates.remove(workerNodeName);

        // Then the job is alive: it has been restarted on another candidate.
        checkGlobalInteractiveJobAlive(execution);
        // And remaining candidate was chosen as a failover worker.
        String failoverWorker = getWorkerNodeNameFromGlobalInteractiveJob();
        assertThat(remoteWorkerCandidates, hasItem(failoverWorker));

        // When cancel job.
        execution.cancelAsync().get(10, TimeUnit.SECONDS);

        // Then it is cancelled.
        assertThat(execution.resultAsync(), willThrow(IgniteException.class));
        // And.
        assertThat(statusSync(execution).state(), is(CANCELED));
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20864")
    void colocatedExecutionWorkerShutdown() throws Exception {
        // Given table.
        initChannels(allNodeNames());
        createTestTableWithOneRow();
        TableImpl table = (TableImpl) node(0).tables().table("test");
        // And partition leader for K=1.
        ClusterNode leader = table.leaderAssignment(table.partition(Tuple.create(1).set("K", 1)));

        // When start colocated job on node that is not leader.
        IgniteImpl entryNode = anyNodeExcept(leader);
        JobExecution<Object> execution = entryNode.compute().executeColocatedAsync(
                "test",
                Tuple.create(1).set("K", 1),
                List.of(),
                GlobalInteractiveJob.class.getName()
        );

        // Then the job is alive.
        checkGlobalInteractiveJobAlive(execution);

        // And it is running on leader node.
        String firstWorkerNodeName = getWorkerNodeNameFromGlobalInteractiveJob();
        assertThat(firstWorkerNodeName, equalTo(leader.name()));
        // And leader node is NOT an entry node, it is remote.
        assertThat(entryNode.name(), not(equalTo(firstWorkerNodeName)));

        // When stop worker node.
        stopNode(nodeByName(firstWorkerNodeName));

        // Then the job is restarted on another node.
        checkGlobalInteractiveJobAlive(execution);

        // And it is running on another node.
        String failoverNodeName = getWorkerNodeNameFromGlobalInteractiveJob();
        assertThat(failoverNodeName, in(allNodeNames()));
        // But.
        assertThat(failoverNodeName, not(equalTo(firstWorkerNodeName)));

    }

    private void stopNode(IgniteImpl ignite) {
        stopNodeByName(ignite.name());
    }

    private void stopNodeByName(String name) {
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

    private String getWorkerNodeNameFromGlobalInteractiveJob() throws InterruptedException {
        GLOBAL_SIGNALS.offer(Signal.GET_WORKER_NAME);
        return (String) GLOBAL_CHANNEL.poll(10, TimeUnit.SECONDS);
    }

    private JobExecution<String> executeGlobalInteractiveJob(IgniteImpl entryNode, Set<String> nodes) {
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

    /**
     * Signals that are sent by test code to the jobs.
     */
    enum Signal {
        /**
         * Signal to the job to continue running and send ACK as a response.
         */
        CONTINUE,
        /**
         * Ask job to throw an exception.
         */
        THROW,
        /**
         * Ask job to return result.
         */
        RETURN,
        /**
         * Signal to the job to continue running and send current worker name to the response channel.
         */
        GET_WORKER_NAME
    }

    /**
     * Interactive job that communicates via {@link #GLOBAL_CHANNEL} and {@link #GLOBAL_SIGNALS}.
     */
    static class GlobalInteractiveJob implements ComputeJob<String> {
        private static Signal listenSignal() {
            Signal recievedSignal;
            try {
                recievedSignal = GLOBAL_SIGNALS.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return recievedSignal;
        }

        @Override
        public String execute(JobExecutionContext context, Object... args) {
            while (true) {
                Signal recievedSignal = listenSignal();
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

    /**
     * Interactive job that communicates via {@link #NODE_CHANNELS} and {@link #NODE_SIGNALS}. Also, keeps track of how many times it was
     * executed via {@link #INTERACTIVE_JOB_RUN_TIMES}.
     */
    static class InteractiveJob implements ComputeJob<String> {
        private static Signal listenSignal(BlockingQueue<Signal> channel) {
            Signal recievedSignal = null;
            try {
                recievedSignal = channel.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return recievedSignal;
        }

        @Override
        public String execute(JobExecutionContext context, Object... args) {
            String workerNodeName = context.ignite().name();

            INTERACTIVE_JOB_RUN_TIMES.put(workerNodeName, INTERACTIVE_JOB_RUN_TIMES.get(workerNodeName) + 1);
            BlockingQueue<Signal> channel = NODE_SIGNALS.get(workerNodeName);

            while (true) {
                Signal recievedSignal = listenSignal(channel);
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
