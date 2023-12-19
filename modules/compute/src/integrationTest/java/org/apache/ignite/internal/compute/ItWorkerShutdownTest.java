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
import static org.hamcrest.Matchers.hasSize;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for worker node shutdown failover.
 */
@SuppressWarnings("resource")
class ItWorkerShutdownTest extends ClusterPerClassIntegrationTest {
    public static BlockingQueue<Signal> SIGNALS = new LinkedBlockingQueue<>();
    public static BlockingQueue<Object> CHANNEL = new LinkedBlockingQueue<>();
    public static Object ack = new Object();

    public static enum Signal {
        CONTINUE, THROW, RETURN, GET_WORKER_NAME;
    }

    private IgniteImpl node(int ind) {
        return CLUSTER.node(ind);
    }

    @BeforeEach
    void setUp() {
        executeSql("DROP TABLE IF EXISTS PUBLIC.TEST");
    }

    @Test
    void remoteExecutionWorkerShutdown() throws Exception {
        // Given entry node
        IgniteImpl entryNode = node(0);
        // And remote candidates to execute a job
        Set<ClusterNode> remoteWorkerCandidatesNodes = new HashSet<>();
        remoteWorkerCandidatesNodes.add(node(1).node());
        remoteWorkerCandidatesNodes.add(node(2).node());

        // When execute job
        CompletableFuture<String> fut = executeInteractive(entryNode, new HashSet<>(remoteWorkerCandidatesNodes));

        // Then one of candidates became a worker and run the job
        String workerNodeName = workerNodeName();
        // And job is running
        checkInteractiveJobAlive();

        // When shop worker node
        stopNode(workerNodeName);
        remoteWorkerCandidatesNodes.removeIf(node -> node.name().equals(workerNodeName));
        assertThat(remoteWorkerCandidatesNodes, hasSize(1));

        // Then the job is alive
        checkInteractiveJobAlive();
        // And remaining candidate was chosen as a failover worker
        String failoverWorker = workerNodeName();
        String tmp = remoteWorkerCandidatesNodes.stream().findFirst().get().name();
        assertThat(failoverWorker, equalTo(tmp));

        // When finish job
        finishJob();

        // Then it is successfully finished
        assertThat(fut.get(10, TimeUnit.SECONDS), equalTo("Done"));
    }

    private void finishJob() {
        SIGNALS.offer(Signal.RETURN);
    }
    private void stopNode(String name) {
        IgniteImpl remoteWorkerNode = CLUSTER.runningNodes().filter(node -> node.name().equals(name)).findFirst().get();
        remoteWorkerNode.stop();
    }

    private void checkInteractiveJobAlive() throws InterruptedException {
        SIGNALS.offer(Signal.CONTINUE);
        assertThat(CHANNEL.poll(10, TimeUnit.SECONDS), equalTo(ack));
    }
    private String workerNodeName() throws InterruptedException {
        SIGNALS.offer(Signal.GET_WORKER_NAME);
        var nodeName = (String) CHANNEL.poll(10, TimeUnit.SECONDS);
        return nodeName;
    }

    private CompletableFuture<String> executeInteractive(IgniteImpl entryNode, Set<ClusterNode> nodes) {
       return entryNode.compute().executeAsync(nodes, List.of(), InteractiveJob.class.getName());
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

    protected final List<List<Object>> executeSql(String sql, Object... args) {
        IgniteImpl ignite = node(0);

        return ClusterPerClassIntegrationTest.sql(ignite, null, sql, args);
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
                   case THROW: throw new RuntimeException();
                   case CONTINUE: CHANNEL.offer(ack); break;
                   case RETURN: return "Done";
                   case GET_WORKER_NAME: CHANNEL.add(context.ignite().name());
               }
           }
       }
    }
}
