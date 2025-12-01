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

package org.apache.ignite.internal.compute.utils;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.network.ClusterNode;

/**
 * Tests DSL for interactive jobs. "Interactive" means that you can send messages and get responses to/from running jobs.
 *
 * <p>For example, you can start {@link GlobalInteractiveJob} on some node, get the name of worker node for this job,
 * ask this job to complete successfully or throw exception. Also, this class gives useful assertions for job states.
 *
 * @see org.apache.ignite.internal.compute.ItWorkerShutdownTest
 */
public final class InteractiveJobs {
    /**
     * ACK for {@link Signal#CONTINUE}. Returned by a job that has received the signal. Used to check that the job is alive.
     */
    private static final Object ACK = new Object();

    /**
     * Class-wide queue that is used as a communication channel between {@link GlobalInteractiveJob} and test code. You can send a signal to
     * the job via this channel and get a response from the job via {@link #GLOBAL_CHANNEL}.
     */
    private static final BlockingQueue<Signal> GLOBAL_SIGNALS = new LinkedBlockingQueue<>();

    /**
     * Class-wide queue that is used as a communication channel between {@link GlobalInteractiveJob} and test code. You can send a signal to
     * the job via {@link #GLOBAL_SIGNALS} and get a response from the job via this channel.
     */
    private static final BlockingQueue<Object> GLOBAL_CHANNEL = new LinkedBlockingQueue<>();

    /**
     * Node-specific queues that are used as a communication channel between {@link InteractiveJob} and test code. The semantics are the
     * same as for {@link #GLOBAL_SIGNALS} except that each node has its own queue. So, test code can communicate with a
     * {@link InteractiveJob} that is running on specific node.
     */
    private static final Map<String, BlockingQueue<Signal>> NODE_SIGNALS = new ConcurrentHashMap<>();

    /**
     * Node-specific queues that are used as a communication channel between {@link InteractiveJob} and test code. The semantics are the
     * same as for {@link #GLOBAL_CHANNEL} except that each node has its own queue. So, test code can communicate with a
     * {@link InteractiveJob} that is running on specific node.
     */
    private static final Map<String, BlockingQueue<Object>> NODE_CHANNELS = new ConcurrentHashMap<>();

    /**
     * Node-specific counters that are used to count how many times {@link InteractiveJob} has been run on specific node.
     */
    private static final Map<String, Integer> INTERACTIVE_JOB_RUN_TIMES = new ConcurrentHashMap<>();

    /**
     * Counts for all running interactive jobs.
     */
    private static final AtomicInteger RUNNING_INTERACTIVE_JOBS_CNT = new AtomicInteger(0);

    /**
     * This counter indicated how many {@link GlobalInteractiveJob} instances running now. This counter increased each time the
     * {@link GlobalInteractiveJob} is called and decreased when the job is done (whatever the result is). Checked in {@link #clearState}.
     */
    private static final AtomicInteger RUNNING_GLOBAL_JOBS_CNT = new AtomicInteger(0);

    /**
     * The timeout in seconds that defines how long should we wait for async calls. Almost all methods use this timeout.
     */
    private static final long WAIT_TIMEOUT_SECONDS = 15;

    /**
     * Clear global state. Must be called before each testing scenario.
     */
    public static void clearState() {
        assertThat(
                "Interactive job is running. Can not clear global state. Please, stop the job first.",
                RUNNING_INTERACTIVE_JOBS_CNT.get(),
                equalTo(0)
        );
        assertThat(
                "Global job is running. Can not clear global state. Please, stop the job first.",
                RUNNING_GLOBAL_JOBS_CNT.get(),
                equalTo(0)
        );

        GLOBAL_SIGNALS.clear();
        GLOBAL_CHANNEL.clear();
        NODE_SIGNALS.clear();
        NODE_CHANNELS.clear();
        INTERACTIVE_JOB_RUN_TIMES.clear();
    }

    public static String interactiveJobName() {
        return InteractiveJob.class.getName();
    }

    public static JobDescriptor<String, String> interactiveJobDescriptor() {
        return JobDescriptor.<String, String>builder(interactiveJobName()).build();
    }

    /**
     * Signals that are sent by test code to the jobs.
     */
    public enum Signal {
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
         * Ask job to complete and return worker name.
         */
        RETURN_WORKER_NAME,

        /**
         * Ask job to complete and return partition number.
         */
        RETURN_PARTITION_ID,

        /**
         * Signal to the job to continue running and send current worker name to the response channel.
         */
        GET_WORKER_NAME
    }

    /**
     * Interactive job that communicates via {@link #GLOBAL_CHANNEL} and {@link #GLOBAL_SIGNALS}.
     */
    public static class GlobalInteractiveJob implements ComputeJob<String, String> {
        private static Signal listenSignal() {
            try {
                return GLOBAL_SIGNALS.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, String arg) {
            RUNNING_INTERACTIVE_JOBS_CNT.incrementAndGet();

            offerArgAsSignal(arg);

            try {
                while (true) {
                    Signal receivedSignal = listenSignal();
                    switch (receivedSignal) {
                        case THROW:
                            throw new RuntimeException();
                        case CONTINUE:
                            GLOBAL_CHANNEL.offer(ACK);
                            break;
                        case RETURN:
                            return completedFuture("Done");
                        case RETURN_WORKER_NAME:
                            return completedFuture(context.ignite().name());
                        case GET_WORKER_NAME:
                            GLOBAL_CHANNEL.add(context.ignite().name());
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + receivedSignal);
                    }
                }
            } finally {
                RUNNING_INTERACTIVE_JOBS_CNT.decrementAndGet();
            }
        }

        /**
         * If argument is a string, convert it to signal and offer to the job.
         *
         * @param arg Job arg.
         */
        private static void offerArgAsSignal(String arg) {
            if (arg == null) {
                return;
            }
            try {
                GLOBAL_SIGNALS.offer(Signal.valueOf(arg));
            } catch (IllegalArgumentException ignored) {
                // Ignore non-signal strings
            }
        }
    }

    /**
     * Interactive job that communicates via {@link #NODE_CHANNELS} and {@link #NODE_SIGNALS}. Also, keeps track of how many times it was
     * executed via {@link #RUNNING_INTERACTIVE_JOBS_CNT}.
     */
    private static class InteractiveJob implements ComputeJob<String, String> {
        private static Signal listenSignal(BlockingQueue<Signal> channel) {
            try {
                return channel.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, String arg) {
            RUNNING_INTERACTIVE_JOBS_CNT.incrementAndGet();

            try {
                String workerNodeName = context.ignite().name();

                INTERACTIVE_JOB_RUN_TIMES.put(workerNodeName, INTERACTIVE_JOB_RUN_TIMES.get(workerNodeName) + 1);
                BlockingQueue<Signal> channel = NODE_SIGNALS.get(workerNodeName);

                while (true) {
                    Signal receivedSignal = listenSignal(channel);
                    switch (receivedSignal) {
                        case THROW:
                            throw new RuntimeException();
                        case CONTINUE:
                            NODE_CHANNELS.get(workerNodeName).offer(ACK);
                            break;
                        case RETURN:
                            return completedFuture("Done");
                        case RETURN_WORKER_NAME:
                            return completedFuture(workerNodeName);
                        case RETURN_PARTITION_ID:
                            return completedFuture(Integer.toString(context.partition().id()));
                        case GET_WORKER_NAME:
                            NODE_CHANNELS.get(workerNodeName).add(workerNodeName);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + receivedSignal);
                    }
                }
            } finally {
                RUNNING_INTERACTIVE_JOBS_CNT.decrementAndGet();
            }
        }
    }

    /**
     * Initializes channels that will be used to communicate with {@link InteractiveJob}. Note: {@link GlobalInteractiveJob} does not
     * require to call this method before communication but if you want to communicate with {@link InteractiveJob} then you must call this
     * method first.
     *
     * @param nodes the list of cluster nodes.
     */
    public static void initChannels(List<String> nodes) {
        for (String nodeName : nodes) {
            NODE_CHANNELS.put(nodeName, new LinkedBlockingQueue<>());
            NODE_SIGNALS.put(nodeName, new LinkedBlockingQueue<>());
            INTERACTIVE_JOB_RUN_TIMES.put(nodeName, 0);
        }
    }

    public static InteractiveJobApi byNode(InternalClusterNode clusterNode) {
        return new InteractiveJobApi(clusterNode);
    }

    public static InteractiveJobApi byNode(ClusterNode clusterNode) {
        return new InteractiveJobApi(ClusterNodeImpl.fromPublicClusterNode(clusterNode));
    }

    /**
     * API for communication with {@link InteractiveJob}.
     */
    public static final class InteractiveJobApi {
        private final InternalClusterNode node;

        private InteractiveJobApi(InternalClusterNode node) {
            this.node = node;
        }

        /**
         * Checks that {@link InteractiveJob} is alive.
         */
        public void assertAlive() {
            NODE_SIGNALS.get(node.name()).offer(Signal.CONTINUE);
            try {
                assertThat(NODE_CHANNELS.get(node.name()).poll(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS), equalTo(ACK));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * API for the interaction with every {@link InteractiveJob} (may run on every cluster node, for example, broadcast).
     */
    public static AllInteractiveJobsApi all() {
        return new AllInteractiveJobsApi();
    }

    /**
     * API for the interaction with every {@link InteractiveJob} (may run on every cluster node, for example, broadcast).
     */
    public static final class AllInteractiveJobsApi {
        private AllInteractiveJobsApi() {
        }

        /**
         * Checks that each instance of {@link InteractiveJob} was called once.
         */
        public static void assertEachCalledOnce() {
            INTERACTIVE_JOB_RUN_TIMES.forEach((nodeName, runTimes) -> assertThat(runTimes, equalTo(1)));
        }

        private static void sendTerminalSignal(Signal signal) {
            NODE_SIGNALS.forEach((nodeName, channel) -> {
                try {
                    channel.offer(signal, WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Can not send signal to the node", e);
                }
            });

            await().untilAsserted(() -> {
                assertThat(
                        "Expect all jobs to be finished",
                        RUNNING_INTERACTIVE_JOBS_CNT.get(),
                        equalTo(0)
                );
            });
        }

        /**
         * Finishes all {@link InteractiveJob}s.
         */
        public void finish() {
            sendTerminalSignal(Signal.RETURN);
        }

        /**
         * Finishes all {@link InteractiveJob}s by returning worker node names.
         */
        public void finishReturnWorkerNames() {
            sendTerminalSignal(Signal.RETURN_WORKER_NAME);
        }

        /**
         * Finishes all {@link InteractiveJob}s by returning partition number.
         */
        public void finishReturnPartitionNumber() {
            sendTerminalSignal(Signal.RETURN_PARTITION_ID);
        }

        /**
         * Finishes all {@link InteractiveJob}s by returning worker node names.
         */
        public void throwException() {
            sendTerminalSignal(Signal.THROW);
        }
    }

    /**
     * API for the interaction with {@link GlobalInteractiveJob}.
     */
    public static GlobalApi globalJob() {
        return new GlobalApi();
    }

    /**
     * API for the interaction with {@link GlobalInteractiveJob}.
     */
    public static final class GlobalApi {

        private GlobalApi() {
        }

        /**
         * Returns the name of the worker node where {@link GlobalInteractiveJob} is running.
         */
        public String currentWorkerName() throws InterruptedException {
            GLOBAL_SIGNALS.offer(Signal.GET_WORKER_NAME);
            String workerName = (String) GLOBAL_CHANNEL.poll(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(
                    "Can not get worker name for global job.", workerName, notNullValue()
            );

            return workerName;
        }

        /**
         * Checks that {@link GlobalInteractiveJob} is alive.
         */
        public void assertAlive() throws InterruptedException {
            GLOBAL_SIGNALS.offer(Signal.CONTINUE);
            assertThat(GLOBAL_CHANNEL.poll(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS), equalTo(ACK));
        }

        /**
         * Finishes {@link GlobalInteractiveJob}.
         */
        public void finish() {
            GLOBAL_SIGNALS.offer(Signal.RETURN);
        }

        /**
         * Returns the class name of {@link GlobalInteractiveJob}.
         */
        public String name() {
            return GlobalInteractiveJob.class.getName();
        }

        public Class<GlobalInteractiveJob> jobClass() {
            return GlobalInteractiveJob.class;
        }
    }
}
