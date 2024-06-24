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
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;

/**
 * Tests DSL for interactive tasks. "Interactive" means that you can send messages and get responses to/from running tasks.
 *
 * <p>For example, you can start {@link GlobalInteractiveMapReduceTask} on some node, get the name of worker node for split or reduce job,
 * ask split or reduce job to complete successfully or throw exception. Also, this class gives useful assertions for task states.
 */
public final class InteractiveTasks {
    /**
     * ACK for {@link Signal#CONTINUE}. Returned by a task that has received the signal. Used to check that the task is alive.
     */
    private static final Object ACK = new Object();

    /**
     * Class-wide queue that is used as a communication channel between {@link GlobalInteractiveMapReduceTask} and test code. You can send a
     * signal to the task via this channel and get a response from it via {@link #GLOBAL_CHANNEL}.
     */
    private static final BlockingQueue<Signal> GLOBAL_SIGNALS = new LinkedBlockingQueue<>();

    /**
     * Class-wide queue that is used as a communication channel between {@link GlobalInteractiveMapReduceTask} and test code. You can send a
     * signal to the task via {@link #GLOBAL_SIGNALS} and get a response from it via this channel.
     */
    private static final BlockingQueue<Object> GLOBAL_CHANNEL = new LinkedBlockingQueue<>();

    /**
     * This counter indicated how many {@link GlobalInteractiveMapReduceTask#splitAsync(TaskExecutionContext, Object...)} methods are
     * running now. This counter increased each time the {@link GlobalInteractiveMapReduceTask#splitAsync(TaskExecutionContext, Object...)}
     * is called and decreased when the method is finished (whatever the result is). Checked in {@link #clearState}.
     */
    private static final AtomicInteger RUNNING_GLOBAL_SPLIT_CNT = new AtomicInteger(0);

    /**
     * This counter indicates how many {@link GlobalInteractiveMapReduceTask#reduceAsync(TaskExecutionContext, Map)} methods are running
     * now. This counter is increased every time the {@link GlobalInteractiveMapReduceTask#reduceAsync(TaskExecutionContext, Map)} is called
     * and decreased when the method is finished (whatever the result is). Checked in {@link #clearState}.
     */
    private static final AtomicInteger RUNNING_GLOBAL_REDUCE_CNT = new AtomicInteger(0);

    /**
     * The timeout in seconds that defines how long should we wait for async calls. Almost all methods use this timeout.
     */
    private static final long WAIT_TIMEOUT_SECONDS = 15;

    /**
     * Clear global state. Must be called before each testing scenario.
     */
    public static void clearState() {
        assertThat(
                "Global split job is running. Can not clear global state. Please, stop the job first.",
                RUNNING_GLOBAL_SPLIT_CNT.get(),
                is(0)
        );
        assertThat(
                "Global reduce job is running. Can not clear global state. Please, stop the job first.",
                RUNNING_GLOBAL_REDUCE_CNT.get(),
                is(0)
        );

        GLOBAL_SIGNALS.clear();
        GLOBAL_CHANNEL.clear();
    }

    /**
     * Signals that are sent by test code to the tasks.
     */
    private enum Signal {
        /**
         * Signal to the task to continue running and send ACK as a response.
         */
        CONTINUE,

        /**
         * Ask task to throw an exception.
         */
        THROW,

        /**
         * Ask split method to return a list of jobs which will be executed on all nodes.
         */
        SPLIT_RETURN_ALL_NODES,

        /**
         * Ask reduce method to return a concatenation of jobs results.
         */
        REDUCE_RETURN,

        /**
         * Ask the task to check for cancellation flag and finish if it's set.
         */
        CHECK_CANCEL
    }

    /**
     * If any of the args are strings, convert them to signals and offer them to the job.
     *
     * @param arg Job args.
     */
    private static void offerArgsAsSignals(Object arg) {
        if (arg instanceof String) {
            String signal = (String) arg;
            try {
                GLOBAL_SIGNALS.offer(Signal.valueOf(signal));
            } catch (IllegalArgumentException ignored) {
                // Ignore non-signal strings
            }
        }
    }

    /**
     * Interactive map reduce task that communicates via {@link #GLOBAL_CHANNEL} and {@link #GLOBAL_SIGNALS}.
     */
    private static class GlobalInteractiveMapReduceTask implements MapReduceTask<String, List<String>> {
        // When listening for signal is interrupted, if this flag is true, then corresponding method will throw exception,
        // otherwise it will clean the interrupted status.
        private boolean throwExceptionOnInterruption = true;

        private static final String NO_INTERRUPT_ARG_NAME = "NO_INTERRUPT";

        private Signal listenSignal() {
            try {
                return GLOBAL_SIGNALS.take();
            } catch (InterruptedException e) {
                if (throwExceptionOnInterruption) {
                    throw new RuntimeException(e);
                } else {
                    Thread.currentThread().interrupt();
                    return Signal.CHECK_CANCEL;
                }
            }
        }

        @Override
        public CompletableFuture<List<MapReduceJob>> splitAsync(TaskExecutionContext context, String args) {
            RUNNING_GLOBAL_SPLIT_CNT.incrementAndGet();

            offerArgsAsSignals(args);
            if (NO_INTERRUPT_ARG_NAME.equals(args)) {
                throwExceptionOnInterruption = false;
            }

            try {
                while (true) {
                    Signal receivedSignal = listenSignal();
                    switch (receivedSignal) {
                        case THROW:
                            throw new RuntimeException();
                        case CONTINUE:
                            GLOBAL_CHANNEL.offer(ACK);
                            break;
                        case SPLIT_RETURN_ALL_NODES:
                            return completedFuture(context.ignite().clusterNodes().stream().map(node ->
                                    MapReduceJob.builder()
                                            .jobDescriptor(JobDescriptor.builder(InteractiveJobs.interactiveJobName()).build())
                                            .nodes(Set.of(node))
                                            .build()
                            ).collect(toList()));
                        case CHECK_CANCEL:
                            if (context.isCancelled()) {
                                throw new RuntimeException("Task is cancelled");
                            }
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + receivedSignal);
                    }
                }
            } finally {
                RUNNING_GLOBAL_SPLIT_CNT.decrementAndGet();
            }
        }

        @Override
        public CompletableFuture<List<String>> reduceAsync(TaskExecutionContext context, Map<UUID, ?> results) {
            RUNNING_GLOBAL_REDUCE_CNT.incrementAndGet();
            try {
                while (true) {
                    Signal receivedSignal = listenSignal();
                    switch (receivedSignal) {
                        case THROW:
                            throw new RuntimeException();
                        case CONTINUE:
                            GLOBAL_CHANNEL.offer(ACK);
                            break;
                        case REDUCE_RETURN:
                            return completedFuture(results.values().stream()
                                    .map(String.class::cast)
                                    .collect(toList()));
                        case CHECK_CANCEL:
                            if (context.isCancelled()) {
                                throw new RuntimeException("Task is cancelled");
                            }
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + receivedSignal);
                    }
                }
            } finally {
                RUNNING_GLOBAL_REDUCE_CNT.decrementAndGet();
            }
        }
    }

    /**
     * API for the interaction with {@link GlobalInteractiveMapReduceTask}.
     */
    public static final class GlobalApi {
        /**
         * Checks that {@link GlobalInteractiveMapReduceTask} is alive.
         */
        public static void assertAlive() throws InterruptedException {
            GLOBAL_SIGNALS.offer(Signal.CONTINUE);
            assertThat(GLOBAL_CHANNEL.poll(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS), equalTo(ACK));
        }

        /**
         * Finishes the split job, returning job runners for all nodes.
         */
        public static void finishSplit() {
            GLOBAL_SIGNALS.offer(Signal.SPLIT_RETURN_ALL_NODES);
        }

        /**
         * Finishes the split job, returning job runners for all nodes.
         */
        public static void throwException() {
            GLOBAL_SIGNALS.offer(Signal.THROW);
        }

        /**
         * Finishes the reduce job, returning final result.
         */
        public static void finishReduce() {
            GLOBAL_SIGNALS.offer(Signal.REDUCE_RETURN);
        }

        public static String name() {
            return GlobalInteractiveMapReduceTask.class.getName();
        }
    }
}
