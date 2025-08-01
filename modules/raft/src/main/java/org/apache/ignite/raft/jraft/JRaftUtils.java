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
package org.apache.ignite.raft.jraft;

import static org.apache.ignite.internal.thread.ThreadOperation.TX_STATE_STORAGE_ACCESS;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.Scheduler;
import org.apache.ignite.raft.jraft.core.TimerManager;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.BootstrapOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.apache.ignite.raft.jraft.util.ThreadPoolUtil;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.jraft.util.concurrent.DefaultFixedThreadsExecutorGroupFactory;
import org.apache.ignite.raft.jraft.util.concurrent.FixedThreadsExecutorGroup;
import org.apache.ignite.raft.jraft.util.concurrent.FixedThreadsExecutorGroupFactory;
import org.apache.ignite.raft.jraft.util.timer.DefaultTimer;
import org.apache.ignite.raft.jraft.util.timer.Timer;

/**
 * Some helper methods for jraft usage.
 */
public final class JRaftUtils {
    private static final IgniteLogger LOG = Loggers.forClass(JRaftUtils.class);

    /**
     * Bootstrap a non-empty raft node.
     *
     * @param opts options of bootstrap
     * @return true if bootstrap success
     */
    public static boolean bootstrap(final BootstrapOptions opts) throws InterruptedException {
        final NodeImpl node = new NodeImpl("bootstrap", new PeerId("127.0.0.1", 0));

        NodeOptions nodeOpts = opts.getNodeOptions();

        if (nodeOpts != null) {
            nodeOpts.setStripes(1);
            nodeOpts.setLogStripesCount(1);
        }

        final boolean ret = node.bootstrap(opts);
        node.shutdown();
        node.join();

        return ret;
    }

    /**
     * Create a executor with size.
     *
     * @param nodeName node name
     * @param poolName pool name
     * @param number thread number
     * @return a new {@link ThreadPoolExecutor} instance
     * @throws IllegalArgumentException If a number of threads is incorrect.
     */
    public static ExecutorService createExecutor(final String nodeName, final String poolName, final int number) {
        return createExecutor(number, createThreadFactory(nodeName, poolName));
    }

    private static ExecutorService createExecutor(int number, IgniteThreadFactory threadFactory) {
        if (number <= 0) {
            throw new IllegalArgumentException();
        }
        return ThreadPoolUtil.newBuilder() //
            .poolName(threadFactory.prefix()) //
            .enableMetric(true) //
            .coreThreads(number) //
            .maximumThreads(number) //
            .keepAliveSeconds(60L) //
            .workQueue(new LinkedBlockingQueue<>()) //
            .threadFactory(threadFactory) //
            .build();
    }

    /**
     * @param opts Node options.
     * @return The executor.
     */
    public static ExecutorService createCommonExecutor(NodeOptions opts) {
        String poolName = "JRaft-Common-Executor";

        return createExecutor(
            opts.getCommonThreadPollSize(),
            IgniteThreadFactory.create(opts.getServerName(), poolName, true, LOG, TX_STATE_STORAGE_ACCESS)
        );
    }

    /**
     * @param opts Node options.
     * @return The executor.
     */
    public static FixedThreadsExecutorGroup createAppendEntriesExecutor(NodeOptions opts) {
        String poolName = "JRaft-AppendEntries-Processor";

        return createStripedExecutor(
                IgniteThread.threadPrefix(opts.getServerName(), "JRaft-AppendEntries-Processor"),
                Utils.APPEND_ENTRIES_THREADS_POOL_SIZE,
                Utils.MAX_APPEND_ENTRIES_TASKS_PER_THREAD,
                new DefaultFixedThreadsExecutorGroupFactory() {
                    @Override
                    protected ThreadFactory newDaemonThreadFactory(String fullPoolName) {
                        return IgniteThreadFactory.create(opts.getServerName(), poolName, true, LOG, TX_STATE_STORAGE_ACCESS);
                    }
                }
        );
    }

    /**
     * @param opts Node options.
     * @return The executor.
     */
    public static ExecutorService createRequestExecutor(NodeOptions opts) {
        return createExecutor(
            opts.getServerName(),
            "JRaft-Request-Processor",
            opts.getRaftRpcThreadPoolSize()
        );
    }

    /**
     * @param opts Options.
     * @param name The name.
     * @return The service.
     */
    public static ExecutorService createClientExecutor(RpcOptions opts, String name) {
        IgniteThreadFactory threadFactory = createThreadFactory(name, "JRaft-Response-Processor");

        return ThreadPoolUtil.newBuilder()
            .poolName(threadFactory.prefix()) //
            .enableMetric(true) //
            .coreThreads(opts.getRpcProcessorThreadPoolSize() / 3) //
            .maximumThreads(opts.getRpcProcessorThreadPoolSize()) //
            .keepAliveSeconds(60L) //
            .workQueue(new ArrayBlockingQueue<>(10000)) //
            .threadFactory(threadFactory) //
            .build();
    }

    /**
     * @param opts Options.
     * @return The scheduler.
     */
    public static Scheduler createScheduler(NodeOptions opts) {
        return new TimerManager(
            opts.getTimerPoolSize(),
            IgniteThread.threadPrefix(opts.getServerName(), "JRaft-Node-Scheduler")
        );
    }

    /**
     * @param opts Node options.
     * @param name The name.
     * @return The timer.
     */
    public static Timer createTimer(NodeOptions opts, String name) {
        return new DefaultTimer(
            opts.getTimerPoolSize(),
            IgniteThread.threadPrefix(opts.getServerName(), name)
        );
    }

    /**
     * Create a striped executor.
     *
     * @param prefix Thread name prefix.
     * @param number Thread number.
     * @param tasksPerThread Max tasks per thread.
     * @return The executor.
     */
    public static FixedThreadsExecutorGroup createStripedExecutor(final String prefix, final int number,
        final int tasksPerThread) {
        return createStripedExecutor(prefix, number, tasksPerThread, DefaultFixedThreadsExecutorGroupFactory.INSTANCE);
    }

    /**
     * Creates a striped executor.
     *
     * @param prefix Thread name prefix.
     * @param number Thread number.
     * @param tasksPerThread Max tasks per thread.
     * @return The executor.
     */
    private static FixedThreadsExecutorGroup createStripedExecutor(final String prefix, final int number,
        final int tasksPerThread, FixedThreadsExecutorGroupFactory executorGroupFactory) {
        return executorGroupFactory
            .newExecutorGroup(
                number,
                prefix,
                tasksPerThread,
                true);
    }

    /**
     * Create a thread factory.
     *
     * @param nodeName the node name
     * @param poolName the pool name
     * @return a new {@link ThreadFactory} instance
     */
    public static IgniteThreadFactory createThreadFactory(final String nodeName, final String poolName) {
        return IgniteThreadFactory.create(nodeName, poolName, true, LOG);
    }

    /**
     * Create a configuration from a string in the form of "host1:port1[:idx],host2:port2[:idx]......", returns a empty
     * configuration when string is blank.
     */
    public static Configuration getConfiguration(final String s) {
        final Configuration conf = new Configuration();
        if (StringUtils.isBlank(s)) {
            return conf;
        }
        if (conf.parse(s)) {
            return conf;
        }
        throw new IllegalArgumentException("Invalid conf str:" + s);
    }

    /**
     * Create a peer from a string in the form of "host:port[:idx]", returns a empty peer when string is blank.
     */
    public static PeerId getPeerId(final String s) {
        final PeerId peer = new PeerId();
        if (StringUtils.isBlank(s)) {
            return peer;
        }
        if (peer.parse(s)) {
            return peer;
        }
        throw new IllegalArgumentException("Invalid peer str:" + s);
    }

    private JRaftUtils() {
    }

    /**
     * Is determined whether an append request is heartbeat.
     *
     * @param request Append entry request.
     * @return True if the request is heartbeat.
     */
    public static boolean isHeartbeatRequest(AppendEntriesRequest request) {
        // No entries and no data means a true heartbeat request.
        // TODO refactor, adds a new flag field? https://issues.apache.org/jira/browse/IGNITE-14832
        return request.entriesList() == null && request.data() == null;
    }
}
