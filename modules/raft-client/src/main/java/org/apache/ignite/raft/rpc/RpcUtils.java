/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.rpc;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.raft.Closure;
import org.apache.ignite.raft.Status;
import org.apache.ignite.raft.rpc.impl.NamedThreadFactory;

/**
 * RPC utilities.
 */
public final class RpcUtils {
    private static final System.Logger LOG = System.getLogger(RpcUtils.class.getName());

    /**
     * Default jraft closure executor pool minimum size.
     */
    public static final int MIN_RPC_CLOSURE_EXECUTOR_POOL_SIZE = 1;

    /**
     * Default jraft closure executor pool maximum size.
     */
    public static final int MAX_RPC_CLOSURE_EXECUTOR_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    /**
     * Global thread pool to run rpc closure.
     */
    public static ThreadPoolExecutor RPC_CLOSURE_EXECUTOR = new ThreadPoolExecutor(MIN_RPC_CLOSURE_EXECUTOR_POOL_SIZE,
        MAX_RPC_CLOSURE_EXECUTOR_POOL_SIZE,
        60L,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new NamedThreadFactory("JRaft-Rpc-Closure-Executor-", true));

    /**
     * Run closure with OK status in thread pool.
     */
    public static Future<?> runClosureInThread(final Closure done) {
        if (done == null) {
            return null;
        }
        return runClosureInThread(done, Status.OK());
    }

    /**
     * Run a task in thread pool, returns the future object.
     */
    public static Future<?> runInThread(final Runnable runnable) {
        return RPC_CLOSURE_EXECUTOR.submit(runnable);
    }

    /**
     * Run closure with status in thread pool.
     */
    public static Future<?> runClosureInThread(final Closure done, final Status status) {
        if (done == null) {
            return null;
        }

        return runInThread(() -> {
            try {
                done.run(status);
            } catch (final Throwable t) {
                LOG.log(System.Logger.Level.WARNING, "Fail to run done closure.", t);
            }
        });
    }

    /**
     * Run closure with status in specified executor
     */
    public static void runClosureInExecutor(final Executor executor, final Closure done, final Status status) {
        if (done == null) {
            return;
        }

        if (executor == null) {
            runClosureInThread(done, status);
            return;
        }

        executor.execute(() -> {
            try {
                done.run(status);
            } catch (final Throwable t) {
                LOG.log(System.Logger.Level.WARNING, "Fail to run done closure.", t);
            }
        });
    }

    public static Message newResponse(final int code, final String fmt, final Object... args) {
        final RpcRequests.ErrorResponse.Builder eBuilder = RpcRequests.ErrorResponse.newBuilder();
        eBuilder.setErrorCode(code);
        if (fmt != null) {
            eBuilder.setErrorMsg(String.format(fmt, args));
        }
        return eBuilder.build();
    }

    private RpcUtils() {
    }
}
