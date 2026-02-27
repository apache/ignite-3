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

package org.apache.ignite.internal.raft.storage.logit;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.NoOpLogSyncer;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.LogStorageException;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.FeatureChecker;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.apache.ignite.raft.jraft.storage.logit.storage.LogitLogStorage;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.StringUtils;
import sun.nio.ch.DirectBuffer;

/**
 * Log storage manager for {@link LogitLogStorage} instances.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-27946 remove this.
public class LogitLogStorageManager implements LogStorageManager {
    private static final IgniteLogger LOG = Loggers.forClass(LogitLogStorageManager.class);

    private static final String LOG_DIR_PREFIX = "log-";

    /** Executor for shared storages. */
    private final ScheduledExecutorService checkpointExecutor;

    private final StoreOptions storeOptions;

    /** Base location of all log storages. */
    private final Path logPath;

    /**
     * Constructor.
     *
     * @param logPath Function to get base path of all log storages, created by this factory.
     * @param storeOptions Logit log storage options.
     */
    public LogitLogStorageManager(String nodeName, StoreOptions storeOptions, Path logPath) {
        this.logPath = logPath;
        this.storeOptions = storeOptions;
        checkpointExecutor = Executors.newSingleThreadScheduledExecutor(
                IgniteThreadFactory.create(nodeName, "logit-checkpoint-executor", LOG)
        );

        checkVmOptions();
    }

    private static void checkVmOptions() {
        try {
            Class.forName(DirectBuffer.class.getName());
        } catch (Throwable e) {
            throw new IgniteInternalException("sun.nio.ch.DirectBuffer is unavailable." + FeatureChecker.JAVA_STARTUP_PARAMS_WARN, e);
        }
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        ExecutorServiceHelper.shutdownAndAwaitTermination(checkpointExecutor);
        return nullCompletedFuture();
    }

    @Override
    public LogStorage createLogStorage(String groupId, RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(groupId), "Blank log storage uri.");

        Path storagePath = resolveLogStoragePath(groupId);

        return new LogitLogStorage(storagePath, storeOptions, raftOptions, checkpointExecutor);
    }

    @Override
    public void destroyLogStorage(String uri) {
        Requires.requireTrue(StringUtils.isNotBlank(uri), "Blank log storage uri.");

        Path storagePath = resolveLogStoragePath(uri);

        if (!IgniteUtils.deleteIfExists(storagePath)) {
            throw new LogStorageException("Cannot delete directory " + storagePath);
        }
    }

    @Override
    public Set<String> raftNodeStorageIdsOnDisk() {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-25988 - implement.
        return Set.of();
    }

    @Override
    public LogSyncer logSyncer() {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-21955
        return new NoOpLogSyncer();
    }

    @Override
    public long totalBytesOnDisk() {
        // The implementation is inefficient, but it's here just for completeness. Logit is not production ready and will never become
        // production ready.
        try (Stream<Path> paths = Files.walk(logPath)) {
            return paths.filter(Files::isRegularFile)
                    .mapToLong(path -> path.toFile().length())
                    .sum();
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, e);
        }
    }

    /** Returns path to log storage by group ID. */
    public Path resolveLogStoragePath(String groupId) {
        return logPath.resolve(LOG_DIR_PREFIX + groupId);
    }
}
