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

package org.apache.ignite.raft.jraft.storage.logit;

import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.apache.ignite.raft.jraft.storage.logit.storage.LogitLogStorage;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.StringUtils;

public class LogitLogStorageFactory implements LogStorageFactory {
    private static final IgniteLogger LOG = Loggers.forClass(LogitLogStorageFactory.class);

    public static final String NEW_STORAGE_PATH = "LogitStorage";

    /** Executor for shared storages. */
    protected final ExecutorService executorService;

    private final StoreOptions storeOptions;

    public LogitLogStorageFactory(StoreOptions storeOptions) {
        this.storeOptions = storeOptions;
        executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2,
                new NamedThreadFactory("raft-shared-log-storage-pool", LOG)
        );
    }

    @Override
    public void start() {

    }

    @Override
    public LogStorage createLogStorage(String uri, RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(uri), "Blank log storage uri.");

        String newStoragePath = Paths.get(uri, NEW_STORAGE_PATH).toString();
        return new LogitLogStorage(newStoragePath, storeOptions, raftOptions, executorService);
    }

    @Override
    public void close() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(executorService);
    }
}
