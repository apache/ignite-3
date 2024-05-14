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

package org.apache.ignite.internal.raft.util;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageFactory;
import org.apache.ignite.internal.raft.storage.logit.LogitLogStorageFactory;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;

/** Utility methods for creating {@link LogStorageFactory}is for the Shared Log. */
public class SharedLogStorageFactoryUtils {
    /**
     * Enables logit log storage. {@code false} by default.
     * This is a temporary property, that should only be used for testing and comparing the two storages.
     */
    public static final String LOGIT_STORAGE_ENABLED_PROPERTY = "LOGIT_STORAGE_ENABLED";

    /** Creates a LogStorageFactory with the {@link DefaultLogStorageFactory} implementation. */
    public static LogStorageFactory create(String nodeName, Path workDir, RaftConfiguration raftConfiguration) {
        return create(nodeName, workDir, raftConfiguration, DefaultLogStorageFactory::new);
    }

    /** Creates a LogStorageFactory with the provided factory or the {@link LogitLogStorageFactory} implementation. */
    public static LogStorageFactory create(
            String nodeName,
            Path workDir,
            RaftConfiguration raftConfiguration,
            BiFunction<String, Supplier<Path>, LogStorageFactory> baseFactory
    ) {
        Supplier<Path> logStoragePath = () -> raftConfiguration.logPath().value().isEmpty()
                ? workDir.resolve("log")
                : Path.of(raftConfiguration.logPath().value());

        return IgniteSystemProperties.getBoolean(LOGIT_STORAGE_ENABLED_PROPERTY, false)
                ? new LogitLogStorageFactory(nodeName, new StoreOptions(), logStoragePath)
                : baseFactory.apply(nodeName, logStoragePath);
    }

    /** Wraps the LogStorageFactory in a IgniteComponent. */
    public static IgniteComponent wrapWithComponent(LogStorageFactory logStorageFactory) {
        return new IgniteComponent() {
            @Override
            public CompletableFuture<Void> startAsync() {
                logStorageFactory.start();
                return nullCompletedFuture();
            }

            @Override
            public CompletableFuture<Void> stopAsync() {
                logStorageFactory.close();
                return nullCompletedFuture();
            }
        };
    }
}
