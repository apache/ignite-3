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

package org.apache.ignite.internal.raft.storage;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;

/** Log storage factory interface. */
public interface LogStorageFactory extends ManuallyCloseable, LogSyncer, IgniteComponent {
    /**
     * Starts the log storage factory.
     */
    void start();

    /**
     * Creates a log storage.
     *
     * @param uri Log storage URI.
     * @param raftOptions Raft options.
     * @return Log storage.
     */
    LogStorage createLogStorage(String uri, RaftOptions raftOptions);

    /**
     * Closes the factory.
     */
    @Override
    void close();

    @Override
    default CompletableFuture<Void> startAsync() {
        start();
        return nullCompletedFuture();
    }

    @Override
    default CompletableFuture<Void> stopAsync() {
        close();
        return nullCompletedFuture();
    }
}
