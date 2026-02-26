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

package org.apache.ignite.raft.jraft.core;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageManager;
import org.apache.ignite.raft.jraft.entity.PeerId;

class PersistentLogStorageFactories {
    private final String dataPath;

    private final Map<PeerId, LogStorageManager> factories = new ConcurrentHashMap<>();

    PersistentLogStorageFactories(String dataPath) {
        this.dataPath = dataPath;
    }

    LogStorageManager factoryFor(PeerId peerId) {
        return factories.computeIfAbsent(peerId, this::startPersistentLogStorageManager);
    }

    private DefaultLogStorageManager startPersistentLogStorageManager(PeerId peerId) {
        Path path = Path.of(dataPath).resolve("logs").resolve(peerId.getConsistentId() + "-" + peerId.getIdx());
        DefaultLogStorageManager persistentLogStorageManager = new DefaultLogStorageManager(path);

        assertThat(persistentLogStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        return persistentLogStorageManager;
    }

    void shutdown() {
        for (LogStorageManager factory : factories.values()) {
            assertThat(factory.stopAsync(), willCompleteSuccessfully());
        }
    }
}
