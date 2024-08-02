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

package org.apache.ignite.internal.raft.storage.impl;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.storage.logit.LogitLogStorageFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.impl.BaseLogStorageTest;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** Logit log storage test. */
public class LogitLogStorageTest extends BaseLogStorageTest {
    /** Log storage provider. */
    private LogitLogStorageFactory logStorageProvider;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        logStorageProvider = new LogitLogStorageFactory("test", new StoreOptions(), () -> this.path);
        assertThat(logStorageProvider.startAsync(new ComponentContext()), willCompleteSuccessfully());

        super.setup();
    }

    @AfterEach
    @Override
    public void teardown() {
        super.teardown();

        assertThat(logStorageProvider.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Override
    protected LogStorage newLogStorage() {
        return logStorageProvider.createLogStorage("test", new RaftOptions());
    }
}
