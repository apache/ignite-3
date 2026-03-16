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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.impl.BaseLogStorageTest;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Shared log storage test. */
public class RocksDbSharedLogStorageTest extends BaseLogStorageTest {
    /** Log storage provider. */
    private DefaultLogStorageManager logStorageProvider;

    /** {@inheritDoc} */
    @BeforeEach
    @Override
    public void setup() throws Exception {
        logStorageProvider = new DefaultLogStorageManager(this.path);
        assertThat(logStorageProvider.startAsync(new ComponentContext()), willCompleteSuccessfully());

        super.setup();
    }

    /** {@inheritDoc} */
    @AfterEach
    @Override
    public void teardown() {
        super.teardown();

        assertThat(logStorageProvider.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    /** {@inheritDoc} */
    @Override
    protected LogStorage newLogStorage() {
        return logStorageProvider.createLogStorage(uri(), new RaftOptions());
    }

    private static String uri() {
        return "test";
    }

    @Test
    public void destroysData() {
        logStorage.appendEntries(TestUtils.mockEntries(15));
        logStorage.shutdown();

        logStorageProvider.destroyLogStorage(uri());

        logStorage = newLogStorage();
        logStorage.init(newLogStorageOptions());

        assertThat(logStorage.getFirstLogIndex(), is(1L));
        assertThat(logStorage.getLastLogIndex(), is(0L));
        assertThat(logStorage.getEntry(1), is(nullValue()));
    }
}
