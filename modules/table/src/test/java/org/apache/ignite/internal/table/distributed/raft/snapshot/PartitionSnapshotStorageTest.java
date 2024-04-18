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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.raft.snapshot.startup.StartupPartitionSnapshotReader;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

class PartitionSnapshotStorageTest extends BaseIgniteAbstractTest {
    @Test
    void returnsNullWhenOpeningOnCleanStorage() {
        var storage = storageForStartupMeta(null);

        assertThat(storage.open(), is(nullValue()));
    }

    @Test
    void returnsStartupReaderWhenOpeningOnStorageHavingSomething() {
        SnapshotMeta metaForCleanStorage = new RaftMessagesFactory().snapshotMeta()
                .lastIncludedIndex(1)
                .lastIncludedTerm(1)
                .build();

        var storage = storageForStartupMeta(metaForCleanStorage);

        assertThat(storage.open(), is(instanceOf(StartupPartitionSnapshotReader.class)));
    }

    private static PartitionSnapshotStorage storageForStartupMeta(@Nullable SnapshotMeta metaForCleanStorage) {
        return new PartitionSnapshotStorage(
                mock(TopologyService.class),
                mock(OutgoingSnapshotsManager.class),
                "",
                mock(RaftOptions.class),
                mock(PartitionAccess.class),
                mock(CatalogService.class),
                metaForCleanStorage,
                mock(Executor.class)
        );
    }
}
