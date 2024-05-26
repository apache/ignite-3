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

package org.apache.ignite.internal.pagememory.persistence;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.pagememory.persistence.TestPartitionMeta.TestPartitionMetaSnapshot;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.jetbrains.annotations.Nullable;

/**
 * Simple implementation of {@link PartitionMeta} for testing purposes.
 */
public class TestPartitionMeta extends PartitionMeta<TestPartitionMetaSnapshot> {

    public static final PartitionMetaFactory<TestPartitionMeta> FACTORY =
            (checkpointId, metaIo, pageAddr) -> new TestPartitionMeta(checkpointId);

    private final AtomicInteger pageCount = new AtomicInteger(0);

    /**
     * Constructor.
     *
     * @param checkpointId Checkpoint ID.
     */
    public TestPartitionMeta(@Nullable UUID checkpointId) {
        super(checkpointId);
    }

    @Override
    protected TestPartitionMetaSnapshot buildSnapshot(@Nullable UUID checkpointId) {
        return new TestPartitionMetaSnapshot(checkpointId);
    }

    public void incrementPageCount(@Nullable UUID ignored) {
        pageCount.incrementAndGet();
    }

    public int pageCount() {
        return pageCount.get();
    }

    /**
     * Simple implementation of {@link PartitionMetaSnapshot} for testing purposes.
     */
    public static class TestPartitionMetaSnapshot implements PartitionMetaSnapshot {
        private final UUID checkpointId;

        public TestPartitionMetaSnapshot(UUID checkpointId) {
            this.checkpointId = checkpointId;
        }

        @Override
        public void writeTo(PartitionMetaIo metaIo, long pageAddr) {
        }

        @Override
        public @Nullable UUID checkpointId() {
            return checkpointId;
        }
    }
}
