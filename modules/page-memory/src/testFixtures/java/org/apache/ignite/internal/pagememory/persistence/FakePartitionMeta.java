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

import static org.apache.ignite.internal.pagememory.TestPageIoModule.FAKE_PARTITION_META_PAGE_IO_TYPE;
import static org.apache.ignite.internal.pagememory.TestPageIoModule.TEST_PAGE_VER;

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.jetbrains.annotations.Nullable;

/** Simple implementation of {@link PartitionMeta} for testing purposes. */
public class FakePartitionMeta extends PartitionMeta {
    public static final FakePartitionMetaFactory FACTORY = new FakePartitionMetaFactory();

    /** Constructor. */
    public FakePartitionMeta() {
        this(0, 1);
    }

    /** Constructor. */
    public FakePartitionMeta(int pageCount, int partitionGeneration) {
        super(pageCount, partitionGeneration);
    }

    /**
     * Initializes the partition meta. Should be called right after the instance is created.
     *
     * @param checkpointId Checkpoint ID.
     * @return This instance.
     */
    public FakePartitionMeta init(@Nullable UUID checkpointId) {
        initSnapshot(checkpointId);

        return this;
    }

    @Override
    protected FakePartitionMetaSnapshot buildSnapshot(@Nullable UUID checkpointId) {
        return new FakePartitionMetaSnapshot(checkpointId, pageCount());
    }

    /**
     * Simple implementation of {@link PartitionMetaSnapshot} for testing purposes.
     */
    public static class FakePartitionMetaSnapshot implements PartitionMetaSnapshot {
        private final UUID checkpointId;

        private final int pageCount;

        FakePartitionMetaSnapshot(@Nullable UUID checkpointId, int pageCount) {
            this.checkpointId = checkpointId;
            this.pageCount = pageCount;
        }

        @Override
        public void writeTo(PartitionMetaIo metaIo, long pageAddr) {
            FakePartitionMetaIo fakePartitionMetaIo = (FakePartitionMetaIo) metaIo;
            fakePartitionMetaIo.setPageCount(pageAddr, pageCount);
        }

        @Override
        public @Nullable UUID checkpointId() {
            return checkpointId;
        }

        @Override
        public int pageCount() {
            return pageCount;
        }
    }

    /**
     * Simple implementation of {@link PartitionMetaIo} for testing purposes.
     */
    public static class FakePartitionMetaIo extends PartitionMetaIo {
        /** I/O versions. */
        public static final IoVersions<FakePartitionMetaIo> VERSIONS = new IoVersions<>(
                new FakePartitionMetaIo(FAKE_PARTITION_META_PAGE_IO_TYPE, TEST_PAGE_VER)
        );

        /**
         * Constructor.
         *
         * @param ver Page format version.
         */
        FakePartitionMetaIo(int type, int ver) {
            super(type, ver);
        }

        @Override
        protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
            sb.app("FakePartitionMeta [").nl()
                    .app("pageCount=").app(getPageCount(addr)).nl()
                    .app(']');
        }
    }

    /** Simple implementation of {@link PartitionMetaFactory} for testing purposes. */
    public static class FakePartitionMetaFactory implements PartitionMetaFactory {
        @Override
        public FakePartitionMeta createPartitionMeta(
                @Nullable UUID checkpointId,
                PartitionMetaIo metaIo,
                long pageAddr,
                int partitionGeneration
        ) {
            return new FakePartitionMeta(metaIo.getPageCount(pageAddr), partitionGeneration).init(checkpointId);
        }

        @Override
        public FakePartitionMetaIo partitionMetaIo() {
            return FakePartitionMetaIo.VERSIONS.latest();
        }
    }
}
